<?php

namespace go1\util_index\tests;

use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Schema\Schema as DBSchema;
use go1\app\DomainService;
use go1\clients\MqClient;
use go1\util\DB;
use go1\util\es\mock\EsInstallTrait;
use go1\util\es\Schema;
use go1\util\schema\InstallTrait;
use go1\util\user\UserHelper;
use go1\util_index\HistoryRepository;
use go1\util_index\IndexSchema;
use PHPUnit\Framework\TestCase;
use ReflectionClass;
use Symfony\Component\HttpFoundation\Request;

abstract class IndexServiceTestCase extends TestCase
{
    use IndexServiceTestHelperTrait;
    use InstallTrait;
    use EsInstallTrait;

    protected $mockMqClientThenConsume = false;
    protected $isUnitTestCase          = false;
    protected $messages;
    protected $contexts   = [];
    protected $logs;
    protected $sqlite;

    protected function getDatabases()
    {
        return [
            'default'   => $this->sqlite = DriverManager::getConnection(['url' => 'sqlite://sqlite::memory:']),
            'go1'       => $this->sqlite,
            'go1_write' => $this->sqlite,
        ];
    }

    private function publicDir()
    {
        $r = new ReflectionClass(DomainService::class);

        return dirname($r->getFileName()) . '/public';
    }

    protected function getApp(): DomainService
    {
        if (!getenv('ES_URL')) {
            putenv('ES_URL=http://localhost:9200');
        }

        /** @var DomainService $app */
        $app = require $this->publicDir() . '/index.php';
        $app['waitForCompletion'] = true;
        $app['dbs'] = $app->extend('dbs', function () { return $this->getDatabases(); });

        $app->extend('history.repository', function () {
            $history = $this
                ->getMockBuilder(HistoryRepository::class)
                ->disableOriginalConstructor()
                ->setMethods(['write', 'bulkLog'])
                ->getMock();

            $history
                ->expects($this->any())
                ->method('write')
                ->willReturnCallback(function ($type, $id, $status, $data = null, $timestamp = null) {
                    $this->logs[] = [
                        'message' => !$data ? null : (is_string($data) ? $data : json_encode($data)),
                        'status'  => $status,
                    ];
                });

            $history
                ->expects($this->any())
                ->method('bulkLog')
                ->willReturnCallback(function (array $response) {
                    if (!$response['errors']) {
                        return null;
                    }

                    foreach ($response['items'] as $item) {
                        foreach ($item as $action => $data) {
                            if (!isset($data['error'])) {
                                continue;
                            }

                            $this->logs[] = [
                                'message' => !$data['error']
                                    ? null
                                    : (is_string($data['error']) ? $data['error'] : json_encode($data['error'])),
                                'status'  => $data['status'],
                            ];
                        }
                    }
                });

            return $history;
        });

        $this->mockMqClientThenConsume
            ? $this->mockMqClientToDoConsume($app)
            : $this->mockMqClient($app);

        $app->extend('go1.client.es_writer', function () use ($app) { return $app['go1.client.es']; });

        $this->appInstall($app);

        return $app;
    }

    protected function mockMqClient(DomainService $app)
    {
        $app->extend('go1.client.mq', function () {
            $queue = $this
                ->getMockBuilder(MqClient::class)
                ->disableOriginalConstructor()
                ->setMethods(['queue', 'publish'])
                ->getMock();

            $queue
                ->expects($this->any())
                ->method('publish')
                ->willReturnCallback(function ($body, $routingKey, $context = []) {
                    $this->messages[$routingKey][] = $body;
                    $this->contexts[$routingKey][] = $context;
                });

            $queue
                ->expects($this->any())
                ->method('queue')
                ->willReturnCallback(function ($body, $routingKey, $context = []) {
                    $this->messages[$routingKey][] = $body;
                    $this->contexts[$routingKey][] = $context;
                });

            return $queue;
        });
    }

    protected function mockMqClientToDoConsume(DomainService $app)
    {
        $app->extend('go1.client.mq', function () use ($app) {
            $mock = $this
                ->getMockBuilder(MqClient::class)
                ->disableOriginalConstructor()
                ->setMethods(['queue'])
                ->getMock();

            $mock
                ->expects($this->any())
                ->method('queue')
                ->willReturnCallback(
                    function ($body, $routingKey, $context) use ($app) {
                        $req = Request::create('/consume?jwt=' . UserHelper::ROOT_JWT, 'POST', [
                            'routingKey' => $routingKey,
                            'body'       => (object) $body,
                            'context'    => $context,
                        ]);
                        $res = $app->handle($req);
                        $this->assertEquals(204, $res->getStatusCode());

                        return true;
                    }
                );

            return $mock;
        });
    }

    protected function appInstall(DomainService $app)
    {
        $this->installGo1Schema($app['dbs']['go1'], $coreOnly = false, $app['accounts_name']);
        DB::install($app['dbs']['go1'], [function (DBSchema $schema) { IndexSchema::install($schema); }]);

        if (!$this->isUnitTestCase) {
            $client = $this->client($app);
            $indices = $this->indices();
            foreach ($indices as $index) {
                if ($client->indices()->exists(['index' => $index])) {
                    $client->indices()->delete(['index' => $index]);
                }
            }
            $this->installEs($client, $indices);
        }
    }
}

<?php

namespace go1\util_index\tests;

use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Schema\Schema as DBSchema;
use go1\clients\MqClient;
use go1\util\DB;
use go1\util\es\mock\EsInstallTrait;
use go1\util\schema\InstallTrait;
use go1\util\user\UserHelper;
use go1\util_index\HistoryRepository;
use go1\util_index\IndexSchema;
use go1\util_index\IndexService;
use PHPUnit\Framework\TestCase;
use Symfony\Component\HttpFoundation\Request;

abstract class IndexServiceTestCase extends TestCase
{
    use IndexServiceTestHelperTrait;
    use InstallTrait;
    use EsInstallTrait;

    protected $mockMqClientThenConsume = false;
    protected $isUnitTestCase          = false;
    protected $messages;
    protected $logs;
    protected $sqlite;

    protected function getDatabases()
    {
        $this->sqlite = DriverManager::getConnection(['url' => 'sqlite://sqlite::memory:']);

        return [
            'default'   => $this->sqlite,
            'go1'       => $this->sqlite,
            'go1_write' => $this->sqlite,
        ];
    }

    protected function getApp(): IndexService
    {
        /** @var IndexService $app */
        $app = require __DIR__ . '/../public/index.php';
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
        $this->appInstall($app);

        return $app;
    }

    protected function mockMqClient(IndexService $app)
    {
        $app->extend('go1.client.mq', function () {
            $mqClient = $this->getMockBuilder(MqClient::class)->disableOriginalConstructor()->setMethods(['queue', 'publish'])->getMock();

            $mqClient
                ->expects($this->any())
                ->method('publish')
                ->willReturnCallback(function ($body, $routingKey) {
                    $this->messages[$routingKey][] = $body;
                });

            $mqClient
                ->expects($this->any())
                ->method('queue')
                ->willReturnCallback(function ($body, $routingKey) {
                    $this->messages[$routingKey][] = $body;
                });

            return $mqClient;
        });
    }

    protected function mockMqClientToDoConsume(IndexService $app)
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
                        if (IndexService::WORKER_TASK_BULK == $routingKey) {
                            foreach ($body as $msg) {
                                $req = Request::create('/consume?jwt=' . UserHelper::ROOT_JWT, 'POST');
                                $req->request->replace(['routingKey' => $msg['routingKey'], 'body' => (object) $msg['body']]);
                                $res = $app->handle($req);
                                $this->assertEquals(204, $res->getStatusCode());
                            }

                            $req = Request::create('/consume?jwt=' . UserHelper::ROOT_JWT, 'POST', [
                                'routingKey' => $routingKey,
                                'body'       => (object) [],
                                'context'    => $context,
                            ]);
                            $res = $app->handle($req);
                            $this->assertEquals(204, $res->getStatusCode());

                            return true;
                        }

                        $req = Request::create('/consume?jwt=' . UserHelper::ROOT_JWT, 'POST');
                        $req->request->replace(['routingKey' => $routingKey, 'body' => (object) $body]);
                        $res = $app->handle($req);
                        $this->assertEquals(204, $res->getStatusCode());

                        return true;
                    }
                );

            return $mock;
        });
    }

    protected function appInstall(IndexService $app)
    {
        $this->installGo1Schema($app['dbs']['go1'], $coreOnly = false);
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

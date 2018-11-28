<?php

namespace go1\util_index\tests;

use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Schema\Schema as DBSchema;
use go1\util\DB;
use go1\util\es\mock\EsInstallTrait;
use go1\util\schema\InstallTrait;
use go1\util_index\HistoryRepository;
use go1\util_index\IndexSchema;
use go1\util_index\IndexService;
use PHPUnit\Framework\TestCase;

abstract class IndexServiceTestCase extends TestCase
{
    use IndexQueueClientMockTrait;
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
        if (!getenv('ES_URL')) {
            putenv('ES_URL=http://localhost:9200');
        }

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
                    if (empty($response['errors'])) {
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

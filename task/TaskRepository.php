<?php

namespace go1\util_index\task;

use Doctrine\DBAL\Connection;
use Elasticsearch\Client;
use Exception;
use go1\clients\MqClient;
use go1\util\DB;
use go1\util\es\Schema;
use go1\util\portal\PortalHelper;
use go1\util_index\IndexService;
use go1\util_index\ReindexInterface;
use ONGR\ElasticsearchDSL\BuilderInterface;
use Pimple\Container;
use Psr\Log\LoggerInterface;

class TaskRepository
{
    const SEARCH_SLOW_TIME_WARN = '5s';
    const SEARCH_SLOW_TIME_INFO = '2s';
    const INDEX_SLOW_TIME_WARN  = '10s';
    const INDEX_SLOW_TIME_INFO  = '2s';

    public  $db;
    public  $go1;
    public  $queue;
    public  $client;
    private $container;
    private $logger;

    public function __construct(
        Connection $db,
        Connection $go1,
        MqClient $mqClient,
        Client $client,
        Container $container,
        LoggerInterface $logger
    ) {
        $this->db = $db;
        $this->go1 = $go1;
        $this->queue = $mqClient;
        $this->client = $client;
        $this->container = $container;
        $this->logger = $logger;
    }

    public function create(Task $task)
    {
        $this->db->insert('index_task', $task->jsonSerialize());
        $task->id = $this->db->lastInsertId('index_task');

        return $task->id;
    }

    public function update(Task $task)
    {
        $task->updated = time();
        $this->db->update('index_task', $task->jsonSerialize(), ['id' => $task->id]);

        return true;
    }

    public function delete(Task $task)
    {
        return DB::transactional($this->db, function () use ($task) {
            $this->db->delete('index_task', ['id' => $task->id]);

            return true;
        });
    }

    public function load(int $id)
    {
        $data = $this->db
            ->executeQuery('SELECT * FROM index_task WHERE id = ?', [$id])
            ->fetch(DB::OBJ);

        if ($data) {
            $task = Task::create($data);
            $task->portal && $task->portal = PortalHelper::load($this->go1, $task->portal);

            return $task;
        }

        return false;
    }

    private function getAlias(Task $task)
    {
        try {
            $alias = $this->client->indices()->getAlias(['name' => $task->aliasName]);
            $alias = $alias ? array_keys($alias) : [];

            return $alias[0] ?? null;
        } catch (Exception $e) {
        }

        return null;
    }

    public function finish(Task $task)
    {
        $this->client->indices()->refresh(['index' => Schema::INDEX]);
        if ($task->alias && ($task->aliasName != $task->index)) {
            if (Schema::INDEX == $task->aliasName) {
                $activeIndex = $this->getAlias($task) ?: Schema::INDEX;
                foreach ($this->client->indices()->getAliases() as $index => $aliases) {
                    if ($index === $activeIndex) {
                        foreach ($aliases['aliases'] as $name => $conf) {
                            $params['body']['actions'][]['add'] = $conf + ['index' => $task->index, 'alias' => $name];
                        }
                    }
                }
            }

            $params['body']['actions'][]['add'] = [
                'index' => $task->index,
                'alias' => $task->aliasName,
            ];
            $params['body']['actions'][]['remove_index'] = [
                'index' => $activeIndex ?? $task->aliasName,
            ];
            $this->client->indices()->updateAliases($params);
            $this->client->indices()->putSettings([
                'index' => $task->index,
                'body'  => [
                    'settings' => [
                        'number_of_replicas'                          => 1,
                        'index.search.slowlog.threshold.query.warn'   => self::SEARCH_SLOW_TIME_WARN,
                        'index.search.slowlog.threshold.query.info'   => self::SEARCH_SLOW_TIME_INFO,
                        'index.indexing.slowlog.threshold.index.warn' => self::INDEX_SLOW_TIME_WARN,
                        'index.indexing.slowlog.threshold.index.info' => self::INDEX_SLOW_TIME_INFO,
                    ],
                ],
            ]);
        }

        $task->status = Task::FINISHED;
        $task->percent = 100;
        $this->update($task);

        $task->removeRedundant && $this->removeRedundant($task);
    }

    public function removeRedundant(Task $task)
    {
        foreach ($task->handlers as $name) {
            $handler = $this->getHandler($name);
            if (method_exists($handler, 'removeRedundant')) {
                /** @var $query BuilderInterface */
                $query = $handler->removeRedundant($task);
                if ($query->toArray()) {
                    $this->client->deleteByQuery([
                        'index'               => Schema::INDEX,
                        'body'                => ['query' => $query->toArray()],
                        'refresh'             => true,
                        'wait_for_completion' => true,
                    ]);
                }
            }
        }
    }

    public function execute(Task $task)
    {
        $settings['settings'] = [
            'number_of_shards'                 => 2,
            'number_of_replicas'               => 0,
            'index.mapping.total_fields.limit' => 5000,
        ];

        if (!$this->client->indices()->exists(['index' => $task->index])) {
            $this->client->indices()->create(['index' => $task->index, 'body' => Schema::BODY + $settings]);
        }

        if ($task->aliasName
            && $task->aliasName != $task->index
            && !$this->client->indices()->exists(['index' => $task->aliasName])
        ) {
            $this->client->indices()->create(['index' => $task->aliasName, 'body' => Schema::BODY + $settings]);
        }

        $task->stats = $this->stats($task);
        foreach ($task->stats as $handlerName => $num) {
            $limit = $task->limit;

            if ($handler = $this->getHandler($handlerName)) {
                $limit = isset($handler::$limit) ? $handler::$limit : $task->limit;
            }

            $task->stats[$handlerName] = ceil($num / $limit);
        }

        $task->totalItems = array_sum($task->stats);
        $task->status = Task::IN_PROGRESS;
        $this->update($task);
        $this->verify($task);
    }

    public function verify(Task $task, $payload = null)
    {
        $generate = empty($payload) || !empty($payload->isLast);

        if (!empty($payload)) {
            $task->processedItems++;
            $task->percent = $this->calculatePercent($task);
        }

        # ---------------------
        # generate unit-of-works
        # ---------------------
        if (!$task->currentHandlerIsCompleted()) {
            if ($generate) {
                $this->generateItems($task);
            }
            $this->update($task);
            return null;
        }

        # ---------------------
        # move to next handler; complete if nothing found.
        # ---------------------
        $task->currentHandler = $task->nextHandler();
        while ($task->currentHandler && (0 == $task->stats[$task->currentHandler] ?? 0)) {
            $task->currentHandler = $task->nextHandler();
        }

        if (empty($payload) && !$task->currentHandler) {
            // we don't have anything to do here :(
            $this->finish($task);
            return;
        }

        if (!$task->currentHandler && !empty($payload->isLast) && $task->percent >= 100) {
            // last chunk exceeded
            $this->finish($task);

            return null;
        }

        $task->currentOffset = 0;
        $task->currentIdFromOffset = 0;
        $this->update($task);

        if ($generate) {
            $this->generateItems($task);
        }
    }

    private function generateItems(Task $task)
    {
        $handler = $this->getHandler($task->currentHandler);
        $packages = [];
        $numOfChunks = 100; // avoid to generate too many tasks once
        $maxOffset = $task->stats[$task->currentHandler] ?? 0;
        $offset = $task->currentOffset;
        for ($i = 0; $i < $numOfChunks && $offset <= $maxOffset; $i++) {
            $offset = $task->currentOffset + $i;
            $isLast = ($i == $numOfChunks - 1) || ($offset >= $maxOffset - 1);
            $idFromOffset = 0;
            if ($offset > 0) {
                $idFromOffset = method_exists($handler, 'offsetToId')
                    ? $handler->offsetToId($task, $task->currentIdFromOffset)
                    : 0;
            }

            $payload = [
                'handler'             => $task->currentHandler,
                'id'                  => $task->id,
                'currentOffset'       => $offset,
                'currentIdFromOffset' => $idFromOffset,
                'isLast'              => $isLast,
            ];

            $packages[] = [
                $payload,
                IndexService::WORKER_TASK_PROCESS,
                ['id' => $task->id],
            ];

            $task->currentIdFromOffset = $idFromOffset;
        }

        $task->currentOffset = $offset;
        $this->update($task);

        foreach ($packages as $package) {
            $this->queue->publish($package[0], $package[1], $package[2]);
        }
    }

    private function calculatePercent(Task $task)
    {
        if ($task->totalItems == 0) {
            return 100;
        }
        return ($task->processedItems / $task->totalItems) * 100;
    }

    public function getHandler(string $name): ?ReindexInterface
    {
        return $this->container["reindex.handler.$name"] ?? null;
    }

    public function stats(Task $task)
    {
        foreach ($task->handlers as $name) {
            $handler = $this->getHandler($name);
            $stats[$name] = $handler ? $handler->count($task) : 0;
        }

        return $stats ?? [];
    }

    public function hash(Task $task)
    {
        $hash = [];
        foreach ($task->handlers as $name) {
            if ($handler = $this->getHandler($name)) {
                $hash[$name] = $handler->hash($task);
            }
        }

        return $hash;
    }
}

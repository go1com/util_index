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
use JsonSerializable;
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
    )
    {
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

    public function createItem(TaskItem $taskItem)
    {
        $this->db->insert('index_task_item', $taskItem->jsonSerialize());
        $taskItem->id = $this->db->lastInsertId('index_task_item');

        return $taskItem;
    }

    public function update(Task $task)
    {
        $task->updated = time();
        $this->db->update('index_task', $task->jsonSerialize(), ['id' => $task->id]);

        return true;
    }

    public function updateTaskItem(TaskItem $taskItem)
    {
        $taskItem->updated = time();
        $this->db->update('index_task_item', $taskItem->jsonSerialize(), ['id' => $taskItem->id]);

        return $taskItem;
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

    public function loadItem(int $id): ?TaskItem
    {
        $item = $this
            ->db
            ->executeQuery('SELECT * FROM index_task_item WHERE id = ?', [$id])
            ->fetch(DB::OBJ);
        if (!$item) {
            return null;
        }

        return TaskItem::create($item);
    }

    private function getAlias(Task $task)
    {
        try {
            $alias = $this->client->indices()->getAlias(['name' => $task->aliasName]);
            $alias = $alias ? array_keys($alias) : [];

            return $alias[0] ?? null;
        }
        catch (Exception $e) {
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

    public function verify(Task $task)
    {
        $task->processedItems = $this->countFinishedItems($task);
        $task->percent = $this->calculatePercent($task);
        $nextItem = $this->generateNextItem($task);

        // move next handler
        if (!$nextItem) {
            $fnMoveNext = function () use ($task) {
                $task->currentHandler = $task->nextHandler();
                $task->currentOffset = 0;
                $task->currentIdFromOffset = 0;
            };
            $fnMoveNext();
            while ($task->currentHandler && (0 == $task->stats[$task->currentHandler] ?? 0)) {
                $fnMoveNext();
            }
            if ($task->currentHandler) {
                $nextItem = $this->generateNextItem($task);
            }
        }

        if (!$nextItem) {
            $this->finish($task);

            return;
        }

        $this->publishTaskItem($nextItem);
        $this->update($task);
    }

    private function generateNextItem(Task $task)
    {
        if (!$task->currentHandler || empty($task->stats[$task->currentHandler])) {
            return null;
        }

        if ($task->currentOffset >= $task->stats[$task->currentHandler]) {
            return null;
        }
        $handler = $this->getHandler($task->currentHandler);
        $idFromOffset = 0;
        if ($task->currentOffset > 0) {
            $idFromOffset = method_exists($handler, $_m = 'offsetToId')
                ? call_user_func([$handler, $_m], $task, $task->currentIdFromOffset)
                : 0;
        }
        $item = new TaskItem();
        $item->taskId = $task->id;
        $item->handler = $task->currentHandler;
        $item->offset = $task->currentOffset;
        $item->offsetId = $idFromOffset;
        $item->service = $task->service;
        $item->status = TaskItem::IN_PROGRESS;
        $this->createItem($item);

        $task->currentOffset++;
        $task->currentIdFromOffset = $idFromOffset;

        return $item;
    }

    private function countFinishedItems(Task $task): int
    {
        return (int)$this
            ->db
            ->executeQuery('SELECT COUNT(1) FROM index_task_item WHERE task_id = ? AND status = ?', [$task->id, TaskItem::FINISHED])
            ->fetchColumn();
    }

    public function publishTaskItem(TaskItem $item)
    {
        $payload = ['id' => $item->id, 'service' => $item->service];
        $this->queue->publish($payload, IndexService::WORKER_TASK_PROCESS);
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

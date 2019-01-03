<?php

namespace go1\util_index\core\consumer;

use Exception;
use go1\util\contract\ServiceConsumerInterface;
use go1\util_index\HistoryRepository;
use go1\util_index\IndexService;
use go1\util_index\task\Task;
use go1\util_index\task\TaskRepository;
use stdClass;

class TaskConsumer implements ServiceConsumerInterface
{
    private $repository;
    private $history;

    public function __construct(TaskRepository $repository, HistoryRepository $history)
    {
        $this->repository = $repository;
        $this->history = $history;
    }

    public function aware(): array
    {
        return [
            IndexService::WORKER_TASK_PROCESS => 'Process reindexing task',
        ];
    }

    public function consume(string $routingKey, stdClass $payload, stdClass $context = null)
    {
        if (IndexService::WORKER_TASK_PROCESS === $routingKey) {
            if ($task = $this->repository->load($payload->id)) {
                try {
                    $processTask = clone $task;
                    $this->process($payload, $processTask);
                    $this->repository->verify($task, $payload);
                } catch (Exception $e) {
                    $this->history->write('task_process', $task->id, 500, ['message' => $e->getMessage(), 'data' => $payload]);
                }
            }
        }
    }

    private function process(stdClass &$payload, Task $task)
    {
        if (!$handler = $this->repository->getHandler($payload->handler)) {
            return;
        }
        $limit = isset($handler::$limit) ? $handler::$limit : $task->limit;
        $task->offset = ($payload->currentOffset ?? 0) * $limit;
        $task->offsetId = $payload->currentIdFromOffset ?? 0;
        $task->currentIdFromOffset = $payload->currentIdFromOffset ?? 0;
        $task->limit = $limit;
        $task->currentHandler = $payload->handler;
        $handler->handle($task);
    }
}

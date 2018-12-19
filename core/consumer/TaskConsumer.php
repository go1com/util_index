<?php

namespace go1\util_index\core\consumer;

use Exception;
use go1\util\contract\ServiceConsumerInterface;
use go1\util_index\HistoryRepository;
use go1\util_index\IndexService;
use go1\util_index\task\Task;
use go1\util_index\task\TaskItem;
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
        if (IndexService::WORKER_TASK_PROCESS != $routingKey || $payload->service != SERVICE_NAME) {
            return;
        }

        if (!$item = $this->repository->loadItem($payload->id)) {
            return;
        }
        if (!$task = $this->repository->load($item->taskId)) {
            return;
        }

        try {
            $processTask = clone $task;
            $item->processed = $this->process($item, $processTask);
            $item->status = TaskItem::FINISHED;
            $this->repository->updateTaskItem($item);
            $this->repository->verify($task);
        } catch (Exception $e) {
            $this->history->write('task_process', $task->id, 500, ['message' => $e->getMessage(), 'data' => $payload]);
        }
    }

    private function process(TaskItem $item, Task $task)
    {
        if (!$handler = $this->repository->getHandler($item->handler)) {
            return 0;
        }

        $limit = isset($handler::$limit) ? $handler::$limit : $task->limit;
        $task->offset = $item->offset;
        $task->offsetId = $item->offsetId;
        $task->limit = $limit;
        $task->currentHandler = $item->handler;
        return $handler->handle($task);
    }
}

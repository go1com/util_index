<?php

namespace go1\util_index\task;

use go1\util\es\Schema;
use JsonSerializable;
use stdClass;

class Task implements JsonSerializable
{
    const NOT_STARTED = 0;
    const IN_PROGRESS = 1;
    const FINISHED    = 2;

    public $id;
    public $title;
    public $status;
    public $offset   = 0;
    public $offsetId = 0;
    public $percent;
    public $created;
    public $updated;
    public $index;
    public $handlers;
    public $currentHandler;
    public $stats;
    public $limit    = 100;
    public $portal;
    public $authorId;
    public $data;
    public $alias;  # Point to main index or NOT. Default true
    public $maxNumItems; # Max number of messages are generated at time.
    public $currentOffset;
    public $currentIdFromOffset;
    public $totalItems;
    public $processedItems;
    public $failureItems;

    public $aliasName;
    public $routing;
    public $removeRedundant;

    public $esWriterRoutingKey;

    public static function create(stdClass $input): Task
    {
        $task = new Task;
        $task->id = $input->id ?? null;
        $task->title = $input->title ?? null;
        $task->portal = $input->instance ?? null;
        $task->data = $input->data ?? null;
        $task->status = $input->status ?? self::NOT_STARTED;
        $task->percent = $input->percent ?? 0;
        $task->authorId = $input->author_id ?? 0;
        $task->created = $input->created ?? time();
        $task->updated = $input->updated ?? time();

        $data = is_scalar($task->data) ? json_decode($task->data, true) : $task->data;
        $task->handlers = $data['handlers'] ?? [];
        $task->currentHandler = $data['currentHandler'] ?? null;
        $task->index = $data['index'] ?? Schema::INDEX;
        $task->stats = $data['stats'] ?? [];
        $task->alias = $data['alias'] ?? false;
        $task->routing = $data['routing'] ?? Schema::INDEX;
        $task->aliasName = $data['aliasName'] ?? $task->index;
        $task->maxNumItems = $data['maxNumItems'] ?? -1;
        $task->currentOffset = $data['currentOffset'] ?? 0;
        $task->currentIdFromOffset = $data['currentIdFromOffset'] ?? 0;
        $task->totalItems = $data['totalItems'] ?? 0;
        $task->processedItems = $data['processedItems'] ?? 0;
        $task->failureItems = $data['failureItems'] ?? 0;
        $task->removeRedundant = $data['removeRedundant'] ?? false;
        $task->esWriterRoutingKey = $data['esWriterRoutingKey'] ?? null;

        return $task;
    }

    public function data(array $data)
    {
        $this->data = $data;

        return $this;
    }

    public function nextHandler()
    {
        $position = array_search($this->currentHandler, $this->handlers);
        if ($position !== false) {
            if (isset($this->handlers[++$position])) {
                return $this->handlers[$position];
            }
        }

        return false;
    }

    public function currentHandlerIsCompleted(): bool
    {
        if (0 == ($this->stats[$this->currentHandler] ?? 0)) {
            return true;
        }

        if ($this->currentOffset >= $this->stats[$this->currentHandler]) {
            return true;
        }

        return false;
    }

    public function jsonSerialize()
    {
        $array = [
            'id'        => $this->id,
            'title'     => $this->title,
            'instance'  => $this->portal ? $this->portal->title : null,
            'percent'   => $this->percent,
            'status'    => $this->status,
            'author_id' => $this->authorId,
            'created'   => $this->created,
            'updated'   => $this->updated,
            'data'      => json_encode([
                'index'               => $this->index,
                'currentHandler'      => $this->currentHandler,
                'stats'               => $this->stats,
                'alias'               => $this->alias,
                'aliasName'           => $this->aliasName,
                'maxNumItems'         => $this->maxNumItems,
                'currentOffset'       => $this->currentOffset,
                'currentIdFromOffset' => $this->currentIdFromOffset,
                'handlers'            => $this->handlers,
                'routing'             => $this->routing,
                'removeRedundant'     => $this->removeRedundant,
                'totalItems'          => $this->totalItems,
                'processedItems'      => $this->processedItems,
                'failureItems'        => $this->failureItems,
            ]),
        ];

        return $array;
    }
}

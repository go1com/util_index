<?php

namespace go1\util_index\task;

use JsonSerializable;
use stdClass;

class TaskItem implements JsonSerializable
{
    const NOT_STARTED = 0;
    const IN_PROGRESS = 1;
    const FINISHED    = 2;

    public $id;
    public $taskId;
    public $handler;
    public $offset;
    public $offsetId;
    public $status;
    public $updated;
    public $data;
    public $processed;
    public $service;
    public $log;

    public static function create($input)
    {
        $item = new static();
        is_array($input) && $input = json_decode(json_encode($input));
        $item->id = $input->id ?? null;
        $item->taskId = $input->task_id ?? 0;
        $item->handler = $input->handler ?? null;
        $item->offset = $input->offset ?? 0;
        $item->offsetId = $input->offset_id ?? 0;
        $item->status = $input->status ?? static::NOT_STARTED;
        $item->data = is_scalar($input->data) ? json_decode($input->data) : json_decode(json_encode($input->data));

        $item->processed = $item->data->processed ?? 0;
        $item->service = $item->data->service ?? 'internal-index';
        $item->log = $item->data->log ?? '';

        return $item;
    }

    public function jsonSerialize()
    {
        $array = [
            'id'        => $this->id,
            'task_id'   => $this->taskId,
            'handler'   => $this->handler,
            'offset'    => $this->offset,
            'offset_id' => $this->offsetId,
            'status'    => $this->status ?? static::NOT_STARTED,
            'updated'   => $this->updated ?? 0,
            'data'      => json_encode([
                'processed' => $this->processed ?? 0,
                'service'   => $this->service,
                'log'       => $this->log,
            ]),
        ];

        return $array;
    }
}

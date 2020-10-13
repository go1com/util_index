<?php

namespace go1\util_index;

use Assert\Assert;
use go1\clients\MqClient;
use RuntimeException;

class EsWriterClient
{
    private $mqClient;
    private $routingKey;

    public function __construct(MqClient $mqClient, string $routingKey)
    {
        $this->mqClient = $mqClient;
        $this->routingKey = $routingKey;
    }

    public function setRoutingKey(string $routingKey)
    {
        $this->routingKey = $routingKey;
    }

    private function validate(array $params, string $requiredFields)
    {
        $assertion = Assert::lazy();
        foreach (explode(",", $requiredFields) as $requiredField) {
            $assertion->that($params[$requiredField] ?? null, $requiredField)->notNull();
        }
        $assertion->verifyNow();
    }

    public function delete($params, bool $batch = false)
    {
        $this->validate($params, 'index,type,id,routing');
        $uri = sprintf(
            '/%s/%s/%s?routing=%s',
            $params['index'],
            $params['type'],
            $params['id'],
            $params['routing']
        );

        $batch
            ? $this->mqClient->batchAdd(['uri' => $uri, 'http_method' => 'DELETE'], $this->routingKey)
            : $this->mqClient->publish(['uri' => $uri, 'http_method' => 'DELETE'], $this->routingKey);
    }

    public function updateByQuery($params, bool $batch = false)
    {
        $this->validate($params, 'index,type,body');

        $uri = sprintf('/%s/%s/_update_by_query', $params['index'], $params['type']);
        isset($params['routing']) && $uri .= sprintf('?routing=%s', $params['routing']);

        $batch
            ? $this->mqClient->batchAdd(['uri' => $uri, 'body' => $params['body']], $this->routingKey)
            : $this->mqClient->publish(['uri' => $uri, 'body' => $params['body']], $this->routingKey);
    }

    public function deleteByQuery($params, bool $batch = false)
    {
        $this->validate($params, 'index,type,body');

        $uri = sprintf('/%s/%s/_delete_by_query', $params['index'], $params['type']);
        isset($params['routing']) && $uri .= sprintf('?routing=%s', $params['routing']);

        $batch
            ? $this->mqClient->batchAdd(['uri' => $uri, 'body' => $params['body']], $this->routingKey)
            : $this->mqClient->publish(['uri' => $uri, 'body' => $params['body']], $this->routingKey);
    }

    public function index($params, bool $batch = false)
    {
        $this->create($params, $batch);
    }

    public function create($params, bool $batch = false)
    {
        $this->validate($params, 'index,type,id,body,routing');
        $uri = sprintf(
            '/%s/%s/%s/_create?routing=%s',
            $params['index'],
            $params['type'],
            $params['id'],
            $params['routing']
        );

        $batch
            ? $this->mqClient->batchAdd(['uri' => $uri, 'body' => $params['body']], $this->routingKey)
            : $this->mqClient->publish(['uri' => $uri, 'body' => $params['body']], $this->routingKey);

    }

    public function update($params, bool $batch = false)
    {
        $this->validate($params, 'index,type,id,body,routing');
        $uri = sprintf(
            '/%s/%s/%s/_update?routing=%s',
            $params['index'],
            $params['type'],
            $params['id'],
            $params['routing']
        );

        $batch
            ? $this->mqClient->batchAdd(['uri' => $uri, 'body' => $params['body']], $this->routingKey)
            : $this->mqClient->publish(['uri' => $uri, 'body' => $params['body']], $this->routingKey);
    }

    public function bulk($params)
    {
        $this->validate($params, 'body');
        # Parse ElasticSearch\Client::bulk into es writer
        $offset = 0;
        $batch = true;
        while (isset($params['body'][$offset])) {
            $op = array_keys($params['body'][$offset])[0];
            $metadata = $params['body'][$offset][$op];

            $_params = [
                'index'   => $metadata['_index'] ?? $params['index'] ?? null,
                'type'    => $metadata['_type'] ?? $params['type'] ?? null,
                'id'      => $metadata['_id'] ?? null,
                'routing' => $metadata['_routing'] ?? null,
                'body'    => $params['body'][$offset + 1],
            ];
            switch ($op) {
                case 'index':
                    $this->index($_params, $batch);
                    break;

                case 'create':
                    $this->create($_params, $batch);
                    break;

                case 'update':
                    $this->update($_params, $batch);
                    break;

                case 'delete':
                    $this->delete($_params, $batch);
                    break;

                default:
                    throw new RuntimeException('Unknown operator.');
            }

            $offset += 2;
        }

        return [];
    }
}

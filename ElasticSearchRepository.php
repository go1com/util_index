<?php

namespace go1\util_index;

use Elasticsearch\Client;
use go1\clients\MqClient;
use go1\util\es\Schema;

class ElasticSearchRepository
{
    public  $client;
    private $waitForCompletion;
    private $queue;
    private $history;
    private $throwBulkException = true;

    public function __construct(Client $client, bool $waitForCompletion, MqClient $queue, HistoryRepository $history)
    {
        $this->client = $client;
        $this->waitForCompletion = $waitForCompletion;
        $this->queue = $queue;
        $this->history = $history;

        $_ = getenv('INDEX_SERVICE_THROW_EXCEPTION_ON_BULK_ERROR');
        if (false !== $_) {
            $this->throwBulkException = boolval($_);
        }
    }

    public function installPortalIndex(int $portalId)
    {
        $index = $portalIndex = Schema::portalIndex($portalId);
        $found = $this->client->indices()->exists($params = ['index' => $index]);
        if (!$found) {
            $params['body']['actions'][]['add'] = [
                'index'   => Schema::INDEX,
                'alias'   => $portalIndex,
                'routing' => $portalId,
                'filter'  => [
                    'term' => ['metadata.instance_id' => $portalId],
                ],
            ];

            $this->client->indices()->updateAliases($params);
        }
    }

    private function bulk(array $data, array $indices, $action = Schema::DO_INDEX)
    {
        $params = ['body' => [], 'refresh' => $this->waitForCompletion];
        foreach ($indices as $index) {
            $params['body'][] = [
                $action => array_filter([
                    '_routing' => ($data['routing'] ?? null) ?: ($index == Schema::MARKETPLACE_INDEX ? Schema::MARKETPLACE_INDEX : null),
                    '_index'   => $data['index'] ?? $index,
                    '_type'    => $data['type'],
                    '_id'      => $data['id'],
                    '_parent'  => $data['parent_indices'][$index] ?? $data['parent'] ?? null,
                ]),
            ];

            if ($action !== Schema::DO_DELETE) {
                $instanceId = $data['body']['metadata']['instance_id'] ?? $data['body']['doc']['metadata']['instance_id'] ?? null;
                $routing = explode(Schema::INDEX . '_portal_', $index);
                $routing = $routing[1] ?? null;
                if ($routing && $instanceId && ($routing != $instanceId)) {
                    (Schema::DO_INDEX === $action) && $data['body']['metadata']['instance_id'] = $routing;
                    (Schema::DO_UPDATE === $action) && $data['body']['doc']['metadata']['instance_id'] = $routing;
                }
                $params['body'][] = $data['body'];
            }
        }

        $response = $this->client->bulk($params);
        $response && $this->history->bulkLog($response);

        if (!empty($response['errors'])) {
            if ($this->throwBulkException) {
                $err = 'has error on bulk request: ';
                $err .= print_r(['request' => $params, 'response' => $response], true);
                throw new ElasticSearchBulkRequestError($err, $response);
            }
        }

        return $response;
    }

    public function create(array $data, array $indices)
    {
        $params = [
            'routing' => $data['routing'] ?: Schema::INDEX,
            'index'   => $data['index'] ?: Schema::INDEX,
            'type'    => $data['type'],
            'id'      => $data['id'],
            'body'    => $data['body'],
            'refresh' => $this->waitForCompletion,
        ];

        try {
            $this->client->create($params);
        } catch (ElasticsearchException $e) {
            $this->history->write($params['type'], $params['id'], $e->getCode(), $e->getMessage());
        }
    }

    public function update(array $data, array $indices)
    {
        $this->bulk($data, $indices, Schema::DO_UPDATE);
    }

    public function delete(array $data, array $indices)
    {
        return $this->bulk($data, $indices, Schema::DO_DELETE);
    }

    public function updateByQuery(string $index, string $type, array $body)
    {
        return $this->client->updateByQuery([
            'index'               => $index,
            'type'                => $type,
            'body'                => $body,
            'refresh'             => $this->waitForCompletion,
            'wait_for_completion' => $this->waitForCompletion,
        ]);
    }
}

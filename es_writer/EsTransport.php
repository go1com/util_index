<?php

namespace go1\util_index\es_writer;

use Elasticsearch\Transport;
use go1\clients\MqClient;
use GuzzleHttp\Ring\Future\CompletedFutureValue;

class EsTransport extends Transport
{
    /** @var Transport */
    private $transport;
    private $queue;
    private $routingKey;

    public function __construct(MqClient $queue, string $routingKey)
    {
        $this->queue = $queue;
        $this->routingKey = $routingKey;
    }

    public function setTransport(Transport $transport)
    {
        $this->transport = $transport;
    }

    public function performRequest($method, $uri, $params = null, $body = null, $options = [])
    {
        if ('GET' === $method) {
            return $this->transport->performRequest($method, $uri, $params, $body, $options);
        }

        dump(
            func_get_args()
        );

        $this->queue->publish(
            [
                'http_method' => $method,
                'uri'         => $uri,
                'body'        => $body,
            ],
            'es.writer.go1'
        );

        return new CompletedFutureValue(['status' => 200, 'errors' => false]);
    }
}

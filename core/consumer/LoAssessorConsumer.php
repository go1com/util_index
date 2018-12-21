<?php

namespace go1\util_index\core\consumer;

use Doctrine\DBAL\Connection;
use Elasticsearch\Client;
use Elasticsearch\Common\Exceptions\ElasticsearchException;
use go1\clients\MqClient;
use go1\util\contract\ServiceConsumerInterface;
use go1\util\es\Schema;
use go1\util\lo\LoHelper;
use go1\util\queue\Queue;
use go1\util_index\core\EnrolmentFormatter;
use go1\util_index\core\IndexFormatterInterface;
use go1\util_index\HistoryRepository;
use ONGR\ElasticsearchDSL\Query\Compound\BoolQuery;
use ONGR\ElasticsearchDSL\Query\TermLevel\IdsQuery;
use ONGR\ElasticsearchDSL\Query\TermLevel\TermQuery;
use stdClass;

class LoAssessorConsumer implements ServiceConsumerInterface
{
    const RETRY_ROUTING_KEY = 'lo-index.message.retry';

    protected $go1;
    protected $es;
    protected $history;
    protected $enrolmentFormatter;
    protected $waitForCompletion;
    protected $queue;

    public function __construct(
        Connection $go1,
        Client $client,
        HistoryRepository $history,
        IndexFormatterInterface $enrolmentFormatter,
        bool $waitForCompletion,
        MqClient $queue
    ) {
        $this->go1 = $go1;
        $this->es = $client;
        $this->history = $history;
        $this->enrolmentFormatter = $enrolmentFormatter;
        $this->waitForCompletion = $waitForCompletion;
        $this->queue = $queue;
    }

    public function aware(): array
    {
        return [
            Queue::LO_SAVE_ASSESSORS => 'TODO: description',
            self::RETRY_ROUTING_KEY  => 'TODO: description',
        ];
    }

    public function consume(string $routingKey, stdClass $body, stdClass $context = null)
    {
        switch ($routingKey) {
            case Queue::LO_SAVE_ASSESSORS:
                $this->onCourseAssessorUpdated($routingKey, $body);
                break;

            case self::RETRY_ROUTING_KEY:
                $this->onMessageRetry($body);
                break;
        }
    }

    protected function onMessageRetry(stdClass $data)
    {
        if ($data->body->numOfRetry < 3) {
            $this->consume($data->routingKey, $data->body);
        }
    }

    private function onCourseAssessorUpdated(string $routingKey, stdClass $body)
    {
        $courseId = $body->id;
        $assessors = LoHelper::assessorIds($this->go1, $courseId);
        try {
            $this->es->updateByQuery([
                'index'               => Schema::LEARNING_RECORD_INDEX,
                'type'                => Schema::O_LO,
                'body'                => [
                    'query'  => (new IdsQuery([$courseId]))->toArray(),
                    'script' => [
                        'inline' => implode(";", [
                            "ctx._source.assessor = params.assessor",
                            "ctx._source.assessors = params.assessors",
                        ]),
                        'params' => [
                            'assessor'  => $this->enrolmentFormatter->assessor($assessors),
                            'assessors' => $assessors,
                        ],
                    ],
                ],
                'wait_for_completion' => $this->waitForCompletion,
                'conflicts'           => 'proceed',
            ]);
        } catch (ElasticsearchException $e) {
            $this->history->write(Schema::O_LO, $courseId, $e->getCode(), $e->getMessage());
        }
    }

    protected function handleConflict($response, $routingKey, $body)
    {
        $numOfConflict = $response['version_conflicts'] ?? 0;
        if ($numOfConflict > 0) {
            $body->numOfRetry = ($body->numOfRetry ?? 0) + 1;
            $this->queue->queue(['routingKey' => $routingKey, 'body' => $body], self::RETRY_ROUTING_KEY);
        }
    }
}

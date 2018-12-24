<?php

namespace go1\util_index\core\consumer;

use Doctrine\DBAL\Connection;
use Elasticsearch\Client;
use Elasticsearch\Common\Exceptions\ElasticsearchException;
use Exception;
use go1\core\learning_record\enrolment\index\Microservice;
use go1\util\contract\ServiceConsumerInterface;
use go1\util\enrolment\EnrolmentHelper;
use go1\util\enrolment\EnrolmentTypes;
use go1\util\es\Schema;
use go1\util\group\GroupHelper;
use go1\util\group\GroupItemTypes;
use go1\util\lo\LoHelper;
use go1\util\lo\LoTypes;
use go1\util\plan\PlanHelper;
use go1\util\plan\PlanTypes;
use go1\util\portal\PortalHelper;
use go1\util\queue\Queue;
use go1\util\user\UserHelper;
use go1\util_index\core\AccountFieldFormatter;
use go1\util_index\core\EnrolmentFormatter;
use go1\util_index\core\LoFormatter;
use go1\util_index\core\UserFormatter;
use go1\util_index\ElasticSearchRepository;
use go1\util_index\HistoryRepository;
use go1\util_index\IndexHelper;
use ONGR\ElasticsearchDSL\Query\Compound\BoolQuery;
use ONGR\ElasticsearchDSL\Query\TermLevel\TermQuery;
use Ramsey\Uuid\Uuid;
use RuntimeException;
use stdClass;

class EnrolmentConsumer implements ServiceConsumerInterface
{
    /**
     * @var Client
     */
    protected $esClient;

    protected $history;
    protected $db;
    protected $go1;
    protected $social;
    protected $accountsName;
    protected $formatter;
    protected $loFormatter;
    protected $userFormatter;
    protected $eckDataFormatter;
    protected $waitForCompletion;
    protected $repository;

    public function __construct(
        $esClient,
        HistoryRepository $history,
        Connection $db,
        Connection $go1,
        ?Connection $social,
        string $accountsName,
        EnrolmentFormatter $formatter,
        LoFormatter $loFormatter,
        UserFormatter $userFormatter,
        AccountFieldFormatter $eckDataFormatter,
        bool $waitForCompletion,
        ElasticSearchRepository $repository
    ) {
        $this->esClient = $esClient;
        $this->history = $history;
        $this->db = $db;
        $this->go1 = $go1;
        $this->social = $social;
        $this->accountsName = $accountsName;
        $this->formatter = $formatter;
        $this->loFormatter = $loFormatter;
        $this->userFormatter = $userFormatter;
        $this->eckDataFormatter = $eckDataFormatter;
        $this->waitForCompletion = $waitForCompletion;
        $this->repository = $repository;
    }

    public function aware(): array
    {
        return [
            Queue::ENROLMENT_CREATE        => 'Create an enrolment document in ES. Delete plan assigned if exist',
            Queue::ENROLMENT_UPDATE        => 'Update an enrolment document in ES',
            Queue::ENROLMENT_DELETE        => 'Delete an enrolment document in ES',
            Queue::LO_UPDATE               => 'Update lo and parent lo on an enrolment document in ES. Update enrolment status of that lo',
            Queue::USER_UPDATE             => 'Update account and assessor on an enrolment document in ES',
            Queue::USER_DELETE             => 'Delete account on an enrolment document in ES',
            Queue::GROUP_ITEM_CREATE       => "Update account's groups on an enrolment document in ES",
            Queue::GROUP_ITEM_DELETE       => "Update account's groups on an enrolment document in ES",
            Queue::ECK_CREATE              => "Update account's custom fields on an enrolment document in ES",
            Queue::ECK_UPDATE              => "Update account's custom fields on an enrolment document in ES",
            Queue::ECK_DELETE              => "Delete account's custom fields on an enrolment document in ES",
            Queue::QUIZ_USER_ANSWER_CREATE => 'Update an enrolment document in ES that is related to the quiz answer',
            Queue::QUIZ_USER_ANSWER_UPDATE => 'Update an enrolment document in ES that is related to the quiz answer',
        ];
    }

    public function consume(string $routingKey, stdClass $enrolment, stdClass $context = null)
    {
        switch ($routingKey) {
            case Queue::ENROLMENT_CREATE:
                if ($lo = LoHelper::load($this->go1, $enrolment->lo_id)) {
                    $this->onCreate($enrolment, $lo);
                }
                break;

            case Queue::ENROLMENT_UPDATE:
                if ($lo = LoHelper::load($this->go1, $enrolment->lo_id)) {
                    $this->onUpdate($enrolment, $lo);
                }
                break;

            case Queue::ENROLMENT_DELETE:
                if ($lo = LoHelper::load($this->go1, $enrolment->lo_id)) {
                    $this->onDelete($enrolment, $lo);
                }
                break;

            case Queue::LO_UPDATE:
                $this->onLoUpdate($enrolment);
                $this->onParentUpdate($enrolment);

                $lo = $enrolment;
                if ((LoTypes::COURSE == $lo->type) && isset($lo->original) && ($lo->original->published != $lo->published)) {
                    $this->updateChildEnrolmentStatus($lo);
                }
                break;

            case Queue::USER_UPDATE:
                $this->onAccountUpdate($enrolment);
                $this->onAssessorUpdate($enrolment);
                break;

            case Queue::USER_DELETE:
                $this->onAccountDelete($enrolment);
                break;

            case Queue::GROUP_ITEM_CREATE:
                $this->onGroupItemCreate($enrolment);
                break;

            case Queue::GROUP_ITEM_DELETE:
                $this->onGroupItemDelete($enrolment);
                break;

            case Queue::ECK_CREATE:
            case Queue::ECK_UPDATE:
                $this->onEckUpdate($enrolment);
                break;

            case Queue::ECK_DELETE:
                $this->onEckDelete($enrolment);
                break;

            case Queue::QUIZ_USER_ANSWER_CREATE:
            case Queue::QUIZ_USER_ANSWER_UPDATE:
                $this->onQuizUserAnswerUpdate($enrolment);
                break;

            case Microservice::BULK_ENROLMENT:
                $this->onBulk($enrolment->enrolments, $enrolment->indexName, $enrolment->marketplace ?? false);
                break;
        }
    }

    protected function format(stdClass $enrolment)
    {
        return $this->formatter->format($enrolment);
    }

    private function onCreate(stdClass $enrolment, stdClass $lo, $indices = null)
    {
        try {
            $this->esClient->create([
                'index'   => Schema::LEARNING_RECORD_INDEX,
                'routing' => $enrolment->taken_instance_id,
                'type'    => Schema::O_ENROLMENT,
                'id'      => $enrolment->id,
                'refresh' => $this->waitForCompletion,
                'body'    => $this->format($enrolment),
            ]);

            $user = UserHelper::loadByProfileId($this->go1, $enrolment->profile_id, $this->accountsName);
            $this->esClient->delete([
                'index'   => Schema::LEARNING_RECORD_INDEX,
                'routing' => $enrolment->taken_instance_id,
                'type'    => Schema::O_ENROLMENT,
                'id'      => EnrolmentTypes::TYPE_PLAN_ASSIGNED."$user->id:$enrolment->lo_id",
            ]);
        } catch (ElasticsearchException $e) {
            $this->history->write(Schema::O_ENROLMENT, $enrolment->id, $e->getCode(), $e->getMessage());
        }
    }

    private function onUpdate(stdClass $enrolment, stdClass $lo, $indices = null)
    {
        try {
            $this->esClient->update([
                'index'   => Schema::LEARNING_RECORD_INDEX,
                'routing' => $enrolment->taken_instance_id,
                'type'    => Schema::O_ENROLMENT,
                'id'      => $enrolment->id,
                'body'    => ['doc' => $this->format($enrolment), 'doc_as_upsert' => true],
                'refresh' => $this->waitForCompletion,
            ]);
        } catch (ElasticsearchException $e) {
            $this->history->write(Schema::O_ENROLMENT, $enrolment->id, $e->getCode(), $e->getMessage());
        }
    }

    private function onDelete(stdClass $enrolment, stdClass $lo, $indices = null)
    {
        try {
            $this->esClient->delete([
                'type'    => Schema::O_ENROLMENT,
                'id'      => $enrolment->id,
                'index'   => Schema::LEARNING_RECORD_INDEX,
                'routing' => $enrolment->taken_instance_id,
            ]);
        } catch (ElasticsearchException $e) {
            $this->history->write(Schema::O_ENROLMENT, $enrolment->id, $e->getCode(), $e->getMessage());
        }
    }

    private function onBulk(array $enrolments, string $indexName, $marketplace = false)
    {
        $params = ['body' => [], 'refresh' => $this->waitForCompletion, 'client' => ['headers' => ['uuid' => Uuid::uuid4()->toString()]]];
        foreach ($enrolments as $enrolment) {
            try {
                $params['body'][] = [
                    'index' => [
                        '_index'   => $indexName,
                        '_type'    => Schema::O_ENROLMENT,
                        '_id'      => $enrolment->id,
                        '_routing' => $enrolment->routing ?? $enrolment->taken_instance_id,
                    ],
                ];
                $params['body'][] = $this->format($enrolment);
            } catch (Exception $e) {
                $this->history->write(Schema::O_ENROLMENT, $enrolment->id, 500, $e->getMessage());
            }
        }
        if ($params['body']) {
            $response = $this->esClient->bulk($params);
            $this->history->bulkLog($response);
        }
    }

    private function onLoUpdate(stdClass $lo)
    {
        $params = [
            'index'               => Schema::LEARNING_RECORD_INDEX,
            'type'                => Schema::O_ENROLMENT,
            'body'                => [
                'query'  => (new TermQuery('lo_id', $lo->id))->toArray(),
                'script' => [
                    'inline' => "ctx._source.lo = params.lo",
                    'params' => ['lo' => $this->loFormatter->format($lo)],
                ],
            ],
            'refresh'             => $this->waitForCompletion,
            'wait_for_completion' => $this->waitForCompletion,
        ];
        $this->esClient->updateByQuery($params);
    }

    private function onParentUpdate(stdClass $lo)
    {
        $params = [
            'index'               => Schema::LEARNING_RECORD_INDEX,
            'type'                => Schema::O_ENROLMENT,
            'body'                => [
                'query'  => (new TermQuery('parent_lo.id', $lo->id))->toArray(),
                'script' => [
                    'inline' => "ctx._source.parent_lo = params.lo",
                    'params' => ['lo' => [
                        'id'    => (int) $lo->id,
                        'type'  => $lo->type,
                        'title' => $lo->title,
                    ]],
                ],
            ],
            'refresh'             => $this->waitForCompletion,
            'wait_for_completion' => $this->waitForCompletion,
        ];
        $this->esClient->updateByQuery($params);
    }

    private function onAccountUpdate(stdClass $account)
    {
        if (($this->accountsName === $account->instance) || !IndexHelper::userIsChanged($account)) {
            return;
        }

        $formattedAccount = $this->userFormatter->format($account, true);
        foreach ($formattedAccount as $k => $v) {
            $lines[] = "ctx._source.account.$k = params.$k";
        }

        if (!empty($lines)) {
            $this->esClient->updateByQuery(
                [
                    'index'               => Schema::LEARNING_RECORD_INDEX,
                    'type'                => Schema::O_ENROLMENT,
                    'body'                => [
                        'query'  => (new TermQuery('metadata.account_id', $account->id))->toArray(),
                        'script' => [
                            'inline' => implode(";", $lines),
                            'params' => $formattedAccount,
                        ],
                    ],
                    'refresh'             => $this->waitForCompletion,
                    'wait_for_completion' => $this->waitForCompletion,
                    'conflicts'           => 'abort',
                ]
            );
        }
    }

    private function onAssessorUpdate(stdClass $user)
    {
        if (($this->accountsName !== $user->instance) || !IndexHelper::userIsChanged($user)) {
            return;
        }

        if (isset($user->data) && is_scalar($user->data)) {
            $user->data = json_decode($user->data);
        }

        if (empty($user->first_name)) {
            $fullName = $user->last_name;
        } elseif (empty($user->last_name)) {
            $fullName = $user->first_name;
        } else {
            $fullName = "{$user->first_name} {$user->last_name}";
        }

        $params = [
            'index'               => Schema::LEARNING_RECORD_INDEX,
            'type'                => Schema::O_ENROLMENT,
            'body'                => [
                'query'  => (new TermQuery('assessor.id', $user->id))->toArray(),
                'script' => [
                    'inline' => "ctx._source.assessor = params.assessor",
                    'params' => ['assessor' => [
                        'id'         => (int) $user->id,
                        'mail'       => $user->mail,
                        'name'       => $fullName,
                        'first_name' => isset($user->first_name) ? $user->first_name : '',
                        'last_name'  => isset($user->last_name) ? $user->last_name : '',
                        'avatar'     => isset($user->avatar) ? $user->avatar : (isset($user->data->avatar->uri) ? $user->data->avatar->uri : null),
                    ]],
                ],
            ],
            'refresh'             => $this->waitForCompletion,
            'wait_for_completion' => $this->waitForCompletion,
            'conflicts'           => 'abort',
        ];
        $this->esClient->updateByQuery($params);
    }

    private function onAccountDelete(stdClass $account)
    {
        if ($this->accountsName === $account->instance) {
            return;
        }

        $params = [
            'index'               => Schema::LEARNING_RECORD_INDEX,
            'type'                => Schema::O_ENROLMENT,
            'body'                => [
                'query'  => (new TermQuery('metadata.account_id', $account->id))->toArray(),
                'script' => [
                    'inline' => 'ctx._source.account = null',
                ],
            ],
            'refresh'             => $this->waitForCompletion,
            'wait_for_completion' => $this->waitForCompletion,
        ];
        $this->esClient->updateByQuery($params);
    }

    private function updateChildEnrolmentStatus(stdClass $lo)
    {
        $query = new BoolQuery();
        $query->add(new TermQuery('metadata.course_id', $lo->id), BoolQuery::MUST);

        $this->esClient->updateByQuery([
            'index'               => Schema::LEARNING_RECORD_INDEX,
            'type'                => Schema::O_ENROLMENT,
            'body'                => [
                'query'  => $query->toArray(),
                'script' => [
                    'inline' => "ctx._source.metadata.status = $lo->published;",
                ],
            ],
            'refresh'             => $this->waitForCompletion,
            'wait_for_completion' => $this->waitForCompletion,
        ]);
    }

    private function onGroupItemCreate(stdClass $groupItem)
    {
        if (GroupItemTypes::USER !== $groupItem->entity_type) {
            return;
        }

        if (!$user = UserHelper::load($this->go1, $groupItem->entity_id)) {
            throw new RuntimeException('Account not found');
        }

        if (!$portalId = PortalHelper::idFromName($this->go1, $user->instance)) {
            throw new RuntimeException('Portal not found');
        }

        $this->updateUserGroups($portalId, $groupItem->entity_id);
    }

    private function onGroupItemDelete(stdClass $groupItem)
    {
        if (GroupItemTypes::USER !== $groupItem->entity_type) {
            return;
        }

        if (!$user = UserHelper::load($this->go1, $groupItem->entity_id)) {
            throw new RuntimeException('Account not found');
        }

        if (!$portalId = PortalHelper::idFromName($this->go1, $user->instance)) {
            throw new RuntimeException('Portal not found');
        }

        $this->updateUserGroups($portalId, $groupItem->entity_id);
    }

    private function updateUserGroups($portalId, $accountId)
    {
        $groups = GroupHelper::userGroups($this->go1, $this->social, $portalId, $accountId, $this->accountsName);
        $query = new BoolQuery();
        $query->add(new TermQuery('metadata.account_id', $accountId), BoolQuery::MUST);

        $params = [
            'index'               => Schema::LEARNING_RECORD_INDEX,
            'type'                => Schema::O_ENROLMENT,
            'body'                => [
                'query'  => $query->toArray(),
                'script' => [
                    'inline' => "ctx._source.account.groups = params.groups",
                    'params' => [
                        'groups' => $groups,
                    ],
                ],
            ],
            'refresh'             => $this->waitForCompletion,
            'wait_for_completion' => $this->waitForCompletion,
        ];
        $this->esClient->updateByQuery($params);
    }

    private function onEckUpdate(stdClass $entity)
    {
        if (($entity->entity_type === Schema::O_ACCOUNT) && IndexHelper::eckEntityIsChanged($entity)) {
            try {
                $formattedEntity = $this->eckDataFormatter->format($entity);
                $this->updateEckEnrollments($formattedEntity, $entity->id);
            } catch (ElasticsearchException $e) {
                $this->history->write($entity->entity_type, $entity->id, $e->getCode(), $e->getMessage());
            }
        }
    }

    private function onEckDelete(stdClass $entity)
    {
        $instanceId = $this->go1->fetchColumn('SELECT id FROM gc_instance WHERE title = ?', [$entity->instance]);
        if (!$instanceId) {
            return;
        }

        if ($entity->entity_type === Schema::O_ACCOUNT) {
            try {
                $formattedEntity = ['fields_' . $instanceId => null];
                $this->updateEckEnrollments($formattedEntity, $entity->id);
            } catch (ElasticsearchException $e) {
                $this->history->write($entity->entity_type, $entity->id, $e->getCode(), $e->getMessage());
            }
        }
    }

    private function updateEckEnrollments($formattedEntity, $id)
    {
        foreach ($formattedEntity as $k => $v) {
            $lines[] = "ctx._source.account.$k = params.$k";
        }
        $params = [
            'index'               => Schema::LEARNING_RECORD_INDEX,
            'type'                => Schema::O_ENROLMENT,
            'body'                => [
                'query'  => (new TermQuery('metadata.account_id', $id))->toArray(),
                'script' => [
                    'inline' => implode(";", $lines),
                    'params' => $formattedEntity,
                ],
            ],
            'refresh'             => $this->waitForCompletion,
            'wait_for_completion' => $this->waitForCompletion,
            'conflicts'           => 'abort',
        ];
        $this->esClient->updateByQuery($params);
    }

    private function onQuizUserAnswerUpdate(stdClass $answer, $indices = null)
    {
        // Some answers have enrolment_id attach to result, some do not. In that case, load enrolment manually.
        if (isset($answer->result->data->enrolment_id)) {
            $enrolment = EnrolmentHelper::load($this->go1, $answer->result->data->enrolment_id);
        } else {
            // @todo Track enrolment id in question's result.
            $portal = PortalHelper::load($this->go1, $answer->question->data->portal_name);
            $portalId = $portal->id;
            $loId = $answer->question->data->li_id;
            $profileId = $answer->person->external_identifier;
            $enrolment = EnrolmentHelper::loadByLoProfileAndPortal($this->go1, $loId, $profileId, $portalId);
        }
        if ($enrolment) {
            if ($lo = LoHelper::load($this->go1, $enrolment->lo_id)) {
                try {
                    $this->esClient->update([
                        'index'   => Schema::LEARNING_RECORD_INDEX,
                        'routing' => $enrolment->taken_instance_id,
                        'type'    => Schema::O_ENROLMENT,
                        'id'      => $enrolment->id,
                        'body'    => ['doc' => $this->format($enrolment), 'doc_as_upsert' => true],
                    ]);
                } catch (ElasticsearchException $e) {
                    $this->history->write(Schema::O_ENROLMENT, $enrolment->id, $e->getCode(), $e->getMessage());
                }
            }
        }
    }

    private function parentId(int $loId, int $takenPortalId)
    {
        if (LoHelper::hasActiveMembership($this->go1, $loId, $takenPortalId)) {
            return LoShareConsumer::id($takenPortalId, $loId);
        }

        if (!is_null($this->social)) {
            if (GroupHelper::isMemberOfContentSharingGroup($this->social, $loId, $takenPortalId)) {
                return LoContentSharingConsumer::id($loId, $takenPortalId);
            }
        }

        return $loId;
    }
}

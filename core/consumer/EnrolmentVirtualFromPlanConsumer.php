<?php

namespace go1\util_index\core\consumer;

use Doctrine\DBAL\Connection;
use Elasticsearch\Common\Exceptions\ElasticsearchException;
use Exception;
use go1\core\learning_record\enrolment\index\Microservice;
use go1\util\award\AwardHelper;
use go1\util\DateTime;
use go1\util\edge\EdgeTypes;
use go1\util\enrolment\EnrolmentHelper;
use go1\util\enrolment\EnrolmentTypes;
use go1\util\es\Schema;
use go1\util\lo\LoHelper;
use go1\util\plan\PlanHelper;
use go1\util\plan\PlanStatuses;
use go1\util\plan\PlanTypes;
use go1\util\portal\PortalHelper;
use go1\util\queue\Queue;
use go1\util\user\UserHelper;
use go1\util_index\core\AccountFieldFormatter;
use go1\util_index\core\AwardEnrolmentFormatter;
use go1\util_index\core\EnrolmentFormatter;
use go1\util_index\core\LoFormatter;
use go1\util_index\core\UserFormatter;
use go1\util_index\ElasticSearchRepository;
use go1\util_index\HistoryRepository;
use go1\util_index\IndexHelper;
use Ramsey\Uuid\Uuid;
use RuntimeException;
use stdClass;

class EnrolmentVirtualFromPlanConsumer extends EnrolmentConsumer
{
    private $award;
    private $awardEnrolmentFormatter;

    public function __construct(
        $esClient,
        HistoryRepository $history,
        Connection $db,
        Connection $go1,
        Connection $social,
        Connection $award,
        string $accountsName,
        EnrolmentFormatter $formatter,
        AwardEnrolmentFormatter $awardEnrolmentFormatter,
        LoFormatter $loFormatter,
        UserFormatter $userFormatter,
        AccountFieldFormatter $eckDataFormatter,
        bool $waitForCompletion,
        ElasticSearchRepository $repository
    ) {
        parent::__construct(
            $esClient,
            $history,
            $db,
            $go1,
            $social,
            $accountsName,
            $formatter,
            $loFormatter,
            $userFormatter,
            $eckDataFormatter,
            $waitForCompletion,
            $repository
        );

        $this->award = $award;
        $this->awardEnrolmentFormatter = $awardEnrolmentFormatter;
    }

    public function aware(): array
    {
        return [
            Queue::PLAN_CREATE      => 'TODO: description',
            Queue::PLAN_UPDATE      => 'TODO: description',
            Queue::PLAN_DELETE      => 'TODO: description',
            Queue::ENROLMENT_DELETE => 'TODO: description',
            Queue::RO_CREATE        => 'TODO: description',
        ];
    }

    public function consume(string $routingKey, stdClass $plan, stdClass $context = null): bool
    {
        try {
            switch ($routingKey) {
                case Queue::PLAN_CREATE:
                    $this->onCreate($plan);
                    break;

                case Queue::PLAN_UPDATE:
                    $this->onUpdate($plan);
                    break;

                case Queue::PLAN_DELETE:
                    $this->onDelete($plan);
                    break;

                case Queue::ENROLMENT_DELETE:
                    $enrolment = clone $plan;
                    if ($lo = LoHelper::load($this->go1, $enrolment->lo_id)) {
                        $this->onEnrolmentDelete($enrolment, $lo);
                    }
                    break;

                case Queue::RO_CREATE:
                    if (EdgeTypes::HAS_PLAN == $plan->type) {
                        $this->onRoCreate($plan);
                    }
                    break;

                case Microservice::BULK_PLAN:
                    $this->onBulk($plan->plans, $plan->indexName);
                    break;
            }
        } catch (Exception $e) {
            $this->history->write($routingKey, $plan->id ?? null, $e->getCode(), $e->getMessage());
        }

        return true;
    }

    private function extractPlan(stdClass $plan)
    {
        $entity = (PlanTypes::ENTITY_AWARD == $plan->entity_type)
            ? AwardHelper::load($this->award, $plan->entity_id)
            : LoHelper::load($this->go1, $plan->entity_id);

        if (!$entity) {
            throw new RuntimeException('Entity not found.');
        }

        if (!$portal = PortalHelper::load($this->go1, $plan->instance_id)) {
            throw new RuntimeException('Portal not found.');
        }

        if (!$user = UserHelper::load($this->go1, $plan->user_id)) {
            throw new RuntimeException('User not found.');
        }

        if (!in_array($plan->status, [PlanStatuses::SCHEDULED, PlanStatuses::ASSIGNED])) {
            throw new RuntimeException('Un-support plan status: ' . $plan->status);
        }

        $enrolment = (PlanTypes::ENTITY_AWARD == $plan->entity_type)
            ? AwardHelper::loadEnrolmentBy($this->award, $plan->entity_id, $user->id, $plan->instance_id)
            : EnrolmentHelper::loadByLoProfileAndPortal($this->go1, $plan->entity_id, $user->profile_id, $plan->instance_id);

        return [$entity, $user, $enrolment ?: null, $portal];
    }

    public static function id($id)
    {
        return EnrolmentTypes::TYPE_PLAN_ASSIGNED . ":$id";
    }

    private function onCreate(stdClass $plan)
    {
        try {
            list($entity, $user, $enrolment) = $this->extractPlan($plan);

            return $enrolment
                ? $this->updateEnrolmentDueDate($plan, $entity, $enrolment)
                : $this->createVirtualEnrolment($plan, $user, $entity);
        } catch (ElasticsearchException $e) {
            $this->history->write(Schema::O_PLAN, $plan->id, $e->getCode(), $e->getMessage());
        }
    }

    private function onUpdate(stdClass $plan)
    {
        try {
            if ($plan->original->due_date != $plan->due_date) {
                list($entity, , $enrolment) = $this->extractPlan($plan);
                $this->updateEnrolmentDueDate($plan, $entity, $enrolment);
            }
        } catch (ElasticsearchException $e) {
            $this->history->write(Schema::O_PLAN, $plan->id, $e->getCode(), $e->getMessage());
        }
    }

    private function onDelete(stdClass $plan)
    {
        try {
            list($entity, , $enrolment) = $this->extractPlan($plan);

            return $enrolment
                ? $this->updateEnrolmentDueDate($plan, $entity, $enrolment, ['due_date' => null, 'is_assigned' => 0, 'assigned_date' => null])
                : $this->esClient->delete([
                    'index' => Schema::portalIndex($plan->instance_id),
                    'type'  => Schema::O_ENROLMENT,
                    'id'    => self::id($plan->id),
                ]);
        } catch (ElasticsearchException $e) {
            $this->history->write(Schema::O_PLAN, $plan->id, $e->getCode(), $e->getMessage());
        }
    }

    private function onRoCreate(stdClass $ro)
    {
        try {
            if ($plan = PlanHelper::load($this->go1, $ro->target_id)) {
                list($entity, , $enrolment) = $this->extractPlan($plan);
                $this->updateEnrolmentDueDate($plan, $entity, $enrolment);
            }
        } catch (ElasticsearchException $e) {
            $this->history->write(Schema::O_PLAN, $ro->target_id, $e->getCode(), $e->getMessage());
        }
    }

    private function onEnrolmentDelete(stdClass $enrolment)
    {
        // If plan's status is assigned, and user is not enrolled to the lo,
        // create a fake assigned enrolment.
        if ($user = UserHelper::loadByProfileId($this->go1, $enrolment->profile_id, $this->accountsName)) {
            if ($plan = PlanHelper::loadByEntityAndUser($this->go1, PlanTypes::ENTITY_LO, $enrolment->lo_id, $user->id)) {
                try {
                    list($entity, $user, $enrolment) = $this->extractPlan($plan);
                    $this->createVirtualEnrolment($plan, $user, $entity);
                } catch (ElasticsearchException $e) {
                    $this->history->write(Schema::O_ENROLMENT, $enrolment->id, $e->getCode(), $e->getMessage());
                }
            }
        }
    }

    private function onBulk(array $plans, string $indexName)
    {
        $params = ['body' => [], 'client' => ['headers' => ['uuid' => Uuid::uuid4()->toString()]]];
        foreach ($plans as $plan) {
            try {
                list($entity, $user, $enrolment) = $this->extractPlan($plan);
                if ($enrolment) {
                    continue;
                }

                if (PlanTypes::ENTITY_AWARD == $plan->entity_type) {
                    $params['body'][] = [
                        'index' => [
                            '_index'   => $indexName,
                            '_type'    => Schema::O_ENROLMENT,
                            '_id'      => EnrolmentTypes::TYPE_PLAN_ASSIGNED . ":{$plan->id}",
                            '_routing' => $plan->instance_id,
                        ],
                    ];
                    $params['body'][] = $this->virtualFormat($plan, $user, $entity);
                } else {
                    $params['body'][] = [
                        'index' => [
                            '_index'   => $indexName,
                            '_type'    => Schema::O_ENROLMENT,
                            '_id'      => self::id($plan->id),
                            '_routing' => $plan->instance_id,
                        ],
                    ];
                    $params['body'][] = $this->virtualFormat($plan, $user, $entity);
                }
            } catch (Exception $e) {
                $this->history->write(Schema::O_PLAN, $plan->id, $e->getCode(), $e->getMessage());
            }
        }
        if ($params['body']) {
            $response = $this->esClient->bulk($params);
            $this->history->bulkLog($response);
        }
    }

    private function virtualFormat(stdClass $plan, stdClass $user, stdClass $entity)
    {
        if (PlanTypes::ENTITY_LO == $plan->entity_type) {
            return $this->formatter->format((object) [
                'id'                => 0,
                'lo_id'             => $plan->entity_id,
                'profile_id'        => $user->profile_id,
                'status'            => PlanStatuses::S_ASSIGNED,
                'taken_instance_id' => $plan->instance_id,
                'due_date'          => !empty($plan->due_date) ? DateTime::formatDate($plan->due_date) : null,
                'assigned_date'     => !empty($plan->created_date) ? DateTime::formatDate($plan->created_date) : null,
                'parent_lo_id'      => null,
            ], EnrolmentTypes::TYPE_PLAN_ASSIGNED, $user);
        }

        return $this->awardEnrolmentFormatter->format((object) [
            'id'            => 0,
            'award_id'      => $entity->id,
            'status'        => PlanStatuses::ASSIGNED,
            'instance_id'   => $plan->instance_id,
            'due_date'      => !empty($plan->due_date) ? DateTime::formatDate($plan->due_date) : null,
            'assigned_date' => !empty($plan->created_date) ? DateTime::formatDate($plan->created_date) : null,
        ], EnrolmentTypes::TYPE_PLAN_ASSIGNED, $user);
    }

    private function createVirtualEnrolment(stdClass $plan, stdClass $user, stdClass $entity)
    {
        try {
            $this->esClient->create([
                'index'   => Schema::INDEX,
                'routing' => $plan->instance_id,
                'type'    => Schema::O_ENROLMENT,
                'id'      => self::id($plan->id),
                'refresh' => $this->waitForCompletion,
                'body'    => $this->virtualFormat($plan, $user, $entity),
            ]);
        } catch (ElasticsearchException $e) {
            $this->history->write(Schema::O_ENROLMENT, $plan->id, $e->getCode(), $e->getMessage());
        }
    }

    private function indices(stdClass $plan, stdClass $entity, stdClass $enrolment = null): array
    {
        switch ($plan->entity_type) {

            case PlanTypes::ENTITY_AWARD:
                return $enrolment
                    ? IndexHelper::awardEnrolmentIndices($enrolment, $entity)
                    : self::planIndices($plan);

            case PlanTypes::ENTITY_LO:
                return $enrolment
                    ? IndexHelper::enrolmentIndices($enrolment)
                    : self::planIndices($plan);
        }

        return [];
    }

    public static function planIndices(stdClass $plan)
    {
        $indices[] = Schema::portalIndex($plan->instance_id);

        return $indices;
    }

    private function updateEnrolmentDueDate(stdClass $plan, stdClass $entity, stdClass $enrolment = null, array $doc = [])
    {
        try {
            $enrolmentId = $enrolment ? $enrolment->id : self::id($plan->id);
            if ((PlanTypes::ENTITY_AWARD == $plan->entity_type) && $enrolment) {
                $enrolmentId = EnrolmentTypes::TYPE_AWARD . ":{$enrolment->id}";
            }

            if (!$doc) {
                $dueDate = $enrolment ? EnrolmentHelper::dueDate($this->go1, $enrolment->id) : false;
                $doc = [
                    'due_date'      => $dueDate ? $dueDate->format(DATE_ISO8601) : (!empty($plan->due_date) ? DateTime::formatDate($plan->due_date) : null),
                    'is_assigned'   => 1,
                    'assigned_date' => !empty($plan->created_date) ? DateTime::formatDate($plan->created_date) : null,
                ];
            }

            $this->repository->update([
                'type' => Schema::O_ENROLMENT,
                'id'   => $enrolmentId,
                'body' => ['doc' => $doc],
            ], $this->indices($plan, $entity, $enrolment));
        } catch (Exception $e) {
            $this->history->write(Schema::O_PLAN, $plan->id, $e->getCode(), $e->getMessage());
        }
    }
}

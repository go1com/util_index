<?php

namespace go1\util_index\core;

use Doctrine\DBAL\Connection;
use go1\util\award\AwardEnrolmentHelper;
use go1\util\award\AwardEnrolmentStatuses;
use go1\util\award\AwardHelper;
use go1\util\DateTime;
use go1\util\enrolment\EnrolmentStatuses;
use go1\util\enrolment\EnrolmentTypes;
use go1\util\plan\PlanHelper;
use go1\util\plan\PlanTypes;
use go1\util\portal\PortalHelper;
use go1\util\user\UserHelper;
use go1\util_index\IndexHelper;
use stdClass;

class AwardEnrolmentFormatter implements IndexFormatterInterface
{
    private $go1;
    private $award;
    private $loFormatter;
    private $userFormatter;

    public function __construct(Connection $go1, Connection $award, LoFormatter $loFormatter, UserFormatter $userFormatter)
    {
        $this->go1 = $go1;
        $this->award = $award;
        $this->loFormatter = $loFormatter;
        $this->userFormatter = $userFormatter;
    }

    public function format(stdClass $enrolment, string $type = EnrolmentTypes::TYPE_AWARD, stdClass $user = null)
    {
        $account = false;
        $user = $user ?? UserHelper::load($this->go1, $enrolment->user_id);
        $assessors = AwardEnrolmentHelper::assessorIds($this->go1, $enrolment->id);
        $hasAssessor = $assessors ? 1 : 0;
        $beginExpireField = null;

        if ($award = AwardHelper::load($this->award, $enrolment->award_id)) {
            $formattedLo = $this->loFormatter->formatAward($award, true);

            if ($user && ($plan = PlanHelper::loadByEntityAndUser($this->go1, PlanTypes::ENTITY_AWARD, $award->id, $user->id))) {
                $enrolment->due_date = $plan->due_date;
            }

            $beginExpireField = $award->expire ? (is_numeric($award->expire) ? 'start_date' : 'end_date') : null;
        }

        if ($user && ($account = $this->findAccount($enrolment, $user))) {
            $account = $this->userFormatter->format($account);
        }

        if (empty($assessors)) {
            $awardAssessors = AwardHelper::assessorIds($this->go1, $enrolment->award_id);
            (count($awardAssessors) == 1) && $assessors = $awardAssessors;
        }

        $lastStatus = AwardEnrolmentStatuses::toEsNumeric($enrolment->status);
        if (!empty($enrolment->original->status) && ($enrolment->status != $enrolment->original->status)) {
            $lastStatus = AwardEnrolmentStatuses::toEsNumeric($enrolment->original->status);
        } else {
            // load from revision
            $dbLastStatus = $this->award
                ->createQueryBuilder()
                ->select('status')
                ->from('award_enrolment_revision')
                ->where('award_enrolment_id = :enrolment_id')
                ->setParameter(':enrolment_id', $enrolment->id)
                ->andWhere('status <> :status')
                ->setParameter(':status', $enrolment->status)
                ->orderBy('created', 'desc')
                ->setMaxResults(1)
                ->execute()
                ->fetchColumn();

            if ($dbLastStatus) {
                $lastStatus = AwardEnrolmentStatuses::toEsNumeric($dbLastStatus);
            }
        }

        $doc = [
            'id'            => (int) $enrolment->id,
            'type'          => $type,
            'profile_id'    => $user->profile_id ?? 0,
            'lo_id'         => $enrolment->award_id,
            'parent_id'     => 0,
            'status'        => AwardEnrolmentStatuses::toEsNumeric($enrolment->status),
            'last_status'   => $lastStatus,
            'quantity'      => isset($enrolment->quantity) ? (float) $enrolment->quantity : 0,
            'result'        => 0,
            'pass'          => (AwardEnrolmentStatuses::COMPLETED == $enrolment->status) ? 1 : 0,
            'assessors'     => $assessors ?? [],
            'start_date'    => !empty($enrolment->start_date) ? DateTime::formatDate($enrolment->start_date) : null,
            'end_date'      => !empty($enrolment->end_date) ? DateTime::formatDate($enrolment->end_date) : null,
            'due_date'      => !empty($enrolment->due_date) ? DateTime::formatDate($enrolment->due_date) : null,
            'assigned_date' => $enrolment->assigned_date ?? null,
            'expire_date'   => !empty($enrolment->expire) ? DateTime::formatDate($enrolment->expire) : null,
            'begin_expire'  => $beginExpireField ? (!empty($enrolment->{$beginExpireField}) ? DateTime::formatDate($enrolment->{$beginExpireField}) : null) : null,
            'changed'       => DateTime::formatDate(!empty($enrolment->updated) ? $enrolment->updated : time()),
            'duration'      => 0,
            'lo'            => $formattedLo ?? null,
            'account'       => $account ?: null,
            'parent_lo'     => null,
            'assessor'      => IndexHelper::firstAssessor($this->go1, $assessors),
            'progress'      => $award ? $this->progress($award, $enrolment) : null,
            'is_assigned'   => ((isset($plan) && $plan) || property_exists($enrolment, 'due_date')) ? 1 : 0,
            'metadata'      => [
                'account_id'   => $account ? $account['id'] : null,
                'award_id'     => $award->id ?? 0,
                'status'       => (int) $award->published ?? 0,
                'user_id'      => $user ? $user->id : 0,
                'instance_id'  => (int) ($enrolment->routing ?? $enrolment->instance_id),
                'updated_at'   => time(),
                'has_assessor' => $hasAssessor,
            ],
        ];

        return $doc;
    }

    private function findAccount(stdClass $enrolment, stdClass $user)
    {
        if (empty($enrolment->instance_id)) {
            $award = AwardHelper::load($this->award, $enrolment->award_id);
            $award && $portal = PortalHelper::load($this->go1, $award->instance_id);
        } else {
            $portal = PortalHelper::load($this->go1, $enrolment->instance_id);
        }

        if (!empty($portal)) {
            if ($account = UserHelper::loadByEmail($this->go1, $portal->title, $user->mail)) {
                return $account;
            }
        }

        return false;
    }

    private function progress(stdClass $award, stdClass $enrolment)
    {
        $enrolmentQuantity = $enrolment->quantity ?? 0;
        $awardQuantity = $award->quantity ?? null;

        if (is_null($awardQuantity) || (0 == $enrolmentQuantity)) {
            return [EnrolmentStatuses::PERCENTAGE => 0];
        }

        $percentage = (0 == $awardQuantity)
            ? ($enrolmentQuantity > 0 ? 100 : 0)
            : round((100 * $enrolmentQuantity) / $awardQuantity);

        return [EnrolmentStatuses::PERCENTAGE => $percentage];
    }
}

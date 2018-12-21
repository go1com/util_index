<?php

namespace go1\util_index\core;

use DateTime as DefaultDateTime;
use Doctrine\DBAL\Connection;
use go1\core\group\group_schema\v1\repository\GroupAssignmentRepository;
use go1\core\group\group_schema\v1\repository\GroupMembershipRepository;
use go1\util\assignment\SubmissionHelper;
use go1\util\DateTime;
use go1\util\DB;
use go1\util\edge\EdgeTypes;
use go1\util\enrolment\CertificateTypes;
use go1\util\enrolment\EnrolmentHelper;
use go1\util\enrolment\EnrolmentStatuses;
use go1\util\enrolment\EnrolmentTypes;
use go1\util\lo\LiTypes;
use go1\util\lo\LoHelper;
use go1\util\lo\LoTypes;
use go1\util\model\Enrolment;
use go1\util\portal\PortalHelper;
use go1\util\quiz\AnswerHelper;
use go1\util\quiz\PersonHelper;
use go1\util\quiz\QuestionHelper;
use go1\util\quiz\QuizHelper;
use go1\util\quiz\ResultHelper;
use go1\util\user\UserHelper;
use go1\util_index\IndexHelper;
use stdClass;

class EnrolmentFormatter implements IndexFormatterInterface
{
    private $go1;
    private $assignment;
    private $quiz;
    private $accountsName;
    private $loFormatter;
    private $userFormatter;

    public function __construct(
        Connection $go1,
        ?Connection $assignment,
        ?Connection $quiz,
        string $accountsName,
        LoFormatter $loFormatter,
        UserFormatter $userFormatter
    ) {
        $this->go1 = $go1;
        $this->assignment = $assignment;
        $this->quiz = $quiz;
        $this->accountsName = $accountsName;
        $this->loFormatter = $loFormatter;
        $this->userFormatter = $userFormatter;
    }

    public function format(stdClass $enrolment, string $type = EnrolmentTypes::TYPE_ENROLMENT, stdClass $user = null)
    {
        $enrolmentModel = Enrolment::create($enrolment);
        $isAssigned = 0;
        $courseEnrolment = false;
        $account = false;
        $user = $user ?: UserHelper::loadByProfileId($this->go1, $enrolment->profile_id, $this->accountsName);
        $assessors = EnrolmentHelper::assessorIds($this->go1, $enrolment->id);
        $hasAssessor = $assessors ? 1 : 0;

        if ($user && ($account = $this->findAccount($enrolment, $user))) {
            $account = $this->userFormatter->format($account);
        }

        if ($lo = LoHelper::load($this->go1, $enrolment->lo_id)) {
            $formattedLo = $this->loFormatter->format($lo, true);
            $isCourse = LoTypes::COURSE == $lo->type;
            if ($isCourse) {
                $courseEnrolment = Enrolment::create($enrolment);
                $course = $lo;
            } else {
                $courseEnrolment = EnrolmentHelper::parentEnrolment($this->go1, $enrolmentModel);
                $course = $courseEnrolment ? $this->go1->executeQuery('SELECT id, published FROM gc_lo WHERE id = ?', [$courseEnrolment->loId])->fetch(DB::OBJ) : null;
            }

            # Find assessor from course enrolment.
            if (!$isCourse && $courseEnrolment && empty($assessors)) {
                $assessors = EnrolmentHelper::assessorIds($this->go1, $courseEnrolment->id);
                $hasAssessor = $assessors ? 1 : 0;
            }

            # Find default course assessor.
            if ($course && empty($assessors)) {
                $courseAssessors = LoHelper::assessorIds($this->go1, $course->id);
                (count($courseAssessors) == 1) && $assessors = $courseAssessors;
            }

            # Get due date
            if ($user && !property_exists($enrolment, 'due_date')) {
                if ($dueDate = EnrolmentHelper::dueDate($this->go1, $enrolment->id)) {
                    $enrolment->due_date = $dueDate->format(DATE_ISO8601);
                }
            }

            if ($user && !property_exists($enrolment, 'assigned_date')) {
                $enrolment->assigned_date = $this->assignedDate($enrolment);
            }

            $isAssigned = ((isset($plan) && $plan) || property_exists($enrolment, 'due_date')) ? 1 : 0;

            $this->submittedMarkedDates($enrolment, $lo);

            $progress = $this->progress($lo, $enrolmentModel);
        }

        $certificates = [];
        if (!empty($enrolment->data->custom_certificate)) {
            $certificates[] = [
                'type' => CertificateTypes::CUSTOM,
                'url'  => $enrolment->data->custom_certificate,
            ];
        }

        $status = EnrolmentStatuses::toNumeric($enrolment->status);

        # GO1P-18541 When I am viewing Explore page, selected `completed` status from `Enrollment status` filter block
        # then I see the latest Completed status of enrolment for course A
        $lastStatus = $this->getEnrolmentLastStatus($enrolment);
        $lastStatus = $lastStatus ? EnrolmentStatuses::toNumeric($lastStatus) : $status;

        $doc = [
            'id'                  => (int) $enrolment->id,
            'parent_enrolment_id' => (int) ($enrolment->parent_enrolment_id ?? 0),
            'type'                => $type,
            'profile_id'          => $enrolment->profile_id,
            'lo_id'               => $enrolment->lo_id,
            'parent_id'           => $enrolment->parent_lo_id ?? 0,
            'status'              => $status,
            'last_status'         => $lastStatus,
            'result'              => isset($enrolment->result) ? (int) $enrolment->result : 0,
            'pass'                => isset($enrolment->pass) ? (int) $enrolment->pass : 0,
            'assessors'           => $assessors ?? [],
            'start_date'          => !empty($enrolment->start_date) ? DateTime::formatDate($enrolment->start_date) : null,
            'end_date'            => !empty($enrolment->end_date) ? DateTime::formatDate($enrolment->end_date) : null,
            'due_date'            => !empty($enrolment->due_date) ? DateTime::formatDate($enrolment->due_date) : null,
            'submitted_date'      => !empty($enrolment->submitted_date) ? DateTime::formatDate($enrolment->submitted_date) : null,
            'marked_date'         => !empty($enrolment->marked_date) ? DateTime::formatDate($enrolment->marked_date) : null,
            'assigned_date'       => $enrolment->assigned_date ?? null,
            'changed'             => DateTime::formatDate(!empty($enrolment->changed) ? $enrolment->changed : time()),
            'created'             => !empty($enrolment->timestamp) ? DateTime::formatDate($enrolment->timestamp) : null,
            'duration'            => 0,
            'lo'                  => $formattedLo ?: null,
            'account'             => $account ?: null,
            'parent_lo'           => isset($enrolment->parent_lo_id) ? $this->parentLo($enrolment->parent_lo_id) : null,
            'assessor'            => $isCourse ? $this->assessor($assessors) : null,
            'is_assigned'         => $isAssigned,
            'progress'            => $progress,
            'certificates'        => $certificates,
            'metadata'            => [
                'account_id'          => $account ? $account['id'] : null,
                'course_enrolment_id' => intval($courseEnrolment->id ?? 0),
                'course_id'           => intval($courseEnrolment->loId ?? 0),
                'status'              => $course ? ((int) $course->published ?? 0) : 0,
                'has_assessor'        => $hasAssessor,
                'user_id'             => $user ? $user->id : 0,
                'instance_id'         => (int) ($enrolment->routing ?? $enrolment->taken_instance_id ?? $enrolment->instance_id),
                'updated_at'          => time(),
                'event_details'       => $lo ? $lo->title : '',
            ],
        ];

        return $doc;
    }

    private function findAccount(stdClass $enrolment, stdClass $user)
    {
        if (empty($enrolment->taken_instance_id)) {
            $lo = LoHelper::load($this->go1, $enrolment->lo_id);
            $lo && $portal = PortalHelper::load($this->go1, $lo->instance_id);
        } else {
            $portal = PortalHelper::load($this->go1, $enrolment->taken_instance_id);
        }

        if ($portal) {
            if ($account = UserHelper::loadByEmail($this->go1, $portal->title, $user->mail)) {
                return $account;
            }
        }

        return false;
    }

    private function parentLo(int $parentLoId)
    {
        return $this->go1
            ->executeQuery('SELECT id, type, title FROM gc_lo WHERE id = ?', [$parentLoId])
            ->fetch(DB::OBJ) ?: null;
    }

    private function progress(stdClass $lo, Enrolment $enrolment)
    {
        $progress = null;

        switch ($lo->type) {

            case LoTypes::COURSE:
                $progress = EnrolmentHelper::childrenProgressCount($this->go1, $enrolment, true, LiTypes::all());
                $progress = array_diff_key($progress, ['total' => 1]);
                break;

            case LiTypes::QUIZ:
                $quiz = QuizHelper::loadByLiId($this->quiz, $lo->id);
                $progress = $quiz ? QuizHelper::progress($this->quiz, $quiz, $enrolment->id) : null;
                break;

            default:
                $progress = [EnrolmentStatuses::PERCENTAGE => (EnrolmentStatuses::COMPLETED == $enrolment->status) ? 100 : 0];
                break;
        }

        if ($progress && (EnrolmentStatuses::COMPLETED == $enrolment->status)) {
            $progress[EnrolmentStatuses::PERCENTAGE] = 100;
        }

        return $progress;
    }

    private function submittedMarkedDates(stdClass &$enrolment, $lo)
    {
        if ($lo->type === LiTypes::ASSIGNMENT) {
            if ($submission = SubmissionHelper::loadByEnrolmentId($this->assignment, $enrolment->id)) {
                $enrolment->submitted_date = SubmissionHelper::getSubmittedDate($this->assignment, $submission->id);
                $enrolment->marked_date = SubmissionHelper::getMarkedDate($this->assignment, $submission->id);
            }
        } elseif (in_array($lo->type, [LiTypes::QUESTION, LiTypes::QUIZ])) {
            if ($person = PersonHelper::loadByExternalId($this->quiz, $enrolment->profile_id)) {
                if ($lo->type === LiTypes::QUESTION && $question = QuestionHelper::load($this->quiz, $lo->id)) {
                    // Note: question is in newest revision.
                    if ($answer = AnswerHelper::loadByQuestionRuuid($this->quiz, $person->person_id, $question->ruuid)) {
                        $enrolment->submitted_date = round($answer->answer_timestamp / 1000);
                        // Note: text answer question can't be stand-alone.
                        $enrolment->marked_date = $enrolment->submitted_date;
                    }
                }
                if ($lo->type === LiTypes::QUIZ) {
                    if ($quiz = QuizHelper::loadByLiId($this->quiz, $lo->id)) {
                        // Note: quiz is in newest revision.
                        $enrolment->submitted_date = ResultHelper::getSubmittedDate($this->quiz, $person->person_id, $quiz->ruuid);
                        $enrolment->marked_date = ResultHelper::getMarkedDate($this->quiz, $person->person_id, $quiz->ruuid);
                    }
                }
            }
        } elseif (in_array($lo->type, LiTypes::all())) {
            // For other learning items than assignment, quiz, question.
            $enrolment->submitted_date = $enrolment->end_date ?? null;
            $enrolment->marked_date = $enrolment->submitted_date ?? null;
        }
    }

    private function assignedDate(stdClass $enrolment)
    {
        $planIds = $this->go1->createQueryBuilder()
                             ->select('target_Id')
                             ->from('gc_ro')
                             ->where('type = :type')
                             ->andWhere('source_id = :enrolmentId')
                             ->setParameter(':type', EdgeTypes::HAS_PLAN)
                             ->setParameter(':enrolmentId', $enrolment->id)
                             ->execute()
                             ->fetchAll(DB::COL);

        if ($planIds) {
            $createdDate = $this->go1->createQueryBuilder()
                                     ->select('created_date')
                                     ->from('gc_plan')
                                     ->where('gc_plan.id IN (:ids)')
                                     ->setParameter(':ids', $planIds, DB::INTEGERS)
                                     ->orderBy('gc_plan.type', 'desc')
                                     ->addOrderBy('gc_plan.id', 'desc')
                                     ->setMaxResults(1)
                                     ->execute()
                                     ->fetch(DB::COL);

            return $createdDate ? DateTime::formatDate($createdDate) : null;
        }

        return null;
    }

    public function assessor(array $assessors)
    {
        return IndexHelper::firstAssessor($this->go1, $assessors);
    }

    private function getEnrolmentLastStatus(stdClass $enrolment): string
    {
        return $this->go1->fetchColumn(
            'SELECT status FROM gc_enrolment_revision WHERE profile_id = ? AND lo_id = ? AND taken_instance_id = ? ORDER BY id DESC LIMIT 1',
            [$enrolment->profile_id, $enrolment->lo_id, $enrolment->taken_instance_id]
        );
    }
}

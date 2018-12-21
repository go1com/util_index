<?php

namespace go1\util_index\core;

use Doctrine\DBAL\Connection;
use go1\util\award\AwardHelper;
use go1\util\DateTime;
use go1\util\DB;
use go1\util\edge\EdgeHelper;
use go1\util\edge\EdgeTypes;
use go1\util\enrolment\EnrolmentAllowTypes;
use go1\util\EntityTypes;
use go1\util\group\GroupHelper;
use go1\util\group\GroupItemStatus;
use go1\util\lo\LiTypes;
use go1\util\lo\LoChecker;
use go1\util\lo\LoHelper;
use go1\util\lo\LoTypes;
use go1\util\lo\TagTypes;
use go1\util\location\Location;
use go1\util\location\LocationRepository;
use go1\util\policy\PolicyHelper;
use go1\util\portal\PortalChecker;
use go1\util\portal\PortalHelper;
use go1\util\quiz\QuizHelper;
use go1\util\Text;
use go1\util\user\UserHelper;
use go1\util\vote\VoteHelper;
use go1\util\vote\VoteTypes;
use stdClass;

class LoFormatter implements IndexFormatterInterface
{
    private $go1;
    private $social;
    private $award;
    private $vote;
    private $quiz;
    private $policy;
    private $collection;
    private $accountsName;
    private $fUser;
    private $fEvent;
    private $portalChecker;
    private $rLocation;

    public function __construct(
        Connection $go1,
        ?Connection $social,
        ?Connection $award,
        ?Connection $vote,
        ?Connection $quiz,
        ?Connection $policy,
        ?Connection $collection,
        string $accountsName,
        UserFormatter $fUser,
        ?EventFormatter $fEvent,
        PortalChecker $portalChecker,
        LocationRepository $rLocation
    ) {
        $this->go1 = $go1;
        $this->social = $social;
        $this->award = $award;
        $this->vote = $vote;
        $this->quiz = $quiz;
        $this->policy = $policy;
        $this->collection = $collection;
        $this->accountsName = $accountsName;
        $this->fUser = $fUser;
        $this->fEvent = $fEvent;
        $this->portalChecker = $portalChecker;
        $this->rLocation = $rLocation;
    }

    public function format(stdClass $lo, $teaser = false)
    {
        $lo->data = isset($lo->data) ? (is_scalar($lo->data) ? json_decode($lo->data) : $lo->data) : new stdClass;
        $portal = PortalHelper::load($this->go1, $lo->instance_id);
        $oldTags = $this->processTags($lo->tags ?? []);
        $newTags = $this->processTags($this->getLoTags($lo->id, $lo->instance_id));
        $tags = array_values(array_unique(array_merge($newTags, $oldTags)));

        // Code clone from https://code.go1.com.au/microservices/algolia/blob/master/domain/LoRepository.php#L86
        if (!is_null($this->vote)) {
            $voteInfo = null;
            $vote = VoteHelper::getEntityVote($this->vote, VoteTypes::ENTITY_TYPE_LO, (int) $lo->id);
            if ($vote) {
                $numLike = isset($vote->data['like']) ? $vote->data['like'] : 0;
                $numDislike = isset($vote->data['dislike']) ? $vote->data['dislike'] : 0;
                $voteInfo = [
                    'percent' => (int) $vote->percent,
                    'rank'    => $vote->percent * $numLike,
                    'like'    => $numLike,
                    'dislike' => $numDislike,
                ];
            }
        }

        $image = $lo->data->image ?? $lo->data->image_url ?? $lo->image ?? '';
        $doc = [
            'id'            => (int) $lo->id,
            'type'          => $lo->type,
            'origin_id'     => $lo->origin_id,
            'remote_id'     => $lo->remote_id ?? 0,
            'status'        => isset($lo->status) ? $lo->status : 0,
            'private'       => isset($lo->private) ? (int) $lo->private : 1,
            // 1: published, 0: unpublished, -1: archived.
            'published'     => isset($lo->published) ? (int) $lo->published : 0,
            'marketplace'   => isset($lo->marketplace) ? (int) $lo->marketplace : 0,
            'sharing'       => isset($lo->sharing) ? $lo->sharing : 0,
            'language'      => $lo->language,
            'instance_id'   => $lo->instance_id,
            'locale'        => $lo->locale ? (is_scalar($lo->locale) ? Text::parseInlineTags($lo->locale) : $lo->locale) : [],
            'title'         => $lo->title,
            'description'   => $lo->description,
            'tags'          => $tags,
            'image'         => is_string($image) ? str_replace('public://', "https://{$this->accountsName}/files/", $image) : '',
            'pricing'       => [
                'currency'     => isset($lo->pricing->currency) ? $lo->pricing->currency : 'USD',
                'price'        => isset($lo->pricing->price) ? $lo->pricing->price : 0.00,
                'tax'          => isset($lo->pricing->tax) ? $lo->pricing->tax : 0.00,
                'tax_included' => isset($lo->pricing->tax_included) ? (int) $lo->pricing->tax_included : 1,
                'tax_display'  => isset($lo->pricing->tax_display) ? (int) $lo->pricing->tax_display : 1,
                'total'        => isset($lo->pricing) ? (empty($lo->pricing->tax_included) ? ($lo->pricing->price + (($lo->pricing->price * $lo->pricing->tax) / 100)) : $lo->pricing->price) : 0.00,
            ],
            'duration'      => intval($lo->data->duration ?? 0),
            'created'       => DateTime::formatDate((!empty($lo->created) ? $lo->created : time())),
            'updated'       => DateTime::formatDate((!empty($lo->updated) ? $lo->updated : time())),
            'vote'          => $voteInfo ?? [],
            'items_count'   => $this->itemCount($lo),
            'data'          => [
                LoHelper::ASSIGNMENT_ALLOW_RESUBMIT => intval($lo->data->allow_resubmit ?? 0),
                'allow_reenrol'                     => (new LoChecker)->allowReEnrol($lo) ? 1 : 0,
                LoHelper::PASS_RATE                 => $lo->data->pass_rate ?? null,
                'url'                               => $lo->data->path ?? null,
                'single_li'                         => LoHelper::isSingleLi($lo) ? 1 : 0,
            ],
            'collection_id' => is_null($this->collection) ? 0 : $this->getCollectionIdsByPortalId($lo->id, (int) ($lo->routing ?? $lo->instance_id)),
        ];

        if (!$teaser) {
            $parentIds = array_values(LoHelper::parentIds($this->go1, $lo->id));
            $doc += [
                'portal_name'     => $portal ? $this->portalChecker->getSiteName($portal) : '',
                'assessors'       => LoHelper::assessorIds($this->go1, $lo->id),
                'authors'         => $this->getAuthors($this->go1, $lo->id),
                'allow_enrolment' => EnrolmentAllowTypes::toNumeric($lo->data->{LoHelper::ENROLMENT_ALLOW} ?? EnrolmentAllowTypes::DEFAULT),
                'totalEnrolment'  => LoHelper::countEnrolment($this->go1, $lo->id),
                'locations'       => $this->getLocation($this->go1, $lo->id),
                'metadata'        => [
                    'parents_authors_ids' => LoHelper::parentsAuthorIds($this->go1, $lo->id, $parentIds),
                    'parents_id'          => $parentIds,
                    'instance_id'         => $portalId = intval($lo->routing ?? $lo->instance_id),
                    'updated_at'          => time(),
                    'shared'              => $lo->shared ?? 0,
                    'realm'               => ($this->policy && ($lo->instance_id == $portalId))
                        ? PolicyHelper::entityRealmOnLO($this->policy, EntityTypes::PORTAL, $portalId, $portalId, $lo->id)
                        : null,
                ],
            ];

            if (LiTypes::EVENT == $lo->type) {
                $doc['event'] = $this->fEvent->format($lo, null, true);
            }
        }

        return $doc;
    }

    private function getAuthors(Connection $db, int $loId)
    {
        $formattedAuthors = [];
        if ($authorIds = LoChecker::authorIds($db, $loId)) {
            $authors = UserHelper::loadMultiple($db, $authorIds);
            foreach ($authors as $author) {
                $formattedAuthors[] = $this->fUser->format($author, true);
            }
        }

        return $formattedAuthors;
    }

    public function groupIds(int $loId, $premiumOnly = true): array
    {
        if (is_null($this->social)) {
            return [];
        }

        $q = $this->social->createQueryBuilder();
        $q = $q->select('DISTINCT _group.id, _group.data')
               ->from('social_group_item', 'item')
               ->innerJoin('item', 'social_group', '_group', '_group.id = item.group_id')
               ->where('item.entity_type = ?')
               ->andWhere('item.entity_id = ?')
               ->andWhere('item.status = ?')
               ->setParameters(['lo', $loId, GroupItemStatus::ACTIVE], [DB::STRING, DB::INTEGER, DB::INTEGER])
               ->execute();

        $premiumGroupIds = [];
        $defaultGroupIds = [];
        while ($group = $q->fetch(DB::OBJ)) {
            $group->data = is_scalar($group->data) ? json_decode($group->data) : json_decode(json_encode($group->data, JSON_FORCE_OBJECT));

            GroupHelper::isPremium($group)
                ? $premiumGroupIds[] = (int) $group->id
                : $defaultGroupIds[] = (int) $group->id;
        }

        return $premiumOnly ? $premiumGroupIds : $defaultGroupIds;
    }

    private function getLocation(Connection $db, int $entityId, $isLo = true)
    {
        $edgeType = $isLo ? EdgeTypes::HAS_LO_LOCATION : EdgeTypes::HAS_AWARD_LOCATION;
        if ($edges = EdgeHelper::edgesFromSource($db, $entityId, [$edgeType])) {
            foreach ($edges as $edge) {
                $location = $this->rLocation->load($edge->target_id);
                if ($location instanceof Location) {
                    $locations[] = [
                        'id'           => $location->id,
                        'country'      => $location->country,
                        'locality'     => $location->locality,
                        'thoroughfare' => $location->thoroughfare,
                    ];
                }
            }
        }

        return $locations ?? [];
    }

    public function formatAward(stdClass $award, $teaser = false)
    {
        $portal = PortalHelper::load($this->go1, $award->instance_id);
        $tags = $this->processTags($award->tags ?? []);
        $data = is_scalar($award->data) ? json_decode($award->data) : $award->data;
        $image = $data->image ?? '';

        $doc = [
            'id'          => (int) $award->id,
            'type'        => LoTypes::AWARD,
            'origin_id'   => 0,
            'remote_id'   => 0,
            'status'      => 0,
            'private'     => 0,
            // 1: published, 0: unpublished, -1: archived.
            'published'   => isset($award->published) ? (int) $award->published : 0,
            'marketplace' => isset($award->marketplace) ? (int) $award->marketplace : 0,
            'sharing'     => 0,
            'language'    => '',
            'instance_id' => $award->instance_id,
            'locale'      => $award->locale ? (is_scalar($award->locale) ? Text::parseInlineTags($award->locale) : $award->locale) : [],
            'title'       => $award->title,
            'description' => $award->description,
            'tags'        => $tags,
            'image'       => $image ?? '',
            'pricing'     => [
                'currency'     => 'USD',
                'price'        => 0.00,
                'tax'          => 0.00,
                'tax_included' => 1,
                'tax_display'  => 1,
                'total'        => 0.00,
            ],
            'quantity'    => $award->quantity,
            'duration'    => 0,
            'created'     => DateTime::formatDate((!empty($award->created) ? $award->created : time())),
            'data'        => ['label' => $data->label ?? ''],
            'assessors'   => AwardHelper::assessorIds($this->go1, $award->id),
        ];

        if (!$teaser) {
            $doc += [
                'portal_name'    => $portal ? $this->portalChecker->getSiteName($portal) : '',
                'authors'        => $this->getAwardAuthors($award),
                'totalEnrolment' => AwardHelper::countEnrolment($this->award, $award->id),
                'locations'      => $this->getLocation($this->go1, $award->id, false),
                'items_count'    => isset($award->revision_id) ? $this->awardItemsCount($award->revision_id) : 0,
                'metadata'       => [
                    'instance_id' => (int) ($award->routing ?? $award->instance_id),
                    'updated_at'  => time(),
                ],
            ];
        }

        return $doc;
    }

    public function formatAwardManual(stdClass $award, stdClass $manualItem)
    {
        $categories = $this->processTags($manualItem->categories ?? []);

        $doc = [
            'id'          => (int) $manualItem->id,
            'type'        => $manualItem->type,
            'origin_id'   => 0,
            'remote_id'   => 0,
            'status'      => 0,
            'private'     => 0,
            'published'   => isset($manualItem->published) ? (int) $manualItem->published : 0,
            'marketplace' => 0,
            'sharing'     => 0,
            'language'    => '',
            'instance_id' => $award->instance_id,
            'locale'      => $award->locale ? (is_scalar($award->locale) ? Text::parseInlineTags($award->locale) : $award->locale) : [],
            'title'       => $manualItem->title,
            'description' => $manualItem->description,
            'tags'        => $categories,
            'image'       => '',
            'pricing'     => [
                'currency'     => 'USD',
                'price'        => 0.00,
                'tax'          => 0.00,
                'tax_included' => 1,
                'tax_display'  => 1,
                'total'        => 0.00,
            ],
            'quantity'    => (float) $manualItem->quantity,
            'duration'    => 0,
            'created'     => DateTime::formatDate((!empty($manualItem->completion_date) ? $manualItem->completion_date : time())),
        ];

        return $doc;
    }

    public function processTags($tags): array
    {
        $processedTags = is_scalar($tags) ? Text::parseInlineTags($tags) : $tags;
        $processedTags = (array) $processedTags;

        $returnTags = [];
        foreach ($processedTags as $processedTag) {
            $returnTags[] = html_entity_decode($processedTag);
        }

        return $returnTags;
    }

    private function awardItemsCount(int $awardRevisionId)
    {
        return $this->award->fetchColumn('SELECT count(*) FROM award_item WHERE award_revision_id = ?', [$awardRevisionId]);
    }

    private function getAwardAuthors(stdClass $award)
    {
        $author = UserHelper::load($this->go1, $award->user_id);

        return $author ? [$this->fUser->format($author, true)] : [];
    }

    private function itemCount($lo)
    {
        switch ($lo->type) {
            case LoTypes::COURSE:
                return LoHelper::countChild($this->go1, $lo->id);

            case LiTypes::QUIZ:
                if ($this->quiz) {
                    $quiz = QuizHelper::load($this->quiz, $lo->remote_id);
                    return $quiz ? QuizHelper::questionCount($this->quiz, $quiz) : null;
                }
                return null;

            case LoTypes::AWARD:
                return $lo->quantity ?? null;
        }

        return null;
    }

    private function getCollectionIdsByPortalId(int $loId, int $portalId): array
    {
        $collectionIds = $this->collection
            ->executeQuery("SELECT id FROM collection_collection WHERE portal_id=? LIMIT 50", [$portalId], [DB::INTEGER])
            ->fetchAll(DB::COL);

        if (!$collectionIds) {
            return [];
        }

        $ids = $this->collection
            ->executeQuery("SELECT collection_id FROM collection_collection_item WHERE lo_id = ? AND collection_id IN(?)", [$loId, $collectionIds], [DB::INTEGER, DB::INTEGERS])
            ->fetchAll(DB::COL);

        return array_map(function ($id) {
            return (int) $id;
        }, $ids);
    }

    private function getLoTags(int $loId, int $portalId, int $type = TagTypes::I_LOCAL): array
    {
        $query = 'SELECT title FROM gc_tags WHERE lo_id = ? AND instance_id = ? AND type = ? LIMIT 200';

        return $this
            ->go1
            ->executeQuery($query, [$loId, $portalId, $type])
            ->fetchAll(DB::COL);
    }
}

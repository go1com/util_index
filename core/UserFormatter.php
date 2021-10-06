<?php

namespace go1\util_index\core;

use Doctrine\DBAL\Connection;
use go1\core\group\group_schema\v1\constant\GroupMembershipMode;
use go1\core\util\client\federation_api\v1\RoleMapper;
use go1\core\util\client\federation_api\v1\schema\object\PortalAccount;
use go1\core\util\client\federation_api\v1\schema\object\User;
use go1\util\customer\CustomerEsSchema;
use go1\util\DateTime;
use go1\util\DB;
use go1\util\eck\EckHelper;
use go1\util\group\GroupHelper;
use go1\util\portal\PortalHelper;
use go1\util\user\ManagerHelper;
use go1\util\user\UserHelper;
use stdClass;

class UserFormatter
{
    private $go1;
    private $group;
    private $eck;
    private $accountsName;
    private $eckDataFormatter;
    private $userHelper;

    public function __construct(
        Connection $go1,
        ?Connection $group,
        ?Connection $eck,
        string $accountsName,
        AccountFieldFormatter $eckDataFormatter
    ) {
        $this->go1 = $go1;
        $this->group = $group;
        $this->eck = $eck;
        $this->accountsName = $accountsName;
        $this->eckDataFormatter = $eckDataFormatter;
        $this->userHelper = new UserHelper;
    }

    public function formatManagers(int $accountId)
    {
        return array_map('intval', ManagerHelper::userManagerIds($this->go1, $accountId));
    }

    public function formatPortalAccount(PortalAccount $account, $teaser = false)
    {
        $portalName = $this->go1->fetchColumn('SELECT instance FROM gc_user WHERE id = ?', [$account->legacyId]);

        $createdAt = ($account->createdAt && $account->createdAt->getTimestamp()) ? $account->createdAt->getTimestamp() : time();
        $timestamp = ($account->timestamp && $account->timestamp->getTimestamp()) ? $account->timestamp->getTimestamp() : time();
        $lastAccessedAt = ($account->lastAccessedAt && $account->lastAccessedAt->getTimestamp()) ? $account->lastAccessedAt->getTimestamp() : time();
        $lastLoggedInAt = ($account->lastLoggedInAt && $account->lastLoggedInAt->getTimestamp()) ? $account->lastLoggedInAt->getTimestamp() : null;

        $doc = [
            'id'           => $account->legacyId,
            'profile_id'   => $account->profileId,
            'mail'         => $account->user->email,
            'name'         => trim("{$account->firstName} {$account->lastName}"),
            'first_name'   => $account->firstName,
            'last_name'    => $account->lastName,
            'created'      => $createdAt ? DateTime::formatDate($createdAt) : null,
            'timestamp'    => $timestamp ? DateTime::formatDate($timestamp) : null,
            'login'        => $lastLoggedInAt ? DateTime::formatDate($lastLoggedInAt) : null,
            'access'       => $lastAccessedAt ? DateTime::formatDate($lastAccessedAt) : null,
            'status'       => ('ACTIVE' === $account->status) ? 1 : 0,
            'allow_public' => $account->user->allowPublic ? 1 : 0,
            'avatar'       => $account->avatarUri,
            'instance'     => $portalName,
        ];

        $entity = EckHelper::load($this->eck, $portalName, CustomerEsSchema::O_ACCOUNT, $account->legacyId);
        $doc += $this->eckDataFormatter->format(json_decode(json_encode($entity)));

        if (!$teaser) {
            $doc += [
                'roles'    => array_map(fn($role) => RoleMapper::fromEnumSymbol($role->name), $account->roles ?? []),
                'managers' => [],
            ];

            $portalName = $this->go1->fetchColumn('SELECT instance FROM gc_accounts WHERE id = ?', [$account->legacyId]);

            if ($this->accountsName !== $portalName) {
                $portalId = PortalHelper::idFromName($this->go1, $portalName);
                if ($this->group) {
                    if ($this->group) {
                        [$groupIds, $groups] = $this->fetchGroupData($portalId, $account->legacyId);
                        $doc['groupIds'] = $groupIds;
                        $doc['groups'] = $groups;
                    }
                }

                $doc['managers'] = $this->formatManagers($account->legacyId);
                $doc['metadata'] = [
                    'instance_id' => $portalId,
                    'updated_at'  => time(),
                    'user_id'     => $account->user->legacyId,
                ];
            }
        }

        return $doc;
    }

    public function formatUser(User $user, $teaser = false)
    {
        $doc = [
            'id'           => (int) $user->id,
            'profile_id'   => $user->profileId,
            'mail'         => $user->email,
            'name'         => trim("{$user->firstName} {$user->lastName}"),
            'first_name'   => $user->firstName,
            'last_name'    => $user->lastName,
            'created'      => DateTime::formatDate(!empty($user->createdAt) ? $user->createdAt->getTimestamp() : time()),
            'timestamp'    => DateTime::formatDate($user->timestamp ? $user->timestamp->getTimestamp() : time()),
            'login'        => !$user->lastLoggedInAt ? null : DateTime::formatDate($user->lastLoggedInAt->getTimestamp()),
            'access'       => DateTime::formatDate(!$user->lastAccessedAt ? time() : $user->lastAccessedAt->getTimestamp()),
            'status'       => ('ACTIVE' === $user->status) ? 1 : 0,
            'allow_public' => $user->allowPublic ? 1 : 0,
            'avatar'       => $user->avatarUri,
        ];

        if (!$teaser) {
            $doc += [
                'roles'    => array_map(fn($role) => RoleMapper::fromEnumSymbol($role->name), $user->roles ?? []),
                'managers' => [],
            ];
        }

        return $doc;
    }

    /**
     * @param stdClass $user
     * @param false    $teaser
     * @return array
     * @throws \Doctrine\DBAL\DBALException
     * @deprecated Use ::formatUser() or ::formatPortalAccount().
     *
     */
    public function format(stdClass $user, $teaser = false)
    {
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

        $doc = [
            'id'           => (int) $user->id,
            'profile_id'   => $user->profile_id,
            'mail'         => $user->mail,
            'name'         => $fullName,
            'first_name'   => isset($user->first_name) ? $user->first_name : '',
            'last_name'    => isset($user->last_name) ? $user->last_name : '',
            'created'      => DateTime::formatDate(!empty($user->created) ? $user->created : time()),
            'timestamp'    => DateTime::formatDate(!empty($user->timestamp) ? $user->timestamp : time()),
            'login'        => !empty($user->login) ? DateTime::formatDate($user->login) : null,
            'access'       => DateTime::formatDate(!empty($user->access) ? $user->access : time()),
            'status'       => isset($user->status) ? (int) $user->status : 1,
            'allow_public' => isset($user->allow_public) ? (int) $user->allow_public : 0,
            'avatar'       => isset($user->avatar) ? $user->avatar : (isset($user->data->avatar->uri) ? $user->data->avatar->uri : null),
        ];

        if ($this->accountsName !== $user->instance) {
            $doc['instance'] = $user->instance;
            $entity = EckHelper::load($this->eck, $user->instance, CustomerEsSchema::O_ACCOUNT, $user->id);
            $doc += $this->eckDataFormatter->format(json_decode(json_encode($entity)));
        }

        if (!$teaser) {
            $doc += [
                'roles'    => $this->userHelper->userRoles($this->go1, $user->id, $user->instance),
                'managers' => [],
            ];

            if ($this->accountsName !== $user->instance) {
                $portalId = PortalHelper::idFromName($this->go1, $user->instance);

                if ($this->group) {
                    [$groupIds, $groups] = $this->fetchGroupData($portalId, $user->id);
                    $doc['groupIds'] = $groupIds;
                    $doc['groups'] = $groups;
                }

                $doc['managers'] = $this->formatManagers($user->id);
                $doc['metadata'] = [
                    'instance_id' => $portalId,
                    'updated_at'  => time(),
                    'user_id'     => $this->go1->fetchColumn(
                        'SELECT id FROM gc_user WHERE instance = ? AND mail = ?',
                        [$this->accountsName, $user->mail]
                    ) ?: null,
                ];
            }
        }

        return $doc;
    }

    private function fetchGroupData(int $portalId, int $userId): array
    {
        $memberships = $this->group
            ->executeQuery(
                'SELECT group_id AS groupId FROM group_membership WHERE portal_id = ? AND status = 1 AND user_id IN (?)',
                [$portalId, [$userId]],
                [DB::INTEGER, DB::INTEGERS]
            )->fetchAll();

        $groupIds = array_map('intval', array_column($memberships, 'groupId'));
        $groups   = empty($doc['groupIds']) ? [] : GroupHelper::userGroupTitles($this->group, $portalId, $userId, GroupMembershipMode::MEMBERS);

        return [$groupIds, $groups];
    }
}

<?php

namespace go1\util_index\core;

use Doctrine\DBAL\Connection;
use go1\util\DateTime;
use go1\util\eck\EckHelper;
use go1\util\es\Schema;
use go1\util\group\GroupHelper;
use go1\util\group\GroupStatus;
use go1\util\portal\PortalHelper;
use go1\util\user\ManagerHelper;
use go1\util\user\UserHelper;
use stdClass;

class UserFormatter implements IndexFormatterInterface
{
    private $go1;
    private $social;
    private $eck;
    private $accountsName;
    private $eckDataFormatter;
    private $userHelper;

    public function __construct(
        Connection $go1,
        ?Connection $social,
        ?Connection $eck,
        string $accountsName,
        AccountFieldFormatter $eckDataFormatter
    ) {
        $this->go1 = $go1;
        $this->social = $social;
        $this->eck = $eck;
        $this->accountsName = $accountsName;
        $this->eckDataFormatter = $eckDataFormatter;
        $this->userHelper = new UserHelper;
    }

    public function formatManagers(int $accountId)
    {
        return ManagerHelper::userManagerIds($this->go1, $accountId);
    }

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
            $entity = EckHelper::load($this->eck, $user->instance, Schema::O_ACCOUNT, $user->id);
            $doc += $this->eckDataFormatter->format(json_decode(json_encode($entity)));
        }

        if (!$teaser) {
            $doc += [
                'roles'    => $this->userHelper->userRoles($this->go1, $user->id, $user->instance),
                'groups'   => [],
                'managers' => [],
            ];

            if ($this->accountsName !== $user->instance) {
                $portalId = PortalHelper::idFromName($this->go1, $user->instance);

                if ($this->social) {
                    $doc['groups'] = GroupHelper::userGroups($this->go1, $this->social, $portalId, $user->id, $this->accountsName, GroupStatus::ARCHIVED);
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
}

<?php

namespace go1\util_index\core;

use Doctrine\DBAL\Connection;
use go1\util\eck\EckHelper;
use stdClass;

class AccountFieldFormatter
{
    private $go1;
    private $eck;

    public function __construct(Connection $go1, Connection $eck)
    {
        $this->go1 = $go1;
        $this->eck = $eck;
    }

    public function format(stdClass $entity)
    {
        $portalId = $this->go1->fetchColumn('SELECT id FROM gc_instance WHERE title = ?', [$entity->instance]);
        if (!$portalId) {
            return [];
        }

        $metadata = EckHelper::metadata($this->eck, $entity->instance, $entity->entity_type, true, false);
        $fields = [];
        foreach ($entity as $fieldName => $values) {
            if (in_array($fieldName, ['instance', 'entity_type', 'id']) || !is_array($values)) {
                continue;
            }
            if ($field = $metadata->field($fieldName)) {
                foreach ($values as $value) {
                    $fields[$fieldName]['value_' . $field->type()][] = $field->format((array) $value, true)['value'];
                }
            }
        }

        return ['fields_' . $portalId => $fields];
    }
}

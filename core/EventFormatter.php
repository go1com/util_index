<?php

namespace go1\util_index\core;

use Doctrine\DBAL\Connection;
use go1\clients\LoClient;
use go1\util\Country;
use go1\util\lo\LoHelper;
use stdClass;

class EventFormatter implements IndexFormatterInterface
{
    private $go1;
    private $loClient;

    public function __construct(Connection $go1, LoClient $loClient)
    {
        $this->go1 = $go1;
        $this->loClient = $loClient;
    }

    public function format(stdClass $lo, $portalId = null, $loTeaser = false)
    {
        $lo = LoHelper::load($this->go1, $lo->id);
        $event = json_decode(json_encode($lo->event), true);
        $location = $event['locations'][0] ?? [];
        $eventId = $event['id'] ?? false;

        $doc = [
            'lo_id'                    => $lo->id,
            'start'                    => $event['start'] ?? null,
            'title'                    => $location['title'] ?? null,
            'end'                      => $event['end'] ?? null,
            'timezone'                 => $event['timezone'] ?? null,
            'seats'                    => $event['seats'] ?? null,
            'available_seats'          => $eventId ? $this->loClient->eventAvailableSeat($eventId) : 0,
            'country'                  => $location['country'] ?? null,
            'country_name'             => Country::getName($location['country'] ?? null),
            'administrative_area'      => $location['administrative_area'] ?? null,
            'administrative_area_name' => Country::getStateName($location['country'] ?? null, $location['administrative_area'] ?? null),
            'sub_administrative_area'  => $location['sub_administrative_area'] ?? null,
            'locality'                 => $location['locality'] ?? null,
            'dependent_locality'       => $location['dependent_locality'] ?? null,
            'thoroughfare'             => $location['thoroughfare'] ?? null,
            'premise'                  => $location['premise'] ?? null,
            'sub_premise'              => $location['sub_premise'] ?? null,
            'organisation_name'        => $location['organisation_name'] ?? null,
            'name_line'                => $location['name_line'] ?? null,
            'postal_code'              => $location['postal_code'] ?? null,
        ];

        if (!$loTeaser) {
            $doc['metadata'] = [
                'instance_id' => $portalId ?: $lo->instance_id,
                'updated_at'  => time(),
            ];
        }

        return $doc;
    }
}

<?php

namespace go1\util_index\core;

use go1\util\location\LocationRepository;
use Pimple\Container;
use Pimple\ServiceProviderInterface;
use ReflectionClass;
use Silex\Api\BootableProviderInterface;
use Silex\Application;

class IndexCoreServiceProvider implements ServiceProviderInterface, BootableProviderInterface
{
    public function register(Container $c)
    {
        $c['location.repository'] = function (Container $c) {
            return new LocationRepository($c['dbs']['go1_write'], $c['go1.client.mq']);
        };

        $c['formatter.eck_data'] = function (Container $c) {
            return new AccountFieldFormatter($c['dbs']['go1'], $c['dbs']['eck']);
        };

        $c['formatter.user'] = function (Container $c) {
            return new UserFormatter(
                $c['dbs']['go1'],
                $c['dbs']['social'] ?? null,
                $c['dbs']['eck'] ?? null,
                $c['accounts_name'],
                $c['formatter.eck_data']
            );
        };

        $c['formatter.award.enrolment'] = function (Container $c) {
            return new AwardEnrolmentFormatter($c['dbs']['go1'], $c['dbs']['award'], $c['formatter.lo'], $c['formatter.user']);
        };

        $c['formatter.lo'] = function (Container $c) {
            return new LoFormatter(
                $c['dbs']['go1'],
                $c['dbs']['social'] ?? null,
                $c['dbs']['award'] ?? null,
                $c['dbs']['vote'] ?? null,
                $c['dbs']['quiz'] ?? null,
                $c['dbs']['policy'] ?? null,
                $c['dbs']['collection'] ?? null,
                $c['accounts_name'],
                $c['formatter.user'],
                $c['formatter.event'] ?? null,
                $c['portal_checker'],
                $c['location.repository']
            );
        };

        $c['formatter.enrolment'] = function (Container $c) {
            return new EnrolmentFormatter(
                $c['dbs']['go1'],
                $c['dbs']['assignment'] ?? null,
                $c['dbs']['quiz'] ?? null,
                $c['accounts_name'],
                $c['formatter.lo'],
                $c['formatter.user']
            );
        };

        $c['consumer.lo.arguments'] = function (Container $c) {
            return [
                $c['go1.client.es'],
                $c['history.repository'],
                $c['dbs']['default'],
                $c['dbs']['go1_write'],
                $c['accounts_name'],
                $c['formatter.lo'],
                $c['formatter.user'],
                $c['waitForCompletion'],
                $c['repository.es'],
                $c['go1.client.mq'],
                $c['portal_checker'],
            ];
        };

        $c['consumer.lo'] = function (Container $c) {
            $reflection = new ReflectionClass(LoConsumer::class);

            return $reflection->newInstanceArgs($c['consumer.lo.arguments']);
        };

        $c['consumer.enrolment'] = function (Container $c) {
            return new EnrolmentConsumer(
                $c['dispatcher'],
                $c['go1.client.es'],
                $c['history.repository'],
                $c['dbs']['default'],
                $c['dbs']['go1_write'],
                $c['dbs']['social_write'] ?? null,
                $c['accounts_name'],
                $c['formatter.enrolment'],
                $c['formatter.lo'],
                $c['formatter.user'],
                $c['formatter.eck_data'],
                $c['waitForCompletion'],
                $c['repository.es']
            );
        };

        $c['consumer.plan.enrolment-virtual'] = function (Container $c) {
            return new EnrolmentVirtualFromPlanConsumer(
                $c['dispatcher'],
                $c['go1.client.es'],
                $c['history.repository'],
                $c['dbs']['default'],
                $c['dbs']['go1_write'],
                $c['dbs']['social_write'],
                $c['dbs']['award_write'],
                $c['accounts_name'],
                $c['formatter.enrolment'],
                $c['formatter.award.enrolment'],
                $c['formatter.lo'],
                $c['formatter.user'],
                $c['formatter.eck_data'],
                $c['waitForCompletion'],
                $c['repository.es']
            );
        };

        $c['consumer.assessor'] = function (Container $c) {
            return new EnrolmentAssessorConsumer(
                $c['go1.client.es'],
                $c['dbs']['default'],
                $c['formatter.enrolment'],
                $c['go1.client.mq']
            );
        };

        $c['consumer.task'] = function (Container $c) {
            return new TaskConsumer(
                $c['dbs']['default'],
                $c,
                $c['go1.client.mq'],
                $c['task.repository'],
                $c['history.repository'],
                $c['logger']
            );
        };

        $c['consumers'] = function (Container $c) {
            return [$c['consumer.task']];
        };

        $c['ctrl.task'] = function (Container $c) {
            return new TaskController(
                $c['env'],
                $c['dbs']['go1_write'],
                $c['dbs']['default'],
                $c['task.repository'],
                $c['access_checker'],
                $c,
                $c['client'],
                $c['go1.client.es'],
                $c['go1.client.mq']
            );
        };

        $c['ctrl.handlers'] = function (Container $c) {
            return new ReindexHandlersController($c);
        };
    }

    public function boot(Application $app)
    {
        $app->get('/reindex/handlers', 'ctrl.handlers:get');
        $app->post('/task', 'ctrl.task:post');
        $app->get('/task/{id}/verify', 'ctrl.task:verify');
        $app->post('/task/{id}/execute', 'ctrl.task:execute');
        $app->delete('/task/{id}', 'ctrl.task:delete');
    }
}

<?php

namespace go1\util_index\tests;

use go1\clients\MqClient;
use go1\util\user\UserHelper;
use go1\util_index\IndexService;
use Symfony\Component\HttpFoundation\Request;

trait IndexQueueClientMockTrait
{
    protected function mockEsTransport()
    {
    }

    protected function mockMqClient(IndexService $app)
    {
        $app->extend('go1.client.mq', function () {
            /** @var IndexServiceTestCase $this */
            $queue = $this
                ->getMockBuilder(MqClient::class)
                ->disableOriginalConstructor()
                ->setMethods(['queue', 'publish'])
                ->getMock();

            $queue
                ->expects($this->any())
                ->method('publish')
                ->willReturnCallback(function ($body, $routingKey) {
                    $this->messages[$routingKey][] = $body;
                });

            $queue
                ->expects($this->any())
                ->method('queue')
                ->willReturnCallback(function ($body, $routingKey) {
                    $this->messages[$routingKey][] = $body;
                });

            return $queue;
        });
    }

    protected function mockMqClientToDoConsume(IndexService $app)
    {
        $app->extend('go1.client.mq', function () use ($app) {
            /** @var IndexServiceTestCase $this */
            $mock = $this
                ->getMockBuilder(MqClient::class)
                ->disableOriginalConstructor()
                ->setMethods(['queue'])
                ->getMock();

            $mock
                ->expects($this->any())
                ->method('queue')
                ->willReturnCallback(
                    function ($body, $routingKey, $context) use ($app) {
                        if (IndexService::WORKER_TASK_BULK == $routingKey) {
                            foreach ($body as $msg) {
                                $req = Request::create('/consume?jwt=' . UserHelper::ROOT_JWT, 'POST');
                                $req->request->replace(['routingKey' => $msg['routingKey'], 'body' => (object) $msg['body']]);
                                $res = $app->handle($req);
                                $this->assertEquals(204, $res->getStatusCode());
                            }

                            $req = Request::create('/consume?jwt=' . UserHelper::ROOT_JWT, 'POST', [
                                'routingKey' => $routingKey,
                                'body'       => (object) [],
                                'context'    => $context,
                            ]);
                            $res = $app->handle($req);
                            $this->assertEquals(204, $res->getStatusCode());

                            return true;
                        }

                        $req = Request::create('/consume?jwt=' . UserHelper::ROOT_JWT, 'POST');
                        $req->request->replace(['routingKey' => $routingKey, 'body' => (object) $body]);
                        $res = $app->handle($req);
                        $this->assertEquals(204, $res->getStatusCode());

                        return true;
                    }
                );

            return $mock;
        });
    }
}

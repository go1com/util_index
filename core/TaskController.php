<?php

namespace go1\util_index\core;

use Assert\Assert;
use Assert\LazyAssertionException;
use Doctrine\DBAL\Connection;
use Elasticsearch\Client;
use Exception;
use go1\clients\MqClient;
use go1\util\AccessChecker;
use go1\util\Error;
use go1\util\es\Schema;
use go1\util\portal\PortalHelper;
use go1\util\Service;
use go1\util\user\UserHelper;
use go1\util_index\task\Task;
use go1\util_index\task\TaskRepository;
use GuzzleHttp\Client as HttpClient;
use Pimple\Container;
use Symfony\Component\HttpFoundation\JsonResponse;
use Symfony\Component\HttpFoundation\Request;

class TaskController
{
    private $env;
    private $go1;
    private $db;
    private $rTask;
    private $accessChecker;
    private $container;
    private $http;
    private $client;
    private $queue;

    public function __construct(
        string $env,
        Connection $go1,
        Connection $db,
        TaskRepository $taskRepository,
        AccessChecker $accessChecker,
        Container $container,
        HttpClient $http,
        Client $client,
        MqClient $queue
    ) {
        $this->env = $env;
        $this->go1 = $go1;
        $this->db = $db;
        $this->rTask = $taskRepository;
        $this->accessChecker = $accessChecker;
        $this->container = $container;
        $this->http = $http;
        $this->client = $client;
        $this->queue = $queue;
    }

    public function post(Request $req)
    {
        if (!$this->accessChecker->isAccountsAdmin($req)) {
            return Error::simpleErrorJsonResponse('Internal resource', 403);
        }

        $defaultHandlers = REINDEX_HANDLERS;
        $actor = $this->accessChecker->validUser($req);
        $title = $req->get('title', '');
        $index = $req->get('index', Schema::INDEX);
        $handlers = $req->get('handlers');
        $execute = $req->get('execute', true);
        $alias = $req->get('alias', false);
        $maxNumItems = $req->get('max_num_items', null);
        $maxNumItems = is_null($maxNumItems) ? $maxNumItems : intval($maxNumItems);
        $portalName = $req->get('instance', null);
        $aliasName = $req->get('alias_name', Schema::INDEX);
        $routing = $req->get('routing', Schema::INDEX);
        $removeRedundant = $req->get('remove_redundant', 0);

        if ($portalName && !$portal = PortalHelper::load($this->go1, $portalName)) {
            return Error::simpleErrorJsonResponse('Portal not found', 400);
        }

        try {
            Assert::lazy()
                  ->that($title, 'title')->string()->minLength(1)
                  ->that($index, 'index')->string()->minLength(1)
                  ->that($routing, 'routing')->string()->minLength(1)
                  ->that($handlers, 'handlers')->nullOr()->all()->inArray($defaultHandlers)
                  ->that($execute, 'execute')->nullOr()->inArray([0, 1, true, false, '0', '1', 'true', 'false'])
                  ->that($alias, 'alias')->nullOr()->inArray([0, 1, true, false, '0', '1', 'true', 'false'])
                  ->that($aliasName, 'alias_name')->string()->minLength(1)
                  ->that($maxNumItems, 'max_num_items')->nullOr()->integer()->min(1)
                  ->that($removeRedundant, 'remove_redundant')->nullOr()->inArray([0, 1, true, false, '0', '1', 'true', 'false'])
                  ->verifyNow();

            $handlers = $handlers ?: $defaultHandlers;
            if ($handlers) {
                $task = Task::create((object) [
                    'title'     => $title,
                    'instance'  => $portal ?? null,
                    'author_id' => $actor->id,
                    'data' => [
                        'handlers'        => $handlers,
                        'currentHandler'  => $handlers[0],
                        'index'           => $index,
                        'alias'           => $alias,
                        'aliasName'       => $aliasName,
                        'routing'         => $routing,
                        'maxNumItems'     => $maxNumItems ?: 100,
                        'removeRedundant' => $removeRedundant,
                        'service'         => SERVICE_NAME,
                    ],
                ]);

                $this->rTask->create($task);
                if ($execute) {
                    $this->rTask->execute($task);
                }

                return new JsonResponse(['id' => $task->id], 200);
            }

            return new JsonResponse(null, 204);
        } catch (LazyAssertionException $e) {
            return Error::createLazyAssertionJsonResponse($e);
        }
    }

    public function verify(int $id, Request $req)
    {
        if (!$this->accessChecker->isAccountsAdmin($req)) {
            return Error::jr403('Internal resource');
        }

        if (!$task = $this->rTask->load($id)) {
            return Error::jr404('Task not found');
        }

        $this->rTask->verify($task);

        return new JsonResponse(null, 204);
    }

    public function execute(int $id, Request $req)
    {
        if (!$this->accessChecker->isAccountsAdmin($req)) {
            return Error::jr403('Internal resource');
        }

        if (!$task = $this->rTask->load($id)) {
            return Error::jr404('Task not found', 404);
        }

        if (Task::NOT_STARTED != $task->status) {
            return Error::jr('Task is running or completed');
        }

        $this->rTask->execute($task);

        return new JsonResponse(null, 204);
    }

    public function delete(int $id, Request $req)
    {
        if (!$this->accessChecker->isAccountsAdmin($req)) {
            return Error::jr403('Internal resource');
        }

        if (!$task = $this->rTask->load($id)) {
            return Error::jr404('Task not found');
        }

        try {
            $this->rTask->delete($task);

            return new JsonResponse(null, 204);
        } catch (Exception $e) {
            return Error::jr500('Failed to delete task');
        }
    }
}

<?php

declare(strict_types=1);

namespace Tests\Feature\V2;

use Illuminate\Database\QueryException;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Event;
use Illuminate\Support\Facades\Queue;
use PDOException;
use RuntimeException;
use Tests\Fixtures\V2\TestFailingWorkflow;
use Tests\Fixtures\V2\TestGreetingWorkflow;
use Tests\TestCase;
use Workflow\Events\ActivityStarted as LegacyActivityStarted;
use Workflow\Events\StateChanged;
use Workflow\Events\WorkflowStarted as LegacyWorkflowStarted;
use Workflow\States\WorkflowRunningStatus;
use Workflow\V2\Contracts\HistoryProjectionRole;
use Workflow\V2\Enums\TaskStatus;
use Workflow\V2\Enums\TaskType;
use Workflow\V2\Events\ActivityCompleted;
use Workflow\V2\Events\ActivityFailed;
use Workflow\V2\Events\ActivityStarted;
use Workflow\V2\Events\FailureRecorded;
use Workflow\V2\Events\WorkflowCompleted;
use Workflow\V2\Events\WorkflowFailed;
use Workflow\V2\Events\WorkflowStarted;
use Workflow\V2\Jobs\RunActivityTask;
use Workflow\V2\Jobs\RunWorkflowTask;
use Workflow\V2\Models\ActivityAttempt;
use Workflow\V2\Models\ActivityExecution;
use Workflow\V2\Models\WorkflowInstance;
use Workflow\V2\Models\WorkflowRun;
use Workflow\V2\Models\WorkflowRunSummary;
use Workflow\V2\Models\WorkflowTask;
use Workflow\V2\Support\DefaultHistoryProjectionRole;
use Workflow\V2\Support\LifecycleEventDispatcher;
use Workflow\V2\WorkflowStub;

final class V2LifecycleEventTest extends TestCase
{
    public function testWorkflowLifecycleListenersRunSynchronouslyOutsideTransactionsInEventOrder(): void
    {
        $run = $this->lifecycleRun();
        $eventOrder = [];
        $stateChanged = null;

        Event::listen(WorkflowStarted::class, static function () use (&$eventOrder): void {
            $eventOrder[] = WorkflowStarted::class;
        });
        Event::listen(LegacyWorkflowStarted::class, static function () use (&$eventOrder): void {
            $eventOrder[] = LegacyWorkflowStarted::class;
        });
        Event::listen(StateChanged::class, static function (StateChanged $event) use (
            &$eventOrder,
            &$stateChanged,
        ): void {
            $eventOrder[] = StateChanged::class;
            $stateChanged = $event;
        });

        LifecycleEventDispatcher::workflowStarted($run);

        $this->assertSame([
            WorkflowStarted::class,
            LegacyWorkflowStarted::class,
            StateChanged::class,
        ], $eventOrder);
        $this->assertInstanceOf(StateChanged::class, $stateChanged);
        $this->assertInstanceOf(WorkflowRun::class, $stateChanged->model);
        $this->assertSame($run->id, $stateChanged->model->id);
        $this->assertSame($stateChanged->model, $stateChanged->finalState?->getModel());
        $this->assertInstanceOf(WorkflowRunningStatus::class, $stateChanged->finalState);
        $this->assertSame('status', $stateChanged->field);
    }

    public function testNestedLifecycleEventsPublishOnlyAfterTheOutermostCommitWithCapturedPayloads(): void
    {
        $run = $this->lifecycleRun();
        $publicEvents = [];
        $legacyEvents = [];
        $stateChangedEvents = [];

        Event::listen(WorkflowStarted::class, static function (WorkflowStarted $event) use (&$publicEvents): void {
            $publicEvents[] = $event;
        });
        Event::listen(
            LegacyWorkflowStarted::class,
            static function (LegacyWorkflowStarted $event) use (&$legacyEvents): void {
                $legacyEvents[] = $event;
            },
        );
        Event::listen(StateChanged::class, static function (StateChanged $event) use (&$stateChangedEvents): void {
            $stateChangedEvents[] = $event;
        });

        DB::transaction(function () use ($run, &$publicEvents, &$legacyEvents, &$stateChangedEvents): void {
            DB::transaction(static function () use ($run): void {
                LifecycleEventDispatcher::workflowStarted($run);

                $run->id = 'mutated-run-id';
                $run->workflow_class = 'MutatedWorkflow';
                $run->instance->id = 'mutated-instance-id';
            });

            $this->assertCount(0, $publicEvents);
            $this->assertCount(0, $legacyEvents);
            $this->assertCount(0, $stateChangedEvents);
        });

        $this->assertCount(1, $publicEvents);
        $this->assertSame('instance-id', $publicEvents[0]->instanceId);
        $this->assertSame('run-id', $publicEvents[0]->runId);
        $this->assertSame(TestGreetingWorkflow::class, $publicEvents[0]->workflowClass);
        $this->assertCount(1, $legacyEvents);
        $this->assertSame('instance-id', $legacyEvents[0]->workflowId);
        $this->assertSame(TestGreetingWorkflow::class, $legacyEvents[0]->class);
        $this->assertCount(1, $stateChangedEvents);
        $this->assertInstanceOf(WorkflowRun::class, $stateChangedEvents[0]->model);
        $this->assertSame('run-id', $stateChangedEvents[0]->model->id);
        $this->assertSame('instance-id', $stateChangedEvents[0]->model->instance->id);
        $this->assertSame($stateChangedEvents[0]->model, $stateChangedEvents[0]->finalState?->getModel());
    }

    public function testNestedLifecycleEventsAreDiscardedWhenTheOuterTransactionRollsBack(): void
    {
        $run = $this->lifecycleRun();
        $listenerCalls = 0;

        Event::listen(WorkflowStarted::class, static function () use (&$listenerCalls): void {
            $listenerCalls++;
        });

        try {
            DB::transaction(function () use ($run, &$listenerCalls): void {
                DB::transaction(static function () use ($run): void {
                    LifecycleEventDispatcher::workflowStarted($run);
                });

                $this->assertSame(0, $listenerCalls);

                throw new RuntimeException('roll back outer transaction');
            });

            $this->fail('Expected the outer transaction to roll back.');
        } catch (RuntimeException $exception) {
            $this->assertSame('roll back outer transaction', $exception->getMessage());
        }

        $this->assertSame(0, $listenerCalls);
    }

    public function testRolledBackTransactionAttemptDoesNotDuplicateLifecycleEventsOnRetry(): void
    {
        $run = $this->lifecycleRun();
        $transactionAttempts = 0;
        $publicListenerCalls = 0;
        $legacyListenerCalls = 0;

        Event::listen(ActivityStarted::class, static function () use (&$publicListenerCalls): void {
            $publicListenerCalls++;
        });
        Event::listen(LegacyActivityStarted::class, static function () use (&$legacyListenerCalls): void {
            $legacyListenerCalls++;
        });

        DB::transaction(function () use ($run, &$transactionAttempts): void {
            $transactionAttempts++;

            LifecycleEventDispatcher::activityStarted(
                $run,
                'activity-execution-id',
                'test-activity',
                'TestActivity',
                1,
                1,
            );

            if ($transactionAttempts === 1) {
                throw $this->retryableLockException();
            }
        }, 2);

        $this->assertSame(2, $transactionAttempts);
        $this->assertSame(1, $publicListenerCalls);
        $this->assertSame(1, $legacyListenerCalls);
    }

    public function testPostCommitListenerExceptionDoesNotReplayCommittedOperation(): void
    {
        config()->set('queue.default', 'redis');
        config()
            ->set('queue.connections.redis.driver', 'redis');
        Queue::fake();

        $workflow = WorkflowStub::make(TestGreetingWorkflow::class, 'post-commit-listener-failure');
        $workflow->start('Taylor');
        $run = WorkflowRun::query()->findOrFail($workflow->runId());
        $transactionAttempts = 0;
        $listenerCalls = 0;

        Event::listen(ActivityCompleted::class, static function () use (&$listenerCalls): void {
            $listenerCalls++;

            throw new RuntimeException('listener failed after commit');
        });

        try {
            DB::transaction(static function () use ($run, &$transactionAttempts): void {
                $transactionAttempts++;

                WorkflowRun::query()
                    ->whereKey($run->id)
                    ->update([
                        'priority' => 47,
                    ]);

                LifecycleEventDispatcher::activityCompleted(
                    $run,
                    'activity-execution-id',
                    'test-activity',
                    'TestActivity',
                    1,
                    1,
                );
            }, 3);

            $this->fail('Expected the post-commit listener exception.');
        } catch (RuntimeException $exception) {
            $this->assertSame('listener failed after commit', $exception->getMessage());
        }

        $this->assertSame(1, $transactionAttempts);
        $this->assertSame(1, $listenerCalls);
        $this->assertSame(47, WorkflowRun::query()->findOrFail($run->id)->priority);
    }

    public function testWorkflowStartedEventIsDispatched(): void
    {
        config()->set('queue.default', 'redis');
        config()
            ->set('queue.connections.redis.driver', 'redis');
        Queue::fake();

        Event::fake([WorkflowStarted::class]);

        $workflow = WorkflowStub::make(TestGreetingWorkflow::class, 'lifecycle-start');
        $workflow->start('Taylor');

        Event::assertDispatched(WorkflowStarted::class, static function (WorkflowStarted $event) use ($workflow): bool {
            return $event->instanceId === $workflow->id()
                && $event->runId === $workflow->runId()
                && $event->workflowType === 'test-greeting-workflow'
                && $event->workflowClass !== ''
                && $event->committedAt !== '';
        });
    }

    public function testWorkflowStartedLifecycleEventsWaitForRetriedStartTransactionCommit(): void
    {
        config()->set('queue.default', 'redis');
        config()
            ->set('queue.connections.redis.driver', 'redis');
        config()
            ->set('workflows.storage.transaction_attempts', 2);
        Queue::fake();

        $customRole = new class(new DefaultHistoryProjectionRole()) implements HistoryProjectionRole {
            public int $projectRunCalls = 0;

            public function __construct(
                private readonly DefaultHistoryProjectionRole $delegate,
            ) {
            }

            public function projectRun(WorkflowRun $run): WorkflowRunSummary
            {
                $this->projectRunCalls++;

                if ($this->projectRunCalls === 1) {
                    throw new QueryException(
                        'sqlite',
                        'insert into workflow_run_summaries',
                        [],
                        new class('SQLSTATE[HY000]: General error: 5 database is locked') extends PDOException {
                            public function __construct(string $message)
                            {
                                parent::__construct($message, 5);
                                $this->errorInfo = ['HY000', 5, 'database is locked'];
                            }
                        },
                    );
                }

                return $this->delegate->projectRun($run);
            }

            public function recordActivityStarted(
                WorkflowRun $run,
                ActivityExecution $execution,
                ActivityAttempt $attempt,
                WorkflowTask $task,
            ): WorkflowRunSummary {
                return $this->delegate->recordActivityStarted($run, $execution, $attempt, $task);
            }
        };

        $this->app->instance(HistoryProjectionRole::class, $customRole);

        Event::fake([WorkflowStarted::class, LegacyWorkflowStarted::class, StateChanged::class]);

        $workflow = WorkflowStub::make(TestGreetingWorkflow::class, 'lifecycle-start-retry');
        $workflow->start('Taylor');

        // The failed transaction, committed transaction, and post-dispatch refresh
        // each project once; lifecycle events still publish only after commit.
        $this->assertSame(3, $customRole->projectRunCalls);
        Event::assertDispatched(WorkflowStarted::class, 1);
        Event::assertDispatched(LegacyWorkflowStarted::class, 1);
        Event::assertDispatched(StateChanged::class, 1);
    }

    public function testSuccessfulWorkflowDispatchesFullLifecycle(): void
    {
        config()->set('queue.default', 'redis');
        config()
            ->set('queue.connections.redis.driver', 'redis');
        Queue::fake();

        Event::fake([
            WorkflowStarted::class,
            WorkflowCompleted::class,
            ActivityStarted::class,
            ActivityCompleted::class,
        ]);

        $workflow = WorkflowStub::make(TestGreetingWorkflow::class, 'lifecycle-full');
        $workflow->start('Taylor');

        $this->drainReadyTasks();

        $this->assertTrue($workflow->refresh()->completed());

        Event::assertDispatched(WorkflowStarted::class, 1);
        Event::assertDispatched(ActivityStarted::class, 1);
        Event::assertDispatched(ActivityCompleted::class, 1);
        Event::assertDispatched(WorkflowCompleted::class, 1);

        Event::assertDispatched(ActivityStarted::class, static function (ActivityStarted $event) use ($workflow): bool {
            return $event->instanceId === $workflow->id()
                && $event->runId === $workflow->runId()
                && $event->activityClass !== ''
                && $event->sequence >= 1
                && $event->attemptNumber >= 1;
        });

        Event::assertDispatched(ActivityCompleted::class, static function (ActivityCompleted $event) use (
            $workflow
        ): bool {
            return $event->instanceId === $workflow->id()
                && $event->runId === $workflow->runId()
                && $event->activityExecutionId !== '';
        });

        Event::assertDispatched(WorkflowCompleted::class, static function (WorkflowCompleted $event) use (
            $workflow
        ): bool {
            return $event->instanceId === $workflow->id()
                && $event->runId === $workflow->runId()
                && $event->workflowType === 'test-greeting-workflow';
        });
    }

    public function testFailedWorkflowDispatchesFailureEvents(): void
    {
        config()->set('queue.default', 'redis');
        config()
            ->set('queue.connections.redis.driver', 'redis');
        Queue::fake();

        Event::fake([
            WorkflowStarted::class,
            WorkflowFailed::class,
            ActivityStarted::class,
            ActivityFailed::class,
            FailureRecorded::class,
        ]);

        $workflow = WorkflowStub::make(TestFailingWorkflow::class, 'lifecycle-fail');
        $workflow->start();

        $this->drainReadyTasks();

        $this->assertTrue($workflow->refresh()->failed());

        Event::assertDispatched(WorkflowStarted::class, 1);
        Event::assertDispatched(ActivityStarted::class, 1);
        Event::assertDispatched(ActivityFailed::class, 1);
        Event::assertDispatched(WorkflowFailed::class, 1);

        Event::assertDispatched(ActivityFailed::class, static function (ActivityFailed $event) use ($workflow): bool {
            return $event->instanceId === $workflow->id()
                && $event->runId === $workflow->runId()
                && $event->exceptionClass === 'RuntimeException'
                && $event->message === 'boom';
        });

        Event::assertDispatched(WorkflowFailed::class, static function (WorkflowFailed $event) use ($workflow): bool {
            return $event->instanceId === $workflow->id()
                && $event->runId === $workflow->runId();
        });

        // FailureRecorded should fire for both the activity failure and the workflow failure.
        Event::assertDispatched(FailureRecorded::class, static function (FailureRecorded $event): bool {
            return $event->sourceKind === 'activity_execution'
                && $event->failureId !== ''
                && $event->exceptionClass === 'RuntimeException'
                && $event->message === 'boom';
        });

        Event::assertDispatched(FailureRecorded::class, static function (FailureRecorded $event): bool {
            return $event->sourceKind === 'workflow_run'
                && $event->failureId !== '';
        });
    }

    public function testNoEventsDispatchedDuringReplay(): void
    {
        config()->set('queue.default', 'redis');
        config()
            ->set('queue.connections.redis.driver', 'redis');
        Queue::fake();

        $workflow = WorkflowStub::make(TestGreetingWorkflow::class, 'lifecycle-no-replay');
        $workflow->start('Taylor');

        // Run workflow task (schedules activity) — no event fake yet.
        $this->runNextReadyTask();

        // Run the activity task — still no event fake.
        $this->runNextReadyTask();

        // Now fake events and run the final workflow task (replay + completion).
        Event::fake([WorkflowStarted::class, ActivityStarted::class]);

        $this->drainReadyTasks();
        $this->assertTrue($workflow->refresh()->completed());

        // WorkflowStarted and ActivityStarted should NOT fire during replay —
        // they are only dispatched from the original start/claim commit points.
        Event::assertNotDispatched(WorkflowStarted::class);
        Event::assertNotDispatched(ActivityStarted::class);
    }

    private function drainReadyTasks(): void
    {
        $deadline = microtime(true) + 10;

        while (microtime(true) < $deadline) {
            /** @var WorkflowTask|null $task */
            $task = WorkflowTask::query()
                ->where('status', TaskStatus::Ready->value)
                ->orderBy('created_at')
                ->first();

            if ($task === null) {
                return;
            }

            $job = match ($task->task_type) {
                TaskType::Workflow => new RunWorkflowTask($task->id),
                TaskType::Activity => new RunActivityTask($task->id),
                TaskType::Timer => new \Workflow\V2\Jobs\RunTimerTask($task->id),
            };

            $this->app->call([$job, 'handle']);
        }

        $this->fail('Timed out draining ready workflow tasks.');
    }

    private function runNextReadyTask(): void
    {
        /** @var WorkflowTask|null $task */
        $task = WorkflowTask::query()
            ->where('status', TaskStatus::Ready->value)
            ->orderBy('created_at')
            ->first();

        if ($task === null) {
            $this->fail('Expected a ready workflow task.');
        }

        $job = match ($task->task_type) {
            TaskType::Workflow => new RunWorkflowTask($task->id),
            TaskType::Activity => new RunActivityTask($task->id),
            TaskType::Timer => new \Workflow\V2\Jobs\RunTimerTask($task->id),
        };

        $this->app->call([$job, 'handle']);
    }

    private function lifecycleRun(): WorkflowRun
    {
        $instance = new WorkflowInstance();
        $instance->id = 'instance-id';

        $run = new WorkflowRun();
        $run->id = 'run-id';
        $run->workflow_instance_id = $instance->id;
        $run->workflow_type = 'test-greeting-workflow';
        $run->workflow_class = TestGreetingWorkflow::class;
        $run->setRelation('instance', $instance);

        return $run;
    }

    private function retryableLockException(): QueryException
    {
        return new QueryException(
            'pgsql',
            'update workflow_runs',
            [],
            new class('SQLSTATE[40001]: Serialization failure') extends PDOException {
                public function __construct(string $message)
                {
                    parent::__construct($message, 40001);
                    $this->errorInfo = ['40001', 40001, 'Serialization failure'];
                }
            },
        );
    }
}

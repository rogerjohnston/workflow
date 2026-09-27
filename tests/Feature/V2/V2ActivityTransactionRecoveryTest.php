<?php

declare(strict_types=1);

namespace Tests\Feature\V2;

use Closure;
use Illuminate\Database\Events\TransactionRolledBack;
use Illuminate\Database\QueryException;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Event;
use Illuminate\Support\Facades\Queue;
use Illuminate\Support\Str;
use PDOException;
use PHPUnit\Framework\Attributes\DataProvider;
use RuntimeException;
use Tests\Fixtures\V2\TestGreetingWorkflow;
use Tests\TestCase;
use Throwable;
use Workflow\V2\Contracts\ExternalPayloadStorageDriver;
use Workflow\V2\Contracts\ExternalPayloadStoragePolicy;
use Workflow\V2\Contracts\HistoryProjectionRole;
use Workflow\V2\Enums\ActivityAttemptStatus;
use Workflow\V2\Enums\ActivityStatus;
use Workflow\V2\Enums\HistoryEventType;
use Workflow\V2\Enums\RunStatus;
use Workflow\V2\Enums\TaskStatus;
use Workflow\V2\Enums\TaskType;
use Workflow\V2\Events\ActivityCompleted;
use Workflow\V2\Events\ActivityStarted;
use Workflow\V2\Jobs\RunWorkflowTask;
use Workflow\V2\Models\ActivityAttempt;
use Workflow\V2\Models\ActivityExecution;
use Workflow\V2\Models\WorkflowHistoryEvent;
use Workflow\V2\Models\WorkflowRun;
use Workflow\V2\Models\WorkflowRunSummary;
use Workflow\V2\Models\WorkflowTask;
use Workflow\V2\Support\ActivityOutcomeRecorder;
use Workflow\V2\Support\ActivityTaskClaim;
use Workflow\V2\Support\ActivityTaskClaimer;
use Workflow\V2\Support\DefaultHistoryProjectionRole;
use Workflow\V2\Support\ExternalPayloads;
use Workflow\V2\TaskWatchdog;
use Workflow\V2\WorkflowStub;

final class V2ActivityTransactionRecoveryTest extends TestCase
{
    public function testClaimRetriesRolledBackWritesWithoutRepeatingCommittedStart(): void
    {
        $task = $this->readyActivity();
        $started = 0;
        Event::listen(ActivityStarted::class, static function () use (&$started): void {
            $started++;
        });
        $role = $this->faultingRole(1);

        $claim = ActivityTaskClaimer::claimDetailed($task->id)['claim'];

        $this->assertInstanceOf(ActivityTaskClaim::class, $claim);
        $this->assertSame(2, $role->calls);
        $this->assertSame(1, $started);
        $this->assertSame(1, ActivityAttempt::query()->count());
        $this->assertSame(1, $task->fresh()->attempt_count);
        $this->assertSame(1, $this->historyCount(HistoryEventType::ActivityStarted));
        $this->assertSame($claim->attemptId(), $claim->execution->fresh()->current_attempt_id);
    }

    public function testOutcomeReusesExternalResultAndPublishesOnlyCommittedCompletion(): void
    {
        $claim = $this->claim();
        $driver = $this->countingStorage();
        $completed = 0;
        Event::listen(ActivityCompleted::class, static function () use (&$completed): void {
            $completed++;
        });
        $role = $this->faultingRole(1);

        $outcome = $this->record($claim);

        $this->assertTrue($outcome['recorded']);
        $this->assertSame(2, $role->calls);
        $this->assertSame(1, $driver->writes);
        $this->assertSame(1, $completed);
        $this->assertSame(1, $this->historyCount(HistoryEventType::ActivityCompleted));
        $this->assertSame(ActivityStatus::Completed, $claim->execution->fresh()->status);
        $this->assertSame(TaskStatus::Completed, $claim->task->fresh()->status);
        $this->assertSame(1, WorkflowTask::query()->where('status', TaskStatus::Ready)->count());
        $this->assertSame(
            'memory://result/1',
            ExternalPayloads::storedEnvelope($claim->execution->fresh()->result)['external_storage']['uri']
        );
    }

    public function testClaimExhaustionOnSecondaryWorkflowConnectionLeavesNoPartialWrites(): void
    {
        [$defaultConnection, $storageConnection] = $this->useSecondaryWorkflowConnection();
        $task = $this->readyActivity();
        $transactionLevels = [];
        $role = $this->faultingRole(
            3,
            beforeFailure: static function () use (
                &$transactionLevels,
                $defaultConnection,
                $storageConnection,
            ): void {
                $transactionLevels[] = [
                    'default' => DB::connection($defaultConnection)->transactionLevel(),
                    'storage' => DB::connection($storageConnection)->transactionLevel(),
                ];
            },
        );
        $published = 0;
        Event::listen(ActivityStarted::class, static function () use (&$published): void {
            $published++;
        });

        $this->assertConcurrencyFailure(static fn () => ActivityTaskClaimer::claimDetailed($task->id));

        $this->assertSame(3, $role->calls);
        $this->assertSame([
            [
                'default' => 0,
                'storage' => 1,
            ],
            [
                'default' => 0,
                'storage' => 1,
            ],
            [
                'default' => 0,
                'storage' => 1,
            ],
        ], $transactionLevels);
        $this->assertSame(TaskStatus::Ready, $task->fresh()->status);
        $this->assertSame(0, $task->fresh()->attempt_count);
        $this->assertSame(0, ActivityAttempt::query()->count());
        $this->assertSame(0, $this->historyCount(HistoryEventType::ActivityStarted));
        $this->assertSame(0, $published);
    }

    public function testOutcomeExhaustionOnSecondaryWorkflowConnectionPreservesTheOriginalLease(): void
    {
        [$defaultConnection, $storageConnection] = $this->useSecondaryWorkflowConnection();
        $claim = $this->claim();
        $transactionLevels = [];
        $role = $this->faultingRole(
            3,
            beforeFailure: static function () use (
                &$transactionLevels,
                $defaultConnection,
                $storageConnection,
            ): void {
                $transactionLevels[] = [
                    'default' => DB::connection($defaultConnection)->transactionLevel(),
                    'storage' => DB::connection($storageConnection)->transactionLevel(),
                ];
            },
        );
        $published = 0;
        Event::listen(ActivityCompleted::class, static function () use (&$published): void {
            $published++;
        });

        $this->assertConcurrencyFailure(fn () => $this->record($claim));

        $this->assertSame(3, $role->calls);
        $this->assertSame([
            [
                'default' => 0,
                'storage' => 1,
            ],
            [
                'default' => 0,
                'storage' => 1,
            ],
            [
                'default' => 0,
                'storage' => 1,
            ],
        ], $transactionLevels);
        $this->assertSame(TaskStatus::Leased, $claim->task->fresh()->status);
        $this->assertSame(ActivityStatus::Running, $claim->execution->fresh()->status);
        $this->assertSame($claim->attemptId(), $claim->execution->fresh()->current_attempt_id);
        $this->assertSame(0, $this->historyCount(HistoryEventType::ActivityCompleted));
        $this->assertSame(0, WorkflowTask::query()->where('status', TaskStatus::Ready)->count());
        $this->assertSame(0, $published);
    }

    public function testNonConcurrencyFailureIsNotRetried(): void
    {
        $claim = $this->claim();
        $role = $this->faultingRole(3, new RuntimeException('projection invalid'));

        try {
            $this->record($claim);
            $this->fail('Expected projection failure.');
        } catch (RuntimeException $exception) {
            $this->assertSame('projection invalid', $exception->getMessage());
        }

        $this->assertSame(1, $role->calls);
        $this->assertSame(TaskStatus::Leased, $claim->task->fresh()->status);
    }

    public function testNestedDeadlockPropagatesToTheOuterTransaction(): void
    {
        $claim = $this->claim();
        $role = $this->faultingRole(3);

        $this->assertConcurrencyFailure(fn () => DB::transaction(fn () => $this->record($claim)));

        $this->assertSame(1, $role->calls);
        $this->assertSame(0, DB::transactionLevel());
        $this->assertSame(TaskStatus::Leased, $claim->task->fresh()->status);
        $this->assertSame(0, $this->historyCount(HistoryEventType::ActivityCompleted));
    }

    public static function cancellationStatuses(): iterable
    {
        yield 'cancelled' => [RunStatus::Cancelled, 'run_cancelled'];
        yield 'terminated' => [RunStatus::Terminated, 'run_terminated'];
    }

    #[DataProvider('cancellationStatuses')]
    public function testCancellationBetweenAttemptsPreventsSuccess(RunStatus $status, string $reason): void
    {
        $claim = $this->claim();
        $this->faultingRole(1);
        $changed = false;
        Event::listen(TransactionRolledBack::class, static function () use ($claim, $status, &$changed): void {
            if (! $changed) {
                $changed = true;
                WorkflowRun::query()->whereKey($claim->run->id)->update([
                    'status' => $status->value,
                ]);
            }
        });
        $completed = 0;
        Event::listen(ActivityCompleted::class, static function () use (&$completed): void {
            $completed++;
        });

        $outcome = $this->record($claim);

        $this->assertFalse($outcome['recorded']);
        $this->assertSame($reason, $outcome['reason']);
        $this->assertNull($outcome['next_task']);
        $this->assertSame(0, $completed);
        $this->assertSame(0, $this->historyCount(HistoryEventType::ActivityCompleted));
        $this->assertSame(TaskStatus::Cancelled, $claim->task->fresh()->status);
    }

    public function testNewAttemptBetweenRetriesCannotBeOverwrittenByOldResult(): void
    {
        $claim = $this->claim();
        $this->faultingRole(1);
        $changed = false;
        $nextAttemptId = (string) \Illuminate\Support\Str::ulid();
        Event::listen(TransactionRolledBack::class, static function () use ($claim, $nextAttemptId, &$changed): void {
            if (! $changed) {
                $changed = true;
                $nextAttempt = $claim->attempt->replicate();
                $nextAttempt->forceFill([
                    'id' => $nextAttemptId,
                    'attempt_number' => 2,
                ])->save();
                ActivityExecution::query()->whereKey($claim->execution->id)->update([
                    'attempt_count' => 2,
                    'current_attempt_id' => $nextAttemptId,
                ]);
                WorkflowTask::query()->whereKey($claim->task->id)->update([
                    'attempt_count' => 2,
                ]);
            }
        });

        $outcome = $this->record($claim);

        $this->assertFalse($outcome['recorded']);
        $this->assertSame('stale_attempt', $outcome['reason']);
        $this->assertNull($outcome['next_task']);
        $this->assertSame($nextAttemptId, $claim->execution->fresh()->current_attempt_id);
        $this->assertSame(ActivityStatus::Running, $claim->execution->fresh()->status);
        $this->assertSame(TaskStatus::Leased, $claim->task->fresh()->status);
        $this->assertSame(0, $this->historyCount(HistoryEventType::ActivityCompleted));
    }

    public function testClaimAgainstTerminalRunDoesNotWriteActivityState(): void
    {
        $task = $this->readyActivity();
        $execution = ActivityExecution::query()->findOrFail($task->payload['activity_execution_id']);
        $run = WorkflowRun::query()->findOrFail($task->workflow_run_id);
        $run->forceFill([
            'status' => RunStatus::Cancelled,
            'closed_at' => now(),
        ])->save();
        $taskBefore = $task->fresh()
            ->getAttributes();
        $executionBefore = $execution->fresh()
            ->getAttributes();

        $result = ActivityTaskClaimer::claimDetailed($task->id, 'terminal-run-worker');

        $this->assertNull($result['claim']);
        $this->assertSame('task_not_claimable', $result['reason']);
        $this->assertSame($taskBefore, $task->fresh()->getAttributes());
        $this->assertSame($executionBefore, $execution->fresh()->getAttributes());
        $this->assertSame(0, ActivityAttempt::query()->count());
        $this->assertSame(0, $this->historyCount(HistoryEventType::ActivityStarted));
    }

    public function testExpiredReclaimedCurrentAttemptCanBeClaimedAgain(): void
    {
        $firstClaim = $this->claim();
        $firstClaim->task->forceFill([
            'lease_expires_at' => now()
                ->subSecond(),
        ])->save();

        $report = TaskWatchdog::runPass(runIds: [$firstClaim->run->id]);

        $this->assertSame(1, $report['repaired_existing_tasks']);
        $this->assertSame(TaskStatus::Ready, $firstClaim->task->fresh()->status);
        $this->assertSame(ActivityAttemptStatus::Expired, $firstClaim->attempt->fresh()->status);

        $secondClaim = ActivityTaskClaimer::claimDetailed($firstClaim->task->id, 'replacement-worker')['claim'];

        $this->assertInstanceOf(ActivityTaskClaim::class, $secondClaim);
        $this->assertSame(2, $secondClaim->attemptNumber());
        $this->assertNotSame($firstClaim->attemptId(), $secondClaim->attemptId());
        $this->assertSame(ActivityAttemptStatus::Expired, $firstClaim->attempt->fresh()->status);
        $this->assertSame(ActivityAttemptStatus::Running, $secondClaim->attempt->fresh()->status);
        $this->assertSame($secondClaim->attemptId(), $secondClaim->execution->fresh()->current_attempt_id);
        $this->assertSame(2, ActivityAttempt::query()->count());
    }

    public function testWatchdogRecoversLegacyLeasedActivityWithoutCurrentAttempt(): void
    {
        $task = $this->readyActivity();
        $execution = ActivityExecution::query()->findOrFail($task->payload['activity_execution_id']);
        $task->forceFill([
            'status' => TaskStatus::Leased,
            'leased_at' => now()
                ->subMinute(),
            'lease_owner' => 'legacy-worker',
            'lease_expires_at' => now()
                ->subSecond(),
        ])->save();
        $execution->forceFill([
            'status' => ActivityStatus::Running,
            'started_at' => now()
                ->subMinute(),
            'attempt_count' => 0,
            'current_attempt_id' => null,
        ])->save();

        $report = TaskWatchdog::runPass(runIds: [$task->workflow_run_id]);

        $this->assertSame(1, $report['repaired_existing_tasks']);
        $this->assertSame([], $report['existing_task_failures']);
        $this->assertSame(TaskStatus::Ready, $task->fresh()->status);
        $this->assertSame(1, $task->fresh()->attempt_count);
        $this->assertSame(1, $execution->fresh()->attempt_count);
        $this->assertNotNull($execution->fresh()->current_attempt_id);
        $attempt = ActivityAttempt::query()->where('activity_execution_id', $execution->id)->sole();
        $this->assertSame($execution->fresh()->current_attempt_id, $attempt->id);
        $this->assertSame(1, $attempt->attempt_number);
        $this->assertSame(ActivityAttemptStatus::Expired, $attempt->status);
        $this->assertSame($task->id, $attempt->workflow_task_id);
        $this->assertSame('legacy-worker', $attempt->lease_owner);
    }

    public function testLateOutcomeOnCancelledRunCancelsTheNewerCurrentAttempt(): void
    {
        $firstClaim = $this->claim();
        $firstClaim->task->forceFill([
            'lease_expires_at' => now()
                ->subSecond(),
        ])->save();
        TaskWatchdog::runPass(runIds: [$firstClaim->run->id]);
        $secondClaim = ActivityTaskClaimer::claimDetailed($firstClaim->task->id, 'replacement-worker')['claim'];
        $this->assertInstanceOf(ActivityTaskClaim::class, $secondClaim);
        WorkflowRun::query()->whereKey($firstClaim->run->id)->update([
            'status' => RunStatus::Cancelled,
            'closed_at' => now(),
        ]);

        $outcome = $this->record($firstClaim);

        $this->assertFalse($outcome['recorded']);
        $this->assertSame('run_cancelled', $outcome['reason']);
        $this->assertSame(ActivityStatus::Cancelled, $firstClaim->execution->fresh()->status);
        $this->assertSame(TaskStatus::Cancelled, $firstClaim->task->fresh()->status);
        $this->assertSame(ActivityAttemptStatus::Expired, $firstClaim->attempt->fresh()->status);
        $this->assertSame(ActivityAttemptStatus::Cancelled, $secondClaim->attempt->fresh()->status);
        $this->assertSame(1, $this->historyCount(HistoryEventType::ActivityCancelled));
        $this->assertSame(0, $this->historyCount(HistoryEventType::ActivityCompleted));
    }

    public function testLateOutcomeOnCancelledRunCancelsClaimedRetryTaskAndCurrentAttempt(): void
    {
        $firstClaim = $this->claim();
        $retryOutcome = ActivityOutcomeRecorder::record(
            taskId: $firstClaim->task->id,
            attemptId: $firstClaim->attemptId(),
            attemptCount: $firstClaim->attemptNumber(),
            result: null,
            throwable: new RuntimeException('retry before cancellation'),
            maxAttempts: 2,
            backoffSeconds: 0,
        );
        $retryTask = $retryOutcome['next_task'];
        $this->assertTrue($retryOutcome['recorded']);
        $this->assertInstanceOf(WorkflowTask::class, $retryTask);
        $this->assertNotSame($firstClaim->task->id, $retryTask->id);

        $secondClaim = ActivityTaskClaimer::claimDetailed($retryTask->id, 'retry-worker')['claim'];
        $this->assertInstanceOf(ActivityTaskClaim::class, $secondClaim);
        $this->assertSame(2, $secondClaim->attemptNumber());
        WorkflowRun::query()->whereKey($firstClaim->run->id)->update([
            'status' => RunStatus::Cancelled,
            'closed_at' => now(),
        ]);

        $outcome = $this->record($firstClaim);

        $this->assertFalse($outcome['recorded']);
        $this->assertSame('run_cancelled', $outcome['reason']);
        $this->assertSame(ActivityStatus::Cancelled, $firstClaim->execution->fresh()->status);
        $this->assertSame(TaskStatus::Cancelled, $firstClaim->task->fresh()->status);
        $this->assertSame(TaskStatus::Cancelled, $retryTask->fresh()->status);
        $this->assertSame(ActivityAttemptStatus::Failed, $firstClaim->attempt->fresh()->status);
        $this->assertSame(ActivityAttemptStatus::Cancelled, $secondClaim->attempt->fresh()->status);
        $this->assertSame(1, $this->historyCount(HistoryEventType::ActivityCancelled));
        $this->assertSame(0, $this->historyCount(HistoryEventType::ActivityCompleted));
    }

    public function testWatchdogCurrentAttemptSnapshotDriftDoesNotWriteActivityState(): void
    {
        $claim = $this->claim();
        $claim->task->forceFill([
            'lease_expires_at' => now()
                ->subSecond(),
        ])->save();
        $claim->execution->forceFill([
            'current_attempt_id' => (string) Str::ulid(),
        ])->save();
        $taskBefore = $claim->task->fresh()
            ->getAttributes();
        $executionBefore = $claim->execution->fresh()
            ->getAttributes();
        $attemptBefore = $claim->attempt->fresh()
            ->getAttributes();

        $report = TaskWatchdog::runPass(runIds: [$claim->run->id]);

        $this->assertSame(1, $report['selected_existing_task_candidates']);
        $this->assertSame(0, $report['repaired_existing_tasks']);
        $this->assertSame([], $report['existing_task_failures']);
        $this->assertSame($taskBefore, $claim->task->fresh()->getAttributes());
        $this->assertSame($executionBefore, $claim->execution->fresh()->getAttributes());
        $this->assertSame($attemptBefore, $claim->attempt->fresh()->getAttributes());
    }

    public function testWatchdogMalformedCurrentAttemptOwnershipDoesNotWriteActivityState(): void
    {
        $claim = $this->claim();
        $workflowTaskId = WorkflowTask::query()
            ->where('workflow_run_id', $claim->run->id)
            ->where('task_type', TaskType::Workflow)
            ->value('id');
        $this->assertIsString($workflowTaskId);
        $claim->task->forceFill([
            'lease_expires_at' => now()
                ->subSecond(),
        ])->save();
        $claim->attempt->forceFill([
            'workflow_task_id' => $workflowTaskId,
        ])->save();
        $taskBefore = $claim->task->fresh()
            ->getAttributes();
        $executionBefore = $claim->execution->fresh()
            ->getAttributes();
        $attemptBefore = $claim->attempt->fresh()
            ->getAttributes();

        $report = TaskWatchdog::runPass(runIds: [$claim->run->id]);

        $this->assertSame(1, $report['selected_existing_task_candidates']);
        $this->assertSame(0, $report['repaired_existing_tasks']);
        $this->assertSame([], $report['existing_task_failures']);
        $this->assertSame($taskBefore, $claim->task->fresh()->getAttributes());
        $this->assertSame($executionBefore, $claim->execution->fresh()->getAttributes());
        $this->assertSame($attemptBefore, $claim->attempt->fresh()->getAttributes());
    }

    private function readyActivity(): WorkflowTask
    {
        config()->set('queue.default', 'database');
        Queue::fake();
        $workflow = WorkflowStub::make(TestGreetingWorkflow::class, 'transaction-recovery');
        $workflow->start('Taylor');
        $task = WorkflowTask::query()->where('workflow_run_id', $workflow->runId())->where(
            'task_type',
            TaskType::Workflow
        )->firstOrFail();
        $this->app->call([new RunWorkflowTask($task->id), 'handle']);

        return WorkflowTask::query()->where('workflow_run_id', $workflow->runId())->where(
            'task_type',
            TaskType::Activity
        )->firstOrFail();
    }

    private function claim(): ActivityTaskClaim
    {
        $claim = ActivityTaskClaimer::claimDetailed($this->readyActivity()->id)['claim'];
        $this->assertInstanceOf(ActivityTaskClaim::class, $claim);

        return $claim;
    }

    private function record(ActivityTaskClaim $claim): array
    {
        return ActivityOutcomeRecorder::record(
            $claim->task->id,
            $claim->attemptId(),
            $claim->attemptNumber(),
            str_repeat('result', 100),
            null,
            1,
            0
        );
    }

    private function historyCount(HistoryEventType $type): int
    {
        return WorkflowHistoryEvent::query()->where('event_type', $type)->count();
    }

    private function assertConcurrencyFailure(Closure $operation): void
    {
        try {
            $operation();
            $this->fail('Expected exhausted or nested concurrency failure.');
        } catch (Throwable $exception) {
            $this->assertStringContainsString('Deadlock found', $exception->getMessage());
        }
    }

    private function faultingRole(
        int $failures,
        ?Throwable $exception = null,
        ?Closure $beforeFailure = null,
    ): HistoryProjectionRole {
        $exception ??= new QueryException(
            'mysql',
            'update workflow_run_summaries',
            [],
            new class() extends PDOException {
                public function __construct()
                {
                    parent::__construct('Deadlock found when trying to get lock; try restarting transaction', 1213);
                    $this->errorInfo = ['40001', 1213, $this->getMessage()];
                }
            }
        );
        $role = new class($failures, $exception, $beforeFailure) implements HistoryProjectionRole {
            public int $calls = 0;

            public function __construct(
                private readonly int $failures,
                private readonly Throwable $exception,
                private readonly ?Closure $beforeFailure,
            ) {
            }

            public function projectRun(WorkflowRun $run): WorkflowRunSummary
            {
                $summary = (new DefaultHistoryProjectionRole())->projectRun($run);
                $this->failIfRequired();

                return $summary;
            }

            public function recordActivityStarted(
                WorkflowRun $run,
                ActivityExecution $execution,
                ActivityAttempt $attempt,
                WorkflowTask $task
            ): WorkflowRunSummary {
                $summary = (new DefaultHistoryProjectionRole())->recordActivityStarted(
                    $run,
                    $execution,
                    $attempt,
                    $task
                );
                $this->failIfRequired();

                return $summary;
            }

            private function failIfRequired(): void
            {
                if ($this->beforeFailure !== null) {
                    ($this->beforeFailure)();
                }

                if (++$this->calls <= $this->failures) {
                    throw $this->exception;
                }
            }
        };
        $this->app->instance(HistoryProjectionRole::class, $role);

        return $role;
    }

    /**
     * @return array{0: string, 1: string}
     */
    private function useSecondaryWorkflowConnection(): array
    {
        $defaultConnection = (string) config('database.default');
        $storageConnection = 'workflow-secondary';
        $database = config("database.connections.{$defaultConnection}");
        $this->assertIsArray($database);

        config()
            ->set("database.connections.{$storageConnection}", $database);
        DB::purge($storageConnection);
        config()
            ->set('workflows.storage.connection', $storageConnection);

        $this->assertNotSame(DB::connection($defaultConnection), DB::connection($storageConnection));

        return [$defaultConnection, $storageConnection];
    }

    private function countingStorage(): ExternalPayloadStorageDriver
    {
        $driver = new class() implements ExternalPayloadStorageDriver {
            public int $writes = 0;

            public function put(string $data, string $sha256, string $codec): string
            {
                return 'memory://result/' . ++$this->writes;
            }

            public function get(string $uri): string
            {
                throw new RuntimeException('Unexpected external read.');
            }

            public function delete(string $uri): void
            {
                throw new RuntimeException('Unexpected external deletion.');
            }
        };
        $this->app->instance(ExternalPayloadStoragePolicy::class, new class(
            $driver
        ) implements ExternalPayloadStoragePolicy {
            public function __construct(
                private readonly ExternalPayloadStorageDriver $driver
            ) {
            }

            public function driverFor(?string $namespace): ?ExternalPayloadStorageDriver
            {
                return $this->driver;
            }

            public function thresholdBytesFor(?string $namespace): ?int
            {
                return 1;
            }
        });

        return $driver;
    }
}

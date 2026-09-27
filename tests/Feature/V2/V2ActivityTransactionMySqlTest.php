<?php

declare(strict_types=1);

namespace Tests\Feature\V2;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Queue;
use Illuminate\Support\Str;
use RuntimeException;
use Tests\Fixtures\V2\TestGreetingWorkflow;
use Tests\Fixtures\V2\TestMySqlCountingActivity;
use Tests\TestCase;
use Throwable;
use Workflow\Serializers\CodecRegistry;
use Workflow\Serializers\Serializer;
use Workflow\V2\Enums\ActivityAttemptStatus;
use Workflow\V2\Enums\ActivityStatus;
use Workflow\V2\Enums\HistoryEventType;
use Workflow\V2\Enums\RunStatus;
use Workflow\V2\Enums\TaskStatus;
use Workflow\V2\Enums\TaskType;
use Workflow\V2\Jobs\RunActivityTask;
use Workflow\V2\Models\ActivityAttempt;
use Workflow\V2\Models\ActivityExecution;
use Workflow\V2\Models\WorkflowHistoryEvent;
use Workflow\V2\Models\WorkflowInstance;
use Workflow\V2\Models\WorkflowRun;
use Workflow\V2\Models\WorkflowTask;
use Workflow\V2\Support\ActivityOutcomeRecorder;
use Workflow\V2\Support\ActivityTaskClaimer;
use Workflow\V2\TaskWatchdog;

final class V2ActivityTransactionMySqlTest extends TestCase
{
    private const BARRIER_TIMEOUT_SECONDS = 10;

    /**
     * @var list<string>
     */
    private array $counterPaths = [];

    protected function setUp(): void
    {
        parent::setUp();

        if (DB::connection()->getDriverName() !== 'mysql') {
            $this->markTestSkipped('This contention regression requires MySQL/InnoDB.');
        }

        if (
            ! function_exists('pcntl_fork')
            || ! function_exists('posix_kill')
            || ! function_exists('stream_socket_pair')
        ) {
            $this->markTestSkipped('This contention regression requires pcntl, posix, and Unix socket pairs.');
        }

        self::stopWorkers();
        Queue::fake();

        config()
            ->set('workflows.v2.compatibility.current', 'build-a');
        config()
            ->set('workflows.v2.compatibility.supported', ['build-a']);
    }

    protected function tearDown(): void
    {
        foreach ($this->counterPaths as $counterPath) {
            if (is_file($counterPath)) {
                unlink($counterPath);
            }
        }

        parent::tearDown();
    }

    public function testClaimUsesTimeoutLockOrderWithoutADeadlock(): void
    {
        [, $run, $execution, $task] = $this->createPendingActivity();

        $child = $this->forkWithLockBarrier(static function () use ($task): array {
            [$claim] = ActivityTaskClaimer::claim($task->id, 'mysql-claim-worker');

            if ($claim === null) {
                throw new RuntimeException('The activity task was not claimed.');
            }

            return [
                'attempt_id' => $claim->attemptId(),
            ];
        });

        $firstLock = $this->awaitBarrier($child);

        DB::beginTransaction();
        try {
            if ($firstLock['table'] === 'workflow_tasks') {
                $this->lockActivityRows($execution->id, $run->id, null, lockTask: false);
                $this->sendCommand($child, [
                    'command' => 'continue_until',
                    'table' => 'activity_executions',
                ]);
                $this->assertSame('activity_executions', $this->awaitBarrier($child)['table']);
                $this->sendCommand($child, [
                    'command' => 'continue',
                ]);
                $this->lockRow('workflow_tasks', $task->id);
            } else {
                $this->lockActivityRows($execution->id, $run->id, null, taskId: $task->id);
                $this->sendCommand($child, [
                    'command' => 'continue',
                ]);
                $this->waitForBlockedLockQuery($firstLock['connection_id']);
            }

            DB::commit();
        } catch (Throwable $throwable) {
            DB::rollBack();
            $this->terminateChild($child);

            throw $throwable;
        }

        $result = $this->awaitChildResult($child);

        $this->assertTrue($result['ok'], $result['error'] ?? 'The claim process failed.');
        $this->assertSame('activity_executions', $firstLock['table']);
        $this->assertSame(
            ['activity_executions', 'workflow_runs', 'workflow_tasks'],
            array_slice($result['trace'], 0, 3),
        );
        $this->assertSame(1, ActivityAttempt::query()->where('activity_execution_id', $execution->id)->count());
        $this->assertSame(1, $this->historyCount($run->id, HistoryEventType::ActivityStarted));
    }

    public function testOutcomeUsesTimeoutLockOrderWithoutADeadlock(): void
    {
        [, $run, $execution, $task] = $this->createPendingActivity();
        [$claim] = ActivityTaskClaimer::claim($task->id, 'mysql-outcome-worker');
        $this->assertNotNull($claim);

        $attemptId = $claim->attemptId();
        $child = $this->forkWithLockBarrier(static function () use ($task, $attemptId): array {
            $outcome = ActivityOutcomeRecorder::record(
                taskId: $task->id,
                attemptId: $attemptId,
                attemptCount: 1,
                result: 'mysql-contention-result',
                throwable: null,
                maxAttempts: 1,
                backoffSeconds: 0,
            );

            return [
                'recorded' => $outcome['recorded'],
            ];
        });

        $firstLock = $this->awaitBarrier($child);

        DB::beginTransaction();
        try {
            if ($firstLock['table'] === 'workflow_tasks') {
                $this->lockActivityRows($execution->id, $run->id, $attemptId, lockTask: false);
                $this->sendCommand($child, [
                    'command' => 'continue_until',
                    'table' => 'activity_executions',
                ]);
                $this->assertSame('activity_executions', $this->awaitBarrier($child)['table']);
                $this->sendCommand($child, [
                    'command' => 'continue',
                ]);
                $this->lockRow('workflow_tasks', $task->id);
            } else {
                $this->lockActivityRows($execution->id, $run->id, $attemptId, $task->id);
                $this->sendCommand($child, [
                    'command' => 'continue',
                ]);
                $this->waitForBlockedLockQuery($firstLock['connection_id']);
            }

            DB::commit();
        } catch (Throwable $throwable) {
            DB::rollBack();
            $this->terminateChild($child);

            throw $throwable;
        }

        $result = $this->awaitChildResult($child);

        $this->assertTrue($result['ok'], $result['error'] ?? 'The outcome process failed.');
        $this->assertSame('activity_attempts', $firstLock['table']);
        $this->assertSame(
            ['activity_attempts', 'activity_executions', 'workflow_runs', 'workflow_tasks'],
            array_slice($result['trace'], 0, 4),
        );
        $this->assertSame(ActivityAttemptStatus::Completed, $claim->attempt->fresh()->status);
        $this->assertSame(1, $this->historyCount($run->id, HistoryEventType::ActivityCompleted));
    }

    public function testConcurrentLegacyWatchdogsNormalizeOneAttemptWithoutGapLockCycle(): void
    {
        [, $run, $execution, $task] = $this->createPendingActivity();
        $task->forceFill([
            'status' => TaskStatus::Leased,
            'leased_at' => now()
                ->subMinute(),
            'lease_expires_at' => now()
                ->subSecond(),
        ])->save();
        $execution->forceFill([
            'status' => ActivityStatus::Running,
            'started_at' => now()
                ->subMinute(),
        ])->save();
        $child = $this->forkWithLockBarrier(static function () use ($run): array {
            $report = TaskWatchdog::runPass(runIds: [$run->id]);

            return [
                'failures' => $report['existing_task_failures'],
                'repaired' => $report['repaired_existing_tasks'],
            ];
        });
        $firstLock = $this->awaitBarrier($child);

        DB::beginTransaction();
        try {
            $this->lockRow('activity_executions', $execution->id);
            if ($firstLock['table'] === 'activity_attempts') {
                $this->sendCommand($child, [
                    'command' => 'continue_until',
                    'table' => 'activity_executions',
                ]);
                $this->assertSame('activity_executions', $this->awaitBarrier($child)['table']);
            }
            $this->sendCommand($child, [
                'command' => 'continue',
            ]);
            $this->waitForBlockedLockQuery($firstLock['connection_id']);
            $report = TaskWatchdog::runPass(runIds: [$run->id]);
            DB::commit();
        } catch (Throwable $throwable) {
            DB::rollBack();
            $this->terminateChild($child);

            throw $throwable;
        }

        $result = $this->awaitChildResult($child);
        $this->assertTrue($result['ok'], $result['error'] ?? 'Concurrent legacy normalization failed.');
        $this->assertSame('activity_executions', $firstLock['table']);
        $this->assertSame([], $report['existing_task_failures']);
        $this->assertSame([], $result['value']['failures']);
        $this->assertSame(1, $report['repaired_existing_tasks']);
        $this->assertSame(0, $result['value']['repaired']);
        $this->assertSame(1, ActivityAttempt::query()->where('activity_execution_id', $execution->id)->count());
        $this->assertSame(TaskStatus::Ready, $task->fresh()->status);
        $this->assertSame(1, $execution->fresh()->attempt_count);
    }

    public function testWatchdogActivityRecoveryUsesCanonicalLockOrder(): void
    {
        [, $run, $execution, $task] = $this->createPendingActivity();
        [$claim] = ActivityTaskClaimer::claim($task->id, 'mysql-watchdog-worker');
        $this->assertNotNull($claim);

        $task->forceFill([
            'lease_expires_at' => now()
                ->subSecond(),
        ])->save();
        $attemptId = $claim->attemptId();

        $child = $this->forkWithLockBarrier(static function () use ($run): array {
            $report = TaskWatchdog::runPass(runIds: [$run->id]);

            return [
                'failures' => $report['existing_task_failures'],
                'repaired' => $report['repaired_existing_tasks'],
            ];
        });

        $firstLock = $this->awaitBarrier($child);

        DB::beginTransaction();
        try {
            if ($firstLock['table'] === 'workflow_tasks') {
                $this->lockRow('activity_attempts', $attemptId);
                $this->sendCommand($child, [
                    'command' => 'continue_until',
                    'table' => 'activity_attempts',
                ]);
                $this->assertSame('activity_attempts', $this->awaitBarrier($child)['table']);
                $this->sendCommand($child, [
                    'command' => 'continue',
                ]);
                $this->lockRow('workflow_tasks', $task->id);
            } else {
                $this->lockActivityRows($execution->id, $run->id, $attemptId, $task->id);
                $this->sendCommand($child, [
                    'command' => 'continue',
                ]);
                $this->waitForBlockedLockQuery($firstLock['connection_id']);
            }

            DB::commit();
        } catch (Throwable $throwable) {
            DB::rollBack();
            $this->terminateChild($child);

            throw $throwable;
        }

        $result = $this->awaitChildResult($child);

        $this->assertTrue($result['ok'], $result['error'] ?? 'The watchdog process failed.');
        $this->assertSame('activity_attempts', $firstLock['table']);
        $this->assertSame([], $result['value']['failures']);
        $this->assertSame(1, $result['value']['repaired']);
        $this->assertSame(TaskStatus::Ready, $task->fresh()->status);
        $this->assertSame(ActivityAttemptStatus::Expired, $claim->attempt->fresh()->status);
    }

    public function testClaimRetriesAnUnrelatedDeadlockWithoutDuplicatingTheActivity(): void
    {
        [$counterPath, $run, $execution, $task] = $this->createPendingActivity();

        $child = $this->forkWithLockBarrier(
            static function () use ($task): array {
                (new RunActivityTask($task->id))->handle();

                return [];
            },
            pauseTable: 'activity_executions',
        );

        $firstLock = $this->awaitBarrier($child);

        if ($firstLock['table'] !== 'activity_executions') {
            $this->terminateChild($child);
        }
        $this->assertSame('activity_executions', $firstLock['table']);

        DB::beginTransaction();
        try {
            $this->lockRow('workflow_tasks', $task->id);
            $weightIds = $this->addDeadlockVictimWeight($run->id);
            $this->sendCommand($child, [
                'command' => 'continue_until',
                'table' => 'workflow_tasks',
            ]);
            $this->assertSame('workflow_tasks', $this->awaitBarrier($child)['table']);
            $this->sendCommand($child, [
                'command' => 'continue',
            ]);
            $this->lockRow('activity_executions', $execution->id);
            DB::table('workflow_history_events')->whereIn('id', $weightIds)->delete();
            DB::commit();
        } catch (Throwable $throwable) {
            DB::rollBack();
            $this->terminateChild($child);

            throw $throwable;
        }

        $result = $this->awaitChildResult($child);

        $this->assertTrue($result['ok'], $result['error'] ?? 'The activity process failed.');
        $this->assertGreaterThanOrEqual(2, $this->traceCount($result['trace'], 'activity_executions'));
        $this->assertCompletedOnce($counterPath, $run->id, $execution->id, $task->id);
    }

    public function testOutcomeRetriesAnUnrelatedDeadlockWithoutReinvokingTheActivity(): void
    {
        [$counterPath, $run, $execution, $task] = $this->createPendingActivity();

        $child = $this->forkWithLockBarrier(
            static function () use ($task): array {
                (new RunActivityTask($task->id))->handle();

                return [];
            },
            pauseTable: 'activity_attempts',
        );

        $firstLock = $this->awaitBarrier($child);

        if ($firstLock['table'] !== 'activity_attempts') {
            $this->terminateChild($child);
        }
        $this->assertSame('activity_attempts', $firstLock['table']);

        DB::beginTransaction();
        try {
            $this->lockRow('workflow_tasks', $task->id);
            $weightIds = $this->addDeadlockVictimWeight($run->id);
            $this->sendCommand($child, [
                'command' => 'continue_until',
                'table' => 'workflow_tasks',
            ]);
            $this->assertSame('workflow_tasks', $this->awaitBarrier($child)['table']);
            $this->sendCommand($child, [
                'command' => 'continue',
            ]);

            $attemptId = ActivityExecution::query()->whereKey($execution->id)->value('current_attempt_id');
            $this->assertIsString($attemptId);
            $this->lockRow('activity_attempts', $attemptId);

            DB::table('workflow_history_events')->whereIn('id', $weightIds)->delete();
            DB::commit();
        } catch (Throwable $throwable) {
            DB::rollBack();
            $this->terminateChild($child);

            throw $throwable;
        }

        $result = $this->awaitChildResult($child);

        $this->assertTrue($result['ok'], $result['error'] ?? 'The activity process failed.');
        $this->assertGreaterThanOrEqual(2, $this->traceCount($result['trace'], 'activity_attempts'));
        $this->assertCompletedOnce($counterPath, $run->id, $execution->id, $task->id);
    }

    /**
     * @param callable(): array<string, mixed> $operation
     * @return array{pid: int, socket: resource}
     */
    private function forkWithLockBarrier(callable $operation, ?string $pauseTable = null): array
    {
        $sockets = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);

        if ($sockets === false) {
            throw new RuntimeException('Unable to create the contention barrier socket pair.');
        }

        $pid = pcntl_fork();

        if ($pid === -1) {
            fclose($sockets[0]);
            fclose($sockets[1]);

            throw new RuntimeException('Unable to fork the contention worker.');
        }

        if ($pid === 0) {
            fclose($sockets[0]);
            $this->runChild($sockets[1], $operation, $pauseTable);
        }

        fclose($sockets[1]);

        return [
            'pid' => $pid,
            'socket' => $sockets[0],
        ];
    }

    /**
     * @param resource $socket
     * @param callable(): array<string, mixed> $operation
     */
    private function runChild($socket, callable $operation, ?string $pauseTable): never
    {
        $trace = [];
        $pauseNextLock = $pauseTable === null;
        $nextPauseTable = $pauseTable;

        try {
            DB::purge();
            $connection = DB::connection();
            $connectionRow = $connection->selectOne('select connection_id() as id');

            if (! is_object($connectionRow) || ! property_exists($connectionRow, 'id')) {
                throw new RuntimeException('MySQL did not return the contention worker connection ID.');
            }

            $connectionId = (int) $connectionRow->id;
            $connection->beforeExecuting(function (string $query) use (
                $socket,
                $connectionId,
                &$trace,
                &$pauseNextLock,
                &$nextPauseTable,
            ): void {
                $table = $this->lockedActivityTable($query);

                if ($table === null) {
                    return;
                }

                $trace[] = $table;

                if (! $pauseNextLock && $nextPauseTable !== $table) {
                    return;
                }

                $pauseNextLock = false;
                $nextPauseTable = null;
                $this->writeMessage($socket, [
                    'type' => 'barrier',
                    'table' => $table,
                    'connection_id' => $connectionId,
                ]);
                $command = $this->readMessage($socket);

                if (($command['command'] ?? null) === 'continue_until') {
                    $target = $command['table'] ?? null;

                    if (! is_string($target)) {
                        throw new RuntimeException('The barrier command did not include a target table.');
                    }

                    $nextPauseTable = $target;
                } elseif (($command['command'] ?? null) !== 'continue') {
                    throw new RuntimeException('The contention worker received an invalid barrier command.');
                }
            });

            $value = $operation();
            $this->writeMessage($socket, [
                'type' => 'result',
                'ok' => true,
                'trace' => $trace,
                'value' => $value,
            ]);
        } catch (Throwable $throwable) {
            $this->writeMessage($socket, [
                'type' => 'result',
                'ok' => false,
                'trace' => $trace,
                'error' => sprintf('%s: %s', $throwable::class, $throwable->getMessage()),
            ]);
        } finally {
            fclose($socket);
        }

        exit(0);
    }

    private function lockedActivityTable(string $query): ?string
    {
        $normalized = strtolower($query);

        if (! str_contains($normalized, 'for update')) {
            return null;
        }

        foreach (['activity_attempts', 'activity_executions', 'workflow_runs', 'workflow_tasks'] as $table) {
            if (str_contains($normalized, $table)) {
                return $table;
            }
        }

        return null;
    }

    /**
     * @param array{pid: int, socket: resource} $child
     * @return array{type: string, table: string, connection_id: int}
     */
    private function awaitBarrier(array $child): array
    {
        $message = $this->readMessage($child['socket']);

        if (
            ($message['type'] ?? null) !== 'barrier'
            || ! is_string($message['table'] ?? null)
            || ! is_int($message['connection_id'] ?? null)
        ) {
            $this->terminateChild($child);
            throw new RuntimeException('The contention worker exited before reaching its database barrier.');
        }

        return $message;
    }

    /**
     * @param array{pid: int, socket: resource} $child
     * @return array{ok: bool, trace: list<string>, value?: array<string, mixed>, error?: string}
     */
    private function awaitChildResult(array $child): array
    {
        $message = $this->readMessage($child['socket']);

        if (($message['type'] ?? null) !== 'result') {
            $this->terminateChild($child);
            throw new RuntimeException('The contention worker did not return a result.');
        }

        fclose($child['socket']);
        pcntl_waitpid($child['pid'], $status);

        return $message;
    }

    /**
     * @param array{pid: int, socket: resource} $child
     * @param array<string, string> $command
     */
    private function sendCommand(array $child, array $command): void
    {
        $this->writeMessage($child['socket'], $command);
    }

    /**
     * @param resource $socket
     * @param array<string, mixed> $message
     */
    private function writeMessage($socket, array $message): void
    {
        $encoded = json_encode($message, JSON_THROW_ON_ERROR) . "\n";
        $written = fwrite($socket, $encoded);

        if ($written !== strlen($encoded)) {
            throw new RuntimeException('Unable to write the complete contention barrier message.');
        }
    }

    /**
     * @param resource $socket
     * @return array<string, mixed>
     */
    private function readMessage($socket): array
    {
        stream_set_timeout($socket, self::BARRIER_TIMEOUT_SECONDS);
        $line = fgets($socket);
        $metadata = stream_get_meta_data($socket);

        if ($line === false) {
            $reason = ($metadata['timed_out'] ?? false) === true ? 'timed out' : 'closed';
            throw new RuntimeException("The contention barrier {$reason} before receiving a message.");
        }

        $message = json_decode($line, true, flags: JSON_THROW_ON_ERROR);

        if (! is_array($message)) {
            throw new RuntimeException('The contention barrier returned a non-object message.');
        }

        return $message;
    }

    /**
     * @param array{pid: int, socket: resource} $child
     */
    private function terminateChild(array $child): void
    {
        posix_kill($child['pid'], SIGTERM);
        pcntl_waitpid($child['pid'], $status);
        fclose($child['socket']);
    }

    private function waitForBlockedLockQuery(int $connectionId): void
    {
        $deadline = hrtime(true) + (self::BARRIER_TIMEOUT_SECONDS * 1_000_000_000);
        $observed = [];

        do {
            $processes = DB::select('show full processlist');

            foreach ($processes as $process) {
                $details = array_change_key_case((array) $process, CASE_LOWER);
                $processId = isset($details['id']) ? (int) $details['id'] : null;
                $command = $details['command'] ?? null;
                $query = $details['info'] ?? null;

                if ($processId === $connectionId) {
                    $observed = $details;
                }

                if (
                    $processId === $connectionId
                    && in_array($command, ['Execute', 'Query'], true)
                    && is_string($query)
                    && str_contains(strtolower($query), 'for update')
                ) {
                    return;
                }
            }
        } while (hrtime(true) < $deadline);

        $diagnostics = json_encode($observed, JSON_UNESCAPED_SLASHES | JSON_INVALID_UTF8_SUBSTITUTE);

        throw new RuntimeException(
            sprintf(
                'MySQL connection %d did not submit its blocked FOR UPDATE query before the barrier timeout. Process: %s',
                $connectionId,
                $diagnostics === false ? 'unavailable' : $diagnostics,
            ),
        );
    }

    private function lockActivityRows(
        string $executionId,
        string $runId,
        ?string $attemptId,
        ?string $taskId = null,
        bool $lockTask = true,
    ): void {
        if ($attemptId !== null) {
            $this->lockRow('activity_attempts', $attemptId);
        }

        $this->lockRow('activity_executions', $executionId);
        $this->lockRow('workflow_runs', $runId);

        if ($lockTask && $taskId !== null) {
            $this->lockRow('workflow_tasks', $taskId);
        }
    }

    private function lockRow(string $table, string $id): void
    {
        $row = DB::table($table)->where('id', $id)->lockForUpdate()->first();

        if ($row === null) {
            throw new RuntimeException("The contention fixture row [{$table}:{$id}] does not exist.");
        }
    }

    /**
     * @return list<string>
     */
    private function addDeadlockVictimWeight(string $runId): array
    {
        $ids = [];
        $timestamp = now();

        for ($index = 0; $index < 8; $index++) {
            $ids[] = (string) Str::ulid();
        }

        DB::table('workflow_history_events')->insert(array_map(
            static fn (string $id, int $index): array => [
                'id' => $id,
                'workflow_run_id' => $runId,
                'sequence' => 4_000_000_000 + $index,
                'event_type' => 'mysql_contention_weight',
                'payload' => null,
                'workflow_task_id' => null,
                'workflow_command_id' => null,
                'recorded_at' => $timestamp,
                'created_at' => $timestamp,
                'updated_at' => $timestamp,
            ],
            $ids,
            array_keys($ids),
        ));

        return $ids;
    }

    private function assertCompletedOnce(
        string $counterPath,
        string $runId,
        string $executionId,
        string $taskId,
    ): void {
        $calls = file($counterPath, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES);

        $this->assertSame(['called'], $calls);
        $this->assertSame(1, ActivityAttempt::query()->where('activity_execution_id', $executionId)->count());
        $this->assertSame(ActivityStatus::Completed, ActivityExecution::query()->findOrFail($executionId)->status);
        $this->assertSame(TaskStatus::Completed, WorkflowTask::query()->findOrFail($taskId)->status);
        $this->assertSame(1, $this->historyCount($runId, HistoryEventType::ActivityStarted));
        $this->assertSame(1, $this->historyCount($runId, HistoryEventType::ActivityCompleted));
    }

    private function historyCount(string $runId, HistoryEventType $eventType): int
    {
        return WorkflowHistoryEvent::query()
            ->where('workflow_run_id', $runId)
            ->where('event_type', $eventType->value)
            ->count();
    }

    /**
     * @param list<string> $trace
     */
    private function traceCount(array $trace, string $table): int
    {
        return count(array_filter($trace, static fn (string $lockedTable): bool => $lockedTable === $table));
    }

    /**
     * @return array{string, WorkflowRun, ActivityExecution, WorkflowTask}
     */
    private function createPendingActivity(): array
    {
        $counterPath = tempnam(sys_get_temp_dir(), 'workflow-mysql-activity-');

        if ($counterPath === false) {
            throw new RuntimeException('Unable to create the activity invocation counter.');
        }

        $this->counterPaths[] = $counterPath;
        $instanceId = 'mysql-contention-' . Str::lower((string) Str::ulid());
        $runId = (string) Str::ulid();
        $executionId = (string) Str::ulid();
        $taskId = (string) Str::ulid();
        $codec = CodecRegistry::defaultCodec();

        WorkflowInstance::query()->create([
            'id' => $instanceId,
            'workflow_class' => TestGreetingWorkflow::class,
            'workflow_type' => 'mysql-contention-workflow',
            'current_run_id' => $runId,
            'run_count' => 1,
            'reserved_at' => now()
                ->subMinute(),
            'started_at' => now()
                ->subMinute(),
        ]);

        $run = WorkflowRun::query()->create([
            'id' => $runId,
            'workflow_instance_id' => $instanceId,
            'run_number' => 1,
            'workflow_class' => TestGreetingWorkflow::class,
            'workflow_type' => 'mysql-contention-workflow',
            'status' => RunStatus::Waiting->value,
            'payload_codec' => $codec,
            'arguments' => Serializer::serializeWithCodec($codec, []),
            'connection' => 'redis',
            'queue' => 'default',
            'compatibility' => 'build-a',
            'started_at' => now()
                ->subMinute(),
            'last_progress_at' => now()
                ->subSeconds(30),
        ]);

        $execution = ActivityExecution::query()->create([
            'id' => $executionId,
            'workflow_run_id' => $runId,
            'activity_class' => TestMySqlCountingActivity::class,
            'activity_type' => TestMySqlCountingActivity::class,
            'sequence' => 1,
            'status' => ActivityStatus::Pending->value,
            'payload_codec' => $codec,
            'arguments' => Serializer::serializeWithCodec($codec, [$counterPath]),
            'connection' => 'redis',
            'queue' => 'default',
            'attempt_count' => 0,
            'retry_policy' => [
                'max_attempts' => 1,
            ],
        ]);

        $task = WorkflowTask::query()->create([
            'id' => $taskId,
            'workflow_run_id' => $runId,
            'task_type' => TaskType::Activity->value,
            'status' => TaskStatus::Ready->value,
            'available_at' => now()
                ->subSecond(),
            'payload' => [
                'activity_execution_id' => $executionId,
            ],
            'connection' => 'redis',
            'queue' => 'default',
            'compatibility' => 'build-a',
            'attempt_count' => 0,
        ]);

        return [$counterPath, $run, $execution, $task];
    }
}

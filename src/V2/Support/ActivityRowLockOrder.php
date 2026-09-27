<?php

declare(strict_types=1);

namespace Workflow\V2\Support;

use Workflow\V2\Enums\ActivityStatus;
use Workflow\V2\Models\ActivityAttempt;
use Workflow\V2\Models\ActivityExecution;

/**
 * Acquires activity rows in the canonical order used by workers and timeout
 * enforcement: attempt, execution, workflow run, then workflow task.
 *
 * This helper owns the shared attempt/execution prefix. Callers acquire the
 * related run and task only after these rows have been returned.
 */
final class ActivityRowLockOrder
{
    /**
     * @return array{
     *     attempt: ActivityAttempt|null,
     *     execution: ActivityExecution|null,
     *     snapshot_attempt_id: string|null
     * }
     */
    public static function lockForExecution(string $executionId, bool $includeClosedAttempts = false): array
    {
        /** @var ActivityExecution|null $snapshot */
        $snapshot = ActivityExecution::query()->find($executionId);
        $snapshotAttemptId = $snapshot instanceof ActivityExecution
            ? self::currentAttemptId($snapshot, $includeClosedAttempts)
            : null;

        /** @var ActivityAttempt|null $attempt */
        $attempt = $snapshotAttemptId === null
            ? null
            : ActivityAttempt::query()
                ->lockForUpdate()
                ->find($snapshotAttemptId);

        // Legacy leased executions can lack the pointer and retain the schema's
        // default counter of one, or a counter explicitly initialized to zero.
        // Prelock the normalizer's fallback before execution, including any
        // existing row left by a previous normalization.
        if (
            $includeClosedAttempts
            && $snapshot instanceof ActivityExecution
            && $snapshot->status === ActivityStatus::Running
            && in_array($snapshot->attempt_count, [0, 1], true)
            && $snapshotAttemptId === null
        ) {
            $fallback = ActivityAttempt::query()
                ->where('activity_execution_id', $executionId)
                ->where('attempt_number', 1)
                ->first();
            // Do not hold a missing-row gap lock while waiting for execution:
            // another normalizer may need that gap to insert attempt one.
            $attempt = $fallback instanceof ActivityAttempt
                ? ActivityAttempt::query()->lockForUpdate()->find($fallback->id)
                : null;
        }

        /** @var ActivityExecution|null $execution */
        $execution = ActivityExecution::query()
            ->lockForUpdate()
            ->find($executionId);

        return [
            'attempt' => $attempt,
            'execution' => $execution,
            'snapshot_attempt_id' => $snapshotAttemptId,
        ];
    }

    /**
     * @return array{
     *     attempt: ActivityAttempt|null,
     *     execution: ActivityExecution|null,
     *     snapshot_attempt_id: string
     * }
     */
    public static function lockForAttempt(string $attemptId): array
    {
        /** @var ActivityAttempt|null $attempt */
        $attempt = ActivityAttempt::query()
            ->lockForUpdate()
            ->find($attemptId);

        /** @var ActivityExecution|null $execution */
        $execution = $attempt instanceof ActivityAttempt
            ? ActivityExecution::query()
                ->lockForUpdate()
                ->find($attempt->activity_execution_id)
            : null;

        return [
            'attempt' => $attempt,
            'execution' => $execution,
            'snapshot_attempt_id' => $attemptId,
        ];
    }

    /**
     * A late outcome may reconcile cancellation of a newer current attempt.
     * Lock both identities before execution, in the same deterministic order.
     * Callers must reject a changed current-attempt snapshot before writing.
     *
     * @return array{attempt: ActivityAttempt|null, current_attempt: ActivityAttempt|null, execution: ActivityExecution|null, snapshot_attempt_id: string|null}
     */
    public static function lockForOutcome(string $executionId, string $attemptId): array
    {
        $snapshot = ActivityExecution::query()->find($executionId);
        $currentId = $snapshot?->current_attempt_id;
        $currentId = is_string($currentId) && $currentId !== '' ? $currentId : null;
        $ids = array_unique(array_filter([$attemptId, $currentId]));
        sort($ids, SORT_STRING);
        $attempts = [];

        foreach ($ids as $id) {
            $attempts[$id] = ActivityAttempt::query()->lockForUpdate()->find($id);
        }

        return [
            'attempt' => $attempts[$attemptId] ?? null,
            'current_attempt' => $currentId === null ? null : ($attempts[$currentId] ?? null),
            'execution' => ActivityExecution::query()->lockForUpdate()->find($executionId),
            'snapshot_attempt_id' => $currentId,
        ];
    }

    private static function currentAttemptId(ActivityExecution $execution, bool $includeClosedAttempts): ?string
    {
        if (! $includeClosedAttempts && $execution->status !== ActivityStatus::Running) {
            return null;
        }

        $attemptId = $execution->current_attempt_id;

        return is_string($attemptId) && $attemptId !== ''
            ? $attemptId
            : null;
    }
}

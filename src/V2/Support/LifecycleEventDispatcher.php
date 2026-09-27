<?php

declare(strict_types=1);

namespace Workflow\V2\Support;

use Illuminate\Database\Eloquent\Model;
use Workflow\Events\ActivityCompleted as LegacyActivityCompleted;
use Workflow\Events\ActivityFailed as LegacyActivityFailed;
use Workflow\Events\ActivityStarted as LegacyActivityStarted;
use Workflow\Events\StateChanged;
use Workflow\Events\WorkflowCompleted as LegacyWorkflowCompleted;
use Workflow\Events\WorkflowFailed as LegacyWorkflowFailed;
use Workflow\Events\WorkflowStarted as LegacyWorkflowStarted;
use Workflow\States\WorkflowCompletedStatus;
use Workflow\States\WorkflowFailedStatus;
use Workflow\States\WorkflowRunningStatus;
use Workflow\V2\Events\ActivityCompleted;
use Workflow\V2\Events\ActivityFailed;
use Workflow\V2\Events\ActivityStarted;
use Workflow\V2\Events\FailureRecorded;
use Workflow\V2\Events\WorkflowCompleted;
use Workflow\V2\Events\WorkflowFailed;
use Workflow\V2\Events\WorkflowStarted;
use Workflow\V2\Models\WorkflowRun;

/**
 * Dispatches V2 lifecycle events from committed durable truth.
 *
 * Lifecycle publication is deferred until the outermost transaction commits,
 * so rolled-back and retried attempts cannot leak duplicate public events.
 *
 * Events carry scalar identity values captured eagerly from the models and
 * are dispatched synchronously within the same request/job that committed
 * the state change.
 */
final class LifecycleEventDispatcher
{
    public static function workflowStarted(WorkflowRun $run): void
    {
        $instanceId = (string) $run->instance?->id;
        $workflowClass = (string) $run->workflow_class;
        $committedAt = now()
            ->toIso8601String();
        $stateModel = self::snapshotRun($run);

        self::publish($run, [
            new WorkflowStarted(
                $instanceId,
                (string) $run->id,
                (string) ($run->workflow_type ?? $run->workflow_class),
                $workflowClass,
                $committedAt,
            ),
            new LegacyWorkflowStarted($instanceId, $workflowClass, '[]', $committedAt),
            new StateChanged(null, new WorkflowRunningStatus($stateModel), $stateModel, 'status'),
        ]);
    }

    public static function workflowCompleted(WorkflowRun $run): void
    {
        $instanceId = (string) $run->instance?->id;
        $workflowClass = (string) $run->workflow_class;
        $committedAt = now()
            ->toIso8601String();
        $stateModel = self::snapshotRun($run);

        self::publish($run, [
            new WorkflowCompleted(
                $instanceId,
                (string) $run->id,
                (string) ($run->workflow_type ?? $run->workflow_class),
                $workflowClass,
                $committedAt,
            ),
            new LegacyWorkflowCompleted($instanceId, '', $committedAt),
            new StateChanged(
                new WorkflowRunningStatus($stateModel),
                new WorkflowCompletedStatus($stateModel),
                $stateModel,
                'status',
            ),
        ]);
    }

    public static function workflowFailed(WorkflowRun $run, string $exceptionClass, string $message): void
    {
        $instanceId = (string) $run->instance?->id;
        $workflowClass = (string) $run->workflow_class;
        $committedAt = now()
            ->toIso8601String();
        $stateModel = self::snapshotRun($run);

        self::publish($run, [
            new WorkflowFailed(
                $instanceId,
                (string) $run->id,
                (string) ($run->workflow_type ?? $run->workflow_class),
                $workflowClass,
                $exceptionClass,
                $message,
                $committedAt,
            ),
            new LegacyWorkflowFailed($instanceId, $exceptionClass . ': ' . $message, $committedAt),
            new StateChanged(
                new WorkflowRunningStatus($stateModel),
                new WorkflowFailedStatus($stateModel),
                $stateModel,
                'status',
            ),
        ]);
    }

    public static function activityStarted(
        WorkflowRun $run,
        string $activityExecutionId,
        string $activityType,
        string $activityClass,
        int $sequence,
        int $attemptNumber,
    ): void {
        $instanceId = (string) $run->instance?->id;
        $committedAt = now()
            ->toIso8601String();

        self::publish($run, [
            new ActivityStarted(
                $instanceId,
                (string) $run->id,
                $activityExecutionId,
                $activityType,
                $activityClass,
                $sequence,
                $attemptNumber,
                $committedAt,
            ),
            new LegacyActivityStarted(
                $instanceId,
                $activityExecutionId,
                $activityClass,
                $sequence,
                '[]',
                $committedAt,
            ),
        ]);
    }

    public static function activityCompleted(
        WorkflowRun $run,
        string $activityExecutionId,
        string $activityType,
        string $activityClass,
        int $sequence,
        int $attemptNumber,
    ): void {
        $instanceId = (string) $run->instance?->id;
        $committedAt = now()
            ->toIso8601String();

        self::publish($run, [
            new ActivityCompleted(
                $instanceId,
                (string) $run->id,
                $activityExecutionId,
                $activityType,
                $activityClass,
                $sequence,
                $attemptNumber,
                $committedAt,
            ),
            new LegacyActivityCompleted(
                $instanceId,
                $activityExecutionId,
                '',
                $committedAt,
                $activityClass,
                $sequence,
            ),
        ]);
    }

    public static function activityFailed(
        WorkflowRun $run,
        string $activityExecutionId,
        string $activityType,
        string $activityClass,
        int $sequence,
        int $attemptNumber,
        string $exceptionClass,
        string $message,
    ): void {
        $instanceId = (string) $run->instance?->id;
        $committedAt = now()
            ->toIso8601String();

        self::publish($run, [
            new ActivityFailed(
                $instanceId,
                (string) $run->id,
                $activityExecutionId,
                $activityType,
                $activityClass,
                $sequence,
                $attemptNumber,
                $exceptionClass,
                $message,
                $committedAt,
            ),
            new LegacyActivityFailed(
                $instanceId,
                $activityExecutionId,
                $exceptionClass . ': ' . $message,
                $committedAt,
                $activityClass,
                $sequence,
            ),
        ]);
    }

    public static function failureRecorded(
        WorkflowRun $run,
        string $failureId,
        string $sourceKind,
        string $sourceId,
        string $exceptionClass,
        string $message,
    ): void {
        $instanceId = (string) $run->instance?->id;
        $committedAt = now()
            ->toIso8601String();

        self::publish($run, [
            new FailureRecorded(
                $instanceId,
                (string) $run->id,
                $failureId,
                $sourceKind,
                $sourceId,
                $exceptionClass,
                $message,
                $committedAt,
            ),
        ]);
    }

    /**
     * @param  list<object>  $events
     */
    private static function publish(WorkflowRun $run, array $events): void
    {
        $publish = static function () use ($events): void {
            foreach ($events as $event) {
                event($event);
            }
        };

        $connection = $run->getConnection();

        if ($connection->transactionLevel() > 0) {
            $connection->afterCommit($publish);

            return;
        }

        $publish();
    }

    private static function snapshotRun(WorkflowRun $run): WorkflowRun
    {
        /** @var WorkflowRun $snapshot */
        $snapshot = clone $run;
        $snapshot->setRelations([]);

        if ($run->relationLoaded('instance')) {
            $instance = $run->getRelation('instance');

            $snapshot->setRelation(
                'instance',
                $instance instanceof Model
                    ? $instance->newInstance($instance->getAttributes(), $instance->exists)
                    : $instance,
            );
        }

        return $snapshot;
    }
}

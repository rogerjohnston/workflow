<?php

declare(strict_types=1);

namespace Tests\Unit\V2;

use Illuminate\Database\Eloquent\Collection;
use Orchestra\Testbench\TestCase;
use PHPUnit\Framework\Attributes\DataProvider;
use Workflow\V2\Enums\HistoryEventType;
use Workflow\V2\Models\WorkflowHistoryEvent;
use Workflow\V2\Models\WorkflowRun;
use Workflow\V2\Support\ActivityAttemptSnapshots;
use Workflow\V2\Support\ActivitySnapshot;
use Workflow\V2\Support\HistoryTimeline;
use Workflow\V2\Support\RunTimerView;

final class ProjectionPayloadReadTest extends TestCase
{
    public function testTimerProjectionReadsEachEventPayloadOnceAndSeesFreshMutations(): void
    {
        $event = $this->countingEvent(HistoryEventType::TimerScheduled, [
            'timer_id' => 'timer-1',
            'sequence' => 7,
            'delay_seconds' => 10,
            'fire_at' => '2026-09-24T12:00:10+00:00',
        ]);
        $run = $this->runWithHistoryEvent($event, ['timers']);

        $this->assertSame(10, RunTimerView::timersForRun($run)[0]['delay_seconds']);
        $this->assertSame(1, $event->payloadReads);

        $event->payload = [
            'timer_id' => 'timer-1',
            'sequence' => 7,
            'delay_seconds' => 20,
            'fire_at' => '2026-09-24T12:00:20+00:00',
        ];

        $this->assertSame(20, RunTimerView::timersForRun($run)[0]['delay_seconds']);
        $this->assertSame(2, $event->payloadReads);
    }

    public function testTimelineProjectionReadsEachEventPayloadOnceAndSeesFreshMutations(): void
    {
        $event = $this->countingEvent(HistoryEventType::ChildRunCompleted, [
            'child_workflow_run_id' => 'child-run-1',
            'child_workflow_type' => 'invoice-child',
        ]);

        $this->assertSame('child-run-1', HistoryTimeline::fromChildResolutionEvent($event)['source_id']);
        $this->assertSame(1, $event->payloadReads);

        $event->payload = [
            'child_workflow_run_id' => 'child-run-2',
            'child_workflow_type' => 'invoice-child',
        ];

        $this->assertSame('child-run-2', HistoryTimeline::fromChildResolutionEvent($event)['source_id']);
        $this->assertSame(2, $event->payloadReads);
    }

    #[DataProvider('timelinePayloadSourceIds')]
    public function testTimelineProjectionPreservesPayloadSourceIdentityAndSeesFreshMutations(
        HistoryEventType $type,
        string $payloadKey,
        string $firstId,
        string $secondId,
    ): void {
        $event = $this->countingEvent($type, [
            $payloadKey => $firstId,
        ]);
        $run = $this->runWithHistoryEvent($event, [
            'commands',
            'tasks',
            'activityExecutions',
            'timers',
            'failures',
        ]);

        $this->assertSame($firstId, HistoryTimeline::fromHistory($run)[0]['source_id']);

        $event->payload = [
            $payloadKey => $secondId,
        ];

        $this->assertSame($secondId, HistoryTimeline::fromHistory($run)[0]['source_id']);
    }

    public function testActivityProjectionReadsEachEventPayloadOnceAndSeesFreshMutations(): void
    {
        $event = $this->countingEvent(HistoryEventType::ActivityStarted, [
            'activity_execution_id' => 'activity-1',
            'activity_type' => 'first-activity',
        ]);

        $this->assertSame('first-activity', ActivitySnapshot::fromEvent($event)['type'] ?? null);
        $this->assertSame(1, $event->payloadReads);

        $event->payload = [
            'activity_execution_id' => 'activity-1',
            'activity_type' => 'second-activity',
        ];

        $this->assertSame('second-activity', ActivitySnapshot::fromEvent($event)['type'] ?? null);
        $this->assertSame(2, $event->payloadReads);
    }

    public function testActivityAttemptProjectionReadsEachEventPayloadOnceAndSeesFreshMutations(): void
    {
        $event = $this->countingEvent(HistoryEventType::ActivityStarted, [
            'activity_execution_id' => 'activity-1',
            'activity_attempt_id' => 'attempt-1',
        ]);
        $run = $this->runWithHistoryEvent($event, ['activityExecutions']);

        $this->assertSame('attempt-1', ActivityAttemptSnapshots::forRun($run)['activity-1'][0]['id']);
        $this->assertSame(1, $event->payloadReads);

        $event->payload = [
            'activity_execution_id' => 'activity-2',
            'activity_attempt_id' => 'attempt-2',
        ];

        $this->assertSame('attempt-2', ActivityAttemptSnapshots::forRun($run)['activity-2'][0]['id']);
        $this->assertSame(2, $event->payloadReads);
    }

    /**
     * @return iterable<string, array{HistoryEventType, string, string, string}>
     */
    public static function timelinePayloadSourceIds(): iterable
    {
        yield 'service call' => [HistoryEventType::ServiceCallStarted, 'service_call_id', 'call-1', 'call-2'];
        yield 'signal wait' => [
            HistoryEventType::SignalWaitOpened,
            'signal_wait_id',
            'signal-wait-1',
            'signal-wait-2',
        ];
        yield 'condition wait' => [
            HistoryEventType::ConditionWaitOpened,
            'condition_wait_id',
            'condition-wait-1',
            'condition-wait-2',
        ];
        yield 'version marker' => [HistoryEventType::VersionMarkerRecorded, 'change_id', 'change-1', 'change-2'];
        yield 'workflow failure' => [HistoryEventType::FailureHandled, 'failure_id', 'failure-1', 'failure-2'];
    }

    /**
     * @param array<string, mixed> $payload
     */
    private function countingEvent(HistoryEventType $type, array $payload): ProjectionPayloadCountingEvent
    {
        $event = new ProjectionPayloadCountingEvent();
        $event->forceFill([
            'id' => 'event-1',
            'sequence' => 1,
            'event_type' => $type->value,
            'payload' => $payload,
        ]);

        return $event;
    }

    /**
     * @param list<string> $emptyRelations
     */
    private function runWithHistoryEvent(WorkflowHistoryEvent $event, array $emptyRelations): WorkflowRun
    {
        $run = new WorkflowRun();
        $run->setRelation('historyEvents', new Collection([$event]));

        foreach ($emptyRelations as $relation) {
            $run->setRelation($relation, new Collection());
        }

        return $run;
    }
}

final class ProjectionPayloadCountingEvent extends WorkflowHistoryEvent
{
    public int $payloadReads = 0;

    public function getAttribute($key)
    {
        if ($key === 'payload') {
            $this->payloadReads++;
        }

        return parent::getAttribute($key);
    }
}

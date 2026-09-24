<?php

declare(strict_types=1);

namespace Tests\Unit\V2;

use Illuminate\Database\Eloquent\Collection as EloquentCollection;
use PHPUnit\Framework\TestCase;
use Tests\Fixtures\V2\TestChildGreetingWorkflow;
use Tests\Fixtures\V2\TestMixedEntryActivity;
use Workflow\V2\Enums\HistoryEventType;
use Workflow\V2\Exceptions\HistoryEventShapeMismatchException;
use Workflow\V2\Models\WorkflowHistoryEvent;
use Workflow\V2\Models\WorkflowRun;
use Workflow\V2\Support\WorkflowStepHistory;

final class WorkflowStepHistoryTest extends TestCase
{
    public function testActivityTypeDetailAcceptsCanonicalTypeAliasForYieldedClass(): void
    {
        $run = $this->runWithHistoryEvents([
            $this->historyEvent(HistoryEventType::ActivityScheduled, [
                'sequence' => 1,
                'activity_type' => 'test-mixed-entry-activity',
                'activity_class' => TestMixedEntryActivity::class,
            ]),
        ]);

        WorkflowStepHistory::assertCompatible($run, 1, WorkflowStepHistory::ACTIVITY, [
            'activity_type' => TestMixedEntryActivity::class,
        ]);

        $this->addToAssertionCount(1);
    }

    public function testActivityTypeDetailRejectsMutatedTypeEvenWhenClassFallbackMatches(): void
    {
        $run = $this->runWithHistoryEvents([
            $this->historyEvent(HistoryEventType::ActivityScheduled, [
                'sequence' => 1,
                'activity_type' => 'changed-activity-type',
                'activity_class' => TestMixedEntryActivity::class,
            ]),
        ]);

        $this->expectException(HistoryEventShapeMismatchException::class);
        $this->expectExceptionMessage('Recorded activity_type [changed-activity-type]');

        WorkflowStepHistory::assertCompatible($run, 1, WorkflowStepHistory::ACTIVITY, [
            'activity_type' => TestMixedEntryActivity::class,
        ]);
    }

    public function testChildWorkflowTypeDetailAcceptsCanonicalTypeAliasForYieldedClass(): void
    {
        $run = $this->runWithHistoryEvents([
            $this->historyEvent(HistoryEventType::ChildWorkflowScheduled, [
                'sequence' => 2,
                'child_workflow_type' => 'test-child-greeting-workflow',
                'child_workflow_class' => TestChildGreetingWorkflow::class,
            ]),
        ]);

        WorkflowStepHistory::assertCompatible($run, 2, WorkflowStepHistory::CHILD_WORKFLOW, [
            'child_workflow_type' => TestChildGreetingWorkflow::class,
        ]);

        $this->addToAssertionCount(1);
    }

    public function testCompatibilityReadsEachTypedPayloadOnceAndSortsOnlyMatchingSequenceEvents(): void
    {
        $unrelated = [];

        for ($index = 1; $index <= 200; $index++) {
            $unrelated[] = $this->countingHistoryEvent(
                HistoryEventType::ActivityScheduled,
                [
                    'sequence' => 100 + $index,
                    'activity_type' => "unrelated-{$index}",
                ],
                $index,
            );
        }

        $matching = [
            $this->countingHistoryEvent(HistoryEventType::ActivityCompleted, [
                'sequence' => 7,
                'activity_type' => 'expected-activity',
            ], 202),
            $this->countingHistoryEvent(HistoryEventType::ActivityScheduled, [
                'sequence' => 7,
                'activity_type' => 'expected-activity',
            ], 201),
        ];
        $run = $this->runWithHistoryEvents([...$unrelated, ...$matching]);

        WorkflowStepHistory::assertCompatible($run, 7, WorkflowStepHistory::ACTIVITY, [
            'activity_type' => 'expected-activity',
        ]);

        foreach ($unrelated as $event) {
            $this->assertSame(1, $event->payloadReads);
            $this->assertSame(0, $event->historySequenceReads);
        }

        foreach ($matching as $event) {
            $this->assertSame(1, $event->payloadReads);
            $this->assertSame(1, $event->historySequenceReads);
        }
    }

    public function testCompatibilityPreservesHistoryOrderShapePrecedenceAndSeesFreshPayloadMutations(): void
    {
        $activity = $this->countingHistoryEvent(HistoryEventType::ActivityScheduled, [
            'sequence' => 3,
            'activity_type' => 'old-activity',
        ], 30);
        $child = $this->countingHistoryEvent(HistoryEventType::ChildWorkflowScheduled, [
            'sequence' => 3,
        ], 10);
        $duplicateActivity = $this->countingHistoryEvent(HistoryEventType::ActivityCompleted, [
            'sequence' => 3,
            'activity_type' => 'old-activity',
        ], 20);
        $run = $this->runWithHistoryEvents([$activity, $child, $duplicateActivity]);

        $this->assertSame(
            [
                HistoryEventType::ChildWorkflowScheduled->value,
                HistoryEventType::ActivityCompleted->value,
                HistoryEventType::ActivityScheduled->value,
            ],
            WorkflowStepHistory::conflictingEventTypesForSequence($run, 3, WorkflowStepHistory::TIMER),
        );

        try {
            WorkflowStepHistory::assertCompatible($run, 3, WorkflowStepHistory::ACTIVITY, [
                'activity_type' => 'new-activity',
            ]);
            $this->fail('Expected the shape mismatch to take precedence over detail drift.');
        } catch (HistoryEventShapeMismatchException $exception) {
            $this->assertStringContainsString('current workflow yielded activity.', $exception->getMessage());
            $this->assertStringNotContainsString('Recorded activity_type', $exception->getMessage());
        }

        $child->forceFill([
            'payload' => [
                'sequence' => 99,
            ],
        ]);

        $this->expectException(HistoryEventShapeMismatchException::class);
        $this->expectExceptionMessage('Recorded activity_type [old-activity]');

        WorkflowStepHistory::assertCompatible($run, 3, WorkflowStepHistory::ACTIVITY, [
            'activity_type' => 'new-activity',
        ]);
    }

    /**
     * @param list<WorkflowHistoryEvent> $events
     */
    private function runWithHistoryEvents(array $events): WorkflowRun
    {
        $run = new WorkflowRun();
        $run->setRelation('historyEvents', new EloquentCollection($events));

        return $run;
    }

    /**
     * @param array<string, mixed> $payload
     */
    private function historyEvent(HistoryEventType $type, array $payload): WorkflowHistoryEvent
    {
        $event = new WorkflowHistoryEvent();
        $event->forceFill([
            'sequence' => $payload['sequence'] ?? 1,
            'event_type' => $type->value,
            'payload' => $payload,
        ]);

        return $event;
    }

    /**
     * @param array<string, mixed> $payload
     */
    private function countingHistoryEvent(
        HistoryEventType $type,
        array $payload,
        int $historySequence,
    ): CountingWorkflowHistoryEvent {
        $event = new CountingWorkflowHistoryEvent();
        $event->forceFill([
            'sequence' => $historySequence,
            'event_type' => $type->value,
            'payload' => $payload,
        ]);

        return $event;
    }
}

final class CountingWorkflowHistoryEvent extends WorkflowHistoryEvent
{
    public int $payloadReads = 0;

    public int $historySequenceReads = 0;

    public function getAttribute($key)
    {
        if ($key === 'payload') {
            $this->payloadReads++;
        }

        if ($key === 'sequence') {
            $this->historySequenceReads++;
        }

        return parent::getAttribute($key);
    }
}

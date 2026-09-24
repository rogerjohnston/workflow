<?php

declare(strict_types=1);

namespace Workflow\V2\Support;

use Illuminate\Support\Carbon;
use Workflow\V2\Enums\HistoryEventType;
use Workflow\V2\Models\WorkflowHistoryEvent;
use Workflow\V2\Models\WorkflowRun;

final class SignalWaits
{
    public static function bufferedWaitIdForCommandId(string $commandId): string
    {
        return sprintf('signal-command:%s', $commandId);
    }

    /**
     * @return list<array{
     *     id: string,
     *     signal_wait_id: string,
     *     signal_id: string|null,
     *     signal_name: string,
     *     sequence: int|null,
     *     status: string,
     *     source_status: string,
     *     opened_at: \Carbon\CarbonInterface|null,
     *     deadline_at: \Carbon\CarbonInterface|null,
     *     resolved_at: \Carbon\CarbonInterface|null,
     *     timeout_fired_at: \Carbon\CarbonInterface|null,
     *     timeout_seconds: int|null,
     *     timer_id: string|null,
     *     command_id: string|null,
     *     command_sequence: int|null,
     *     command_status: string|null,
     *     command_outcome: string|null
     * }>
     */
    public static function forRun(WorkflowRun $run): array
    {
        $run->loadMissing('historyEvents');

        $waits = [];
        $openWaitIdsByName = [];

        $events = $run->historyEvents->filter(
            static fn (mixed $event): bool =>
            $event instanceof WorkflowHistoryEvent && in_array($event->event_type, [
                HistoryEventType::SignalWaitOpened,
                HistoryEventType::TimerScheduled,
                HistoryEventType::TimerFired,
                HistoryEventType::TimerCancelled,
                HistoryEventType::SelectionOperationCancelled,
                HistoryEventType::SignalReceived,
                HistoryEventType::SignalApplied,
                HistoryEventType::WorkflowCompleted,
                HistoryEventType::WorkflowFailed,
                HistoryEventType::WorkflowCancelled,
                HistoryEventType::WorkflowTerminated,
                HistoryEventType::WorkflowContinuedAsNew,
            ], true)
        );

        foreach ($events->sortBy('sequence') as $event) {
            if (! $event instanceof WorkflowHistoryEvent) {
                continue;
            }

            $eventType = $event->event_type;
            $payload = $event->payload;
            $payload = is_array($payload) ? $payload : [];
            $signalName = self::stringValue($payload['signal_name'] ?? null);

            if ($eventType === HistoryEventType::SignalWaitOpened) {
                if ($signalName === null) {
                    continue;
                }

                $waitId = self::waitIdForOpenedEvent($payload);

                if ($waitId === null) {
                    continue;
                }

                $waits[$waitId] = [
                    'id' => $waitId,
                    'signal_wait_id' => $waitId,
                    'signal_id' => null,
                    'signal_name' => $signalName,
                    'sequence' => self::intValue($payload['sequence'] ?? null),
                    'status' => 'open',
                    'source_status' => 'waiting',
                    'opened_at' => $event->recorded_at ?? $event->created_at,
                    'deadline_at' => null,
                    'resolved_at' => null,
                    'timeout_fired_at' => null,
                    'timeout_seconds' => self::intValue($payload['timeout_seconds'] ?? null),
                    'timer_id' => null,
                    'command_id' => null,
                    'command_sequence' => null,
                    'command_status' => null,
                    'command_outcome' => null,
                ];

                $openWaitIdsByName[$signalName] ??= [];
                $openWaitIdsByName[$signalName][] = $waitId;

                continue;
            }

            if (
                in_array($eventType, [
                    HistoryEventType::TimerScheduled,
                    HistoryEventType::TimerFired,
                    HistoryEventType::TimerCancelled,
                ], true)
                && self::stringValue($payload['timer_kind'] ?? null) === 'signal_timeout'
            ) {
                if ($signalName === null) {
                    continue;
                }

                $explicitWaitId = self::stringValue($payload['signal_wait_id'] ?? null);
                $waitId = $eventType === HistoryEventType::TimerScheduled
                    ? $explicitWaitId
                    : self::consumeOpenWaitId($openWaitIdsByName, $signalName, $explicitWaitId);

                if ($waitId === null || ! isset($waits[$waitId])) {
                    continue;
                }

                $waits[$waitId]['timer_id'] = self::stringValue($payload['timer_id'] ?? null)
                    ?? $waits[$waitId]['timer_id'];
                $waits[$waitId]['timeout_seconds'] = self::intValue($payload['delay_seconds'] ?? null)
                    ?? self::intValue($payload['timeout_seconds'] ?? null)
                    ?? $waits[$waitId]['timeout_seconds'];
                $waits[$waitId]['deadline_at'] = self::timestamp($payload['fire_at'] ?? null)
                    ?? $waits[$waitId]['deadline_at'];

                if ($eventType === HistoryEventType::TimerScheduled) {
                    continue;
                }

                if (($waits[$waitId]['status'] ?? null) !== 'open') {
                    continue;
                }

                $waits[$waitId]['status'] = $eventType === HistoryEventType::TimerFired
                    ? 'resolved'
                    : 'cancelled';
                $waits[$waitId]['source_status'] = $eventType === HistoryEventType::TimerFired
                    ? 'timed_out'
                    : 'timeout_cancelled';
                $waits[$waitId]['resolved_at'] = $event->recorded_at ?? $event->created_at;
                $waits[$waitId]['timeout_fired_at'] = $eventType === HistoryEventType::TimerFired
                    ? self::timestamp($payload['fired_at'] ?? null) ?? ($event->recorded_at ?? $event->created_at)
                    : $waits[$waitId]['timeout_fired_at'];

                continue;
            }

            if ($eventType === HistoryEventType::SelectionOperationCancelled) {
                self::closeCancelledSelectionWaits($waits, $openWaitIdsByName, $event, $payload);

                continue;
            }

            if (in_array($eventType, [HistoryEventType::SignalReceived, HistoryEventType::SignalApplied], true)) {
                if ($signalName === null) {
                    continue;
                }

                $waitId = self::consumeOpenWaitId(
                    $openWaitIdsByName,
                    $signalName,
                    self::stringValue($payload['signal_wait_id'] ?? null),
                );

                if ($waitId === null || ! isset($waits[$waitId])) {
                    continue;
                }

                $currentStatus = $waits[$waitId]['status'] ?? null;
                $currentSourceStatus = $waits[$waitId]['source_status'] ?? null;
                $canApplyReceivedSignal = $eventType === HistoryEventType::SignalApplied
                    && $currentStatus === 'resolved'
                    && $currentSourceStatus === 'received';

                if ($currentStatus !== 'open' && ! $canApplyReceivedSignal) {
                    continue;
                }

                $waits[$waitId]['status'] = 'resolved';
                $waits[$waitId]['source_status'] = $eventType === HistoryEventType::SignalApplied
                    ? 'applied'
                    : 'received';
                $waits[$waitId]['signal_id'] = self::stringValue($payload['signal_id'] ?? null)
                    ?? $waits[$waitId]['signal_id'];
                $waits[$waitId]['resolved_at'] = $event->recorded_at ?? $event->created_at;
                $waits[$waitId]['command_id'] = self::stringValue($event->workflow_command_id)
                    ?? self::stringValue($payload['workflow_command_id'] ?? null)
                    ?? self::stringValue(self::commandSnapshot($payload)['id'] ?? null);
                $waits[$waitId]['command_sequence'] = self::commandSequence($payload);
                $waits[$waitId]['command_status'] = self::commandStatus($event, $payload);
                $waits[$waitId]['command_outcome'] = self::commandOutcome($event, $payload);

                continue;
            }

            if (in_array($eventType, [
                HistoryEventType::WorkflowCompleted,
                HistoryEventType::WorkflowFailed,
                HistoryEventType::WorkflowCancelled,
                HistoryEventType::WorkflowTerminated,
                HistoryEventType::WorkflowContinuedAsNew,
            ], true)) {
                self::closeOpenWaits($waits, $openWaitIdsByName, $event);
            }
        }

        return array_values($waits);
    }

    public static function openWaitIdForName(WorkflowRun $run, string $signalName): ?string
    {
        $openWaits = array_values(array_filter(
            self::forRun($run),
            static fn (array $wait): bool => $wait['signal_name'] === $signalName && $wait['status'] === 'open',
        ));

        if ($openWaits === []) {
            return null;
        }

        usort($openWaits, static function (array $left, array $right): int {
            $leftSequence = $left['sequence'] ?? PHP_INT_MIN;
            $rightSequence = $right['sequence'] ?? PHP_INT_MIN;

            if ($leftSequence !== $rightSequence) {
                return $leftSequence <=> $rightSequence;
            }

            $leftOpenedAt = $left['opened_at']?->getTimestampMs() ?? PHP_INT_MIN;
            $rightOpenedAt = $right['opened_at']?->getTimestampMs() ?? PHP_INT_MIN;

            if ($leftOpenedAt !== $rightOpenedAt) {
                return $leftOpenedAt <=> $rightOpenedAt;
            }

            return $left['signal_wait_id'] <=> $right['signal_wait_id'];
        });

        return end($openWaits)['signal_wait_id'] ?? null;
    }

    /**
     * @param array<string, array<string, mixed>> $waits
     * @param array<string, list<string>> $openWaitIdsByName
     * @param array<string, mixed> $payload
     */
    private static function closeCancelledSelectionWaits(
        array &$waits,
        array &$openWaitIdsByName,
        WorkflowHistoryEvent $event,
        array $payload,
    ): void {
        $baseSequence = self::intValue($payload['member_base_sequence'] ?? null);
        $memberSize = self::intValue($payload['member_size'] ?? null);

        if ($baseSequence === null || $memberSize === null || $memberSize < 1) {
            return;
        }

        foreach ($waits as $waitId => &$wait) {
            $sequence = self::intValue($wait['sequence'] ?? null);
            if ($sequence === null
                || $sequence < $baseSequence
                || $sequence >= $baseSequence + $memberSize
                || ($wait['status'] ?? null) !== 'open') {
                continue;
            }

            $wait['status'] = 'cancelled';
            $wait['source_status'] = 'selection_cancelled';
            $wait['resolved_at'] = $event->recorded_at ?? $event->created_at;

            $signalName = self::stringValue($wait['signal_name'] ?? null);
            if ($signalName === null || ! isset($openWaitIdsByName[$signalName])) {
                continue;
            }

            $index = array_search($waitId, $openWaitIdsByName[$signalName], true);
            if ($index !== false) {
                array_splice($openWaitIdsByName[$signalName], $index, 1);
            }
        }
        unset($wait);
    }

    /**
     * @param array<string, array<string, mixed>> $waits
     * @param array<string, list<string>> $openWaitIdsByName
     */
    private static function closeOpenWaits(
        array &$waits,
        array &$openWaitIdsByName,
        WorkflowHistoryEvent $event,
    ): void {
        $sourceStatus = match ($event->event_type) {
            HistoryEventType::WorkflowCancelled => 'cancelled',
            HistoryEventType::WorkflowTerminated => 'terminated',
            HistoryEventType::WorkflowContinuedAsNew => 'continued',
            default => 'closed',
        };

        foreach ($openWaitIdsByName as $signalName => $waitIds) {
            while ($waitIds !== []) {
                $waitId = array_shift($waitIds);

                if ($waitId === null || ! isset($waits[$waitId])) {
                    continue;
                }

                $waits[$waitId]['status'] = 'cancelled';
                $waits[$waitId]['source_status'] = $sourceStatus;
                $waits[$waitId]['resolved_at'] = $event->recorded_at ?? $event->created_at;
            }

            $openWaitIdsByName[$signalName] = [];
        }
    }

    /**
     * @param array<string, list<string>> $openWaitIdsByName
     */
    private static function consumeOpenWaitId(
        array &$openWaitIdsByName,
        string $signalName,
        ?string $explicitWaitId,
    ): ?string {
        $openWaitIdsByName[$signalName] ??= [];

        if ($explicitWaitId !== null) {
            $index = array_search($explicitWaitId, $openWaitIdsByName[$signalName], true);

            if ($index !== false) {
                array_splice($openWaitIdsByName[$signalName], $index, 1);
            }

            return $explicitWaitId;
        }

        $waitId = array_shift($openWaitIdsByName[$signalName]);

        return is_string($waitId) && $waitId !== ''
            ? $waitId
            : null;
    }

    /**
     * @param array<string, mixed> $payload
     */
    private static function waitIdForOpenedEvent(array $payload): ?string
    {
        $waitId = self::stringValue($payload['signal_wait_id'] ?? null);

        if ($waitId !== null) {
            return $waitId;
        }

        $signalName = self::stringValue($payload['signal_name'] ?? null);
        $sequence = self::intValue($payload['sequence'] ?? null);

        if ($signalName === null || $sequence === null) {
            return null;
        }

        return sprintf('signal:%d:%s', $sequence, $signalName);
    }

    private static function stringValue(mixed $value): ?string
    {
        return is_string($value) && $value !== ''
            ? $value
            : null;
    }

    private static function intValue(mixed $value): ?int
    {
        if (is_int($value)) {
            return $value;
        }

        return is_string($value) && is_numeric($value)
            ? (int) $value
            : null;
    }

    private static function timestamp(mixed $value): ?\Carbon\CarbonInterface
    {
        if ($value instanceof \Carbon\CarbonInterface) {
            return $value;
        }

        return is_string($value) && $value !== ''
            ? Carbon::parse($value)
            : null;
    }

    /**
     * @param array<string, mixed> $payload
     * @return array<string, mixed>
     */
    private static function commandSnapshot(array $payload): array
    {
        $snapshot = $payload['command'] ?? null;

        return is_array($snapshot)
            ? $snapshot
            : [];
    }

    /**
     * @param array<string, mixed> $payload
     */
    private static function commandSequence(array $payload): ?int
    {
        return self::intValue(self::commandSnapshot($payload)['sequence'] ?? null);
    }

    /**
     * @param array<string, mixed> $payload
     */
    private static function commandStatus(WorkflowHistoryEvent $event, array $payload): ?string
    {
        $status = self::stringValue(self::commandSnapshot($payload)['status'] ?? null);

        return match ($event->event_type) {
            HistoryEventType::SignalReceived,
            HistoryEventType::SignalApplied => 'accepted',
            default => $status,
        };
    }

    /**
     * @param array<string, mixed> $payload
     */
    private static function commandOutcome(WorkflowHistoryEvent $event, array $payload): ?string
    {
        $outcome = self::stringValue($payload['outcome'] ?? null)
            ?? self::stringValue(self::commandSnapshot($payload)['outcome'] ?? null);

        return $outcome ?? match ($event->event_type) {
            HistoryEventType::SignalReceived,
            HistoryEventType::SignalApplied => 'signal_received',
            default => null,
        };
    }
}

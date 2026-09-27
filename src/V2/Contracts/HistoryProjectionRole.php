<?php

declare(strict_types=1);

namespace Workflow\V2\Contracts;

use Workflow\V2\Models\ActivityAttempt;
use Workflow\V2\Models\ActivityExecution;
use Workflow\V2\Models\WorkflowRun;
use Workflow\V2\Models\WorkflowRunSummary;
use Workflow\V2\Models\WorkflowTask;

/**
 * Binding seam for the history/projection role.
 *
 * Claim and command paths use this contract when they must synchronously
 * record durable history side effects and refresh operator-visible
 * projections without hard-coding the in-process implementation.
 *
 * These methods can run more than once when their owning transaction retries.
 * Keep durable writes on the same database connection. Publish notifications
 * and other non-transactional effects through that connection's afterCommit
 * callback so rolled-back attempts cannot publish externally visible state.
 */
interface HistoryProjectionRole
{
    public function projectRun(WorkflowRun $run): WorkflowRunSummary;

    public function recordActivityStarted(
        WorkflowRun $run,
        ActivityExecution $execution,
        ActivityAttempt $attempt,
        WorkflowTask $task,
    ): WorkflowRunSummary;
}

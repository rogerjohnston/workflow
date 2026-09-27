<?php

declare(strict_types=1);

namespace Tests\Fixtures\V2;

use RuntimeException;
use Workflow\V2\Activity;

final class TestMySqlCountingActivity extends Activity
{
    public function handle(string $counterPath): string
    {
        $written = file_put_contents($counterPath, "called\n", FILE_APPEND | LOCK_EX);

        if ($written === false) {
            throw new RuntimeException('Unable to record the test activity invocation.');
        }

        return 'mysql-contention-result';
    }
}

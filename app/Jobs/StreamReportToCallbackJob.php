<?php

namespace App\Jobs;

use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\SerializesModels;
use App\Reports\ReportFactory;

class StreamReportToCallbackJob implements ShouldQueue
{
    use Dispatchable, InteractsWithQueue, Queueable, SerializesModels;

    // Extend timeout execution window safety rules for massive data streams
    public $timeout = 1200;

    protected string $reportKey;
    protected array $params;
    protected string $callbackUrl;
    protected ?string $jobId;

    public function __construct(string $reportKey, array $params, string $callbackUrl, ?string $jobId)
    {
        $this->reportKey   = $reportKey;
        $this->params      = $params;
        $this->callbackUrl = $callbackUrl;
        $this->jobId       = $jobId;
    }

    public function handle()
    {
        // 1. Resolve the handler using your factory inside the background process
        $handler = ReportFactory::make($this->reportKey);

        // 2. Inject the async routing destinations right into the active handler
        if (method_exists($handler, 'setAsyncProperties')) {
            $handler->setAsyncProperties($this->callbackUrl, $this->jobId);
        }

        // 3. Execute the standard script logic. The handler will now chunk back to the webhook!
        $handler->execute($this->params);
    }
}

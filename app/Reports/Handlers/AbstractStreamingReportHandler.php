<?php

namespace App\Reports\Handlers;

use App\Reports\Contracts\ReportHandlerInterface;
use Illuminate\Support\Facades\Http;
use Illuminate\Support\Facades\Log;

abstract class AbstractStreamingReportHandler implements ReportHandlerInterface
{
    protected ?string $callbackUrl = null;
    protected ?string $jobId = null;

    /**
     * Set properties when invoked through an async background context loop.
     */
    public function setAsyncProperties(string $callbackUrl, ?string $jobId): void
    {
        $this->callbackUrl = $callbackUrl;
        $this->jobId       = $jobId;
    }

    /**
     * Normalizes host configuration layouts for local Mac environments.
     * Prevents self-signed SSL blocks and missing VirtualHost 404 traps.
     */
    protected function getNormalizedCallbackUrl(): string
    {
        $url = $this->callbackUrl;
        if (app()->environment('local') && !empty($url)) {
            $url = str_replace('http://', 'https://', $url);
        }
        return $url;
    }

    /**
     * Unified network sender component that includes local SSL verification bypass.
     */
    protected function transmitBatch(array $payload, bool $isFinal): void
    {
        if (empty($payload) && !$isFinal) {
            return;
        }

        $url = $this->getNormalizedCallbackUrl();
        Log::debug("DWH Streamer: Transmitting packet to target webhook: {$url} (Job #{$this->jobId}, EOF: " . ($isFinal ? 'True' : 'False') . ")");

        $shouldVerifySsl = !app()->environment(['local', 'dev', 'development']);
        $response = Http::withOptions(['verify' => $shouldVerifySsl])
        ->withHeaders([
            'X-Report-Job-ID'    => $this->jobId ?? '0',
            'X-Report-Batch-EOF' => $isFinal ? 'true' : 'false',
            'Accept'             => 'application/json'
        ])
            ->post($url, [
                'data' => $payload
            ]);

        if ($response->failed()) {
            Log::error("DWH Streamer: Network call dropped. Status: " . $response->status() . " | Body: " . $response->body());
            throw new \Exception("Data stream delivery failed during chunk execution loop.");
        }
    }
}

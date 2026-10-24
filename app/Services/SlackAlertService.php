<?php

namespace App\Services;

use Illuminate\Support\Facades\Http;
use Illuminate\Support\Facades\Log;

class SlackAlertService
{
    protected ?string $webhookUrl;

    public function __construct()
    {
        $this->webhookUrl = config('services.slack.webhook_url') ?? env('LOG_SLACK_WEBHOOK_URL');
    }

    /**
     * Send the final overall end-of-sync status notification.
     */
    public function sendSyncStatus(string $headerText, string $bodyMarkdown, string $color = '#2EB886'): void
    {
        if (empty($this->webhookUrl)) {
            return;
        }

        $blocks = [
            [
                'type' => 'header',
                'text' => [
                    'type' => 'plain_text',
                    'text' => $headerText,
                    'emoji' => false,
                ],
            ],
            [
                'type' => 'section',
                'text' => [
                    'type' => 'mrkdwn',
                    'text' => $bodyMarkdown,
                ],
            ],
            [
                'type' => 'context',
                'elements' => [
                    [
                        'type' => 'mrkdwn',
                        'text' => "Environment: *" . app()->environment() . "* | Timestamp: " . now()->toDateTimeString(),
                    ],
                ],
            ],
        ];

        \Illuminate\Support\Facades\Http::post($this->webhookUrl, [
            'attachments' => [
                [
                    'color'  => $color,
                    'blocks' => $blocks,
                ],
            ],
        ]);
    }

    public function sendSyncError(string $tableName, string $errorMessage, array $failedRecords = [], ?string $trace = null): void
    {
        if (empty($this->webhookUrl)) {
            return;
        }

        $blocks = [
            [
                'type' => 'header',
                'text' => [
                    'type' => 'plain_text',
                    'text' => "❌ DWH Sync Failure: {$tableName}",
                    'emoji' => true,
                ],
            ],
            [
                'type' => 'section',
                'text' => [
                    'type' => 'mrkdwn',
                    'text' => "*Error Details:*\n```{$errorMessage}```",
                ],
            ],
        ];

        if (!empty($failedRecords)) {
            $failedSummary = collect($failedRecords)->take(10)->map(function ($item) {
                return "• ID: `{$item['id']}` - {$item['reason']}";
            })->implode("\n");

            $totalFailed = count($failedRecords);
            $blocks[] = [
                'type' => 'section',
                'text' => [
                    'type' => 'mrkdwn',
                    'text' => "*Failed Records ({$totalFailed} total):*\n{$failedSummary}" .
                        ($totalFailed > 10 ? "\n_...and " . ($totalFailed - 10) . " more._" : ""),
                ],
            ];
        }

        if ($trace) {
            $blocks[] = [
                'type' => 'section',
                'text' => [
                    'type' => 'mrkdwn',
                    'text' => "*Stack Snippet:*\n```" . substr($trace, 0, 500) . "```",
                ],
            ];
        }

        \Illuminate\Support\Facades\Http::post($this->webhookUrl, ['blocks' => $blocks]);
    }

    /**
     * Send an audit alert block to Slack.
     */
    public function sendAuditFailure(array $auditResults, array $orphanReport = []): void
    {
        if (empty($this->webhookUrl)) {
            Log::warning('Slack webhook URL not configured; alert skipped.');
            return;
        }

        $fields = [];
        foreach ($auditResults as $entity => $data) {
            $icon = $data['match'] ? '✅' : '⚠️';
            $fields[] = [
                'type' => 'mrkdwn',
                'text' => "*{$entity}:*\n{$icon} Source: {$data['source_count']} | DWH: {$data['dwh_count']}",
            ];
        }

        $blocks = [
            [
                'type' => 'header',
                'text' => [
                    'type' => 'plain_text',
                    'text' => '🚨 DWH Sync Quality Audit: Discrepancies Found',
                    'emoji' => true,
                ],
            ],
            [
                'type' => 'section',
                'text' => [
                    'type' => 'mrkdwn',
                    'text' => "The post-sync data reconciliation run detected mismatched row counts between *Source* (`mysql_src`) and *DWH* (`mysql`).",
                ],
            ],
            [
                'type' => 'section',
                'fields' => $fields,
            ],
        ];

        // Append orphan diagnostic section if present
        if (!empty($orphanReport['total'])) {
            $blocks[] = [
                'type' => 'divider',
            ];
            $blocks[] = [
                'type' => 'section',
                'text' => [
                    'type' => 'mrkdwn',
                    'text' => "*Orphaned Source Records:*\n" .
                        "• Total: *{$orphanReport['total']}*\n" .
                        "• Missing Instructors: {$orphanReport['summary']['missing_instructors']}\n" .
                        "• Missing Providers: {$orphanReport['summary']['missing_providers']}",
                ],
            ];
        }

        Http::post($this->webhookUrl, ['blocks' => $blocks]);
    }
}

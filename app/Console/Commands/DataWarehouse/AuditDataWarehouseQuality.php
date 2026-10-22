<?php

namespace App\Console\Commands\DataWarehouse;

use App\Services\DataWarehouseQualityService;
use Illuminate\Console\Command;

class AuditDataWarehouseQuality extends Command
{
    protected $signature = 'dwh:audit {--limit=10 : Number of sample missing IDs to show}';
    protected $description = 'Verifies record parity and prints missing IDs between source and DWH';

    public function handle(DataWarehouseQualityService $qualityService): int
    {
        $this->info('[' . now()->format('Y-m-d H:i:s') . '] Auditing Source vs DWH parity...');

        $limit = (int) $this->option('limit');
        $results = $qualityService->auditAll($this->output, $limit);

        $tableRows = [];
        $failed = false;

        foreach ($results as $entity => $data) {
            $status = $data['match'] ? '<fg=green>MATCH</>' : '<fg=red>MISMATCH</>';
            if (!$data['match']) {
                $failed = true;
            }

            $missingList = $data['missing_ids'] ?? [];

            $missingText = $data['missing_count'] > 0
                ? (!empty($missingList)
                    ? "{$data['missing_count']} missing: [" . implode(', ', array_slice($missingList, 0, $limit)) . "]"
                    : "{$data['missing_count']} missing")
                : 'None';

            $tableRows[] = [
                $entity,
                $data['source_count'],
                $data['dwh_count'],
                $status,
                $missingText,
            ];
        }

        $this->table(['Entity', 'Source Count', 'DWH Count', 'Status', 'Discrepancies'], $tableRows);

        $orphanReport = $qualityService->auditInstructorProviderOrphans();

        if ($orphanReport['total'] > 0) {
            $this->newLine();
            $this->warn("⚠️  Post-Audit Diagnostic: {$orphanReport['total']} Orphaned / Inactive Source Linkages Detected");

            $this->table(
                ['Metric', 'Count'],
                [
                    ['Hard-deleted / Missing Instructors', $orphanReport['summary']['missing_instructors']],
                    ['Hard-deleted / Missing Providers',   $orphanReport['summary']['missing_providers']],
                    ['Soft-deleted Instructors (Active Link)', $orphanReport['summary']['deleted_instructors']],
                    ['Soft-deleted Providers (Active Link)',   $orphanReport['summary']['deleted_providers']],
                ]
            );

            // Render sample records cleanly in a secondary table
            $sample = array_slice($orphanReport['records'], 0, 10);
            $sampleRows = array_map(function ($r) {
                return [
                    $r->instructor_id,
                    $r->training_provider_id,
                    $r->instructor_status !== 'OK' ? '<fg=red>' . $r->instructor_status . '</>' : ($r->instructor_deleted_at ? '<fg=yellow>Soft-deleted</>' : 'OK'),
                    $r->provider_status !== 'OK'   ? '<fg=red>' . $r->provider_status . '</>'   : ($r->provider_deleted_at ? '<fg=yellow>Soft-deleted</>' : 'OK'),
                    $r->link_deleted_at ?: 'Active',
                ];
            }, $sample);

            $this->newLine();
            $this->comment("Sample Orphan Rows (first 10 of {$orphanReport['total']}):");
            $this->table(
                ['Instructor ID', 'Provider ID', 'Instructor State', 'Provider State', 'Link Status'],
                $sampleRows
            );
        }

        return $failed ? self::FAILURE : self::SUCCESS;
    }
}

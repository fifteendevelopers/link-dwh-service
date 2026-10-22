<?php


namespace App\Services;

use Illuminate\Support\Facades\DB;
use Symfony\Component\Console\Output\OutputInterface;

class DataWarehouseQualityService
{
    protected $source;
    protected $dwh;
    protected int $sourceSystemKey;

    public function __construct()
    {
        $this->source = DB::connection('mysql_src');
        $this->dwh = DB::connection('mysql');
        $this->sourceSystemKey = (int)($this->dwh->table('Dim_Source_System')
            ->where('System_Name', 'link')
            ->value('Source_System_Key') ?? 1);
    }

    /**
     * Run all entity integrity checks and return a structured summary.
     */
    public function auditAll(?OutputInterface $output = null, int $sampleLimit = 5): array
    {
        $entities = [
            'Deliveries' => [
                'source_table' => 'deliveries',
                'source_id_col' => 'id',
                'source_deleted_col' => 'deleted_at',
                'dwh_table' => 'Dim_Delivery_Header',
                'dwh_source_id_col' => 'Source_Delivery_Id',
                'dwh_deleted_col' => 'Source_Deleted_At',
            ],
            'Courses' => [
                'source_table' => 'courses',
                'source_id_col' => 'id',
                'source_deleted_col' => null, // If courses has no deleted_at
                'dwh_table' => 'Dim_Course',
                'dwh_source_id_col' => 'Source_Course_Id',
                'dwh_deleted_col' => null,
            ],
            'Training Providers' => [
                'source_table' => 'training_providers',
                'source_id_col' => 'id',
                'source_deleted_col' => 'deleted_at',
                'dwh_table' => 'Dim_Training_Provider',
                'dwh_source_id_col' => 'Source_Provider_Id',
                'dwh_deleted_col' => 'Source_Deleted_At',
                'extra_dwh_filter' => ['Is_Current' => 1],
            ],
            'Instructors' => [
                'source_table' => 'instructors',
                'source_id_col' => 'instructor_number',
                'source_deleted_col' => 'deleted_at',
                'dwh_table' => 'Dim_Instructor',
                'dwh_source_id_col' => 'Instructor_Number',
                'dwh_deleted_col' => null,
            ],
            'Instructor Provider Links' => [
                'source_table'       => 'join_instructors_training_providers',
                'source_id_col'      => null,
                'source_deleted_col' => null, // Set to null to include all records
                'dwh_table'          => 'Fact_Instructor_Training_Provider',
                'dwh_source_id_col'  => null,
                'dwh_deleted_col'    => null, // Set to null to include all records
                'composite'          => true,
            ],
        ];

        $results = [];

        foreach ($entities as $label => $config) {
            $check = !empty($config['composite'])
                ? $this->auditCompositePivot($config, $sampleLimit)
                : $this->auditEntity($config, $sampleLimit);

            $results[$label] = $check;
        }

        return $results;
    }

    /**
     * Standard primary key entity comparison.
     */
    protected function auditEntity(array $cfg, int $sampleLimit): array
    {
        // 1. Source Active IDs
        $srcQuery = $this->source->table($cfg['source_table']);
        if (!empty($cfg['source_deleted_col'])) {
            $srcQuery->whereNull($cfg['source_deleted_col']);
        }
        $sourceIds = $srcQuery->pluck($cfg['source_id_col'])->map(fn($v) => (int)$v)->toArray();

        // 2. DWH Active IDs
        $dwhQuery = $this->dwh->table($cfg['dwh_table'])
            ->where('Source_System_Key', $this->sourceSystemKey)
            ->whereNotNull($cfg['dwh_source_id_col']);

        if (!empty($cfg['dwh_deleted_col'])) {
            $dwhQuery->whereNull($cfg['dwh_deleted_col']);
        }
        if (!empty($cfg['extra_dwh_filter'])) {
            foreach ($cfg['extra_dwh_filter'] as $col => $val) {
                $dwhQuery->where($col, $val);
            }
        }
        $dwhIds = $dwhQuery->pluck($cfg['dwh_source_id_col'])->map(fn($v) => (int)$v)->toArray();

        $sourceCount = count($sourceIds);
        $dwhCount = count($dwhIds);

        // Find Missing (In Source but absent in DWH)
        $dwhLookup = array_flip($dwhIds);
        $missingInDwh = [];
        foreach ($sourceIds as $sId) {
            if (!isset($dwhLookup[$sId])) {
                $missingInDwh[] = $sId;
            }
        }

        // Find Ghost records (In DWH but removed/missing on Source)
        $sourceLookup = array_flip($sourceIds);
        $orphanedInDwh = [];
        foreach ($dwhIds as $dId) {
            if (!isset($sourceLookup[$dId])) {
                $orphanedInDwh[] = $dId;
            }
        }

        return [
            'source_count' => $sourceCount,
            'dwh_count' => $dwhCount,
            'match' => ($sourceCount === $dwhCount && empty($missingInDwh) && empty($orphanedInDwh)),
            'missing_count' => count($missingInDwh),
            'missing_ids' => array_slice($missingInDwh, 0, $sampleLimit),
            'orphan_count' => count($orphanedInDwh),
            'orphan_ids' => array_slice($orphanedInDwh, 0, $sampleLimit),
        ];
    }

    /**
     * Pivot/Bridge table comparison (e.g. Instructor - Provider link).
     */
    protected function auditCompositePivot(array $cfg, int $sampleLimit): array
    {
        $srcLinks = $this->source->table($cfg['source_table'])
            ->selectRaw("CONCAT(instructor_id, '-', training_provider_id) as link_key")
            ->pluck('link_key')
            ->toArray();

        $dwhLinks = $this->dwh->table($cfg['dwh_table'])
            ->where('Source_System_Key', $this->sourceSystemKey)
            ->selectRaw("CONCAT(Source_Instructor_Id, '-', Source_Provider_Id) as link_key")
            ->pluck('link_key')
            ->toArray();

        $dwhLookup = array_flip($dwhLinks);

        $missing = [];
        foreach ($srcLinks as $k) {
            if (!isset($dwhLookup[$k])) {
                $missing[] = $k;
            }
        }

        $sourceCount  = count($srcLinks);
        $dwhCount     = count($dwhLinks);
        $missingCount = count($missing);

        return [
            'source_count'  => $sourceCount,
            'dwh_count'     => $dwhCount,
            'match'         => ($sourceCount === $dwhCount && $missingCount === 0),
            'missing_count' => $missingCount,
            'missing_ids'   => array_slice($missing, 0, $sampleLimit), // <-- added here
        ];
    }

    // In app/Services/DataWarehouseQualityService.php

    /**
     * Runs a post-audit diagnostic on the source connection to detect orphaned
     * or soft-deleted parent relationships in join_instructors_training_providers.
     */
    public function auditInstructorProviderOrphans(): array
    {
        $rows = $this->source->table('join_instructors_training_providers as j')
            ->leftJoin('instructors as i', 'j.instructor_id', '=', 'i.id')
            ->leftJoin('training_providers as tp', 'j.training_provider_id', '=', 'tp.id')
            ->where(function ($q) {
                $q->whereNull('i.id')
                    ->orWhereNull('tp.id')
                    ->orWhereNotNull('i.deleted_at')
                    ->orWhereNotNull('tp.deleted_at');
            })
            ->select([
                'j.instructor_id',
                'j.training_provider_id',
                'j.deleted_at as link_deleted_at',
                DB::raw("CASE WHEN i.id IS NULL THEN 'Missing Instructor' ELSE 'OK' END AS instructor_status"),
                DB::raw("CASE WHEN tp.id IS NULL THEN 'Missing Provider' ELSE 'OK' END AS provider_status"),
                'i.deleted_at as instructor_deleted_at',
                'tp.deleted_at as provider_deleted_at',
            ])
            ->get();

        if ($rows->isEmpty()) {
            return [
                'total' => 0,
                'summary' => [
                    'missing_instructors' => 0,
                    'missing_providers'   => 0,
                    'deleted_instructors' => 0,
                    'deleted_providers'   => 0,
                ],
                'records' => [],
            ];
        }

        $summary = [
            'missing_instructors' => $rows->where('instructor_status', 'Missing Instructor')->count(),
            'missing_providers'   => $rows->where('provider_status', 'Missing Provider')->count(),
            'deleted_instructors' => $rows->filter(fn($r) => $r->instructor_status === 'OK' && !empty($r->instructor_deleted_at))->count(),
            'deleted_providers'   => $rows->filter(fn($r) => $r->provider_status === 'OK' && !empty($r->provider_deleted_at))->count(),
        ];

        return [
            'total'   => $rows->count(),
            'summary' => $summary,
            'records' => $rows->toArray(),
        ];
    }

}

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
                'source_deleted_col' => null,
                'dwh_table' => 'Dim_Delivery_Header',
                'dwh_source_id_col' => 'Source_Delivery_Id',
                'dwh_deleted_col' => null,
            ],
            'Consents' => [
                'source_table'       => 'consents',
                'source_id_col'      => 'id',
                'source_deleted_col' => null,
                'dwh_table'          => 'Dim_Consent',
                'dwh_source_id_col'  => 'Source_Consent_Id',
                'dwh_deleted_col'    => null,
            ],
            'Courses' => [
                'source_table'       => 'courses',
                'source_id_col'      => 'id',
                'source_deleted_col' => null,
                'dwh_table'          => 'Dim_Course',
                'dwh_source_id_col'  => 'Source_Course_Id',
                'dwh_deleted_col'    => null,
                'dwh_query_modifier' => function ($query) {
                    $query->join('Dim_Delivery_Header', 'Dim_Course.Delivery_Key', '=', 'Dim_Delivery_Header.Delivery_Key')
                        ->whereNotNull('Dim_Course.Source_Course_Id');
                },
            ],
            'Training Providers' => [
                'source_table' => 'training_providers',
                'source_id_col' => 'id',
                'source_deleted_col' => 'deleted_at',
                'dwh_table' => 'Dim_Training_Provider',
                'dwh_source_id_col' => 'Source_Provider_Id',
                'dwh_deleted_col' => 'Source_Deleted_At',
                'dwh_query_modifier'    => function ($query) {
                    // Only compare against the active version for SCD Type 2
                    $query->where('Dim_Training_Provider.Is_Current', 1);
                },
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
            'Grants' => [
                'source_table'       => 'grants',
                'source_id_col'      => 'id',
                'source_deleted_col' => null, // or 'deleted_at' if soft deletes are pruned
                'dwh_table'          => 'Dim_Grant',
                'dwh_source_id_col'  => 'Source_Grant_Id',
                'dwh_deleted_col'    => null,
            ],
            'Grant Recipients' => [
                'source_table'       => 'grant_recipients',
                'source_id_col'      => 'id',
                'source_deleted_col' => null,
                'dwh_table'          => 'Dim_Grant_Recipient',
                'dwh_source_id_col'  => 'Source_Recipient_Id',
                'dwh_deleted_col'    => null,
                'dwh_query_modifier' => function ($query) {
                    $query->where('Dim_Grant_Recipient.Is_Current', 1);
                },
            ],
            'Organisations' => [
                'source_table'       => 'organisations',
                'source_id_col'      => 'id',
                'source_deleted_col' => null,
                'dwh_table'          => 'Dim_Organisation',
                'dwh_source_id_col'  => 'Source_Organisation_Id',
                'dwh_deleted_col'    => null,
            ],
            'Riders' => [
                'source_table'       => 'riders',
                'source_id_col'      => 'id',
                'source_deleted_col' => null,
                'dwh_table'          => 'Dim_Rider',
                'dwh_source_id_col'  => 'Source_Rider_Id',
                'dwh_deleted_col'    => null,
            ],
            'Schools' => [
                'source_table'       => 'vendor_edubase',
                'source_id_col'      => 'urn', // Maps on School_Urn natural key
                'source_deleted_col' => null,
                'dwh_table'          => 'Dim_School',
                'dwh_source_id_col'  => 'School_Urn',
                'dwh_deleted_col'    => null,
                'dwh_query_modifier' => function ($query) {
                    // If Dim_School tracks SCD Type 2 versions or has a default fallback row:
                    $query->whereNotNull('Dim_School.School_Urn');
                },
            ],
            'Teacher Trainers' => [
                'source_table'       => 'teacher_trainers',
                'source_id_col'      => 'id',
                'source_deleted_col' => null,
                'dwh_table'          => 'Dim_Teacher_Trainer',
                'dwh_source_id_col'  => 'Source_Teacher_Trainer_Id',
                'dwh_deleted_col'    => null,
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
        // 1. Build Source Query
        $srcQuery = $this->source->table($cfg['source_table']);

        if (!empty($cfg['source_deleted_col'])) {
            $srcQuery->whereNull("{$cfg['source_table']}.{$cfg['source_deleted_col']}");
        }

        if (!empty($cfg['source_query_modifier']) && is_callable($cfg['source_query_modifier'])) {
            $cfg['source_query_modifier']($srcQuery);
        }

        $srcIds = $srcQuery->pluck("{$cfg['source_table']}.{$cfg['source_id_col']}")
            ->map(fn($id) => (int) $id)
            ->toArray();

        // 2. Build DWH Query
        $dwhQuery = $this->dwh->table($cfg['dwh_table'])
            ->where("{$cfg['dwh_table']}.Source_System_Key", $this->sourceSystemKey);

        if (!empty($cfg['dwh_deleted_col'])) {
            $dwhQuery->whereNull("{$cfg['dwh_table']}.{$cfg['dwh_deleted_col']}");
        }

        if (!empty($cfg['dwh_query_modifier']) && is_callable($cfg['dwh_query_modifier'])) {
            $cfg['dwh_query_modifier']($dwhQuery);
        }

        $dwhIds = $dwhQuery->pluck("{$cfg['dwh_table']}.{$cfg['dwh_source_id_col']}")
            ->filter()
            ->map(fn($id) => (int) $id)
            ->toArray();

        $dwhLookup = array_flip($dwhIds);

        $missing = [];
        foreach ($srcIds as $id) {
            if (!isset($dwhLookup[$id])) {
                $missing[] = $id;
            }
        }

        $srcCount = count($srcIds);
        $dwhCount = count($dwhIds);

        return [
            'source_count'  => $srcCount,
            'dwh_count'     => $dwhCount,
            'match'         => ($srcCount === $dwhCount && empty($missing)),
            'missing_count' => count($missing),
            'missing_ids'   => array_slice($missing, 0, $sampleLimit),
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

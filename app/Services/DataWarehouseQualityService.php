<?php

namespace App\Services;

use Illuminate\Support\Facades\DB;
use Symfony\Component\Console\Output\OutputInterface;

class DataWarehouseQualityService
{
    protected $source;
    protected $dwh;
    protected int $sourceSystemKey;

    protected array $sourceTimestampCache = [];
    protected array $dwhTimestampCache = [];

    public function __construct()
    {
        $this->source = DB::connection('mysql_src');
        $this->dwh = DB::connection('mysql');
        $this->sourceSystemKey = (int)($this->dwh->table('Dim_Source_System')
            ->where('System_Name', 'link')
            ->value('Source_System_Key') ?? 1);
    }

    private function getWatermark(string $tableName): string
    {
        return $this->dwh->table('Sync_Log')
            ->where('Table_Name', $tableName)
            ->value('Last_Synced_At') ?? '1900-01-01 00:00:00';
    }

    protected function getSourceTimestampColumns(string $tableName): array
    {
        if (isset($this->sourceTimestampCache[$tableName])) {
            return $this->sourceTimestampCache[$tableName];
        }

        $schema = $this->source->getSchemaBuilder();

        return $this->sourceTimestampCache[$tableName] = [
            'has_updated_at' => $schema->hasColumn($tableName, 'updated_at'),
            'has_created_at' => $schema->hasColumn($tableName, 'created_at'),
        ];
    }

    protected function getDwhTimestampColumns(string $tableName): array
    {
        if (isset($this->dwhTimestampCache[$tableName])) {
            return $this->dwhTimestampCache[$tableName];
        }

        $schema = $this->dwh->getSchemaBuilder();

        return $this->dwhTimestampCache[$tableName] = [
            'has_created_at' => $schema->hasColumn($tableName, 'Source_Created_At'),
            'has_updated_at' => $schema->hasColumn($tableName, 'Source_Updated_At'),
        ];
    }

    public function auditAll(?OutputInterface $output = null, int $sampleLimit = 5): array
    {
        $entities = [
            'Consents' => [
                'source_table'       => 'consents',
                'source_id_col'      => 'id',
                'source_deleted_col' => null,
                'dwh_table'          => 'Dim_Consent',
                'dwh_source_id_col'  => 'Source_Consent_Id',
                'dwh_deleted_col'    => null,
                'audit_watermark'    => true,
            ],
            'Courses' => [
                'source_table'          => 'courses',
                'source_id_col'         => 'id',
                'source_deleted_col'    => null,
                'source_query_modifier' => function ($query) {
                    $query->join('deliveries', 'courses.delivery_id', '=', 'deliveries.id');
                },
                'dwh_table'             => 'Dim_Course',
                'dwh_source_id_col'     => 'Source_Course_Id',
                'dwh_deleted_col'       => null,
                'dwh_query_modifier'    => function ($query) {
                    // No unindexed join needed; Source_Course_Id not null represents digitised courses
                    $query->whereNotNull('Dim_Course.Source_Course_Id');
                },
                'audit_watermark'       => true,
            ],
            'Deliveries' => [
                'source_table'       => 'deliveries',
                'source_id_col'      => 'id',
                'source_deleted_col' => null,
                'dwh_table'          => 'Dim_Delivery_Header',
                'dwh_source_id_col'  => 'Source_Delivery_Id',
                'dwh_deleted_col'    => null,
                'audit_watermark'    => true,
            ],
            'External Systems' => [
                'source_table'       => 'external_systems',
                'source_id_col'      => 'id',
                'source_deleted_col' => null,
                'dwh_table'          => 'Dim_External_Systems',
                'dwh_source_id_col'  => 'Source_External_System_Id',
                'dwh_deleted_col'    => null,
                'audit_watermark'    => true,
            ],
            'Get Cycling Courses' => [
                'source_table'       => 'get_cycling_courses',
                'source_id_col'      => 'id',
                'source_deleted_col' => null,
                'dwh_table'          => 'Dim_Get_Cycling_Course',
                'dwh_source_id_col'  => 'Source_Course_Id',
                'dwh_deleted_col'    => null,
                'audit_watermark'    => true,
            ],
            'Get Cycling Riders' => [
                'source_table'       => 'get_cycling_riders',
                'source_id_col'      => 'id',
                'source_deleted_col' => null,
                'dwh_table'          => 'Dim_Get_Cycling_Rider',
                'dwh_source_id_col'  => 'Source_Rider_Id',
                'dwh_deleted_col'    => null,
                'audit_watermark'    => true,
            ],
            'Get Cycling Rider Courses' => [
                'source_table'          => 'get_cycling_join_riders_courses',
                'source_id_col'         => 'id',
                'source_query_modifier' => function ($query) {
                    $query->join('get_cycling_riders', 'get_cycling_join_riders_courses.rider_id', '=', 'get_cycling_riders.id');
                },
                'dwh_table'             => 'Fact_Get_Cycling_Rider_Course',
                'dwh_source_id_col'     => 'Source_Join_Id',
                'audit_watermark'       => true,
            ],
            'Get Cycling Rider Notes' => [
                'source_table'       => 'get_cycling_rider_notes',
                'source_id_col'      => 'id',
                'source_deleted_col' => null,
                'dwh_table'          => 'Dim_Get_Cycling_Rider_Note',
                'dwh_source_id_col'  => 'Source_Note_Id',
                'dwh_deleted_col'    => null,
                'audit_watermark'    => true,
            ],
            'Grants' => [
                'source_table'       => 'grants',
                'source_id_col'      => 'id',
                'source_deleted_col' => null,
                'dwh_table'          => 'Dim_Grant',
                'dwh_source_id_col'  => 'Source_Grant_Id',
                'dwh_deleted_col'    => null,
                'audit_watermark'    => true,
            ],
            'Grant Claims' => [
                'source_table'       => 'grant_claims',
                'source_id_col'      => 'id',
                'source_deleted_col' => null,
                'dwh_table'          => 'Fact_Grant_Claims',
                'dwh_source_id_col'  => 'Source_Claim_Id',
                'dwh_deleted_col'    => null,
                'audit_watermark'    => true,
            ],
            'Grant Reallocations' => [
                'source_table'       => 'grant_reallocations',
                'source_id_col'      => 'id',
                'source_deleted_col' => null,
                'dwh_table'          => 'Fact_Grant_Reallocations',
                'dwh_source_id_col'  => 'Source_Reallocation_Id',
                'dwh_deleted_col'    => null,
                'audit_watermark'    => true,
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
                'audit_watermark'    => true,
            ],
            'Grant Recipient Renewals' => [
                'composite'          => false,
                'source_table'       => 'grant_recipient_renewals',
                'source_id_col'      => 'id',
                'source_deleted_col' => null,
                'dwh_table'          => 'Fact_Grant_Recipient_Renewal',
                'dwh_source_id_col'  => 'Source_Renewal_Id',
                'dwh_deleted_col'    => null,
                'audit_watermark'    => true,
            ],
            'Grant Recipient - Training Provider (Join)' => [
                'composite'             => true,
                'source_table'          => 'join_grant_recipients_training_providers',
                'source_composite_cols' => ['grant_recipient_id', 'training_provider_id'],
                'source_deleted_col'    => 'deleted_at',
                'dwh_table'             => 'Fact_Grant_Recipient_Training_Provider',
                'dwh_composite_cols'    => ['Source_Grant_Recipient_Id', 'Source_Training_Provider_Id'],
                'dwh_deleted_col'       => 'Source_Deleted_At',
                'dwh_query_modifier'    => null,
                'audit_watermark'       => false,
            ],
            'Instructors' => [
                'source_table'       => 'instructors',
                'source_id_col'      => 'instructor_number',
                'source_deleted_col' => 'deleted_at',
                'dwh_table'          => 'Dim_Instructor',
                'dwh_source_id_col'  => 'Instructor_Number',
                'dwh_deleted_col'    => null,
                'audit_watermark'    => true,
            ],
            'Instructor Courses' => [
                'composite'             => true,
                'source_table'          => 'join_instructors_courses',
                'source_composite_cols' => ['instructor_id', 'course_id'],
                'source_deleted_col'    => null,
                'dwh_table'             => 'Fact_Instructor_Course',
                'dwh_composite_cols'    => ['Dim_Instructor.Source_Instructor_Id', 'Dim_Course.Source_Course_Id'],
                'dwh_deleted_col'       => null,
                'dwh_query_modifier'    => function ($query) {
                    $query->join('Dim_Instructor', 'Fact_Instructor_Course.Instructor_Key', '=', 'Dim_Instructor.Instructor_Key')
                        ->join('Dim_Course', 'Fact_Instructor_Course.Course_Key', '=', 'Dim_Course.Course_Key');
                },
                'audit_watermark'       => false,
            ],
            'Instructor Deliveries' => [
                'composite'             => true,
                'use_straight_join'     => true,
                'source_table'          => 'join_deliveries_instructors',
                'source_composite_cols' => ['join_deliveries_instructors.delivery_id', 'join_deliveries_instructors.instructor_id'],
                'source_deleted_col'    => 'deleted_at',
                'source_query_modifier' => function ($query) {
                    $query->join('instructors', 'join_deliveries_instructors.instructor_id', '=', 'instructors.id')
                        ->join('deliveries', 'join_deliveries_instructors.delivery_id', '=', 'deliveries.id')
                        ->whereNull('deliveries.deleted_at')
                        ->whereNull('instructors.deleted_at');
                },
                'dwh_table'             => 'Fact_Instructor_Delivery',
                'dwh_composite_cols'    => ['Dim_Delivery_Header.Source_Delivery_Id', 'Dim_Instructor.Source_Instructor_Id'],
                'dwh_deleted_col'       => 'Source_Deleted_At',
                'dwh_query_modifier'    => function ($query) {
                    $query->join('Dim_Delivery_Header', 'Fact_Instructor_Delivery.Delivery_Key', '=', 'Dim_Delivery_Header.Delivery_Key')
                        ->join('Dim_Instructor', 'Fact_Instructor_Delivery.Instructor_Key', '=', 'Dim_Instructor.Instructor_Key')
                        ->whereNull('Dim_Delivery_Header.Source_Deleted_At');
                },
                'audit_watermark'       => false,
            ],
            'Instructor Feedback Lookup' => [
                'source_table'       => 'instructor_feedback_LOOKUP',
                'source_id_col'      => 'id',
                'source_deleted_col' => null,
                'dwh_table'          => 'Dim_Instructor_Feedback_Lookup',
                'dwh_source_id_col'  => 'Feedback_Lookup_Key',
                'dwh_deleted_col'    => null,
                'audit_watermark'    => false,
            ],
            'Instructor - Training Provider (Join)' => [
                'composite'             => true,
                'source_table'          => 'join_instructors_training_providers',
                'source_composite_cols' => ['instructor_id', 'training_provider_id'],
                'source_deleted_col'    => null,
                'dwh_table'             => 'Fact_Instructor_Training_Provider',
                'dwh_composite_cols'    => ['Dim_Instructor.Source_Instructor_Id', 'Dim_Training_Provider.Source_Provider_Id'],
                'dwh_deleted_col'       => null,
                'dwh_query_modifier'    => function ($query) {
                    $query->join('Dim_Instructor', 'Fact_Instructor_Training_Provider.Instructor_Key', '=', 'Dim_Instructor.Instructor_Key')
                        ->join('Dim_Training_Provider', 'Fact_Instructor_Training_Provider.Provider_Key', '=', 'Dim_Training_Provider.Provider_Key');
                },
                'audit_watermark'       => false,
            ],
            'Instructor Renewals' => [
                'composite'          => false,
                'source_table'       => 'instructor_renewals',
                'source_id_col'      => 'id',
                'source_deleted_col' => null,
                'dwh_table'          => 'Fact_Instructor_Renewal',
                'dwh_source_id_col'  => 'Source_Renewal_Id',
                'dwh_deleted_col'    => null,
                'audit_watermark'    => true,
            ],
            'Instructor Qualifications' => [
                'source_table'          => 'instructor_qualifications',
                'source_id_col'         => 'id',
                'source_deleted_col'    => null,
                'source_query_modifier' => function ($query) {
                    $query->join('instructors', 'instructor_qualifications.instructor_id', '=', 'instructors.id');
                },
                'dwh_table'             => 'Fact_Instructor_Qualification',
                'dwh_source_id_col'     => 'Source_Qual_Id',
                'dwh_deleted_col'       => null,
                'audit_watermark'       => false,
            ],
            'Organisations' => [
                'source_table'       => 'organisations',
                'source_id_col'      => 'id',
                'source_deleted_col' => null,
                'dwh_table'          => 'Dim_Organisation',
                'dwh_source_id_col'  => 'Source_Organisation_Id',
                'dwh_deleted_col'    => null,
                'audit_watermark'    => true,
            ],
            'Parent Surveys' => [
                'source_table'          => 'parent_survey',
                'source_id_col'         => 'id',
                'source_deleted_col'    => null,
                'source_query_modifier' => function ($query) {
                    $query->join('courses', 'parent_survey.course_id', '=', 'courses.id')
                        ->join('deliveries', 'parent_survey.delivery_id', '=', 'deliveries.id');
                },
                'dwh_table'             => 'Fact_Parent_Survey',
                'dwh_source_id_col'     => 'Source_Survey_Id',
                'dwh_deleted_col'       => null,
                'audit_watermark'       => true,
            ],
            'Riders' => [
                'source_table'       => 'riders',
                'source_id_col'      => 'id',
                'source_deleted_col' => null,
                'dwh_table'          => 'Dim_Rider',
                'dwh_source_id_col'  => 'Source_Rider_Id',
                'dwh_deleted_col'    => null,
                'audit_watermark'    => true,
            ],
            'Rider Courses' => [
                'composite'             => true,
                'source_table'          => 'join_riders_courses',
                'source_composite_cols' => ['rider_id', 'course_id'],
                'source_deleted_col'    => null,
                'dwh_table'             => 'Fact_Rider_Course',
                'dwh_composite_cols'    => ['Source_Rider_Id', 'Source_Course_Id'],
                'dwh_deleted_col'       => null,
                'audit_watermark'       => false,
            ],
            'Schools' => [
                'source_table'       => 'vendor_edubase',
                'source_id_col'      => 'urn',
                'source_deleted_col' => null,
                'dwh_table'          => 'Dim_School',
                'dwh_source_id_col'  => 'School_Urn',
                'dwh_deleted_col'    => null,
                'dwh_query_modifier' => function ($query) {
                    $query->whereNotNull('Dim_School.School_Urn');
                },
                'audit_watermark'    => true,
            ],
            'Teacher Trainers' => [
                'source_table'       => 'teacher_trainers',
                'source_id_col'      => 'id',
                'source_deleted_col' => null,
                'dwh_table'          => 'Dim_Teacher_Trainer',
                'dwh_source_id_col'  => 'Source_Teacher_Trainer_Id',
                'dwh_deleted_col'    => null,
                'audit_watermark'    => true,
            ],
            'Teacher Trainer Deliveries' => [
                'source_table'       => 'teacher_trainer_deliveries',
                'source_id_col'      => 'id',
                'source_deleted_col' => 'deleted_at',
                'dwh_table'          => 'Dim_Teacher_Trainer_Delivery',
                'dwh_source_id_col'  => 'Source_TT_Delivery_Id',
                'dwh_deleted_col'    => 'Source_Deleted_At',
                'audit_watermark'    => true,
            ],

            'Teacher Trainer Deliveries - Trainers (Join)' => [
                'composite'             => true,
                'source_table'          => 'join_deliveries_teacher_trainers',
                'source_composite_cols' => ['teacher_trainer_delivery_id', 'teacher_trainer_id'],
                'source_deleted_col'    => 'deleted_at',
                'dwh_table'             => 'Fact_Deliveries_Teacher_Trainers',
                'dwh_composite_cols'    => ['Source_TT_Delivery_Id', 'Source_Teacher_Trainer_Id'],
                'dwh_deleted_col'       => 'Source_Deleted_At',
                'dwh_query_modifier'    => null,
                'audit_watermark'       => false,
            ],
            'Training Organisations' => [
                'source_table'       => 'vendor_training_organisations',
                'source_id_col'      => 'id',
                'source_deleted_col' => null,
                'dwh_table'          => 'Dim_Training_Organisation',
                'dwh_source_id_col'  => 'Source_Organisation_Id',
                'dwh_deleted_col'    => null,
                'audit_watermark'    => false,
            ],
            'Training Providers' => [
                'source_table'          => 'training_providers',
                'source_id_col'         => 'id',
                'source_deleted_col'    => 'deleted_at',
                'dwh_table'             => 'Dim_Training_Provider',
                'dwh_source_id_col'     => 'Source_Provider_Id',
                'dwh_deleted_col'       => 'Source_Deleted_At',
                'dwh_query_modifier'    => function ($query) {
                    $query->where('Dim_Training_Provider.Is_Current', 1);
                },
                'audit_watermark'       => true,
            ],
            'Training Provider Renewals' => [
                'composite'          => false,
                'source_table'       => 'training_provider_renewals',
                'source_id_col'      => 'id',
                'source_deleted_col' => null,
                'dwh_table'          => 'Fact_Training_Provider_Renewal',
                'dwh_source_id_col'  => 'Source_Renewal_Id',
                'dwh_deleted_col'    => null,
                'audit_watermark'    => true,
            ]
        ];

        $results = [];

        foreach ($entities as $name => $cfg) {
            $isComposite = !empty($cfg['composite'])
                || !empty($cfg['source_composite_cols'])
                || (isset($cfg['type']) && $cfg['type'] === 'composite');

            if ($isComposite) {
                $results[$name] = $this->auditCompositeKey($cfg, $sampleLimit);
            } else {
                $results[$name] = $this->auditEntity($cfg, $sampleLimit);
            }
        }

        $results['Courses (Non-Digitised JSON Modules)'] = $this->auditNonDigitisedCourses($sampleLimit);

        return $results;
    }

    /**
     * Audits non-digitised delivery_details JSON entries against synthetic Dim_Course records.
     * Single grouped query to eliminate all N+1 overhead.
     */
    public function auditNonDigitisedCourses(int $sampleLimit = 5): array
    {
        // 1. Fetch DWH synthetic course counts in ONE single grouped query
        $dwhCourseCounts = $this->dwh->table('Dim_Course as c')
            ->join('Dim_Delivery_Header as d', 'c.Delivery_Key', '=', 'd.Delivery_Key')
            ->where('d.Digitisation_Booking', 0)
            ->where('c.Source_System_Key', $this->sourceSystemKey)
            ->whereNull('c.Source_Course_Id')
            ->groupBy('d.Source_Delivery_Id')
            ->pluck(DB::raw('COUNT(*) as total_courses'), 'd.Source_Delivery_Id')
            ->toArray();

        // 2. Stream source deliveries and compare against the in-memory map
        $nonDigitisedDeliveries = $this->source->table('deliveries as d')
            ->leftJoin('courses as c', 'd.id', '=', 'c.delivery_id')
            ->whereNull('c.id') // Crucial guard: only check JSON if no relational courses exist
            ->where('d.digitisation_booking', 0)
            ->whereNull('d.deleted_at')
            ->whereNotNull('d.delivery_details')
            ->where('d.delivery_details', '<>', '[]')
            ->select(['d.id', 'd.delivery_details'])
            ->cursor();

        $expectedModules = 0;
        $missingDeliveries = [];

        foreach ($nonDigitisedDeliveries as $deliv) {
            $details = json_decode($deliv->delivery_details, true);
            if (!is_array($details)) {
                continue;
            }

            $moduleCount = 0;
            foreach ($details as $entry) {
                if (!empty($entry['module']['id'])) {
                    $moduleCount++;
                }
            }

            if ($moduleCount === 0) {
                continue;
            }

            $expectedModules += $moduleCount;

            $dwhCount = (int) ($dwhCourseCounts[$deliv->id] ?? 0);

            if ($dwhCount < $moduleCount) {
                $missingDeliveries[] = [
                    'delivery_id' => (int) $deliv->id,
                    'expected'    => $moduleCount,
                    'actual'      => $dwhCount,
                    'missing'     => $moduleCount - $dwhCount,
                ];
            }
        }

        $dwhTotal = array_sum($dwhCourseCounts);
        $missingCount = max(0, $expectedModules - $dwhTotal);

        return [
            'source_count'  => $expectedModules,
            'dwh_count'     => $dwhTotal,
            'match'         => ($expectedModules === $dwhTotal && empty($missingDeliveries)),
            'missing_count' => $missingCount,
            'missing_ids'   => array_slice($missingDeliveries, 0, $sampleLimit),
        ];
    }

    /**
     * Diagnostic comparison for digitised and non-digitised courses.
     */
    public function auditCoursesBreakdown(int $sampleLimit = 10): array
    {
        // 1. Digitised courses count (source table vs Dim_Course)
        // Direct count on Dim_Course where Source_Course_Id is not null avoids unindexed join
        $srcDigitised = $this->source->table('courses')
            ->join('deliveries', 'courses.delivery_id', '=', 'deliveries.id')
            ->where('deliveries.digitisation_booking', 1)
            ->whereNull('deliveries.deleted_at')
            ->count();

        $dwhDigitised = $this->dwh->table('Dim_Course')
            ->where('Source_System_Key', $this->sourceSystemKey)
            ->whereNotNull('Source_Course_Id')
            ->count();

        // 2. Non-Digitised modules vs synthetic Dim_Course rows
        $nonDigitisedAudit = $this->auditNonDigitisedCourses($sampleLimit);

        return [
            'digitised' => [
                'source_courses_count' => $srcDigitised,
                'dwh_courses_count'    => $dwhDigitised,
                'match'                => ($srcDigitised === $dwhDigitised),
                'difference'           => $srcDigitised - $dwhDigitised,
            ],
            'non_digitised' => [
                'source_expected_modules' => $nonDigitisedAudit['source_count'],
                'dwh_synthetic_courses'   => $nonDigitisedAudit['dwh_count'],
                'match'                   => $nonDigitisedAudit['match'],
                'difference'              => $nonDigitisedAudit['missing_count'],
                'sample_missing'          => $nonDigitisedAudit['missing_ids'],
            ],
        ];
    }

    protected function auditEntity(array $cfg, int $sampleLimit): array
    {
        $watermark = null;
        if (!empty($cfg['audit_watermark'])) {
            $watermark = $this->getWatermark($cfg['dwh_table']);
        }

        $srcQuery = $this->source->table($cfg['source_table']);

        if (!empty($cfg['source_deleted_col'])) {
            $srcQuery->whereNull("{$cfg['source_table']}.{$cfg['source_deleted_col']}");
        }

        if ($watermark && $watermark !== '1900-01-01 00:00:00') {
            $tsInfo = $this->getSourceTimestampColumns($cfg['source_table']);

            if ($tsInfo['has_created_at'] && $tsInfo['has_updated_at']) {
                $srcQuery->where(function ($q) use ($cfg, $watermark) {
                    $q->where("{$cfg['source_table']}.created_at", '<=', $watermark)
                        ->orWhere(function ($fallback) use ($cfg, $watermark) {
                            $fallback->whereNull("{$cfg['source_table']}.created_at")
                                ->where("{$cfg['source_table']}.updated_at", '<=', $watermark);
                        });
                });
            } elseif ($tsInfo['has_created_at']) {
                $srcQuery->where("{$cfg['source_table']}.created_at", '<=', $watermark);
            } elseif ($tsInfo['has_updated_at']) {
                $srcQuery->where("{$cfg['source_table']}.updated_at", '<=', $watermark);
            }
        }

        if (!empty($cfg['source_query_modifier']) && is_callable($cfg['source_query_modifier'])) {
            $cfg['source_query_modifier']($srcQuery);
        }

        $srcIds = $srcQuery->pluck("{$cfg['source_table']}.{$cfg['source_id_col']}")
            ->map(fn($id) => (int) $id)
            ->toArray();

        $dwhQuery = $this->dwh->table($cfg['dwh_table'])
            ->where("{$cfg['dwh_table']}.Source_System_Key", $this->sourceSystemKey);

        if (!empty($cfg['dwh_deleted_col'])) {
            $dwhQuery->whereNull("{$cfg['dwh_table']}.{$cfg['dwh_deleted_col']}");
        }

        if ($watermark && $watermark !== '1900-01-01 00:00:00') {
            $dwhTsInfo = $this->getDwhTimestampColumns($cfg['dwh_table']);
            $dwhTable = $cfg['dwh_table'];

            if ($dwhTsInfo['has_created_at'] && $dwhTsInfo['has_updated_at']) {
                $dwhQuery->where(function ($q) use ($dwhTable, $watermark) {
                    $q->where("{$dwhTable}.Source_Created_At", '<=', $watermark)
                        ->orWhere(function ($fallback) use ($dwhTable, $watermark) {
                            $fallback->whereNull("{$dwhTable}.Source_Created_At")
                                ->where("{$dwhTable}.Source_Updated_At", '<=', $watermark);
                        });
                });
            } elseif ($dwhTsInfo['has_created_at']) {
                $dwhQuery->where("{$dwhTable}.Source_Created_At", '<=', $watermark);
            } elseif ($dwhTsInfo['has_updated_at']) {
                $dwhQuery->where("{$dwhTable}.Source_Updated_At", '<=', $watermark);
            }
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

    protected function auditCompositeKey(array $cfg, int $sampleLimit = 5): array
    {
        $separator = $cfg['composite_delimiter'] ?? '-';
        $watermark = !empty($cfg['audit_watermark']) ? $this->getWatermark($cfg['dwh_table']) : null;

        $wrapCol = function (string $col): string {
            $parts = explode('.', trim($col));
            return implode('.', array_map(fn($part) => "`" . str_replace('`', '', $part) . "`", $parts));
        };

        $srcCols = (array) ($cfg['source_composite_cols'] ?? $cfg['source_id_col']);
        $srcCols = array_map('trim', explode(',', implode(',', $srcCols)));

        $dwhCols = (array) ($cfg['dwh_composite_cols'] ?? $cfg['dwh_source_id_col']);
        $dwhCols = array_map('trim', explode(',', implode(',', $dwhCols)));

        $srcColsSql = implode(', ', array_map($wrapCol, $srcCols));
        $dwhColsSql = implode(', ', array_map($wrapCol, $dwhCols));

        $srcQuery = $this->source->table($cfg['source_table']);

        if (!empty($cfg['source_deleted_col'])) {
            $srcQuery->whereNull("{$cfg['source_table']}.{$cfg['source_deleted_col']}");
        }

        if ($watermark && $watermark !== '1900-01-01 00:00:00') {
            $tsInfo = $this->getSourceTimestampColumns($cfg['source_table']);

            if ($tsInfo['has_created_at'] && $tsInfo['has_updated_at']) {
                $srcQuery->where(function ($q) use ($cfg, $watermark) {
                    $q->where("{$cfg['source_table']}.created_at", '<=', $watermark)
                        ->orWhere(function ($fallback) use ($cfg, $watermark) {
                            $fallback->whereNull("{$cfg['source_table']}.created_at")
                                ->where("{$cfg['source_table']}.updated_at", '<=', $watermark);
                        });
                });
            } elseif ($tsInfo['has_created_at']) {
                $srcQuery->where("{$cfg['source_table']}.created_at", '<=', $watermark);
            } elseif ($tsInfo['has_updated_at']) {
                $srcQuery->where("{$cfg['source_table']}.updated_at", '<=', $watermark);
            }
        }

        if (!empty($cfg['source_query_modifier']) && is_callable($cfg['source_query_modifier'])) {
            $cfg['source_query_modifier']($srcQuery);
        }

        $srcLinks = $srcQuery
            ->selectRaw("CONCAT_WS('{$separator}', {$srcColsSql}) as link_key")
            ->pluck('link_key')
            ->map(fn($val) => (string) $val)
            ->toArray();

        $dwhQuery = $this->dwh->table($cfg['dwh_table'])
            ->where("{$cfg['dwh_table']}.Source_System_Key", $this->sourceSystemKey);

        if (!empty($cfg['dwh_deleted_col'])) {
            $dwhQuery->whereNull("{$cfg['dwh_table']}.{$cfg['dwh_deleted_col']}");
        }

        if ($watermark && $watermark !== '1900-01-01 00:00:00') {
            $tsInfo = $this->getSourceTimestampColumns($cfg['source_table']);

            if ($tsInfo['has_created_at']) {
                $srcQuery->where(function ($q) use ($cfg, $watermark) {
                    $q->where("{$cfg['source_table']}.created_at", '<=', $watermark)
                        ->orWhereNull("{$cfg['source_table']}.created_at");
                });
            } elseif ($tsInfo['has_updated_at']) {
                $srcQuery->where("{$cfg['source_table']}.updated_at", '<=', $watermark);
            }

            $dwhTsInfo = $this->getDwhTimestampColumns($cfg['dwh_table']);
            $dwhTable = $cfg['dwh_table'];

            if ($dwhTsInfo['has_created_at']) {
                $dwhQuery->where(function ($q) use ($dwhTable, $watermark) {
                    $q->where("{$dwhTable}.Source_Created_At", '<=', $watermark)
                        ->orWhereNull("{$dwhTable}.Source_Created_At");
                });
            } elseif ($dwhTsInfo['has_updated_at']) {
                $dwhQuery->where("{$dwhTable}.Source_Updated_At", '<=', $watermark);
            }
        }

        if (!empty($cfg['dwh_query_modifier']) && is_callable($cfg['dwh_query_modifier'])) {
            $cfg['dwh_query_modifier']($dwhQuery);
        }

        $selectPrefix = !empty($cfg['use_straight_join']) ? 'STRAIGHT_JOIN ' : '';

        $dwhLinks = $dwhQuery
            ->selectRaw("{$selectPrefix}CONCAT_WS('{$separator}', {$dwhColsSql}) as link_key")
            ->pluck('link_key')
            ->map(fn($val) => (string) $val)
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
            'missing_ids'   => array_slice($missing, 0, $sampleLimit),
        ];
    }

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

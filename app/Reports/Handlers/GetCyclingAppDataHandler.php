<?php

namespace App\Reports\Handlers;

use Carbon\Carbon;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Validator;

class GetCyclingAppDataHandler extends AbstractStreamingReportHandler
{
    public function validate(array $parameters): array
    {
        return Validator::make($parameters, [
            'start_date'  => 'nullable|string',
            'end_date'    => 'nullable|string',
            'school_urn'  => 'nullable|integer',
            'course_type' => 'nullable|string',
        ])->validate();
    }

    public function execute(array $params): array
    {
        ini_set('memory_limit', '512M');
        ini_set('max_execution_time', '600');

        $rows = $this->buildData($params);

        if (empty($this->callbackUrl)) {
            return $rows;
        }

        collect($rows)->chunk(250)->each(function ($chunk) {
            $this->transmitBatch($chunk->values()->toArray(), false);
        });

        $this->transmitBatch([], true);

        return ['status' => 'async_completed'];
    }

    protected function buildData(array $params): array
    {
        // 1. Fetch Active Courses & Activities Catalog
        $courseDefinitions = DB::connection('mysql')->table('Dim_Get_Cycling_Course')
            ->orderBy('Source_Course_Id')
            ->get();

        $courseIds = $courseDefinitions->pluck('Source_Course_Id')->toArray();

        // Query activities from child table if synced, or source relation
        $activityDefinitions = DB::connection('mysql')->table('Fact_Get_Cycling_Rider_Course_Activity as a')
            ->join('Fact_Get_Cycling_Rider_Course as rc', 'a.GC_Rider_Course_Key', '=', 'rc.GC_Rider_Course_Key')
            ->join('Dim_Get_Cycling_Course as c', 'rc.GC_Course_Key', '=', 'c.GC_Course_Key')
            ->select('a.Activity_Id')
            ->distinct()
            ->orderBy('a.Activity_Id')
            ->pluck('Activity_Id')
            ->toArray();

        $courseIdMap = array_flip($courseIds);
        $activityIdMap = array_flip($activityDefinitions);

        // 2. Main Query
        $query = DB::connection('mysql')->table('Fact_Get_Cycling_Rider_Course as rc')
            ->join('Dim_Get_Cycling_Rider as r', 'rc.GC_Rider_Key', '=', 'r.GC_Rider_Key')
            ->join('Dim_Get_Cycling_Course as c', 'rc.GC_Course_Key', '=', 'c.GC_Course_Key')
            ->leftJoin('Dim_School as s', 'rc.School_Key', '=', 's.School_Key')
            ->select([
                'r.School_Urn',
                DB::raw("IFNULL(s.School_Name, r.School_Urn) as school_name"),
                DB::raw("IFNULL(s.La_Name, '') as la_name"),
                'rc.GC_Rider_Course_Key',
                'rc.Source_Rider_Id',
                'rc.Source_Course_Id',
                'rc.Source_Updated_At',
            ])
            ->whereNotNull('r.School_Urn')
            ->where('r.School_Urn', '!=', '');

        if (!empty($params['start_date']) && !empty($params['end_date'])) {
            $query->whereBetween(DB::raw('COALESCE(rc.Source_Updated_At, rc.Updated_At)'), [
                $params['start_date'],
                $params['end_date'],
            ]);
        }

        if (!empty($params['school_urn'])) {
            $query->where('r.School_Urn', $params['school_urn']);
        }

        if (!empty($params['course_type']) && $params['course_type'] !== 'All') {
            $query->where('c.Course_Code', $params['course_type']);
        }

        $records = $query->get();

        if ($records->isEmpty()) {
            return [];
        }

        // 3. Batch load activity attendance
        $rcKeys = $records->pluck('GC_Rider_Course_Key')->toArray();
        $activityRecords = DB::connection('mysql')->table('Fact_Get_Cycling_Rider_Course_Activity')
            ->whereIn('GC_Rider_Course_Key', $rcKeys)
            ->where('Activity_Score', '>', 0)
            ->select('GC_Rider_Course_Key', 'Activity_Id')
            ->get()
            ->groupBy('GC_Rider_Course_Key');

        // 4. Aggregate per School
        $schools = [];
        $riderSeenCourses = [];
        $riderSeenActivities = [];

        foreach ($records as $row) {
            $urn = (string) $row->School_Urn;

            if (!isset($schools[$urn])) {
                $schools[$urn] = [
                    'school_name'      => $row->school_name,
                    'school_urn'       => $urn,
                    'la_name'          => $row->la_name,
                    'last_activity_at' => null,
                    'riders'           => [],
                    'course_counts'    => array_fill(0, count($courseIds), 0),
                    'activity_counts'  => array_fill(0, count($activityDefinitions), 0),
                ];
            }

            $schools[$urn]['riders'][$row->Source_Rider_Id] = true;

            $updatedAt = $row->Source_Updated_At ? Carbon::parse($row->Source_Updated_At) : null;
            if ($updatedAt && ($schools[$urn]['last_activity_at'] === null || $updatedAt->greaterThan($schools[$urn]['last_activity_at']))) {
                $schools[$urn]['last_activity_at'] = $updatedAt;
            }

            // Deduplicated course type counts per rider
            $courseId = (int)$row->Source_Course_Id;
            $rcCourseKey = $row->Source_Rider_Id . ':' . $courseId;
            if (isset($courseIdMap[$courseId]) && !isset($riderSeenCourses[$rcCourseKey])) {
                $riderSeenCourses[$rcCourseKey] = true;
                $schools[$urn]['course_counts'][$courseIdMap[$courseId]]++;
            }

            // Deduplicated activity attendance per rider
            if (isset($activityRecords[$row->GC_Rider_Course_Key])) {
                foreach ($activityRecords[$row->GC_Rider_Course_Key] as $act) {
                    $actId = (int)$act->Activity_Id;
                    $riderActKey = $row->Source_Rider_Id . ':' . $actId;
                    if (isset($activityIdMap[$actId]) && !isset($riderSeenActivities[$riderActKey])) {
                        $riderSeenActivities[$riderActKey] = true;
                        $schools[$urn]['activity_counts'][$activityIdMap[$actId]]++;
                    }
                }
            }
        }

        // Sort by School Name
        uasort($schools, fn($a, $b) => strcasecmp($a['school_name'], $b['school_name']));

        $output = [];
        foreach ($schools as $row) {
            $output[] = [
                'school_name'        => $row['school_name'],
                'school_urn'         => $row['school_urn'],
                'la_name'            => $row['la_name'],
                'last_activity_date' => $row['last_activity_at']?->format('d/m/Y') ?? '',
                'total_riders'       => count($row['riders']),
                'course_counts'      => $row['course_counts'],
                'activity_counts'    => $row['activity_counts'],
            ];
        }

        return $output;
    }
}

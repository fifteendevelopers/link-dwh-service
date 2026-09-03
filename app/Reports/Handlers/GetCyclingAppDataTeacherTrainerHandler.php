<?php

namespace App\Reports\Handlers;

use Carbon\Carbon;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Validator;

class GetCyclingAppDataTeacherTrainerHandler extends AbstractStreamingReportHandler
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
        $courseDefinitions = DB::connection('mysql')->table('Dim_Get_Cycling_Course')
            ->orderBy('Source_Course_Id')
            ->get();

        $courseIds = $courseDefinitions->pluck('Source_Course_Id')->toArray();

        $activityDefinitions = DB::connection('mysql')->table('Fact_Get_Cycling_Rider_Course_Activity as a')
            ->join('Fact_Get_Cycling_Rider_Course as rc', 'a.GC_Rider_Course_Key', '=', 'rc.GC_Rider_Course_Key')
            ->select('a.Activity_Id')
            ->distinct()
            ->orderBy('a.Activity_Id')
            ->pluck('Activity_Id')
            ->toArray();

        $courseIdMap = array_flip($courseIds);
        $activityIdMap = array_flip($activityDefinitions);

        $query = DB::connection('mysql')->table('Fact_Get_Cycling_Rider_Course as rc')
            ->join('Dim_Get_Cycling_Rider as r', 'rc.GC_Rider_Key', '=', 'r.GC_Rider_Key')
            ->join('Dim_Get_Cycling_Course as c', 'rc.GC_Course_Key', '=', 'c.GC_Course_Key')
            ->leftJoin('Dim_Teacher_Trainer as tt', 'rc.Teacher_Trainer_Key', '=', 'tt.Teacher_Trainer_Key')
            ->leftJoin('Dim_School as s', 'rc.School_Key', '=', 's.School_Key')
            ->select([
                'r.School_Urn',
                DB::raw("IFNULL(s.School_Name, r.School_Urn) as school_name"),
                DB::raw("IFNULL(s.La_Name, '') as la_name"),
                'rc.Source_Teacher_Trainer_Id',
                DB::raw("CASE
                    WHEN tt.First_Name IS NOT NULL AND tt.First_Name != '' THEN CONCAT(tt.First_Name, ' ', tt.Last_Name)
                    WHEN rc.Source_Teacher_Trainer_Id IS NOT NULL THEN CONCAT('Teacher Trainer #', rc.Source_Teacher_Trainer_Id)
                    ELSE 'Unknown'
                END as teacher_trainer_name"),
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

        $rcKeys = $records->pluck('GC_Rider_Course_Key')->toArray();
        $activityRecords = DB::connection('mysql')->table('Fact_Get_Cycling_Rider_Course_Activity')
            ->whereIn('GC_Rider_Course_Key', $rcKeys)
            ->where('Activity_Score', '>', 0)
            ->select('GC_Rider_Course_Key', 'Activity_Id')
            ->get()
            ->groupBy('GC_Rider_Course_Key');

        $groups = [];
        $seenCourseTypes = [];
        $seenActivities = [];

        foreach ($records as $row) {
            $groupKey = $row->School_Urn . ':' . ($row->Source_Teacher_Trainer_Id ?? 0);

            if (!isset($groups[$groupKey])) {
                $groups[$groupKey] = [
                    'school_name'          => $row->school_name,
                    'school_urn'           => $row->School_Urn,
                    'la_name'              => $row->la_name,
                    'teacher_trainer_name' => $row->teacher_trainer_name,
                    'last_activity_date'   => $row->Source_Updated_At ? Carbon::parse($row->Source_Updated_At)->format('d/m/Y') : '',
                    'rider_ids'            => [],
                    'course_counts'        => array_fill(0, count($courseIds), 0),
                    'activity_counts'      => array_fill(0, count($activityDefinitions), 0),
                ];
            }

            $groups[$groupKey]['rider_ids'][$row->Source_Rider_Id] = true;

            $courseId = (int)$row->Source_Course_Id;
            $cKey = $row->Source_Rider_Id . ':' . $courseId;
            if (isset($courseIdMap[$courseId]) && !isset($seenCourseTypes[$groupKey][$cKey])) {
                $seenCourseTypes[$groupKey][$cKey] = true;
                $groups[$groupKey]['course_counts'][$courseIdMap[$courseId]]++;
            }

            if (isset($activityRecords[$row->GC_Rider_Course_Key])) {
                foreach ($activityRecords[$row->GC_Rider_Course_Key] as $act) {
                    $actId = (int)$act->Activity_Id;
                    $aKey = $row->Source_Rider_Id . ':' . $actId;
                    if (isset($activityIdMap[$actId]) && !isset($seenActivities[$groupKey][$aKey])) {
                        $seenActivities[$groupKey][$aKey] = true;
                        $groups[$groupKey]['activity_counts'][$activityIdMap[$actId]]++;
                    }
                }
            }
        }

        uasort($groups, function ($a, $b) {
            $cmp = strcasecmp($a['school_name'], $b['school_name']);
            return $cmp !== 0 ? $cmp : strcasecmp($a['teacher_trainer_name'], $b['teacher_trainer_name']);
        });

        $output = [];
        foreach ($groups as $row) {
            $output[] = [
                'school_name'          => $row['school_name'],
                'school_urn'           => $row['school_urn'],
                'la_name'              => $row['la_name'],
                'teacher_trainer_name' => $row['teacher_trainer_name'],
                'last_activity_date'   => $row['last_activity_date'],
                'total_riders'         => count($row['rider_ids']),
                'course_counts'        => $row['course_counts'],
                'activity_counts'      => $row['activity_counts'],
            ];
        }

        return $output;
    }
}

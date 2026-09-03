<?php

namespace App\Reports\Handlers;

use Carbon\Carbon;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Validator;

class GetCyclingAppDataDetailedHandler extends AbstractStreamingReportHandler
{
    protected const ETHNICITIES = [
        'Asian - Bangladeshi',
        'Asian - Chinese',
        'Asian - Indian',
        'Asian - Pakistani',
        'Asia - Any other Asian background',
        'Black - Black African',
        'Black - Black Caribbean',
        'Black - Any other Black background',
        'Mixed - White and Asian',
        'Mixed - White and Black African',
        'Mixed - White and Black Caribbean',
        'Mixed - Any other Mixed background',
        'White - Gypsy/Roma',
        'White - Irish',
        'White - Traveller of Irish heritage',
        'White - White British',
        'White - Any other White background',
        'Any other ethnic group',
    ];

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
        $ethnicityMap = $this->buildEthnicityMap();

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
                'r.Pupil_Premium',
                'r.Send_Code',
                'r.Ethnicity',
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

        $schools = [];
        $riderSeenCourses = [];
        $riderSeenActivities = [];

        foreach ($records as $row) {
            $urn = (string) $row->School_Urn;

            if (!isset($schools[$urn])) {
                $schools[$urn] = [
                    'school_name'         => $row->school_name,
                    'school_urn'          => $urn,
                    'la_name'             => $row->la_name,
                    'last_activity_at'    => null,
                    'riders'              => [],
                    'pupil_premium_count' => 0,
                    'send_count'          => 0,
                    'ethnicity_counts'    => array_fill(0, count(self::ETHNICITIES), 0),
                    'course_counts'       => array_fill(0, count($courseIds), 0),
                    'activity_counts'     => array_fill(0, count($activityDefinitions), 0),
                ];
            }

            // Demographics tracked once per distinct rider
            if (!isset($schools[$urn]['riders'][$row->Source_Rider_Id])) {
                $schools[$urn]['riders'][$row->Source_Rider_Id] = true;

                if ((int)$row->Pupil_Premium === 1) {
                    $schools[$urn]['pupil_premium_count']++;
                }

                if (!empty(trim((string)$row->Send_Code))) {
                    $schools[$urn]['send_count']++;
                }

                $normEth = strtolower(trim((string)$row->Ethnicity));
                if ($normEth !== '' && isset($ethnicityMap[$normEth])) {
                    $schools[$urn]['ethnicity_counts'][$ethnicityMap[$normEth]]++;
                }
            }

            $updatedAt = $row->Source_Updated_At ? Carbon::parse($row->Source_Updated_At) : null;
            if ($updatedAt && ($schools[$urn]['last_activity_at'] === null || $updatedAt->greaterThan($schools[$urn]['last_activity_at']))) {
                $schools[$urn]['last_activity_at'] = $updatedAt;
            }

            $courseId = (int)$row->Source_Course_Id;
            $rcCourseKey = $row->Source_Rider_Id . ':' . $courseId;
            if (isset($courseIdMap[$courseId]) && !isset($riderSeenCourses[$rcCourseKey])) {
                $riderSeenCourses[$rcCourseKey] = true;
                $schools[$urn]['course_counts'][$courseIdMap[$courseId]]++;
            }

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

        uasort($schools, fn($a, $b) => strcasecmp($a['school_name'], $b['school_name']));

        $output = [];
        foreach ($schools as $row) {
            $output[] = [
                'school_name'         => $row['school_name'],
                'school_urn'          => $row['school_urn'],
                'la_name'             => $row['la_name'],
                'last_activity_date'  => $row['last_activity_at']?->format('d/m/Y') ?? '',
                'total_riders'        => count($row['riders']),
                'pupil_premium_count' => $row['pupil_premium_count'],
                'send_count'          => $row['send_count'],
                'ethnicity_counts'    => $row['ethnicity_counts'],
                'course_counts'       => $row['course_counts'],
                'activity_counts'     => $row['activity_counts'],
            ];
        }

        return $output;
    }

    protected function buildEthnicityMap(): array
    {
        $map = [];
        foreach (self::ETHNICITIES as $index => $label) {
            $clean = strtolower(trim($label));
            $map[$clean] = $index;

            // Common aliases matching Characteristic definitions
            $alias = match ($clean) {
                'asian - bangladeshi' => 'bangladeshi',
                'asian - chinese'     => 'chinese',
                'asian - indian'      => 'indian',
                'asian - pakistani'   => 'pakistani',
                'asia - any other asian background',
                'asian - any other asian background' => 'any other asian background',
                'black - black african'   => 'black - african',
                'black - black caribbean' => 'black caribbean',
                'black - any other black background' => 'any other black background',
                'mixed - white and asian' => 'white and asian',
                'mixed - white and black african' => 'white and black african',
                'mixed - white and black caribbean' => 'white and black caribbean',
                'mixed - any other mixed background' => 'any other mixed background',
                'white - gypsy/roma'     => 'gypsy/roma',
                'white - white british'  => 'white - british',
                'white - any other white background' => 'any other white background',
                'any other ethnic group' => 'any other ethnic group',
                default => null,
            };

            if ($alias) {
                $map[$alias] = $index;
            }
        }

        return $map;
    }
}

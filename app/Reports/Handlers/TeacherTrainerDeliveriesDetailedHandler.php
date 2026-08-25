<?php

namespace App\Reports\Handlers;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Validator;

class TeacherTrainerDeliveriesDetailedHandler extends AbstractStreamingReportHandler
{
    public function validate(array $parameters): array
    {
        return Validator::make($parameters, [
            'start_date'  => 'nullable|string',
            'end_date'    => 'nullable|string',
            'school_urn'  => 'nullable|string',
            'course_type' => 'nullable|string',
        ])->validate();
    }

    public function execute(array $params): array
    {
        ini_set('memory_limit', '512M');
        ini_set('max_execution_time', '600');

        $query = $this->buildQuery($params);

        if (empty($this->callbackUrl)) {
            return $query->get()->map(fn($row) => (array)$row)->toArray();
        }

        $query->chunk(500, function ($rows) {
            $chunkArray = $rows->map(fn($row) => (array)$row)->toArray();
            $this->transmitBatch($chunkArray, false);
        });

        $this->transmitBatch([], true);

        return ['status' => 'async_completed'];
    }

    protected function buildQuery(array $params)
    {
        // 1. Pivot Module Metrics
        $metricPivots = DB::connection('mysql')->table('Fact_Teacher_Trainer_Delivery_Metric')
            ->select('TT_Delivery_Module_Key',
                DB::raw("SUM(CASE WHEN Category = 'attended' AND Sub_Category = 'total' THEN Metric_Value ELSE 0 END) as attended_total"),
                DB::raw("SUM(CASE WHEN Category = 'free_school_meals' AND Sub_Category = 'free_school_meals' THEN Metric_Value ELSE 0 END) as fsm_total"),
                DB::raw("SUM(CASE WHEN Category = 'free_school_meals' AND Sub_Category = 'free_school_meals_na' THEN Metric_Value ELSE 0 END) as fsm_na"),
                DB::raw("SUM(CASE WHEN Category = 'send' AND Sub_Category = 'send' THEN Metric_Value ELSE 0 END) as send_total"),
                DB::raw("SUM(CASE WHEN Category = 'send' AND Sub_Category = 'send_na' THEN Metric_Value ELSE 0 END) as send_na"),
                DB::raw("SUM(CASE WHEN Category = 'gender' AND Sub_Category = 'male' THEN Metric_Value ELSE 0 END) as gender_male"),
                DB::raw("SUM(CASE WHEN Category = 'gender' AND Sub_Category = 'female' THEN Metric_Value ELSE 0 END) as gender_female"),
                DB::raw("SUM(CASE WHEN Category = 'gender' AND Sub_Category = 'other' THEN Metric_Value ELSE 0 END) as gender_other"),
                DB::raw("SUM(CASE WHEN Category = 'gender' AND Sub_Category = 'na' THEN Metric_Value ELSE 0 END) as gender_na"),
                DB::raw("SUM(CASE WHEN Category = 'ethnicity' AND Sub_Category = 'asian_bangladeshi' THEN Metric_Value ELSE 0 END) as eth_asian_bangladeshi"),
                DB::raw("SUM(CASE WHEN Category = 'ethnicity' AND Sub_Category = 'asian_chinese' THEN Metric_Value ELSE 0 END) as eth_asian_chinese"),
                DB::raw("SUM(CASE WHEN Category = 'ethnicity' AND Sub_Category = 'asian_indian' THEN Metric_Value ELSE 0 END) as eth_asian_indian"),
                DB::raw("SUM(CASE WHEN Category = 'ethnicity' AND Sub_Category = 'asian_pakistani' THEN Metric_Value ELSE 0 END) as eth_asian_pakistani"),
                DB::raw("SUM(CASE WHEN Category = 'ethnicity' AND Sub_Category = 'asian' THEN Metric_Value ELSE 0 END) as eth_asian_other"),
                DB::raw("SUM(CASE WHEN Category = 'ethnicity' AND Sub_Category = 'black_african' THEN Metric_Value ELSE 0 END) as eth_black_african"),
                DB::raw("SUM(CASE WHEN Category = 'ethnicity' AND Sub_Category = 'black_caribbean' THEN Metric_Value ELSE 0 END) as eth_black_caribbean"),
                DB::raw("SUM(CASE WHEN Category = 'ethnicity' AND Sub_Category = 'black' THEN Metric_Value ELSE 0 END) as eth_black_other"),
                DB::raw("SUM(CASE WHEN Category = 'ethnicity' AND Sub_Category = 'mixed_white_asian' THEN Metric_Value ELSE 0 END) as eth_mixed_white_asian"),
                DB::raw("SUM(CASE WHEN Category = 'ethnicity' AND Sub_Category = 'mixed_white_african' THEN Metric_Value ELSE 0 END) as eth_mixed_white_african"),
                DB::raw("SUM(CASE WHEN Category = 'ethnicity' AND Sub_Category = 'mixed_white_caribbean' THEN Metric_Value ELSE 0 END) as eth_mixed_white_caribbean"),
                DB::raw("SUM(CASE WHEN Category = 'ethnicity' AND Sub_Category = 'mixed' THEN Metric_Value ELSE 0 END) as eth_mixed_other"),
                DB::raw("SUM(CASE WHEN Category = 'ethnicity' AND Sub_Category = 'white_gypsy' THEN Metric_Value ELSE 0 END) as eth_white_gypsy"),
                DB::raw("SUM(CASE WHEN Category = 'ethnicity' AND Sub_Category = 'white_irish' THEN Metric_Value ELSE 0 END) as eth_white_irish"),
                DB::raw("SUM(CASE WHEN Category = 'ethnicity' AND Sub_Category = 'white_traveller' THEN Metric_Value ELSE 0 END) as eth_white_traveller"),
                DB::raw("SUM(CASE WHEN Category = 'ethnicity' AND Sub_Category = 'white_british' THEN Metric_Value ELSE 0 END) as eth_white_british"),
                DB::raw("SUM(CASE WHEN Category = 'ethnicity' AND Sub_Category = 'white' THEN Metric_Value ELSE 0 END) as eth_white_other"),
                DB::raw("SUM(CASE WHEN Category = 'ethnicity' AND Sub_Category = 'other' THEN Metric_Value ELSE 0 END) as eth_other_any"),
                DB::raw("SUM(CASE WHEN Category = 'ethnicity' AND Sub_Category = 'other_arab' THEN Metric_Value ELSE 0 END) as eth_other_arab"),
                DB::raw("SUM(CASE WHEN Category = 'ethnicity' AND Sub_Category = 'na' THEN Metric_Value ELSE 0 END) as eth_na")
            )
            ->groupBy('TT_Delivery_Module_Key');

        // 2. Pivot Hands Up Survey Responses by School and Course
        $surveyPivots = DB::connection('mysql')->table('Fact_Get_Cycling_Survey_Response as sr')
            ->join('Dim_Get_Cycling_Course as c', 'sr.GC_Course_Key', '=', 'c.GC_Course_Key')
            ->select(
                'sr.School_Urn',
                'c.Course_Code',
                DB::raw("SUM(CASE WHEN sr.Option_Id = '1' THEN sr.Response_Count ELSE 0 END) as survey_enjoyed"),
                DB::raw("SUM(CASE WHEN sr.Option_Id = '2' THEN sr.Response_Count ELSE 0 END) as survey_did_not_enjoy"),
                DB::raw("SUM(CASE WHEN sr.Option_Id = '3' THEN sr.Response_Count ELSE 0 END) as survey_not_sure"),
                DB::raw("SUM(CASE WHEN sr.Option_Id = '4' THEN sr.Response_Count ELSE 0 END) as survey_more_confident"),
                DB::raw("SUM(CASE WHEN sr.Option_Id = '5' THEN sr.Response_Count ELSE 0 END) as survey_less_confident"),
                DB::raw("SUM(CASE WHEN sr.Option_Id = '6' THEN sr.Response_Count ELSE 0 END) as survey_no_difference")
            )
            ->groupBy('sr.School_Urn', 'c.Course_Code');

        // 3. Assemble Primary Query
        $query = DB::connection('mysql')->table('Fact_Teacher_Trainer_Delivery_Module as m')
            ->join('Fact_Teacher_Trainer_Delivery as d', 'm.TT_Delivery_Key', '=', 'd.TT_Delivery_Key')
            ->leftJoin('Dim_School as s', 'd.School_Key', '=', 's.School_Key')
            ->leftJoin('Dim_Teacher_Trainer as tt', 'd.Teacher_Trainer_Key', '=', 'tt.Teacher_Trainer_Key')
            ->leftJoinSub($metricPivots, 'mp', 'm.TT_Delivery_Module_Key', '=', 'mp.TT_Delivery_Module_Key')
            ->leftJoinSub($surveyPivots, 'sp', function ($join) {
                $join->on('d.School_Urn', '=', 'sp.School_Urn')
                    ->on('m.Module_Id', '=', 'sp.Course_Code');
            })
            ->select([
                DB::raw("IFNULL(tt.Source_Teacher_Trainer_Id, '') as teacher_id"),
                DB::raw("IFNULL(tt.First_Name, '') as first_name"),
                DB::raw("IFNULL(tt.Last_Name, '') as last_name"),
                'd.School_Urn as school_urn',
                DB::raw("IFNULL(s.School_Name, 'N/A') as school_name"),
                DB::raw("IFNULL(s.La_Name, '') as la_name"),
                DB::raw("IFNULL(DATE_FORMAT(COALESCE(d.Completion_Date, d.Date_Delivery_End), '%d/%m/%Y'), '') as completion_date"),
                DB::raw("IFNULL(m.Module_Label, m.Module_Id) as course_label"),
                DB::raw("IFNULL(mp.attended_total, 0) as no_attended"),
                DB::raw("IFNULL(mp.fsm_total, 0) as no_pupil_premium"),
                DB::raw("IFNULL(mp.send_total, 0) as no_send"),
                DB::raw("IFNULL(mp.send_total, 0) as send_count"),
                DB::raw("IFNULL(mp.send_na, 0) as send_na"),
                DB::raw("IFNULL(mp.fsm_total, 0) as fsm_count"),
                DB::raw("IFNULL(mp.fsm_na, 0) as fsm_na"),
                DB::raw("IFNULL(mp.gender_male, 0) as gender_male"),
                DB::raw("IFNULL(mp.gender_female, 0) as gender_female"),
                DB::raw("IFNULL(mp.gender_other, 0) as gender_other"),
                DB::raw("IFNULL(mp.gender_na, 0) as gender_na"),
                DB::raw("IFNULL(mp.eth_asian_bangladeshi, 0) as eth_asian_bangladeshi"),
                DB::raw("IFNULL(mp.eth_asian_chinese, 0) as eth_asian_chinese"),
                DB::raw("IFNULL(mp.eth_asian_indian, 0) as eth_asian_indian"),
                DB::raw("IFNULL(mp.eth_asian_pakistani, 0) as eth_asian_pakistani"),
                DB::raw("IFNULL(mp.eth_asian_other, 0) as eth_asian_other"),
                DB::raw("IFNULL(mp.eth_black_african, 0) as eth_black_african"),
                DB::raw("IFNULL(mp.eth_black_caribbean, 0) as eth_black_caribbean"),
                DB::raw("IFNULL(mp.eth_black_other, 0) as eth_black_other"),
                DB::raw("IFNULL(mp.eth_mixed_white_asian, 0) as eth_mixed_white_asian"),
                DB::raw("IFNULL(mp.eth_mixed_white_african, 0) as eth_mixed_white_african"),
                DB::raw("IFNULL(mp.eth_mixed_white_caribbean, 0) as eth_mixed_white_caribbean"),
                DB::raw("IFNULL(mp.eth_mixed_other, 0) as eth_mixed_other"),
                DB::raw("IFNULL(mp.eth_white_gypsy, 0) as eth_white_gypsy"),
                DB::raw("IFNULL(mp.eth_white_irish, 0) as eth_white_irish"),
                DB::raw("IFNULL(mp.eth_white_traveller, 0) as eth_white_traveller"),
                DB::raw("IFNULL(mp.eth_white_british, 0) as eth_white_british"),
                DB::raw("IFNULL(mp.eth_white_other, 0) as eth_white_other"),
                DB::raw("IFNULL(mp.eth_other_any, 0) as eth_other_any"),
                DB::raw("IFNULL(mp.eth_other_arab, 0) as eth_other_arab"),
                DB::raw("IFNULL(mp.eth_na, 0) as eth_na"),
                DB::raw("IFNULL(sp.survey_enjoyed, 0) as survey_enjoyed"),
                DB::raw("IFNULL(sp.survey_did_not_enjoy, 0) as survey_did_not_enjoy"),
                DB::raw("IFNULL(sp.survey_not_sure, 0) as survey_not_sure"),
                DB::raw("IFNULL(sp.survey_more_confident, 0) as survey_more_confident"),
                DB::raw("IFNULL(sp.survey_less_confident, 0) as survey_less_confident"),
                DB::raw("IFNULL(sp.survey_no_difference, 0) as survey_no_difference"),
            ]);

        if (!empty($params['start_date']) && !empty($params['end_date'])) {
            $query->whereBetween(DB::raw('COALESCE(d.Completion_Date, d.Date_Delivery_End)'), [
                $params['start_date'],
                $params['end_date'],
            ]);
        }

        if (!empty($params['school_urn'])) {
            $query->where('d.School_Urn', $params['school_urn']);
        }

        if (!empty($params['course_type']) && $params['course_type'] !== 'All') {
            $query->where('m.Module_Id', $params['course_type']);
        }

        return $query->orderBy('d.Completion_Date', 'desc')
            ->orderBy('d.TT_Delivery_Key', 'asc')
            ->orderBy('m.TT_Delivery_Module_Key', 'asc');
    }
}

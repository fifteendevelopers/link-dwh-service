<?php

namespace App\Reports\Handlers;

use App\Reports\Contracts\ReportHandlerInterface;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Validator;

class RiderActivityOutcomesHandler extends AbstractStreamingReportHandler implements ReportHandlerInterface
{
    public function validate(array $parameters): array
    {
        return Validator::make($parameters, [
            'year'         => 'nullable|integer',
            'start_date'   => 'nullable|string',
            'end_date'     => 'nullable|string',
            'recipient_id' => 'nullable|integer',
            'provider_id'  => 'nullable|integer',
            'course_code'  => 'nullable|string',
        ])->validate();
    }

    public function execute(array $params): array
    {
        ini_set('memory_limit', '512M');
        ini_set('max_execution_time', '600');

        $query = $this->buildQuery($params);

        if (empty($this->callbackUrl)) {
            return $query->get()->map(fn($row) => (array) $row)->toArray();
        }

        $chunkSize = 250;

        $query->chunk($chunkSize, function ($rows) {
            $chunkArray = $rows->map(fn($row) => (array) $row)->toArray();
            $this->transmitBatch($chunkArray, false);
        });

        $this->transmitBatch([], true);

        return ['status' => 'async_completed'];
    }

    protected function buildQuery(array $params)
    {
        $query = DB::connection('mysql')->table('Fact_Rider_Activity_Outcome as fao')
            ->join('Dim_Delivery_Header as dh', 'fao.Delivery_Key', '=', 'dh.Delivery_Key')
            ->join('Dim_Course as c', 'fao.Course_Key', '=', 'c.Course_Key')
            ->join('Dim_Course_Activity as a', 'fao.Activity_Key', '=', 'a.Activity_Key')
            ->join('Dim_Training_Provider as tp', 'dh.Training_Provider_Key', '=', 'tp.Provider_Key')
            ->leftJoin('Fact_Course_Delivery as fcd', function ($join) {
                $join->on('fcd.Delivery_Key', '=', 'fao.Delivery_Key')
                    ->on('fcd.Course_Key', '=', 'fao.Course_Key');
            })
            ->leftJoin('Dim_School as s', 'dh.School_Key', '=', 's.School_Key')
            ->leftJoin('Dim_Grant as g', 'dh.Grant_Key', '=', 'g.Grant_Key')
            ->leftJoin('Dim_Grant_Recipient as gr', 'g.Grant_Recipient_Key', '=', 'gr.Recipient_Key')
            ->select([
                'dh.Source_Delivery_Id as delivery_id',
                DB::raw("IFNULL(s.School_Urn, '') as school_id"),
                DB::raw("IFNULL(s.School_Name, 'N/A') as school_name"),
                DB::raw("IFNULL(s.La_Name, '') as la_name"),
                'tp.Provider_Name as tp_name',
                DB::raw("IFNULL(s.Imd_Decile, '') as imd"),
                DB::raw("IFNULL(s.Rural_Urban_Classification, '') as rural_classification"),
                DB::raw("IFNULL(fcd.Count_Attended_Confirmed, 0) as total_attended"),
                DB::raw("IFNULL(fcd.Count_SEND, 0) as send_count"),
                DB::raw("IFNULL(fcd.Count_Pupil_Premium, 0) as pupil_premium_count"),
                DB::raw("IFNULL(dh.Fleet_Cycles_Used, 0) as fleet_cycles_used"),
                DB::raw("IFNULL(c.Course_Level, a.Course_Code) as course_level"),

                // Level 1
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'l1-ac2' AND fao.Outcome_Score = 1 THEN 1 ELSE 0 END) as l1_ac2_on_own"),
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'l1-ac2' AND fao.Outcome_Score = 2 THEN 1 ELSE 0 END) as l1_ac2_practice"),
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'l1-ac2' AND fao.Outcome_Score = 3 THEN 1 ELSE 0 END) as l1_ac2_assistance"),
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'l1-ac2' AND fao.Outcome_Score = 4 THEN 1 ELSE 0 END) as l1_ac2_not_seen"),

                DB::raw("SUM(CASE WHEN a.Activity_Code = 'l1-ac3' AND fao.Outcome_Score = 1 THEN 1 ELSE 0 END) as l1_ac3_on_own"),
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'l1-ac3' AND fao.Outcome_Score = 2 THEN 1 ELSE 0 END) as l1_ac3_practice"),
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'l1-ac3' AND fao.Outcome_Score = 3 THEN 1 ELSE 0 END) as l1_ac3_assistance"),
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'l1-ac3' AND fao.Outcome_Score = 4 THEN 1 ELSE 0 END) as l1_ac3_not_seen"),

                DB::raw("SUM(CASE WHEN a.Activity_Code = 'l1-ac4' AND fao.Outcome_Score = 1 THEN 1 ELSE 0 END) as l1_ac4_on_own"),
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'l1-ac4' AND fao.Outcome_Score = 2 THEN 1 ELSE 0 END) as l1_ac4_practice"),
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'l1-ac4' AND fao.Outcome_Score = 3 THEN 1 ELSE 0 END) as l1_ac4_assistance"),
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'l1-ac4' AND fao.Outcome_Score = 4 THEN 1 ELSE 0 END) as l1_ac4_not_seen"),

                DB::raw("SUM(CASE WHEN a.Activity_Code = 'l1-ac5' AND fao.Outcome_Score = 1 THEN 1 ELSE 0 END) as l1_ac5_on_own"),
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'l1-ac5' AND fao.Outcome_Score = 2 THEN 1 ELSE 0 END) as l1_ac5_practice"),
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'l1-ac5' AND fao.Outcome_Score = 3 THEN 1 ELSE 0 END) as l1_ac5_assistance"),
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'l1-ac5' AND fao.Outcome_Score = 4 THEN 1 ELSE 0 END) as l1_ac5_not_seen"),

                // Level 2
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'l2-ac1' AND fao.Outcome_Score = 1 THEN 1 ELSE 0 END) as l2_ac1_on_own"),
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'l2-ac1' AND fao.Outcome_Score = 2 THEN 1 ELSE 0 END) as l2_ac1_practice"),
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'l2-ac1' AND fao.Outcome_Score = 3 THEN 1 ELSE 0 END) as l2_ac1_assistance"),
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'l2-ac1' AND fao.Outcome_Score = 4 THEN 1 ELSE 0 END) as l2_ac1_not_seen"),

                DB::raw("SUM(CASE WHEN a.Activity_Code = 'l2-ac2' AND fao.Outcome_Score = 1 THEN 1 ELSE 0 END) as l2_ac2_on_own"),
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'l2-ac2' AND fao.Outcome_Score = 2 THEN 1 ELSE 0 END) as l2_ac2_practice"),
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'l2-ac2' AND fao.Outcome_Score = 3 THEN 1 ELSE 0 END) as l2_ac2_assistance"),
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'l2-ac2' AND fao.Outcome_Score = 4 THEN 1 ELSE 0 END) as l2_ac2_not_seen"),

                DB::raw("SUM(CASE WHEN a.Activity_Code = 'l2-ac3' AND fao.Outcome_Score = 1 THEN 1 ELSE 0 END) as l2_ac3_on_own"),
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'l2-ac3' AND fao.Outcome_Score = 2 THEN 1 ELSE 0 END) as l2_ac3_practice"),
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'l2-ac3' AND fao.Outcome_Score = 3 THEN 1 ELSE 0 END) as l2_ac3_assistance"),
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'l2-ac3' AND fao.Outcome_Score = 4 THEN 1 ELSE 0 END) as l2_ac3_not_seen"),

                DB::raw("SUM(CASE WHEN a.Activity_Code = 'l2-ac4' AND fao.Outcome_Score = 1 THEN 1 ELSE 0 END) as l2_ac4_on_own"),
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'l2-ac4' AND fao.Outcome_Score = 2 THEN 1 ELSE 0 END) as l2_ac4_practice"),
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'l2-ac4' AND fao.Outcome_Score = 3 THEN 1 ELSE 0 END) as l2_ac4_assistance"),
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'l2-ac4' AND fao.Outcome_Score = 4 THEN 1 ELSE 0 END) as l2_ac4_not_seen"),

                DB::raw("SUM(CASE WHEN a.Activity_Code = 'l2-ac5' AND fao.Outcome_Score = 1 THEN 1 ELSE 0 END) as l2_ac5_on_own"),
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'l2-ac5' AND fao.Outcome_Score = 2 THEN 1 ELSE 0 END) as l2_ac5_practice"),
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'l2-ac5' AND fao.Outcome_Score = 3 THEN 1 ELSE 0 END) as l2_ac5_assistance"),
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'l2-ac5' AND fao.Outcome_Score = 4 THEN 1 ELSE 0 END) as l2_ac5_not_seen"),

                DB::raw("SUM(CASE WHEN a.Activity_Code = 'l2-ac6' AND fao.Outcome_Score = 1 THEN 1 ELSE 0 END) as l2_ac6_on_own"),
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'l2-ac6' AND fao.Outcome_Score = 2 THEN 1 ELSE 0 END) as l2_ac6_practice"),
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'l2-ac6' AND fao.Outcome_Score = 3 THEN 1 ELSE 0 END) as l2_ac6_assistance"),
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'l2-ac6' AND fao.Outcome_Score = 4 THEN 1 ELSE 0 END) as l2_ac6_not_seen"),

                DB::raw("SUM(CASE WHEN a.Activity_Code = 'l2-ac7' AND fao.Outcome_Score = 1 THEN 1 ELSE 0 END) as l2_ac7_on_own"),
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'l2-ac7' AND fao.Outcome_Score = 2 THEN 1 ELSE 0 END) as l2_ac7_practice"),
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'l2-ac7' AND fao.Outcome_Score = 3 THEN 1 ELSE 0 END) as l2_ac7_assistance"),
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'l2-ac7' AND fao.Outcome_Score = 4 THEN 1 ELSE 0 END) as l2_ac7_not_seen"),

                // Level 3
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'l3-ac1' AND fao.Outcome_Score = 1 THEN 1 ELSE 0 END) as l3_ac1_on_own"),
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'l3-ac1' AND fao.Outcome_Score = 2 THEN 1 ELSE 0 END) as l3_ac1_practice"),
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'l3-ac1' AND fao.Outcome_Score = 3 THEN 1 ELSE 0 END) as l3_ac1_assistance"),
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'l3-ac1' AND fao.Outcome_Score = 4 THEN 1 ELSE 0 END) as l3_ac1_not_seen"),

                DB::raw("SUM(CASE WHEN a.Activity_Code = 'l3-ac2' AND fao.Outcome_Score = 1 THEN 1 ELSE 0 END) as l3_ac2_on_own"),
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'l3-ac2' AND fao.Outcome_Score = 2 THEN 1 ELSE 0 END) as l3_ac2_practice"),
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'l3-ac2' AND fao.Outcome_Score = 3 THEN 1 ELSE 0 END) as l3_ac2_assistance"),
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'l3-ac2' AND fao.Outcome_Score = 4 THEN 1 ELSE 0 END) as l3_ac2_not_seen"),

                DB::raw("SUM(CASE WHEN a.Activity_Code = 'l3-ac3' AND fao.Outcome_Score = 1 THEN 1 ELSE 0 END) as l3_ac3_on_own"),
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'l3-ac3' AND fao.Outcome_Score = 2 THEN 1 ELSE 0 END) as l3_ac3_practice"),
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'l3-ac3' AND fao.Outcome_Score = 3 THEN 1 ELSE 0 END) as l3_ac3_assistance"),
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'l3-ac3' AND fao.Outcome_Score = 4 THEN 1 ELSE 0 END) as l3_ac3_not_seen"),

                DB::raw("SUM(CASE WHEN a.Activity_Code = 'l3-ac4' AND fao.Outcome_Score = 1 THEN 1 ELSE 0 END) as l3_ac4_on_own"),
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'l3-ac4' AND fao.Outcome_Score = 2 THEN 1 ELSE 0 END) as l3_ac4_practice"),
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'l3-ac4' AND fao.Outcome_Score = 3 THEN 1 ELSE 0 END) as l3_ac4_assistance"),
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'l3-ac4' AND fao.Outcome_Score = 4 THEN 1 ELSE 0 END) as l3_ac4_not_seen"),

                DB::raw("SUM(CASE WHEN a.Activity_Code = 'l3-ac5' AND fao.Outcome_Score = 1 THEN 1 ELSE 0 END) as l3_ac5_on_own"),
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'l3-ac5' AND fao.Outcome_Score = 2 THEN 1 ELSE 0 END) as l3_ac5_practice"),
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'l3-ac5' AND fao.Outcome_Score = 3 THEN 1 ELSE 0 END) as l3_ac5_assistance"),
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'l3-ac5' AND fao.Outcome_Score = 4 THEN 1 ELSE 0 END) as l3_ac5_not_seen"),

                DB::raw("SUM(CASE WHEN a.Activity_Code = 'l3-ac6' AND fao.Outcome_Score = 1 THEN 1 ELSE 0 END) as l3_ac6_on_own"),
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'l3-ac6' AND fao.Outcome_Score = 2 THEN 1 ELSE 0 END) as l3_ac6_practice"),
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'l3-ac6' AND fao.Outcome_Score = 3 THEN 1 ELSE 0 END) as l3_ac6_assistance"),
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'l3-ac6' AND fao.Outcome_Score = 4 THEN 1 ELSE 0 END) as l3_ac6_not_seen"),

                DB::raw("SUM(CASE WHEN a.Activity_Code = 'l3-ac7' AND fao.Outcome_Score = 1 THEN 1 ELSE 0 END) as l3_ac7_on_own"),
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'l3-ac7' AND fao.Outcome_Score = 2 THEN 1 ELSE 0 END) as l3_ac7_practice"),
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'l3-ac7' AND fao.Outcome_Score = 3 THEN 1 ELSE 0 END) as l3_ac7_assistance"),
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'l3-ac7' AND fao.Outcome_Score = 4 THEN 1 ELSE 0 END) as l3_ac7_not_seen"),

                DB::raw("SUM(CASE WHEN a.Activity_Code = 'l3-ac8' AND fao.Outcome_Score = 1 THEN 1 ELSE 0 END) as l3_ac8_on_own"),
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'l3-ac8' AND fao.Outcome_Score = 2 THEN 1 ELSE 0 END) as l3_ac8_practice"),
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'l3-ac8' AND fao.Outcome_Score = 3 THEN 1 ELSE 0 END) as l3_ac8_assistance"),
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'l3-ac8' AND fao.Outcome_Score = 4 THEN 1 ELSE 0 END) as l3_ac8_not_seen"),

                // Plus Balance
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'pb-ac1' AND fao.Outcome_Score = 1 THEN 1 ELSE 0 END) as pb_ac1_on_own"),
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'pb-ac1' AND fao.Outcome_Score = 2 THEN 1 ELSE 0 END) as pb_ac1_practice"),
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'pb-ac1' AND fao.Outcome_Score = 3 THEN 1 ELSE 0 END) as pb_ac1_assistance"),
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'pb-ac1' AND fao.Outcome_Score = 4 THEN 1 ELSE 0 END) as pb_ac1_not_seen"),

                DB::raw("SUM(CASE WHEN a.Activity_Code = 'pb-ac2' AND fao.Outcome_Score = 1 THEN 1 ELSE 0 END) as pb_ac2_on_own"),
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'pb-ac2' AND fao.Outcome_Score = 2 THEN 1 ELSE 0 END) as pb_ac2_practice"),
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'pb-ac2' AND fao.Outcome_Score = 3 THEN 1 ELSE 0 END) as pb_ac2_assistance"),
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'pb-ac2' AND fao.Outcome_Score = 4 THEN 1 ELSE 0 END) as pb_ac2_not_seen"),

                DB::raw("SUM(CASE WHEN a.Activity_Code = 'pb-ac3' AND fao.Outcome_Score = 1 THEN 1 ELSE 0 END) as pb_ac3_on_own"),
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'pb-ac3' AND fao.Outcome_Score = 2 THEN 1 ELSE 0 END) as pb_ac3_practice"),
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'pb-ac3' AND fao.Outcome_Score = 3 THEN 1 ELSE 0 END) as pb_ac3_assistance"),
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'pb-ac3' AND fao.Outcome_Score = 4 THEN 1 ELSE 0 END) as pb_ac3_not_seen"),

                // Plus Learn
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'pl-ac1' AND fao.Outcome_Score = 1 THEN 1 ELSE 0 END) as pl_ac1_on_own"),
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'pl-ac1' AND fao.Outcome_Score = 2 THEN 1 ELSE 0 END) as pl_ac1_practice"),
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'pl-ac1' AND fao.Outcome_Score = 3 THEN 1 ELSE 0 END) as pl_ac1_assistance"),
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'pl-ac1' AND fao.Outcome_Score = 4 THEN 1 ELSE 0 END) as pl_ac1_not_seen"),

                DB::raw("SUM(CASE WHEN a.Activity_Code = 'pl-ac2' AND fao.Outcome_Score = 1 THEN 1 ELSE 0 END) as pl_ac2_on_own"),
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'pl-ac2' AND fao.Outcome_Score = 2 THEN 1 ELSE 0 END) as pl_ac2_practice"),
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'pl-ac2' AND fao.Outcome_Score = 3 THEN 1 ELSE 0 END) as pl_ac2_assistance"),
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'pl-ac2' AND fao.Outcome_Score = 4 THEN 1 ELSE 0 END) as pl_ac2_not_seen"),

                DB::raw("SUM(CASE WHEN a.Activity_Code = 'pl-ac3' AND fao.Outcome_Score = 1 THEN 1 ELSE 0 END) as pl_ac3_on_own"),
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'pl-ac3' AND fao.Outcome_Score = 2 THEN 1 ELSE 0 END) as pl_ac3_practice"),
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'pl-ac3' AND fao.Outcome_Score = 3 THEN 1 ELSE 0 END) as pl_ac3_assistance"),
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'pl-ac3' AND fao.Outcome_Score = 4 THEN 1 ELSE 0 END) as pl_ac3_not_seen"),

                DB::raw("SUM(CASE WHEN a.Activity_Code = 'pl-ac4' AND fao.Outcome_Score = 1 THEN 1 ELSE 0 END) as pl_ac4_on_own"),
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'pl-ac4' AND fao.Outcome_Score = 2 THEN 1 ELSE 0 END) as pl_ac4_practice"),
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'pl-ac4' AND fao.Outcome_Score = 3 THEN 1 ELSE 0 END) as pl_ac4_assistance"),
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'pl-ac4' AND fao.Outcome_Score = 4 THEN 1 ELSE 0 END) as pl_ac4_not_seen"),

                DB::raw("SUM(CASE WHEN a.Activity_Code = 'pl-ac5' AND fao.Outcome_Score = 1 THEN 1 ELSE 0 END) as pl_ac5_on_own"),
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'pl-ac5' AND fao.Outcome_Score = 2 THEN 1 ELSE 0 END) as pl_ac5_practice"),
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'pl-ac5' AND fao.Outcome_Score = 3 THEN 1 ELSE 0 END) as pl_ac5_assistance"),
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'pl-ac5' AND fao.Outcome_Score = 4 THEN 1 ELSE 0 END) as pl_ac5_not_seen"),

                // Plus Fix
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'pf-ac1' AND fao.Outcome_Score = 1 THEN 1 ELSE 0 END) as pf_ac1_on_own"),
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'pf-ac1' AND fao.Outcome_Score = 2 THEN 1 ELSE 0 END) as pf_ac1_practice"),
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'pf-ac1' AND fao.Outcome_Score = 3 THEN 1 ELSE 0 END) as pf_ac1_assistance"),
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'pf-ac1' AND fao.Outcome_Score = 4 THEN 1 ELSE 0 END) as pf_ac1_not_seen"),

                DB::raw("SUM(CASE WHEN a.Activity_Code = 'pf-ac2' AND fao.Outcome_Score = 1 THEN 1 ELSE 0 END) as pf_ac2_on_own"),
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'pf-ac2' AND fao.Outcome_Score = 2 THEN 1 ELSE 0 END) as pf_ac2_practice"),
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'pf-ac2' AND fao.Outcome_Score = 3 THEN 1 ELSE 0 END) as pf_ac2_assistance"),
                DB::raw("SUM(CASE WHEN a.Activity_Code = 'pf-ac2' AND fao.Outcome_Score = 4 THEN 1 ELSE 0 END) as pf_ac2_not_seen"),
            ]);

        if (!empty($params['year'])) {
            $query->where('g.Grant_Period_Start_Year', (int) $params['year']);
        }

        if (!empty($params['recipient_id'])) {
            $query->where('gr.Source_Recipient_Id', (int) $params['recipient_id']);
        }

        if (!empty($params['provider_id'])) {
            $query->where('tp.Source_Provider_Id', (int) $params['provider_id']);
        }

        if (!empty($params['course_code'])) {
            $query->where('a.Course_Code', $params['course_code']);
        }

        if (!empty($params['start_date']) && !empty($params['end_date'])) {
            $query->whereBetween('dh.Date_Delivery_Start', [$params['start_date'], $params['end_date']]);
        }

        return $query->groupBy([
            'dh.Source_Delivery_Id',
            's.School_Urn',
            's.School_Name',
            's.La_Name',
            'tp.Provider_Name',
            's.Imd_Decile',
            's.Rural_Urban_Classification',
            'fcd.Count_Attended_Confirmed',
            'fcd.Count_SEND',
            'fcd.Count_Pupil_Premium',
            'dh.Fleet_Cycles_Used',
            'c.Course_Level',
            'a.Course_Code',
        ])
            ->orderBy('dh.Source_Delivery_Id', 'asc');
    }
}

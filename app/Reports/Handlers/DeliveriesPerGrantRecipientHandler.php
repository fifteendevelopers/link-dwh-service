<?php

namespace App\Reports\Handlers;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Validator;

class DeliveriesPerGrantRecipientHandler extends AbstractStreamingReportHandler
{
    public function validate(array $parameters): array
    {
        return Validator::make($parameters, [
            'year'         => 'nullable|integer',
            'recipient_id' => 'nullable|integer',
            'start_date'   => 'nullable|string',
            'end_date'     => 'nullable|string',
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

        $chunkSize = 500;

        $query->chunk($chunkSize, function ($rows) {
            $chunkArray = $rows->map(fn($row) => (array)$row)->toArray();
            $this->transmitBatch($chunkArray, false);
        });

        $this->transmitBatch([], true);

        return ['status' => 'async_completed'];
    }

    protected function buildQuery(array $params)
    {
        $query = DB::connection('mysql')->table('Fact_Course_Delivery as fcd')
            ->join('Dim_Delivery_Header as dh', 'fcd.Delivery_Key', '=', 'dh.Delivery_Key')
            ->join('Dim_Course as c', 'fcd.Course_Key', '=', 'c.Course_Key')
            ->join('Dim_Grant as g', 'fcd.Grant_Key', '=', 'g.Grant_Key')
            ->join('Dim_Grant_Recipient as gr', 'g.Grant_Recipient_Key', '=', 'gr.Recipient_Key')
            ->leftJoin('Dim_Training_Provider as tp', 'dh.Training_Provider_Key', '=', 'tp.Provider_Key')
            ->leftJoin('Dim_School as s', 'dh.School_Key', '=', 's.School_Key')
            ->select([
                'dh.Source_Delivery_Id as delivery_id',
                'gr.Recipient_Name as recipient_name',
                'gr.Recipient_Number as recipient_number',
                'g.Grant_Number as grant_number',
                'g.Grant_Source as grant_source',
                'g.Grant_Period_Start_Year as financial_year',
                'tp.Provider_Name as tp_name',
                DB::raw("IFNULL(s.School_Urn, '') as school_urn"),
                DB::raw("IFNULL(s.School_Name, 'N/A') as school_name"),
                DB::raw("IFNULL(s.La_Name, '') as la_name"),
                DB::raw("IFNULL(s.Rural_Urban_Classification, '') as rural_classification"),
                'c.Course_Code as course_code',
                'c.Course_Level as course_level',
                DB::raw("IFNULL(DATE_FORMAT(dh.Date_Delivery_Start, '%d/%m/%Y'), '') as date_delivery_start"),
                DB::raw("IFNULL(DATE_FORMAT(dh.Date_Delivery_End, '%d/%m/%Y'), '') as date_delivery_end"),
                'dh.Delivery_Status as delivery_status',
                DB::raw("IFNULL(fcd.Riders_Enrolled_Count, 0) as riders_booked"),
                DB::raw("IFNULL(fcd.Count_Attended_Confirmed, 0) as riders_attended"),
                DB::raw("IFNULL(fcd.Count_Male, 0) as count_male"),
                DB::raw("IFNULL(fcd.Count_Female, 0) as count_female"),
                DB::raw("IFNULL(fcd.Count_SEND, 0) as count_send"),
                DB::raw("IFNULL(fcd.Count_Pupil_Premium, 0) as count_pupil_premium"),
                DB::raw("IFNULL(fcd.Count_Ethnic_Minority, 0) as count_ethnic_minority"),
            ])
            ->whereNull('c.Parent_Course_Key');

        if (!empty($params['year'])) {
            $query->where('g.Grant_Period_Start_Year', (int)$params['year']);
        }

        if (!empty($params['recipient_id'])) {
            $query->where('gr.Source_Recipient_Id', (int)$params['recipient_id']);
        }

        if (!empty($params['start_date']) && !empty($params['end_date'])) {
            $query->whereBetween('dh.Date_Delivery_Start', [$params['start_date'], $params['end_date']]);
        }

        return $query->orderBy('gr.Recipient_Name', 'asc')
            ->orderBy('dh.Source_Delivery_Id', 'asc');
    }
}

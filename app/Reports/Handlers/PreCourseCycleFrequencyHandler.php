<?php

namespace App\Reports\Handlers;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;
use Illuminate\Support\Facades\Validator;

class PreCourseCycleFrequencyHandler extends AbstractStreamingReportHandler
{
    public function validate(array $parameters): array
    {
        return Validator::make($parameters, [
            'grant_id'     => 'nullable|integer',
            'recipient_id' => 'nullable|integer',
            'provider_id'  => 'nullable|integer',
            'start_date'   => 'nullable|date_format:Y-m-d',
            'end_date'     => 'nullable|date_format:Y-m-d',
        ])->validate();
    }

    public function execute(array $params): array
    {
        ini_set('memory_limit', '1024M');
        ini_set('max_execution_time', '300');

        $query = DB::connection('mysql')->table('Dim_Consent as dc')
            ->join('Dim_Rider as r', 'dc.Rider_Key', '=', 'r.Rider_Key')
            ->join('Dim_Delivery_Header as dh', 'dc.Delivery_Key', '=', 'dh.Delivery_Key')
            ->join('Dim_Grant as g', 'dh.Grant_Key', '=', 'g.Grant_Key')
            ->join('Dim_Grant_Recipient as gr', 'g.Grant_Recipient_Key', '=', 'gr.Recipient_Key')
            ->join('Dim_Training_Provider as tp', 'dh.Training_Provider_Key', '=', 'tp.Provider_Key')
            ->leftJoin('Dim_School as s', 'dh.School_Key', '=', 's.School_Key')
            ->leftJoin('Dim_Organisation as o', 'dh.Organisation_Key', '=', 'o.Organisation_Key')
            ->select([
                'g.Grant_Number',
                'g.Grant_Source',
                'gr.Recipient_Name',
                'dh.Source_Delivery_Id as Delivery_ID',
                'tp.Provider_Name as Training_Provider',
                DB::raw("COALESCE(NULLIF(s.School_Name, ''), NULLIF(o.Organisation_Name, ''), 'N/A') as School_Name"),
                'r.Source_Rider_Id as Rider_ID',
                'dc.Year_Group',
                'dh.Consent_Cutoff_Date',

                // Direct Integer Evaluation
                DB::raw("CASE dc.Pre_Freq_To_School
                    WHEN 5 THEN 'Not applicable: My child cannot yet cycle'
                    WHEN 6 THEN 'Never'
                    WHEN 7 THEN 'Less than once a month'
                    WHEN 8 THEN 'Once or twice a month'
                    WHEN 9 THEN 'One to three days a week'
                    WHEN 10 THEN 'Four or more days a week'
                    ELSE 'Not Provided'
                END as Frequency_School"),

                DB::raw("CASE dc.Pre_Freq_Leisure
                    WHEN 5 THEN 'Not applicable: My child cannot yet cycle'
                    WHEN 6 THEN 'Never'
                    WHEN 7 THEN 'Less than once a month'
                    WHEN 8 THEN 'Once or twice a month'
                    WHEN 9 THEN 'One to three days a week'
                    WHEN 10 THEN 'Four or more days a week'
                    ELSE 'Not Provided'
                END as Frequency_Leisure"),

                DB::raw("CASE dc.Pre_Freq_Exercise
                    WHEN 5 THEN 'Not applicable: My child cannot yet cycle'
                    WHEN 6 THEN 'Never'
                    WHEN 7 THEN 'Less than once a month'
                    WHEN 8 THEN 'Once or twice a month'
                    WHEN 9 THEN 'One to three days a week'
                    WHEN 10 THEN 'Four or more days a week'
                    ELSE 'Not Provided'
                END as Frequency_Exercise"),

                DB::raw("CASE dc.Pre_Freq_Other
                    WHEN 5 THEN 'Not applicable: My child cannot yet cycle'
                    WHEN 6 THEN 'Never'
                    WHEN 7 THEN 'Less than once a month'
                    WHEN 8 THEN 'Once or twice a month'
                    WHEN 9 THEN 'One to three days a week'
                    WHEN 10 THEN 'Four or more days a week'
                    ELSE 'Not Provided'
                END as Frequency_Other")
            ]);

        if (isset($params['grant_id']) && $params['grant_id'] !== '' && $params['grant_id'] !== null) {
            $query->where('g.Source_Grant_Id', $params['grant_id']);
        }

        if (isset($params['recipient_id']) && $params['recipient_id'] !== '' && $params['recipient_id'] !== null) {
            $query->where('gr.Source_Recipient_Id', $params['recipient_id']);
        }

        if (isset($params['provider_id']) && $params['provider_id'] !== '' && $params['provider_id'] !== null) {
            $query->where('tp.Source_Provider_Id', $params['provider_id']);
        }

        if (isset($params['start_date']) && $params['start_date'] !== '' && $params['start_date'] !== null) {
            $query->where('dh.Consent_Cutoff_Date', '>=', $params['start_date']);
        }

        if (isset($params['end_date']) && $params['end_date'] !== '' && $params['end_date'] !== null) {
            $query->where('dh.Consent_Cutoff_Date', '<=', $params['end_date']);
        }

        $results = $query->orderBy('g.Grant_Number')
            ->orderBy('dh.Source_Delivery_Id')
            ->get();

        // Convert explicitly into an array of associative dictionaries
        return $results->map(function ($row) {
            return [
                'Grant_Number'       => $row->Grant_Number ?? 'N/A',
                'Grant_Source'       => $row->Grant_Source ?? 'N/A',
                'Recipient_Name'     => $row->Recipient_Name ?? 'Unlinked',
                'Delivery_ID'        => $row->Delivery_ID ?? '',
                'Training_Provider'  => $row->Training_Provider ?? '',
                'School_Name'        => $row->School_Name ?? 'N/A',
                'Rider_ID'           => $row->Rider_ID ?? '',
                'Year_Group'         => $row->Year_Group ?? '',
                'Consent_Cutoff_Date'=> $row->Consent_Cutoff_Date ?? null,
                'Frequency_School'   => $row->Frequency_School ?? 'Not Provided',
                'Frequency_Leisure'  => $row->Frequency_Leisure ?? 'Not Provided',
                'Frequency_Exercise' => $row->Frequency_Exercise ?? 'Not Provided',
                'Frequency_Other'    => $row->Frequency_Other ?? 'Not Provided',
            ];
        })->toArray();
    }

    private function translateFreq($val): string
    {
        return match ((int) $val) {
            5 => 'Not applicable', 6 => 'Never', 7 => 'Less than once a month',
            8 => 'Once or twice a month', 9 => 'One to three days a week',
            10 => 'Four or more days a week', default => 'Not Provided'
        };
    }
}

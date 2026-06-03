<?php

namespace App\Reports\Handlers;

use App\Reports\Contracts\ReportHandlerInterface;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Validator;

class PreCourseFrequencyHandler implements ReportHandlerInterface
{
    /**
     * Define and validate acceptable parameters for the detailed frequency report.
     */
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

    /**
     * Execute the detailed pre-course cycling frequency query.
     */
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
                'g.Grant_Number as Grant Number',
                'g.Grant_Source as Grant Source',
                'gr.Recipient_Name as Grant Recipient',
                'dh.Source_Delivery_Id as Delivery ID',
                'tp.Provider_Name as Training Provider',
                DB::raw("COALESCE(s.School_Name, o.Organisation_Name, 'N/A') as 'School/Organisation'"),
                'r.Source_Rider_Id as Rider ID',
                'dc.Year_Group as Year Group',
                DB::raw("DATE_FORMAT(dh.Consent_Cutoff_Date, '%d/%m/%Y') as 'Consent Cutoff Date'"),

                // Frequency: To/From School Mapping
                DB::raw("CASE dc.Pre_Freq_To_School
                    WHEN 5 THEN 'Not applicable: My child cannot yet cycle'
                    WHEN 6 THEN 'Never'
                    WHEN 7 THEN 'Less than once a month'
                    WHEN 8 THEN 'Once or twice a month'
                    WHEN 9 THEN 'One to three days a week'
                    WHEN 10 THEN 'Four or more days a week'
                    ELSE 'Not Provided'
                END as 'Frequency: To/From School'"),

                // Frequency: Leisure Mapping
                DB::raw("CASE dc.Pre_Freq_Leisure
                    WHEN 5 THEN 'Not applicable: My child cannot yet cycle'
                    WHEN 6 THEN 'Never'
                    WHEN 7 THEN 'Less than once a month'
                    WHEN 8 THEN 'Once or twice a month'
                    WHEN 9 THEN 'One to three days a week'
                    WHEN 10 THEN 'Four or more days a week'
                    ELSE 'Not Provided'
                END as 'Frequency: Leisure'"),

                // Frequency: Exercise Mapping
                DB::raw("CASE dc.Pre_Freq_Exercise
                    WHEN 5 THEN 'Not applicable: My child cannot yet cycle'
                    WHEN 6 THEN 'Never'
                    WHEN 7 THEN 'Less than once a month'
                    WHEN 8 THEN 'Once or twice a month'
                    WHEN 9 THEN 'One to three days a week'
                    WHEN 10 THEN 'Four or more days a week'
                    ELSE 'Not Provided'
                END as 'Frequency: Exercise'"),

                // Frequency: Other Mapping
                DB::raw("CASE dc.Pre_Freq_Other
                    WHEN 5 THEN 'Not applicable: My child cannot yet cycle'
                    WHEN 6 THEN 'Never'
                    WHEN 7 THEN 'Less than once a month'
                    WHEN 8 THEN 'Once or twice a month'
                    WHEN 9 THEN 'One to three days a week'
                    WHEN 10 THEN 'Four or more days a week'
                    ELSE 'Not Provided'
                END as 'Frequency: Other'")
            ]);

        // --- Strict Param Isolations ---
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

        return $query->orderBy('g.Grant_Number')
            ->orderBy('dh.Source_Delivery_Id')
            ->get()
            ->toArray();
    }
}

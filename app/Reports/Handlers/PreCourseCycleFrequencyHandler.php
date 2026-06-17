<?php

namespace App\Reports\Handlers;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Validator;

class PreCourseCycleFrequencyHandler extends AbstractStreamingReportHandler
{
    public function validate(array $parameters): array
    {
        return Validator::make($parameters, [
            'grant_id'    => 'nullable|integer',
            'provider_id' => 'nullable|integer',
            'start_date'  => 'nullable|date_format:Y-m-d',
            'end_date'    => 'nullable|date_format:Y-m-d',
        ])->validate();
    }

    public function execute(array $params): array
    {
        ini_set('memory_limit', '512M');
        ini_set('max_execution_time', '0');

        $query = DB::connection('mysql')->table('Dim_Consent as dc')
            ->join('Dim_Rider as r', 'dc.Rider_Key', '=', 'r.Rider_Key')
            ->join('Dim_Delivery_Header as dh', 'dc.Delivery_Key', '=', 'dh.Delivery_Key')
            ->join('Dim_Grant as g', 'dh.Grant_Key', '=', 'g.Grant_Key')
            ->join('Dim_Grant_Recipient as gr', 'g.Grant_Recipient_Key', '=', 'gr.Recipient_Key')
            ->join('Dim_Training_Provider as tp', 'dh.Training_Provider_Key', '=', 'tp.Provider_Key')
            ->leftJoin('Dim_School as s', 'dh.School_Key', '=', 's.School_Key')
            ->leftJoin('Dim_Organisation as o', 'dh.Organisation_Key', '=', 'o.Organisation_Key')
            ->select([
                'g.Grant_Number', 'g.Grant_Source', 'gr.Recipient_Name', 'dh.Source_Delivery_Id',
                'tp.Provider_Name', 'r.Source_Rider_Id', 'dc.Year_Group', 'dh.Consent_Cutoff_Date',
                'dc.Pre_Freq_To_School', 'dc.Pre_Freq_Leisure', 'dc.Pre_Freq_Exercise', 'dc.Pre_Freq_Other',
                DB::raw("COALESCE(s.School_Name, o.Organisation_Name, 'N/A') as School_Org")
            ])
            ->where('dc.Consent_Status', 1);

        if (!empty($params['grant_id']))    $query->where('g.Source_Grant_Id', $params['grant_id']);
        if (!empty($params['provider_id'])) $query->where('tp.Source_Provider_Id', $params['provider_id']);
        if (!empty($params['start_date']))  $query->where('dh.Consent_Cutoff_Date', '>=', $params['start_date']);
        if (!empty($params['end_date']))    $query->where('dh.Consent_Cutoff_Date', '<=', $params['end_date']);

        $query->orderBy('g.Grant_Number')->orderBy('dh.Source_Delivery_Id');

        // Synchronous track fallback
        if (empty($this->callbackUrl)) {
            return $query->get()->toArray();
        }

        // Asynchronous Streaming Flow Engine
        $records = $query->lazy();
        $batch = [];
        $batchSize = 5000;

        foreach ($records as $row) {
            $batch[] = [
                'grant_number'      => $row->Grant_Number,
                'grant_source'      => $row->Grant_Source,
                'grant_recipient'   => $row->Recipient_Name,
                'delivery_id'       => $row->Source_Delivery_Id,
                'training_provider' => $row->Provider_Name,
                'school_org'        => $row->School_Org,
                'rider_id'          => $row->Source_Rider_Id,
                'year_group'        => $row->Year_Group,
                'consent_cutoff'    => !empty($row->Consent_Cutoff_Date) ? date('d/m/Y', strtotime($row->Consent_Cutoff_Date)) : null,
                'freq_to_school'    => $this->translateFreq($row->Pre_Freq_To_School),
                'freq_leisure'      => $this->translateFreq($row->Pre_Freq_Leisure),
                'freq_exercise'     => $this->translateFreq($row->Pre_Freq_Exercise),
                'freq_other'        => $this->translateFreq($row->Pre_Freq_Other),
            ];

            if (count($batch) >= $batchSize) {
                $this->transmitBatch($batch, false);
                $batch = [];
            }
        }

        // Finalize stream
        $this->transmitBatch($batch, true);

        return ['status' => 'async_completed'];
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

<?php

namespace App\Reports\Handlers;

use App\Reports\Contracts\ReportHandlerInterface;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Validator;

class ParentFollowUpSurveyHandler extends AbstractStreamingReportHandler implements ReportHandlerInterface
{
    public function validate(array $parameters): array
    {
        return Validator::make($parameters, [
            'year'                 => 'required|integer',
            'recipient_id'   => 'nullable|integer',
            'provider_id' => 'nullable|integer',
        ])->validate();
    }

    /**
     * Executes queries and streams records back via explicit chunk offsets.
     */
    public function execute(array $params): array
    {
        ini_set('memory_limit', '512M');
        ini_set('max_execution_time', '600');

        $query = $this->buildQuery($params);

        if (empty($this->callbackUrl)) {
            return $query->get()->map(fn($row) => $this->mapRowToPayload($row))->toArray();
        }

        $chunkSize = 250;

        $query->chunk($chunkSize, function ($rows) {
            $chunkArray = $rows->map(function ($row) {
                return $this->mapRowToPayload($row);
            })->toArray();

            // Transmit batch down to local app chunk server endpoint securely
            $this->transmitBatch($chunkArray, false);
        });

        // Trigger closing handshake EOF packet
        $this->transmitBatch([], true);

        return ['status' => 'async_completed'];
    }

    /**
     * Standardizes column arrays passing down to the source app server mapping loop.
     */
    protected function mapRowToPayload($row): array
    {
        // 1. Resolve module prefix from Course_Label_Raw
        $moduleKey = match (strtolower(trim($row->Course_Label_Raw))) {
            'level 1' => 'level_1',
            'level 2' => 'level_2',
            'level 1 & 2 combined', 'level 1 & 2' => 'level_1_2',
            'level 3' => 'level_3',
            'learn to ride' => 'learn_to_ride',
            default => str_replace(' ', '_', strtolower(trim($row->Course_Label_Raw)))
        };

        // Helper to rebuild full code (e.g. 'o5' -> 'level_1_2_q1a_o5')
        $formatCode = fn($qKey, $val) => (!empty($val) && $val !== 'N/A') ? "{$moduleKey}_{$qKey}_{$val}" : null;

        // Reconstruct q6 array (encouragement factors)
        $q6Codes = [];
        if (!empty($row->q6_encouragement_factors)) {
            $rawQ6 = is_string($row->q6_encouragement_factors)
                ? json_decode($row->q6_encouragement_factors, true)
                : $row->q6_encouragement_factors;

            if (is_array($rawQ6)) {
                $q6Codes = array_map(function($item) use ($moduleKey) {
                    // Handles both full strings or short 'o1' strings stored in DWH
                    return str_contains($item, '_o') ? $item : "{$moduleKey}_q6_{$item}";
                }, $rawQ6);
            }
        }

        return [
            'grant_number'             => $row->Grant_Number ?? 'N/A',
            'grant_source'             => $row->Grant_Source ?? 'N/A',
            'recipient_name'           => $row->Recipient_Name ?? 'Non Grant Delivery',
            'delivery_id'              => $row->Source_Delivery_Id,
            'rider_id'                 => $row->Source_Rider_Id,
            'provider_name'            => $row->Provider_Name,
            'establishment_name'       => $row->School_Name ?? '',
            'school_urn'               => $row->School_Urn ?? '',
            'course_label'             => $row->Course_Label_Raw,
            'created_at'               => $row->Survey_Created_At,
            'invitation_month'         => $row->Invitation_Month,

            // Reconstructed payload array matching the local JSON array format
            'survey_json' => array_filter([
                $formatCode('q1a', $row->q1a_freq_school),
                $formatCode('q1b', $row->q1b_freq_leisure),
                $formatCode('q1c', $row->q1c_freq_exercise),
                $formatCode('q2a', $row->q2a_conf_use_cycle),
                $formatCode('q2b', $row->q2b_conf_cycle_roads),
                $formatCode('q3a', $row->q3a_enc_use_cycle),
                $formatCode('q3b', $row->q3b_enc_cycle_roads),
                $formatCode('q4',  $row->q4_safety_roads),
                $formatCode('q5',  $row->q5_child_desire),
                !empty($q6Codes) ? $q6Codes : null,
                $formatCode('q7',  $row->q7_conf_change),
                $formatCode('q8',  $row->q8_physical_activity),
            ])
        ];
    }

    /**
     * Relational query mapping table schemas and compiling a synthetic source-compatible JSON payload.
     */
    protected function buildQuery(array $params)
    {
        $query = DB::connection('mysql')->table('Fact_Follow_Up_Survey as pfu')
            ->join('Dim_Delivery_Header as d', 'pfu.Delivery_Key', '=', 'd.Delivery_Key')
            ->join('Dim_Training_Provider as tp', 'd.Training_Provider_Key', '=', 'tp.Provider_Key')
            ->leftJoin('Dim_Grant as g', 'd.Grant_Key', '=', 'g.Grant_Key')
            ->leftJoin('Dim_Grant_Recipient as gr', 'g.Grant_Recipient_Key', '=', 'gr.Recipient_Key')
            ->leftJoin('Dim_School as s', 'd.School_Key', '=', 's.School_Key')
            ->leftJoin('Dim_Rider as r', 'pfu.Rider_Key', '=', 'r.Rider_Key')
            ->select([
                'g.Grant_Number',
                'g.Grant_Source',
                'gr.Recipient_Name',
                'd.Source_Delivery_Id',
                'r.Source_Rider_Id',
                'tp.Provider_Name',
                's.School_Name',
                's.School_Urn',
                'pfu.Course_Label_Raw',
                'pfu.Source_Created_At as Survey_Created_At',
                'pfu.Invitation_Month',
                'pfu.q1a_freq_school',
                'pfu.q1b_freq_leisure',
                'pfu.q1c_freq_exercise',
                'pfu.q2a_conf_use_cycle',
                'pfu.q2b_conf_cycle_roads',
                'pfu.q3a_enc_use_cycle',
                'pfu.q3b_enc_cycle_roads',
                'pfu.q4_safety_roads',
                'pfu.q5_child_desire',
                'pfu.q6_encouragement_factors',
                'pfu.q7_conf_change',
                'pfu.q8_physical_activity'
            ])
            ->where('g.Grant_Period_Start_Year', (int)$params['year'])
            ->where('d.Digitisation_Booking', true);

        if (!empty($params['recipient_id'])) {
            $query->where('gr.Source_Recipient_Id', (int)$params['recipient_id']);
        }

//        if (!empty($params['provider_id'])) {
//            $query->where('tp.Source_Provider_Id', (int)$params['provider_id']);
//        }

        return $query->orderBy('pfu.Source_Created_At', 'desc');
    }
}

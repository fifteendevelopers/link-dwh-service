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
            'grant_recipient_id'   => 'nullable|integer',
            'training_provider_id' => 'nullable|integer',
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
        return [
            'grant_number'            => $row->Grant_Number,
            'grant_source'            => $row->Grant_Source,
            'recipient_name'          => $row->Recipient_Name ?? 'Non Grant Delivery',
            'delivery_id'             => $row->Source_Delivery_Id,
            'rider_id'                => $row->Source_Rider_Id,
            'provider_name'           => $row->Provider_Name,
            'establishment_name'      => $row->School_Name,
            'school_urn'              => $row->School_Urn,
            'course_label'            => $row->Course_Label_Raw,
            'created_at'              => $row->Survey_Created_At,
            'invitation_month'        => $row->Invitation_Month,
            'parent_follow_up_survey' => $row->Parent_Follow_Up_Survey_Json, // Pass survey JSON to be unpacked locally
        ];
    }

    /**
     * Relational query mapping table schemas and compiling a synthetic source-compatible JSON payload.
     */
    protected function buildQuery(array $params)
    {
        // Reconstruct a JSON array matching the source ParentFollowUpSurveyHelper mapping.
        // It strings together single entries or multi-select JSON blocks into an identical legacy footprint.
        $jsonExpression = "
            JSON_MERGE_PRESERVE(
                JSON_ARRAY(
                    pfu.q1a_freq_school,
                    pfu.q1b_freq_leisure,
                    pfu.q1c_freq_exercise,
                    pfu.q2a_conf_use_cycle,
                    pfu.q2b_conf_cycle_roads,
                    pfu.q3a_enc_use_cycle,
                    pfu.q3b_enc_cycle_roads,
                    pfu.q4_safety_roads,
                    pfu.q5_child_desire,
                    pfu.q7_conf_change,
                    pfu.q8_physical_activity
                ),
                COALESCE(pfu.q6_encouragement_factors, JSON_ARRAY())
            )
        ";

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
                DB::raw("{$jsonExpression} as Parent_Follow_Up_Survey_Json")
            ])
            ->where('g.Grant_Period_Start_Year', (int)$params['year'])
            ->where('d.Digitisation_Booking', true);

        if (!empty($params['grant_recipient_id'])) {
            $query->where('gr.Source_Recipient_Id', (int)$params['grant_recipient_id']);
        }

        if (!empty($params['training_provider_id'])) {
            $query->where('tp.Source_Provider_Id', (int)$params['training_provider_id']);
        }

        return $query->orderBy('pfu.Source_Created_At', 'desc');
    }
}

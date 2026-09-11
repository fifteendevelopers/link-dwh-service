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
        return [
            'grant_number'             => $row->Grant_Number,
            'grant_source'             => $row->Grant_Source,
            'recipient_name'           => $row->Recipient_Name ?? 'Non Grant Delivery',
            'delivery_id'              => $row->Source_Delivery_Id,
            'rider_id'                 => $row->Source_Rider_Id,
            'provider_name'            => $row->Provider_Name,
            'establishment_name'       => $row->School_Name,
            'school_urn'               => $row->School_Urn,
            'course_label'             => $row->Course_Label_Raw,
            'created_at'               => $row->Survey_Created_At,
            'invitation_month'         => $row->Invitation_Month,
            'q1a_freq_school'          => $row->q1a_freq_school,
            'q1b_freq_leisure'         => $row->q1b_freq_leisure,
            'q1c_freq_exercise'        => $row->q1c_freq_exercise,
            'q2a_conf_use_cycle'       => $row->q2a_conf_use_cycle,
            'q2b_conf_cycle_roads'     => $row->q2b_conf_cycle_roads,
            'q3a_enc_use_cycle'        => $row->q3a_enc_use_cycle,
            'q3b_enc_cycle_roads'      => $row->q3b_enc_cycle_roads,
            'q4_safety_roads'          => $row->q4_safety_roads,
            'q5_child_desire'          => $row->q5_child_desire,
            'q6_encouragement_factors' => $row->q6_encouragement_factors,
            'q7_conf_change'           => $row->q7_conf_change,
            'q8_physical_activity'     => $row->q8_physical_activity,
            'Rural_Urban_Classification' => $row->Rural_Urban_Classification ?? 'N/A',
            'Imd_Decile'          => $row->Imd_Decile ?? 'N/A'
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
            ->leftJoin('Dim_Organisation as o', 'd.Organisation_Key', '=', 'o.Organisation_Key')
            ->leftJoin('Dim_Rider as r', 'pfu.Rider_Key', '=', 'r.Rider_Key')
            ->select([
                DB::raw("IFNULL(g.Grant_Number, 'N/A') as Grant_Number"),
                DB::raw("IFNULL(g.Grant_Source, 'N/A') as Grant_Source"),
                DB::raw("IFNULL(gr.Recipient_Name, 'Non Grant Delivery') as Recipient_Name"),
                'd.Source_Delivery_Id',
                'r.Source_Rider_Id',
                'tp.Provider_Name',
                DB::raw("COALESCE(NULLIF(s.School_Name, ''), NULLIF(o.Organisation_Name, ''), 'N/A') as School_Name"),
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
                'pfu.q8_physical_activity',
                's.Rural_Urban_Classification',
                's.Imd_Decile'
            ]);

        // Filter by the financial year the survey was completed (01/04/{year} to 31/03/{year+1})
        if (!empty($params['year'])) {
            $year     = (int) $params['year'];
            $dateFrom = $year . '-04-01 00:00:00';
            $dateTo   = ($year + 1) . '-03-31 23:59:59';

            $query->whereBetween('pfu.Source_Created_At', [$dateFrom, $dateTo]);
        }

        if (!empty($params['recipient_id'])) {
            $query->where('gr.Source_Recipient_Id', (int) $params['recipient_id']);
        }

        if (!empty($params['provider_id'])) {
            $query->where('tp.Source_Provider_Id', (int) $params['provider_id']);
        }

        return $query->orderBy('pfu.Source_Created_At', 'desc');
    }
}

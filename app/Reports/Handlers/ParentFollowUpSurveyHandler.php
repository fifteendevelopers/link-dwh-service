<?php

namespace App\Reports\Handlers;

use App\Helpers\DwhParentFollowUpSurveyHelper;
use App\Reports\Contracts\ReportHandlerInterface;
use Carbon\Carbon;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Validator;

class ParentFollowUpSurveyHandler extends AbstractStreamingReportHandler implements ReportHandlerInterface
{
    public function validate(array $parameters): array
    {
        return Validator::make($parameters, [
            'financial_year' => 'nullable',
            'year'           => 'nullable|integer',
            'recipient_id'   => 'nullable|integer',
            'provider_id'    => 'nullable|integer',
            'start_date'     => 'nullable|date',
            'end_date'       => 'nullable|date',
        ])->validate();
    }

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

            $this->transmitBatch($chunkArray, false);
        });

        $this->transmitBatch([], true);

        return ['status' => 'async_completed'];
    }

    /**
     * Standardizes column arrays passing down to the client app export mapping loop.
     */
    protected function mapRowToPayload($row): array
    {
        $createdDate = !empty($row->Survey_Created_At)
            ? Carbon::parse($row->Survey_Created_At)->format('d/m/Y')
            : '';

        return [
            // Fixed Metadata (Matching exact keys expected by the client receiver)
            'grant_number'               => (string) ($row->Grant_Number ?? ''),
            'grant_source'               => (string) ($row->Grant_Source ?? ''),
            'recipient_name'             => (string) ($row->Recipient_Name ?? 'Non Grant Delivery'),
            'delivery_id'                => $row->Source_Delivery_Id,
            'rider_id'                   => $row->Source_Rider_Id,
            'provider_name'              => (string) ($row->Provider_Name ?? ''),
            'establishment_name'         => (string) ($row->School_Name ?? ''),
            'school_urn'                 => (string) ($row->School_Urn ?? ''),
            'course_label'               => (string) ($row->Course_Label_Resolved ?? ''),
            'created_at'                 => $createdDate,
            'invitation_month'           => $row->Invitation_Month ?? '',

            // Survey Question Responses (Translated to human-readable strings via Helper)
            'q1a_freq_school'            => $this->translateSingle('q1a_freq_school', $row->q1a_freq_school),
            'q1b_freq_leisure'           => $this->translateSingle('q1b_freq_leisure', $row->q1b_freq_leisure),
            'q1c_freq_exercise'          => $this->translateSingle('q1c_freq_exercise', $row->q1c_freq_exercise),
            'q2a_conf_use_cycle'         => $this->translateSingle('q2a_conf_use_cycle', $row->q2a_conf_use_cycle),
            'q2b_conf_cycle_roads'       => $this->translateSingle('q2b_conf_cycle_roads', $row->q2b_conf_cycle_roads),
            'q3a_enc_use_cycle'          => $this->translateSingle('q3a_enc_use_cycle', $row->q3a_enc_use_cycle),
            'q3b_enc_cycle_roads'        => $this->translateSingle('q3b_enc_cycle_roads', $row->q3b_enc_cycle_roads),
            'q4_safety_roads'            => $this->translateSingle('q4_safety_roads', $row->q4_safety_roads),
            'q5_child_desire'            => $this->translateSingle('q5_child_desire', $row->q5_child_desire),
            'q6_encouragement_factors'   => DwhParentFollowUpSurveyHelper::getMultiSelectLabels($row->q6_encouragement_factors),
            'q7_conf_change'             => $this->translateSingle('q7_conf_change', $row->q7_conf_change),
            'q8_physical_activity'       => $this->translateSingle('q8_physical_activity', $row->q8_physical_activity),

            // Demographics (provide both casing variants so receiver never misses them)
            'Rural_Urban_Classification' => (string) ($row->Rural_Urban_Classification ?? 'N/A'),
            'Imd_Decile'                 => (string) ($row->Imd_Decile ?? 'N/A'),
            'rural_urban'                => (string) ($row->Rural_Urban_Classification ?? 'N/A'),
            'imd_decile'                 => (string) ($row->Imd_Decile ?? 'N/A'),
        ];
    }

    /**
     * Helper to format code '1' or 'o1' and map to label.
     */
    protected function translateSingle(string $questionCol, ?string $value): string
    {
        if (is_null($value) || $value === '') {
            return '';
        }

        // Format to "o1", "o2", etc.
        $optKey = str_starts_with($value, 'o') ? $value : "o{$value}";

        return DwhParentFollowUpSurveyHelper::getLabel($questionCol, $optKey);
    }

    protected function buildQuery(array $params)
    {
        $query = DB::connection('mysql')->table('Fact_Follow_Up_Survey as pfu')
            ->join('Dim_Delivery_Header as d', 'pfu.Delivery_Key', '=', 'd.Delivery_Key')
            ->join('Dim_Training_Provider as tp', 'd.Training_Provider_Key', '=', 'tp.Provider_Key')
            ->leftJoin('Dim_Course as c', 'pfu.Course_Key', '=', 'c.Course_Key')
            ->leftJoin('Dim_Grant as g', 'd.Grant_Key', '=', 'g.Grant_Key')
            ->leftJoin('Dim_Grant_Recipient as gr', 'g.Grant_Recipient_Key', '=', 'gr.Recipient_Key')
            ->leftJoin('Dim_School as s', 'd.School_Key', '=', 's.School_Key')
            ->leftJoin('Dim_Organisation as o', 'd.Organisation_Key', '=', 'o.Organisation_Key')
            ->leftJoin('Dim_Rider as r', 'pfu.Rider_Key', '=', 'r.Rider_Key')
            ->select([
                DB::raw("IFNULL(g.Grant_Number, '') as Grant_Number"),
                DB::raw("IFNULL(g.Grant_Source, '') as Grant_Source"),
                DB::raw("IFNULL(gr.Recipient_Name, 'Non Grant Delivery') as Recipient_Name"),
                'd.Source_Delivery_Id',
                'r.Source_Rider_Id',
                'tp.Provider_Name',
                DB::raw("COALESCE(NULLIF(s.School_Name, ''), NULLIF(o.Organisation_Name, ''), '') as School_Name"),
                's.School_Urn',
                // Resolve Course Level: fall back to Dim_Course if Course_Label_Raw is null
                DB::raw("COALESCE(NULLIF(pfu.Course_Label_Raw, ''), NULLIF(c.Course_Level, ''), 'Level 2') as Course_Label_Resolved"),
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
                's.Imd_Decile',
            ]);

        if (isset($params['start_date']) && isset($params['end_date'])) {
            $query->whereBetween('d.Date_Delivery_Start', [$params['start_date'], $params['end_date']]);
        }

        $query->whereNull('d.Source_Deleted_At');
        $query->where('d.Digitisation_Booking', 1);

        if (!empty($params['recipient_id'])) {
            $query->where('gr.Source_Recipient_Id', (int) $params['recipient_id']);
        }

        if (!empty($params['provider_id'])) {
            $query->where('tp.Source_Provider_Id', (int) $params['provider_id']);
        }

        return $query->orderBy('d.Date_Delivery_Start', 'desc')
            ->orderBy('pfu.Follow_Up_Key', 'asc');
    }
}

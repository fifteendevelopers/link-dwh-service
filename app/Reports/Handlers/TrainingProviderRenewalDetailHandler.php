<?php

namespace App\Reports\Handlers;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Validator;

class TrainingProviderRenewalDetailHandler extends AbstractStreamingReportHandler
{
    public function validate(array $parameters): array
    {
        return Validator::make($parameters, [
            'financial_year' => 'required|integer',
        ])->validate();
    }

    public function execute(array $params): array
    {
        ini_set('memory_limit', '1024M');
        ini_set('max_execution_time', '900');

        $query = $this->buildQuery($params);

        if (empty($this->callbackUrl)) {
            return $query->get()->map(fn($row) => (array)$row)->toArray();
        }

        $chunkSize = 500;

        $query->chunk($chunkSize, function ($rows) {
            $chunkArray = $rows->map(function ($row) {
                return [
                    $row->id,
                    $row->provider_number,
                    $row->provider_name,
                    $row->address_01,
                    $row->address_02,
                    $row->city,
                    $row->postcode,
                    $row->website,
                    $row->telephone,
                    $row->primary_email,
                    $row->secondary_email,
                    $row->public_email,
                    $row->public_telephone,
                    $row->provider_type,
                    $row->status,
                    $row->date_inception,
                    $row->date_renewal,
                    $row->date_deregistered,
                    $row->date_eqa_visit,
                    $row->date_insurance_expiry,
                    $row->pref_level_1,
                    $row->pref_level_2,
                    $row->pref_level_3,
                    $row->pref_plus_balance,
                    $row->pref_plus_bus,
                    $row->pref_plus_fix,
                    $row->pref_plus_learn,
                    $row->pref_plus_on_show,
                    $row->pref_plus_parents,
                    $row->pref_plus_promotion,
                    $row->pref_plus_recycled,
                    $row->pref_plus_ride,
                    $row->pref_plus_transition,
                    $row->pref_plus_family,
                    $row->pref_plus_adult,
                    $row->legacy_areas_of_operation,
                    $row->account_notes,
                    $row->tp_created_at,
                    $row->tp_updated_at,
                    $row->tp_deleted_at,
                    $row->renewal_id,
                    $row->training_provider_id,
                    $row->renewal_status,
                    $row->renewal_year,
                    $row->date_due,
                    $row->date_completed,
                    $row->date_approved,
                    $row->flag_reminder_warning,
                    $row->flag_overdue_warning,
                    $row->q_organisation_category,
                    $row->q_confirm_details,
                    $row->q_contracted_deliveries,
                    $row->q_delivered_places,
                    $row->q_instructor_types_breakdown,
                    $row->q_confirm_instructor_dbcert,
                    $row->q_confirm_instructor_safetypolicies,
                    $row->q_delivery_amount_level_1,
                    $row->q_delivery_amount_level_1_2,
                    $row->q_delivery_amount_level_2,
                    $row->q_delivery_amount_level_3,
                    $row->q_delivery_amount_plus_balance,
                    $row->q_delivery_amount_plus_bus,
                    $row->q_delivery_amount_plus_fix,
                    $row->q_delivery_amount_plus_learn,
                    $row->q_delivery_amount_plus_on_show,
                    $row->q_delivery_amount_plus_parents,
                    $row->q_delivery_amount_plus_promotion,
                    $row->q_delivery_amount_plus_recycled,
                    $row->q_delivery_amount_plus_ride,
                    $row->q_delivery_amount_plus_transition,
                    $row->q_delivery_model,
                    $row->q_confirm_guidelines_commitment,
                    $row->q_confirm_risk_assessment,
                    $row->q_confirm_policy_review,
                    $row->q_confirm_valid_insurance,
                    $row->q_confirm_iqa_plan,
                    $row->IQA_Strength,
                    $row->IQA_Goal,
                    $row->IQA_Actions,
                    $row->q_priority_1,
                    $row->q_action_plan_1,
                    $row->q_priority_2,
                    $row->q_action_plan_2,
                    $row->q_priority_3,
                    $row->q_action_plan_3,
                    $row->tpr_created_at,
                    $row->tpr_updated_at,
                    $row->q_children,
                    $row->q_adults,
                    $row->q_families,
                    $row->q_confirm_incidents,
                    $row->q_stage_1_complaints,
                    $row->q_serious_complaints,
                    $row->q_confirm_assurance_one,
                    $row->q_confirm_assurance_two,
                    $row->q_confirm_assurance_three,
                    $row->q_improvements, // Kept raw for local processing unpacking
                    $row->q_confirm_final_one,
                    $row->q_confirm_final_two,
                    $row->instructor_count,
                    $row->confirm_instructors,
                    $row->all_instructors_have_dbs,
                    $row->all_instructors_completed_essential_training,
                    $row->q_confirm_access,
                    $row->q_safeguarding_lead,
                    $row->q_health_and_safety_lead,
                    $row->q_iqa_lead,
                    $row->q_iqa_organisation,
                    $row->q_adult_training_only,
                    $row->q_expected_delivery_empty,
                    $row->q_expected_delivery,
                    $row->q_confirm_final_three,
                    $row->q_name,
                    $row->q_safeguarding // Kept raw for local processing unpacking
                ];
            })->toArray();

            $this->transmitBatch($chunkArray, false);
        });

        $this->transmitBatch([], true);

        return ['status' => 'async_completed'];
    }

    protected function buildQuery(array $params)
    {
        return DB::connection('mysql')->table('training_providers as tp')
            ->join('training_provider_renewals as tpr', 'tpr.training_provider_id', '=', 'tp.id')
            ->leftJoin('join_instructors_training_providers as ji', 'ji.training_provider_id', '=', 'tpr.training_provider_id')
            ->where('tpr.renewal_year', (int)$params['financial_year'])
            ->groupBy('tp.id', 'tpr.id')
            ->select([
                'tp.id', 'tp.provider_number', 'tp.provider_name', 'tp.address_01', 'tp.address_02', 'tp.city', 'tp.postcode',
                'tp.website', 'tp.telephone', 'tp.primary_email', 'tp.secondary_email', 'tp.public_email', 'tp.public_telephone',
                'tp.provider_type', 'tp.status', 'tp.date_inception', 'tp.date_renewal', 'tp.date_deregistered', 'tp.date_eqa_visit', 'tp.date_insurance_expiry',
                'tp.pref_level_1', 'tp.pref_level_2', 'tp.pref_level_3', 'tp.pref_plus_balance', 'tp.pref_plus_bus', 'tp.pref_plus_fix', 'tp.pref_plus_learn',
                'tp.pref_plus_on_show', 'tp.pref_plus_parents', 'tp.pref_plus_promotion', 'tp.pref_plus_recycled', 'tp.pref_plus_ride', 'tp.pref_plus_transition', 'tp.pref_plus_family', 'tp.pref_plus_adult',
                'tp.legacy_areas_of_operation', 'tp.account_notes', 'tp.created_at as tp_created_at', 'tp.updated_at as tp_updated_at', 'tp.deleted_at as tp_deleted_at',
                'tpr.id as renewal_id', 'tpr.training_provider_id',
                DB::raw("CASE WHEN tpr.status = 0 THEN 'In-Progress' WHEN tpr.status = 1 THEN 'Requires Approval' WHEN tpr.status = 2 THEN 'Approved' WHEN tpr.status = 3 THEN 'Failed' ELSE 'Unknown' END AS renewal_status"),
                'tpr.renewal_year', 'tpr.date_due', 'tpr.date_completed', 'tpr.date_approved', 'tpr.flag_reminder_warning', 'tpr.flag_overdue_warning',
                'tpr.q_organisation_category', 'tpr.q_confirm_details', 'tpr.q_contracted_deliveries', 'tpr.q_delivered_places', 'tpr.q_instructor_types_breakdown',
                'tpr.q_confirm_instructor_dbcert', 'tpr.q_confirm_instructor_safetypolicies',
                'tpr.q_delivery_amount_level_1', 'tpr.q_delivery_amount_level_1_2', 'tpr.q_delivery_amount_level_2', 'tpr.q_delivery_amount_level_3',
                'tpr.q_delivery_amount_plus_balance', 'tpr.q_delivery_amount_plus_bus', 'tpr.q_delivery_amount_plus_fix', 'tpr.q_delivery_amount_plus_learn',
                'tpr.q_delivery_amount_plus_on_show', 'tpr.q_delivery_amount_plus_parents', 'tpr.q_delivery_amount_plus_promotion', 'tpr.q_delivery_amount_plus_recycled',
                'tpr.q_delivery_amount_plus_ride', 'tpr.q_delivery_amount_plus_transition', 'tpr.q_delivery_model', 'tpr.q_confirm_guidelines_commitment',
                'tpr.q_confirm_risk_assessment', 'tpr.q_confirm_policy_review', 'tpr.q_confirm_valid_insurance', 'tpr.q_confirm_iqa_plan',
                DB::raw("JSON_UNQUOTE(JSON_EXTRACT(tpr.q_strengths, '$[0].strength')) as IQA_Strength"),
                DB::raw("JSON_UNQUOTE(JSON_EXTRACT(tpr.q_improvements, '$[0].goal')) as IQA_Goal"),
                DB::raw("JSON_UNQUOTE(JSON_EXTRACT(tpr.q_improvements, '$[0].actions')) as IQA_Actions"),
                'tpr.q_priority_1', 'tpr.q_action_plan_1', 'tpr.q_priority_2', 'tpr.q_action_plan_2', 'tpr.q_priority_3', 'tpr.q_action_plan_3',
                'tpr.created_at as tpr_created_at', 'tpr.updated_at as tpr_updated_at', 'tpr.q_children', 'tpr.q_adults', 'tpr.q_families',
                'tpr.q_confirm_incidents', 'tpr.q_stage_1_complaints', 'tpr.q_serious_complaints', 'tpr.q_confirm_assurance_one', 'tpr.q_confirm_assurance_two', 'tpr.q_confirm_assurance_three',
                'tpr.q_improvements', 'tpr.q_confirm_final_one', 'tpr.q_confirm_final_two',
                DB::raw("COUNT(ji.instructor_id) as instructor_count"),
                'tpr.q_associated_instructors', 'tpr.q_associated_instructors_two', 'tpr.q_associated_instructors_three', 'tpr.q_confirm_access',
                'tpr.q_safeguarding_lead', 'tpr.q_health_and_safety_lead', 'tpr.q_iqa_lead', 'tpr.q_iqa_organisation', 'tpr.q_adult_training_only',
                'tpr.q_expected_delivery_empty', 'tpr.q_expected_delivery', 'tpr.q_confirm_final_three', 'tpr.q_name', 'tpr.q_safeguarding'
            ]);
    }
}

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
            'provider_id'    => 'nullable|integer',
        ])->validate();
    }

    public function execute(array $params): array
    {
        ini_set('memory_limit', '1024M');
        ini_set('max_execution_time', '900');

        $query = $this->buildQuery($params);

        // Fallback to synchronous array return when callbackUrl is empty (e.g. testing)
        if (empty($this->callbackUrl)) {
            return $query->get()->map(fn($row) => (array)$row)->toArray();
        }

        // Asynchronous CSV Chunk Streaming
        $chunkSize = 500;

        $query->chunk($chunkSize, function ($rows) {
            $memBuffer = fopen('php://temp', 'r+');

            foreach ($rows as $row) {
                fputcsv($memBuffer, (array)$row);
            }

            rewind($memBuffer);
            $csvStringChunk = stream_get_contents($memBuffer);
            fclose($memBuffer);

            // Transmit the CSV packet downstream
            $this->transmitBatch([$csvStringChunk], false);
        });

        // Closing EOF packet
        $this->transmitBatch([], true);

        return ['status' => 'async_completed'];
    }

    protected function buildQuery(array $params)
    {
        $query = DB::connection('mysql')->table('Fact_Training_Provider_Renewal as fpr')
            ->join('Dim_Training_Provider as tp', 'fpr.Provider_Key', '=', 'tp.Provider_Key')
            ->select([
                // Training Provider Dimension Attributes
                'tp.Source_Provider_Id as id',
                'tp.Provider_Number as provider_number',
                'tp.Provider_Name as provider_name',
                'tp.Address_Line_1 as address_01',
                'tp.Address_Line_2 as address_02',
                'tp.City as city',
                'tp.Postcode as postcode',
                'tp.Website as website',
                'tp.Telephone as telephone',
                'tp.Primary_Email as primary_email',
                'tp.Secondary_Email as secondary_email',
                'tp.Public_Email as public_email',
                'tp.Public_Telephone as public_telephone',
                'tp.Provider_Type as provider_type',
                'tp.Status as status',
                'tp.Date_Inception as date_inception',
                'tp.Date_Renewal as date_renewal',
                'tp.Date_Deregistered as date_deregistered',
                'tp.Date_Eqa_Visit as date_eqa_visit',
                'tp.Date_Insurance_Expiry as date_insurance_expiry',
                'tp.Pref_Level_1 as pref_level_1',
                'tp.Pref_Level_2 as pref_level_2',
                'tp.Pref_Level_3 as pref_level_3',
                'tp.Pref_Plus_Balance as pref_plus_balance',
                'tp.Pref_Plus_Bus as pref_plus_bus',
                'tp.Pref_Plus_Fix as pref_plus_fix',
                'tp.Pref_Plus_Learn as pref_plus_learn',
                'tp.Pref_Plus_On_Show as pref_plus_on_show',
                'tp.Pref_Plus_Parents as pref_plus_parents',
                'tp.Pref_Plus_Promotion as pref_plus_promotion',
                'tp.Pref_Plus_Recycled as pref_plus_recycled',
                'tp.Pref_Plus_Ride as pref_plus_ride',
                'tp.Pref_Plus_Transition as pref_plus_transition',
                'tp.Pref_Plus_Family as pref_plus_family',
                'tp.Pref_Plus_Adult as pref_plus_adult',
//                'tp.Legacy_Areas_Of_Operation as legacy_areas_of_operation',
                'tp.Account_Notes as account_notes',
                'tp.Source_Created_At as tp_created_at',
                'tp.Source_Updated_At as tp_updated_at',
                'tp.Source_Deleted_At as tp_deleted_at',

                // Renewal Fact Attributes
                'fpr.Source_Renewal_Id as renewal_id',
                'tp.Source_Provider_Id as training_provider_id',
                DB::raw("CASE
                    WHEN fpr.Status = 0 THEN 'In-Progress'
                    WHEN fpr.Status = 1 THEN 'Requires Approval'
                    WHEN fpr.Status = 2 THEN 'Approved'
                    WHEN fpr.Status = 3 THEN 'Failed'
                    ELSE 'Unknown'
                END AS renewal_status"),
                'fpr.Renewal_Year as renewal_year',
                'fpr.Date_Due as date_due',
                'fpr.Date_Completed as date_completed',
                'fpr.Date_Approved as date_approved',
                'fpr.Flag_Reminder_Warning as flag_reminder_warning',
                'fpr.Flag_Overdue_Warning as flag_overdue_warning',
                'fpr.Q_Organisation_Category as q_organisation_category',
                'fpr.Q_Confirm_Details as q_confirm_details',
//                'fpr.Q_Contracted_Deliveries as q_contracted_deliveries',
//                'fpr.Q_Delivered_Places as q_delivered_places',
//                'fpr.Q_Instructor_Types_Breakdown as q_instructor_types_breakdown',
//                'fpr.Q_Confirm_Instructor_Dbscert as q_confirm_instructor_dbscert',
//                'fpr.Q_Confirm_Instructor_Safetypolicies as q_confirm_instructor_safetypolicies',
//                'fpr.Q_Delivery_Amount_Level_1 as q_delivery_amount_level_1',
//                'fpr.Q_Delivery_Amount_Level_1_2 as q_delivery_amount_level_1_2',
//                'fpr.Q_Delivery_Amount_Level_2 as q_delivery_amount_level_2',
//                'fpr.Q_Delivery_Amount_Level_3 as q_delivery_amount_level_3',
//                'fpr.Q_Delivery_Amount_Plus_Balance as q_delivery_amount_plus_balance',
//                'fpr.Q_Delivery_Amount_Plus_Bus as q_delivery_amount_plus_bus',
//                'fpr.Q_Delivery_Amount_Plus_Fix as q_delivery_amount_plus_fix',
//                'fpr.Q_Delivery_Amount_Plus_Learn as q_delivery_amount_plus_learn',
//                'fpr.Q_Delivery_Amount_Plus_On_Show as q_delivery_amount_plus_on_show',
//                'fpr.Q_Delivery_Amount_Plus_Parents as q_delivery_amount_plus_parents',
//                'fpr.Q_Delivery_Amount_Plus_Promotion as q_delivery_amount_plus_promotion',
//                'fpr.Q_Delivery_Amount_Plus_Recycled as q_delivery_amount_plus_recycled',
//                'fpr.Q_Delivery_Amount_Plus_Ride as q_delivery_amount_plus_ride',
//                'fpr.Q_Delivery_Amount_Plus_Transition as q_delivery_amount_plus_transition',
//                'fpr.Q_Delivery_Model as q_delivery_model',
//                'fpr.Q_Confirm_Guidelines_Commitment as q_confirm_guidelines_commitment',
//                'fpr.Q_Confirm_Risk_Assessment as q_confirm_risk_assessment',
//                'fpr.Q_Confirm_Policy_Review as q_confirm_policy_review',
//                'fpr.Q_Confirm_Valid_Insurance as q_confirm_valid_insurance',
//                'fpr.Q_Confirm_Iqa_Plan as q_confirm_iqa_plan',
                DB::raw("JSON_UNQUOTE(JSON_EXTRACT(fpr.Q_Strengths, '$[0].strength')) as IQA_Strength"),
                DB::raw("JSON_UNQUOTE(JSON_EXTRACT(fpr.Q_Improvements, '$[0].goal')) as IQA_Goal"),
                DB::raw("JSON_UNQUOTE(JSON_EXTRACT(fpr.Q_Improvements, '$[0].actions')) as IQA_Actions"),
                'fpr.Q_Priority_1 as q_priority_1',
                'fpr.Q_Action_Plan_1 as q_action_plan_1',
                'fpr.Q_Priority_2 as q_priority_2',
                'fpr.Q_Action_Plan_2 as q_action_plan_2',
                'fpr.Q_Priority_3 as q_priority_3',
                'fpr.Q_Action_Plan_3 as q_action_plan_3',
                'fpr.Source_Created_At as tpr_created_at',
                'fpr.Source_Updated_At as tpr_updated_at',
                'fpr.Q_Children as q_children',
                'fpr.Q_Adults as q_adults',
                'fpr.Q_Families as q_families',
                'fpr.Q_Confirm_Incidents as q_confirm_incidents',
                'fpr.Q_Stage_1_Complaints as q_stage_1_complaints',
                'fpr.Q_Serious_Complaints as q_serious_complaints',
                'fpr.Q_Confirm_Assurance_One as q_confirm_assurance_one',
                'fpr.Q_Confirm_Assurance_Two as q_confirm_assurance_two',
                'fpr.Q_Confirm_Assurance_Three as q_confirm_assurance_three',
                'fpr.Q_Improvements as q_improvements',
                'fpr.Q_Confirm_Final_One as q_confirm_final_one',
                'fpr.Q_Confirm_Final_Two as q_confirm_final_two',

                // Read directly from Fact column if persisted; otherwise use a non-locking scalar subquery
                DB::raw("(
                    SELECT COUNT(*)
                    FROM Fact_Instructor_Training_Provider fitp
                    WHERE fitp.Provider_Key = fpr.Provider_Key
                ) as instructor_count"),

                'fpr.Q_Associated_Instructors as confirm_instructors',
                'fpr.Q_Associated_Instructors_Two as all_instructors_have_dbs',
                'fpr.Q_Associated_Instructors_Three as all_instructors_completed_essential_training',
                'fpr.Q_Confirm_Access as q_confirm_access',
                'fpr.Q_Safeguarding_Lead as q_safeguarding_lead',
                'fpr.Q_Health_And_Safety_Lead as q_health_and_safety_lead',
                'fpr.Q_Iqa_Lead as q_iqa_lead',
                'fpr.Q_Iqa_Organisation as q_iqa_organisation',
                'fpr.Q_Adult_Training_Only as q_adult_training_only',
                'fpr.Q_Expected_Delivery_Empty as q_expected_delivery_empty',
                'fpr.Q_Expected_Delivery as q_expected_delivery',
                'fpr.Q_Confirm_Final_Three as q_confirm_final_three',
                'fpr.Q_Name as q_name',
                'fpr.Q_Safeguarding as q_safeguarding',
            ])
            ->where('fpr.Renewal_Year', (int)$params['financial_year'])
            ->where('tp.Is_Current', 1);

        if (!empty($params['provider_id'])) {
            $query->where('tp.Source_Provider_Id', (int)$params['provider_id']);
        }

        return $query->orderBy('tp.Source_Provider_Id', 'asc');
    }
}

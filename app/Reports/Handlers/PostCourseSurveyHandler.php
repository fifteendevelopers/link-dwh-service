<?php

namespace App\Reports\Handlers;

use App\Reports\Contracts\ReportHandlerInterface;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Validator;

class PostCourseSurveyHandler implements ReportHandlerInterface
{
    /**
     * Define and validate acceptable optional filters for the survey report.
     */
    public function validate(array $parameters): array
    {
        return Validator::make($parameters, [
            'year'        => 'nullable|integer|digits:4',
            'grant_id'    => 'nullable|integer',
            'delivery_id' => 'nullable|integer',
        ])->validate();
    }

    /**
     * Execute the comprehensive parent/rider feedback survey extraction query.
     */
    public function execute(array $params): array
    {
        // Allocate robust processing limits for massive survey rows parsing
        ini_set('memory_limit', '1024M');
        ini_set('max_execution_time', '300');

        $query = DB::connection('mysql')->table('Fact_Parent_Survey as f')
            ->join('Dim_Course as c', 'f.Course_Key', '=', 'c.Course_Key')
            ->join('Dim_Delivery_Header as dh', 'f.Delivery_Key', '=', 'dh.Delivery_Key')
            ->join('Dim_Rider as r', 'f.Rider_Key', '=', 'r.Rider_Key')
            ->join('Dim_Grant as g', 'f.Grant_Key', '=', 'g.Grant_Key')
            ->join('Dim_Grant_Recipient as gr', 'g.Grant_Recipient_Key', '=', 'gr.Recipient_Key')
            ->join('Dim_Training_Provider as tp', 'dh.Training_Provider_Key', '=', 'tp.Provider_Key')
            ->leftJoin('Dim_School as s', 'dh.School_Key', '=', 's.School_Key')
            ->leftJoin('Dim_Organisation as o', 'dh.Organisation_Key', '=', 'o.Organisation_Key')
            ->select([
                // 1. Header Information
                'g.Grant_Number as Grant Number',
                'g.Grant_Source as Grant Funding Source',
                'gr.Recipient_Name as Grant Recipient',
                'dh.Source_Delivery_Id as Delivery ID',
                'r.Source_Rider_Id as Rider ID',
                'tp.Provider_Name as Training Provider',
                DB::raw("COALESCE(s.School_Name, o.Organisation_Name) as 'School/Organisation'"),

                // Course Name Labeling Text Logic
                DB::raw("CASE
                    WHEN c.Course_Level = 'level_1_2' THEN 'Level 1 & 2 (Combined)'
                    WHEN c.Parent_Course_Key IS NOT NULL AND c.Course_Level = 'level_1' THEN 'Level 1 (Combined)'
                    WHEN c.Parent_Course_Key IS NOT NULL AND c.Course_Level = 'level_2' THEN 'Level 2 (Combined)'
                    ELSE REPLACE(REPLACE(c.Course_Level, 'level_', 'Level '), '_', ' ')
                END as 'Course Name'"),

                DB::raw("DATE_FORMAT(f.Source_Created_At, '%d %b %Y') as 'Received Date'"),

                // Consent / Participation (q1)
                DB::raw("CASE f.Like_To_Participate WHEN 1 THEN 'Yes' ELSE 'No' END as 'q1.1: Voluntary Participation'"),
                DB::raw("CASE f.Like_To_Answer_Survey WHEN 1 THEN 'Yes' ELSE 'No' END as 'q1.2: Child Research Consent'"),
                DB::raw("CASE f.Pref_Join_Bikeability WHEN 1 THEN 'Yes' ELSE 'No' END as 'q1.3: Bikeability Club'"),

                // Rider Emotion (q2)
                DB::raw("CASE f.rider_emotion
                    WHEN 1 THEN 'Sad'
                    WHEN 2 THEN 'Neutral'
                    WHEN 3 THEN 'Happy'
                    ELSE 'No Response'
                END as 'Rider Emotion'"),

                // Experience (q3)
                DB::raw("IF(f.Feedback_Is_Fun = 1, 'Yes', 'No') as 'q3.1: Cycling is fun'"),
                DB::raw("IF(f.Feedback_Is_Hard = 1, 'Yes', 'No') as 'q3.2: Cycling is hard'"),
                DB::raw("IF(f.Feedback_Is_Healthy = 1, 'Yes', 'No') as 'q3.3: Keeps me healthy'"),
                DB::raw("IF(f.Feedback_Still_New = 1, 'Yes', 'No') as 'q3.4: Still new to cycling'"),
                DB::raw("IF(f.Feedback_Family_Friends = 1, 'Yes', 'No') as 'q3.5: Enjoy with family/friends'"),
                DB::raw("IF(f.Feedback_Dont_See_Others_Like_Me = 1, 'Yes', 'No') as 'q3.6: Don’t see others like me'"),
                DB::raw("IF(f.Feedback_On_Own = 1, 'Yes', 'No') as 'q3.7: Enjoy on my own'"),
                DB::raw("IF(f.Feedback_Not_Enjoy = 1, 'Yes', 'No') as 'q3.8: Do not enjoy cycling'"),
                'f.Feedback_None_Apply_Input as q3.10: Other Feedback',

                // Confidence Likert Scale (q4)
                DB::raw("CASE f.Confidence_Bike_General WHEN 1 THEN 'Much more confident' WHEN 2 THEN 'A little more confident' WHEN 3 THEN 'No difference' ELSE 'N/A' END as 'q4.1: Use a cycle (general)'"),
                DB::raw("CASE f.Confidence_Road WHEN 1 THEN 'Much more confident' WHEN 2 THEN 'A little more confident' WHEN 3 THEN 'No difference' ELSE 'N/A' END as 'q4.2: Cycling on roads'"),
                DB::raw("CASE f.Confidence_Independent WHEN 1 THEN 'Much more confident' WHEN 2 THEN 'A little more confident' WHEN 3 THEN 'No difference' ELSE 'N/A' END as 'q4.3: Cycle independently'"),

                // q5: Frequency Metrics
                DB::raw("CASE f.Frequency_School
                    WHEN 1 THEN '4 or more days a week' WHEN 2 THEN 'Between 1 to 3 days a week' WHEN 3 THEN 'Once or twice a month' WHEN 4 THEN 'Less than once a month' WHEN 5 THEN 'Never'
                    WHEN 6 THEN 'Not applicable (My child doesn\'t know how to use a cycle yet )' ELSE 'N/A'
                END as 'q5.1: Frequency To & From School'"),

                DB::raw("CASE f.Frequency_Leisure
                    WHEN 1 THEN '4 or more days a week' WHEN 2 THEN 'Between 1 to 3 days a week' WHEN 3 THEN 'Once or twice a month' WHEN 4 THEN 'Less than once a month' WHEN 5 THEN 'Never'
                    WHEN 6 THEN 'Not applicable (My child doesn\'t know how to use a cycle yet )' ELSE 'N/A'
                END as 'q5.2: Frequency Leisure & Social'"),

                DB::raw("CASE f.Frequency_Exercise
                    WHEN 1 THEN '4 or more days a week' WHEN 2 THEN 'Between 1 to 3 days a week' WHEN 3 THEN 'Once or twice a month' WHEN 4 THEN 'Less than once a month' WHEN 5 THEN 'Never'
                    WHEN 6 THEN 'Not applicable (My child doesn\'t know how to use a cycle yet )' ELSE 'N/A'
                END as 'q5.3: Frequency for Exercise'"),

                // q6: Likely to Encourage
                DB::raw("CASE f.Encouragement_Use_Bike WHEN 1 THEN 'Very likely' WHEN 2 THEN 'Likely' WHEN 3 THEN 'Neither' ELSE 'N/A' END as 'q6.1: Use of Bike - Likely to Encourage '"),
                DB::raw("CASE f.Encouragement_Use_Bike_On_Road WHEN 1 THEN 'Very likely' WHEN 2 THEN 'Likely' WHEN 3 THEN 'Neither' ELSE 'N/A' END as 'q6.2: Use of Bike on Road - Likely to Encourage'"),

                // q7 & q8: Future Training Options
                DB::raw("IF(f.Pref_More_Training = 1, 'Yes', 'No') as 'q7: Like More Traiing'"),
                DB::raw("IF(f.Pref_Interest_In_Training = 1, 'Yes', 'No') as 'q8.1: Interest in Training at Current Level'"),
                DB::raw("IF(f.Pref_Interest_In_Training = 2, 'Yes', 'No') as 'q8.2: Interest in Training - Challenging Skills'"),

                // q9: Infrastructure Matrix Block
                DB::raw("IF(f.Encourage_More_Direct_Routes = 1, 'Yes', 'No') as 'q9.1'"),
                DB::raw("IF(f.Encourage_Local_Route_Awareness = 1, 'Yes', 'No') as 'q9.2'"),
                DB::raw("IF(f.Encourage_Storage = 1, 'Yes', 'No') as 'q9.3'"),
                DB::raw("IF(f.Encourage_Road_Surfaces = 1, 'Yes', 'No') as 'q9.4'"),
                DB::raw("IF(f.Encourage_Confidence = 1, 'Yes', 'No') as 'q9.5'"),
                DB::raw("IF(f.Encourage_Cycle_Maintenance = 1, 'Yes', 'No') as 'q9.6'"),
                DB::raw("IF(f.Encourage_Local_Initiatives = 1, 'Yes', 'No') as 'q9.7'"),
                DB::raw("IF(f.Encourage_Purchase_Ability = 1, 'Yes', 'No') as 'q9.8'"),
                DB::raw("IF(f.Encourage_Doesnt_Want_To_Cycle_More = 1, 'Yes', 'No') as 'q9.9'"),
                DB::raw("IF(f.Encourage_None = 1, 'Yes', 'No') as 'q9.10'"),
                'f.Encourage_Other_Reason as q9.11',

                // q10: Core Likert Agreement Matrices
                DB::raw("CASE f.Likert_Life_Skill WHEN 1 THEN 'Strongly Agree' WHEN 2 THEN 'Agree' WHEN 3 THEN 'Neither' WHEN 4 THEN 'Disagree' WHEN 5 THEN 'Strongly Disagree' ELSE 'N/A' END as 'q10.1'"),
                DB::raw("CASE f.Likert_Self_Esteem WHEN 1 THEN 'Strongly Agree' WHEN 2 THEN 'Agree' WHEN 3 THEN 'Neither' ELSE 'N/A' END as 'q10.2'"),
                DB::raw("CASE f.Likert_Fitness WHEN 1 THEN 'Strongly Agree' WHEN 2 THEN 'Agree' WHEN 3 THEN 'Neither' ELSE 'N/A' END as 'q10.3'"),
                DB::raw("CASE f.Likert_Active WHEN 1 THEN 'Strongly Agree' WHEN 2 THEN 'Agree' WHEN 3 THEN 'Neither' ELSE 'N/A' END as 'q10.4'"),
                DB::raw("CASE f.Likert_Mindfulness WHEN 1 THEN 'Strongly Agree' WHEN 2 THEN 'Agree' WHEN 3 THEN 'Neither' ELSE 'N/A' END as 'q10.5'"),
                DB::raw("CASE f.Likert_Improve_Self_Regulate WHEN 1 THEN 'Strongly Agree' WHEN 2 THEN 'Agree' WHEN 3 THEN 'Neither' ELSE 'N/A' END as 'q10.6'"),
                DB::raw("CASE f.Likert_Improve_Concentration WHEN 1 THEN 'Strongly Agree' WHEN 2 THEN 'Agree' WHEN 3 THEN 'Neither' ELSE 'N/A' END as 'q10.7'"),
                DB::raw("CASE f.Likert_Improve_Academic_Performance WHEN 1 THEN 'Strongly Agree' WHEN 2 THEN 'Agree' WHEN 3 THEN 'Neither' ELSE 'N/A' END as 'q10.8'"),
                DB::raw("CASE f.Likert_Independence WHEN 1 THEN 'Strongly Agree' WHEN 2 THEN 'Agree' WHEN 3 THEN 'Neither' ELSE 'N/A' END as 'q10.9'"),
                DB::raw("CASE f.Likert_Improve_Road_Awareness WHEN 1 THEN 'Strongly Agree' WHEN 2 THEN 'Agree' WHEN 3 THEN 'Neither' ELSE 'N/A' END as 'q10.10'"),
                DB::raw("CASE f.Likert_Improve_Environment_Awareness WHEN 1 THEN 'Strongly Agree' WHEN 2 THEN 'Agree' WHEN 3 THEN 'Neither' ELSE 'N/A' END as 'q10.11'"),
                DB::raw("CASE f.Likert_Help_Socialise WHEN 1 THEN 'Strongly Agree' WHEN 2 THEN 'Agree' WHEN 3 THEN 'Neither' ELSE 'N/A' END as 'q10.12'"),
                DB::raw("CASE f.Likert_Make_Children_Happy WHEN 1 THEN 'Strongly Agree' WHEN 2 THEN 'Agree' WHEN 3 THEN 'Neither' ELSE 'N/A' END as 'q10.13'"),
                DB::raw("CASE f.Likert_Keep_Children_Occupied WHEN 1 THEN 'Strongly Agree' WHEN 2 THEN 'Agree' WHEN 3 THEN 'Neither' ELSE 'N/A' END as 'q10.14'"),
                DB::raw("CASE f.Likert_Encourage_Children_Outside WHEN 1 THEN 'Strongly Agree' WHEN 2 THEN 'Agree' WHEN 3 THEN 'Neither' ELSE 'N/A' END as 'q10.15'"),
                DB::raw("CASE f.Likert_Children_Less_Dependent WHEN 1 THEN 'Strongly Agree' WHEN 2 THEN 'Agree' WHEN 3 THEN 'Neither' ELSE 'N/A' END as 'q10.16'"),
                DB::raw("CASE f.Likert_Reduce_Other_Transport_Expense WHEN 1 THEN 'Strongly Agree' WHEN 2 THEN 'Agree' WHEN 3 THEN 'Neither' ELSE 'N/A' END as 'q10.17'"),
                DB::raw("CASE f.Likert_Enable_Cycle_As_Family WHEN 1 THEN 'Strongly Agree' WHEN 2 THEN 'Agree' WHEN 3 THEN 'Neither' ELSE 'N/A' END as 'q10.18'"),

                // q11 & q12 Columns
                DB::raw("CASE f.Likely_To_Recommend WHEN 1 THEN 'Very likely' WHEN 2 THEN 'Likely' WHEN 3 THEN 'Unlikely' ELSE 'N/A' END as 'q11'"),
                DB::raw("IF(f.Likert_Life_Skill IS NULL AND f.Likert_Fitness IS NULL, 'Yes', 'No') as 'q12: Optional Questions Skipped'")
            ]);

        // --- Year Filter Rule Block ---
        // Dynamically scales to requested target parameter, defaulting directly to 2025
        if (isset($params['year']) && $params['year'] !== '' && $params['year'] !== null) {
            $query->whereRaw('YEAR(f.Source_Created_At) = ?', [$params['year']]);
        } else {
            $query->whereRaw('YEAR(f.Source_Created_At) = 2025');
        }

        // --- Runtime Optional Parameters Whitelisting ---
        if (isset($params['grant_id']) && $params['grant_id'] !== '' && $params['grant_id'] !== null) {
            $query->where('f.Grant_Key', $params['grant_id']);
        }

        if (isset($params['delivery_id']) && $params['delivery_id'] !== '' && $params['delivery_id'] !== null) {
            $query->where('dh.Source_Delivery_Id', $params['delivery_id']);
        }

        // Return chronological descending track matching: ORDER BY f.Source_Created_At DESC
        return $query->orderBy('f.Source_Created_At', 'desc')
            ->get()
            ->toArray();
    }
}

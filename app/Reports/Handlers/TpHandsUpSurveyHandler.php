<?php

namespace App\Reports\Handlers;

use App\Reports\Contracts\ReportHandlerInterface;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Validator;

class TpHandsUpSurveyHandler implements ReportHandlerInterface
{
    /**
     * Define and validate acceptable parameters for the Hands Up Survey report.
     */
    public function validate(array $parameters): array
    {
        return Validator::make($parameters, [
            'provider_id' => 'nullable|integer',
            'delivery_id' => 'nullable|integer',
            'year'        => 'nullable|integer|digits:4', // the 4-digit financial year (e.g., 2026)
        ])->validate();
    }

    /**
     * Execute the Hands-Up Survey perception metric report.
     */
    public function execute(array $params): array
    {
        // Allocate an isolated memory expansion block for this processing sequence
        ini_set('memory_limit', '1024M');
        ini_set('max_execution_time', '300');

//        $query = DB::connection('mysql')->table('Fact_HandsUp_Survey as f')
//            ->join('Dim_Course as c', 'f.Course_Key', '=', 'c.Course_Key')
//            ->join('Dim_Delivery_Header as dh', 'c.Delivery_Key', '=', 'dh.Delivery_Key')
//            ->join('Dim_Training_Provider as tp', 'dh.Training_Provider_Key', '=', 'tp.Provider_Key')
//            ->select([
//                'dh.Source_Delivery_Id as Delivery ID',
//                'tp.Provider_Name as Training Provider',
//                'c.Course_Level as Module',
//
//                // Enjoyment Metric Aligned Groups
//                'f.Exp_Enjoyed as Enjoyed',
//                'f.Exp_Did_Not_Enjoy as Did Not Enjoy',
//
//                // Safety Perception Aligned Groups
//                'f.Safe_More as Feel Safer',
//                'f.Safe_Less as Feel Less Safe',
//
//                // Confidence Perception Aligned Groups
//                'f.Conf_More as More Confident',
//                'f.Conf_Less as Less Confident'
//            ]);

        $query = DB::connection('mysql')->table('Fact_HandsUp_Survey as f')
            ->join('Dim_Course as c', 'f.Course_Key', '=', 'c.Course_Key')
            ->join('Dim_Delivery_Header as dh', 'c.Delivery_Key', '=', 'dh.Delivery_Key')
            ->join('Dim_Training_Provider as tp', 'dh.Training_Provider_Key', '=', 'tp.Provider_Key')
            ->leftJoin('Dim_Grant as g', 'dh.Grant_Key', '=', 'g.Grant_Key')

            // Join BOTH possible destination dimension tables
            ->leftJoin('Dim_School as s', 'dh.School_Key', '=', 's.School_Key')
            ->leftJoin('Dim_Organisation as o', 'dh.Organisation_Key', '=', 'o.Organisation_Key')

            //Get absent figures
            ->leftJoin('Fact_Rider_Course as rc', 'c.Source_Course_Id', '=', 'rc.Source_Course_Id')

            ->select([
                'g.Grant_Number as Grant number',
                'g.Grant_Source as Grant funding source',
                'g.Grant_Recipient_Key as Grant recipient',
                'dh.Source_Delivery_Id as Delivery ID',
                'tp.Provider_Name as Training provider',

                // 🪄 Dynamic Case/When Switch Matrix: Resolves School, then Org, then defaults to "None"
                DB::raw("
            CASE
                WHEN dh.School_Key IS NOT NULL AND s.School_Name IS NOT NULL THEN s.School_Name
                WHEN dh.Organisation_Key IS NOT NULL AND o.Organisation_Name IS NOT NULL THEN o.Organisation_Name
                ELSE 'None'
            END as `School/Organisation`
        "),

                'c.Course_Level as Module',
                'dh.Date_Delivery_End as Course Completion Date',

                // Positional Metric Mapping Blocks
                'f.Exp_Enjoyed as Q1_Enjoyed',
                'f.Exp_Did_Not_Enjoy as Q1_Did_Not_Enjoy',
                'f.Exp_Not_Sure as Q1_Not_Sure',
                DB::raw("COUNT(CASE WHEN rc.Attended = 0 AND rc.Withdrawn = 0 THEN 1 END) as `Q1_Absent`"),

                'f.Base_Yes as Q2_Yes',
                'f.Base_No as Q2_No',
                'f.Base_Not_Sure as Q2_Not_Sure',

                'f.Safe_More as Q3_More',
                'f.Safe_Less as Q3_Less',
                'f.Safe_No_Diff as Q3_No_Diff',
                'f.Safe_Not_Sure as Q3_Not_Sure',

                'f.Conf_More as Q4_More',
                'f.Conf_Less as Q4_Less',
                'f.Conf_No_Diff as Q4_No_Diff',
                'f.Conf_Not_Sure as Q4_Not_Sure',
            ]);

        // 📅 --- Financial Year Date Range Calculator Layer ---
        if (isset($params['year']) && $params['year'] !== '' && $params['year'] !== null) {
            $startYear = (int) $params['year'];

            // Porting logic: From April 1st of start year to March 31st of next year
            $dateFrom = $startYear . '-04-01';
            $dateTo   = ($startYear + 1) . '-03-31';

            $query->whereBetween('dh.Date_Delivery_Start', [$dateFrom, $dateTo]);
        }

        // Strict Parameter Checks to dynamically filter without clipping arrays on nulls
        if (isset($params['provider_id']) && $params['provider_id'] !== '' && $params['provider_id'] !== null) {
            $query->where('tp.Source_Provider_Id', $params['provider_id']);
        }

        if (isset($params['delivery_id']) && $params['delivery_id'] !== '' && $params['delivery_id'] !== null) {
            $query->where('dh.Source_Delivery_Id', $params['delivery_id']);
        }

        $query->groupBy([
        'g.Grant_Number', 'g.Grant_Source', 'g.Grant_Recipient_Key', 'dh.Source_Delivery_Id',
        'tp.Provider_Name', 'dh.School_Key', 's.School_Name', 'dh.Organisation_Key',
        'o.Organisation_Name', 'c.Course_Level', 'dh.Date_Delivery_End', 'c.Source_Course_Id',
        'f.Exp_Enjoyed', 'f.Exp_Did_Not_Enjoy', 'f.Exp_Not_Sure', 'f.Exp_Absent',
        'f.Base_Yes', 'f.Base_No', 'f.Base_Not_Sure', 'f.Safe_More', 'f.Safe_Less',
        'f.Safe_No_Diff', 'f.Safe_Not_Sure', 'f.Conf_More', 'f.Conf_Less', 'f.Conf_No_Diff', 'f.Conf_Not_Sure']);

        // Matches: ORDER BY tp.Provider_Name, dh.Source_Delivery_Id
        return $query->orderBy('tp.Provider_Name')
            ->orderBy('dh.Source_Delivery_Id')
            ->get()
            ->toArray();
    }
}

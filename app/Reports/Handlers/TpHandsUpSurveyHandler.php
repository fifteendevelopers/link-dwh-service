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
            'year'        => 'nullable|integer|digits:4', // ✅ Accept a 4-digit financial start year (e.g., 2026)
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

        $query = DB::connection('mysql')->table('Fact_HandsUp_Survey as f')
            ->join('Dim_Course as c', 'f.Course_Key', '=', 'c.Course_Key')
            ->join('Dim_Delivery_Header as dh', 'c.Delivery_Key', '=', 'dh.Delivery_Key')
            ->join('Dim_Training_Provider as tp', 'dh.Training_Provider_Key', '=', 'tp.Provider_Key')
            ->select([
                'dh.Source_Delivery_Id as Delivery ID',
                'tp.Provider_Name as Training Provider',
                'c.Course_Level as Module',

                // Enjoyment Metric Aligned Groups
                'f.Exp_Enjoyed as Enjoyed',
                'f.Exp_Did_Not_Enjoy as Did Not Enjoy',

                // Safety Perception Aligned Groups
                'f.Safe_More as Feel Safer',
                'f.Safe_Less as Feel Less Safe',

                // Confidence Perception Aligned Groups
                'f.Conf_More as More Confident',
                'f.Conf_Less as Less Confident'
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

        // Matches: ORDER BY tp.Provider_Name, dh.Source_Delivery_Id
        return $query->orderBy('tp.Provider_Name')
            ->orderBy('dh.Source_Delivery_Id')
            ->get()
            ->toArray();
    }
}

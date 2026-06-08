<?php

namespace App\Reports\Handlers;

use App\Reports\Contracts\ReportHandlerInterface;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Validator;

class SchoolDeliveriesHandler implements ReportHandlerInterface
{
    /**
     * Define and validate acceptable optional filters for the school reports tracker.
     */
    public function validate(array $parameters): array
    {
        return Validator::make($parameters, [
            'year'      => 'nullable|integer|digits:4',
            'school_id' => 'nullable|integer',
        ])->validate();
    }

    /**
     * Execute the School Deliveries tracking layout map.
     */
    public function execute(array $params): array
    {
        // Enforce safe memory allocation thresholds for comprehensive table scans
        ini_set('memory_limit', '1024M');
        ini_set('max_execution_time', '300');

        // Capture or default the year value early so we can safely inject it into the LEFT JOIN boundary context
        $targetYear = isset($params['year']) && $params['year'] !== '' && $params['year'] !== null
            ? (int) $params['year']
            : (int) date('Y');

        // Start from Dim_School to ensure we never drop locations that lack matching course schedules
        $query = DB::connection('mysql')->table('Dim_School as s')
            ->leftJoin('Dim_Delivery_Header as dh', function ($join) use ($targetYear) {
                $join->on('s.School_Key', '=', 'dh.School_Key')
                    ->whereRaw('YEAR(dh.Date_Delivery_Start) = ?', [$targetYear]);
            })
            ->leftJoin('Dim_Training_Provider as tp', 'dh.Training_Provider_Key', '=', 'tp.Provider_Key')
            ->select([
                's.School_Name as School Name',
                's.Source_School_Id as School ID',

                // 🚀 If Delivery ID is NULL, output the exact required warning string
                DB::raw("COALESCE(CAST(dh.Source_Delivery_Id AS CHAR), 'No Deliveries for this School') as 'Delivery ID'"),

                DB::raw("COALESCE(dh.Delivery_Status, 'N/A') as 'Delivery Status'"),
                DB::raw("COALESCE(tp.Provider_Name, 'N/A') as 'Training Provider'"),
                DB::raw("COALESCE(DATE_FORMAT(dh.Date_Delivery_Start, '%d/%m/%Y'), 'N/A') as 'Start Date'"),
                DB::raw("COALESCE(DATE_FORMAT(dh.Date_Delivery_End, '%d/%m/%Y'), 'N/A') as 'End Date'")
            ]);

        // Optional filter to restrict execution down to an isolated single school entity
        if (isset($params['school_id']) && $params['school_id'] !== '' && $params['school_id'] !== null) {
            $query->where('s.Source_School_Id', $params['school_id']);
        }

        // Matches requirement: ordered cleanly by schools alphabet layouts
        return $query->orderBy('s.School_Name', 'asc')
            ->orderBy('dh.Date_Delivery_Start', 'desc')
            ->get()
            ->toArray();
    }
}

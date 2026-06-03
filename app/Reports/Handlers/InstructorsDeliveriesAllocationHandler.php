<?php

namespace App\Reports\Handlers;

use App\Reports\Contracts\ReportHandlerInterface;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Validator;

class InstructorsDeliveriesAllocationHandler implements ReportHandlerInterface
{
    /**
     * Define and validate optional runtime filters for this report.
     */
    public function validate(array $parameters): array
    {
        return Validator::make($parameters, [
            'instructor_id' => 'nullable|integer',
            'delivery_id'   => 'nullable|integer',
            'year'          => 'nullable|integer|digits:4', // Allows overriding current year for historical deep-dives
        ])->validate();
    }

    /**
     * Execute the Instructor Allocation Matrix query.
     */
    public function execute(array $params): array
    {
        // Allocate runtime performance extensions for DWH multi-table joins
        ini_set('memory_limit', '1024M');
        ini_set('max_execution_time', '300');

        $query = DB::connection('mysql')->table('Fact_Instructor_Delivery as fi')
            ->join('Dim_Delivery_Header as dh', 'fi.Delivery_Key', '=', 'dh.Delivery_Key')
            ->join('Dim_Instructor as i', 'fi.Instructor_Key', '=', 'i.Instructor_Key')
            ->leftJoin('Dim_School as s', 'dh.School_Key', '=', 's.School_Key')
            ->leftJoin('Dim_Organisation as o', 'dh.Organisation_Key', '=', 'o.Organisation_Key')
            ->select([
                'dh.Source_Delivery_Id as Delivery ID',
                DB::raw("COALESCE(s.School_Name, 'N/A') as 'School Name'"),
                DB::raw("COALESCE(o.Organisation_Name, 'N/A') as 'Organisation Name'"),
                'i.Source_Instructor_Id as Instructor ID',
                DB::raw("CONCAT(i.First_Name,' ',i.Last_Name) as 'Instructor Name'"),
                'dh.Delivery_Status as Delivery Status'
            ]);

        // --- Handle Year Constraint ---
        // If an explicit override year is sent in parameters, use it. Otherwise, default strictly to Current Year
        if (isset($params['year']) && $params['year'] !== '' && $params['year'] !== null) {
            $query->whereRaw('YEAR(dh.Date_Delivery_Start) = ?', [$params['year']]);
        } else {
            $query->whereRaw('YEAR(dh.Date_Delivery_Start) = YEAR(CURDATE())');
        }

        // --- Dynamic Contextual Parameter Filters ---
        if (isset($params['instructor_id']) && $params['instructor_id'] !== '' && $params['instructor_id'] !== null) {
            $query->where('i.Source_Instructor_Id', $params['instructor_id']);
        }

        if (isset($params['delivery_id']) && $params['delivery_id'] !== '' && $params['delivery_id'] !== null) {
            $query->where('dh.Source_Delivery_Id', $params['delivery_id']);
        }

        // Return sorted output listing instructors grouped by delivery tracks cleanly
        return $query->orderBy('dh.Source_Delivery_Id')
            ->orderBy('Instructor Name')
            ->get()
            ->toArray();
    }
}

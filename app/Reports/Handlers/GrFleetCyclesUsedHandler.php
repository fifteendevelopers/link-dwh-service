<?php

namespace App\Reports\Handlers;

use App\Reports\Contracts\ReportHandlerInterface;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Validator;

class GrFleetCyclesUsedHandler implements ReportHandlerInterface
{
    /**
     * Define and validate acceptable parameters for the Fleet Cycles report.
     */
    public function validate(array $parameters): array
    {
        return Validator::make($parameters, [
            'recipient_id' => 'nullable|integer',
            'provider_id'  => 'nullable|integer',
            'year'         => 'nullable|integer|digits:4',
        ])->validate();
    }

    /**
     * Execute the DWH Query for Grant Recipient and Training Provider fleet cycle aggregates.
     */
    public function execute(array $params): array
    {
        $query = DB::connection('mysql')->table('Dim_Delivery_Header as dh')
            ->join('Dim_Grant as g', 'dh.Grant_Key', '=', 'g.Grant_Key')
            ->join('Dim_Grant_Recipient as gr', 'g.Grant_Recipient_Key', '=', 'gr.Recipient_Key')
            ->join('Dim_Training_Provider as tp', 'dh.Training_Provider_Key', '=', 'tp.Provider_Key')
            ->select([
                'gr.Recipient_Name as GR Name',
                'tp.Provider_Name as TP Name',
                DB::raw("CASE WHEN tp.Is_Active = 'Y' THEN 'Active' ELSE 'Inactive' END as 'TP Active?'"),
                DB::raw("MONTHNAME(dh.Date_Delivery_Start) as 'Delivery Month'"),
                DB::raw("YEAR(dh.Date_Delivery_Start) as 'Delivery Year'"),
                DB::raw("COUNT(dh.Delivery_Key) as 'Delivery Count'"),
                DB::raw("SUM(COALESCE(dh.fleet_cycles_used, 0)) as 'Total Fleet Cycles Used'")
            ])
            // Standard SCD Filter to ensure we capture current active versions
            ->where('gr.Is_Current', 1)
            ->where('tp.Is_Current', 1);

        // Apply Optional Contextual Filters dynamically
        if (!empty($params['recipient_id'])) {
            $query->where('gr.Source_Recipient_Id', $params['recipient_id']);
        }

        if (!empty($params['provider_id'])) {
            $query->where('tp.Source_Provider_Id', $params['provider_id']);
        }

        if (!empty($params['year'])) {
            $query->whereRaw('YEAR(dh.Date_Delivery_Start) = ?', [$params['year']]);
        }

        // Apply strict DWH Aggregation grouping rules
        $query->groupBy([
            'gr.Recipient_Name',
            'tp.Provider_Name',
            'tp.Is_Active',
            DB::raw('YEAR(dh.Date_Delivery_Start)'),
            DB::raw('MONTH(dh.Date_Delivery_Start)'),
            DB::raw('MONTHNAME(dh.Date_Delivery_Start)')
        ]);

        // Sort rows exactly as requested by your analytical template
        return $query->orderBy('gr.Recipient_Name', 'asc')
            ->orderBy('tp.Provider_Name')
            ->orderBy(DB::raw('YEAR(dh.Date_Delivery_Start)'))
            ->orderBy(DB::raw('MONTH(dh.Date_Delivery_Start)'))
            ->get()
            ->toArray();
    }
}

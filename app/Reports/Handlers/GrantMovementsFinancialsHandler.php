<?php

namespace App\Reports\Handlers;

use App\Reports\Contracts\ReportHandlerInterface;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Validator;

class GrantMovementsFinancialsHandler implements ReportHandlerInterface
{
    /**
     * Define and validate acceptable query parameter overrides.
     */
    public function validate(array $parameters): array
    {
        return Validator::make($parameters, [
            'grant_source' => 'nullable|string',
            'start_date'   => 'nullable|date_format:Y-m-d',
            'end_date'     => 'nullable|date_format:Y-m-d',
        ])->validate();
    }

    /**
     * Execute the Grant Movements Financials data extraction query via relational pivoting.
     */
    public function execute(array $params): array
    {
        ini_set('memory_limit', '1024M');
        ini_set('max_execution_time', '300');

        // Define the target keys we need to pivot out of the child table rows
        $modules = [
            'level_1'        => ['Level 1 Places', 'Level 1 Value'],
            'level_1_2'      => ['Level 1 & 2 Places', 'Level 1 & 2 Value'],
            'level_2'        => ['Level 2 Places', 'Level 2 Value'],
            'level_3'        => ['Level 3 Places', 'Level 3 Value'],
            'plus_balance'   => ['Plus Balance Places', 'Plus Balance Value'],
            'plus_bus'       => ['Plus Bus Places', 'Plus Bus Value'],
            'plus_fix'       => ['Plus Fix Places', 'Plus Fix Value'],
            'plus_learn'     => ['Plus Learn Places', 'Plus Learn Value'],
            'plus_on_show'   => ['Plus On Show Places', 'Plus On Show Value'],
            'plus_parents'   => ['Plus Parents Places', 'Plus Parents Value'],
            'plus_promotion' => ['Plus Promotion Places', 'Plus Promotion Value'],
            'plus_recycled'  => ['Plus Recycled Places', 'Plus Recycled Value'],
            'plus_ride'      => ['Plus Ride Places', 'Plus Ride Value'],
            'plus_transition'=> ['Plus Transition Places', 'Plus Transition Value'],
            'plus_family'    => ['Plus Family Places', 'Plus Family Value'],
            'plus_adult'     => ['Plus Adult Places', 'Plus Adult Value']
        ];

        $selectFields = [
            'g.Grant_Number as Grant Number',
            'g.Grant_Source as Grant Source',
            'gr.Recipient_Name as Grant Recipient',
            DB::raw("CASE WHEN gr.Is_SGO = 1 THEN 'SGO' ELSE '' END as 'SGO Status'"),
            'f.Reallocation_Number as Funding Change Number',
            DB::raw("CONCAT(UPPER(LEFT(f.reallocation_type, 1)), SUBSTRING(f.reallocation_type, 2)) as 'Funding Change Type'"),
            'f.status as Funding Change Status',
            'f.reallocation_notes as Notes',
            'f.reallocation_increase_reasons as Reasons for Increase',
            DB::raw("DATE_FORMAT(f.created_at, '%d/%m/%Y') as 'Created Date'"),
            DB::raw("DATE_FORMAT(f.date_approved, '%d/%m/%Y') as 'Approval Date'"),
            DB::raw("'' as 'Spacer'")
        ];

        // Dynamically build the conditional pivot aggregations for each module type
        foreach ($modules as $moduleKey => $aliases) {
            $selectFields[] = DB::raw("MAX(CASE WHEN rl.Module_Key = '{$moduleKey}' THEN rl.Value_Count END) as '{$aliases[0]}'");
            $selectFields[] = DB::raw("MAX(CASE WHEN rl.Module_Key = '{$moduleKey}' THEN rl.Amount END) as '{$aliases[1]}'");
        }

        $query = DB::connection('mysql')->table('Fact_Grant_Reallocations as f')
            ->join('Dim_Grant as g', 'f.grant_id', '=', 'g.Source_Grant_Id')
            ->join('Dim_Grant_Recipient as gr', 'g.Grant_Recipient_Key', '=', 'gr.Recipient_Key')
            // Join onto your brand new normalized child table
            // Note: If your parent table PK uses f.Reallocation_Key instead of f.id, swap 'f.id' to match
            ->leftJoin('Fact_Grant_Reallocation_Logs as rl', 'f.id', '=', 'rl.Reallocation_Key')
            ->select($selectFields)
            ->where('f.status', 1);

        // --- Contextual Parameter Dynamic Filtering ---
        if (isset($params['grant_source']) && $params['grant_source'] !== '' && $params['grant_source'] !== null) {
            $query->where('g.Grant_Source', $params['grant_source']);
        }

        if (isset($params['start_date']) && $params['start_date'] !== '' && $params['start_date'] !== null) {
            $query->where('f.created_at', '>=', $params['start_date']);
        }

        if (isset($params['end_date']) && $params['end_date'] !== '' && $params['end_date'] !== null) {
            $query->where('f.created_at', '<=', $params['end_date']);
        }

        // Enforce strict MySQL GROUP BY criteria grouping all parent metadata
        $query->groupBy([
            'g.Grant_Number',
            'g.Grant_Source',
            'gr.Recipient_Name',
            'gr.pref_sgoh',
            'f.id',
            'f.reallocation_number',
            'f.reallocation_type',
            'f.status',
            'f.reallocation_notes',
            'f.reallocation_increase_reasons',
            'f.created_at',
            'f.date_approved'
        ]);

        return $query->orderBy('g.Grant_Number')
            ->orderBy('f.created_at')
            ->get()
            ->toArray();
    }
}

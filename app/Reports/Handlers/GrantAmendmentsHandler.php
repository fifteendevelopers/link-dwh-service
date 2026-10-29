<?php

namespace App\Reports\Handlers;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Validator;

class GrantAmendmentsHandler extends AbstractStreamingReportHandler
{
    public function validate(array $parameters): array
    {
        return Validator::make($parameters, [
            'date_from' => 'nullable|date',
            'date_to' => 'nullable|date',
            'grant_source' => 'nullable|string',
        ])->validate();
    }

    public function execute(array $params): array
    {
        ini_set('memory_limit', '1024M');
        ini_set('max_execution_time', '300');

        $query = $this->buildQuery($params);

        if (empty($this->callbackUrl)) {
            return $query->get()->map(fn($row) => (array)$row)->toArray();
        }

        $chunkSize = 1000;

        $query->chunk($chunkSize, function ($rows) {
            $memBuffer = fopen('php://temp', 'r+');

            foreach ($rows as $row) {
                fputcsv($memBuffer, (array)$row);
            }

            rewind($memBuffer);
            $csvStringChunk = stream_get_contents($memBuffer);
            fclose($memBuffer);

            $this->transmitBatch([$csvStringChunk], false);
        });

        $this->transmitBatch([], true);

        return ['status' => 'async_completed'];
    }

    protected function buildQuery(array $params)
    {
        $db = DB::connection('mysql');

        $modules = [
            'level_1', 'level_1_2', 'level_2', 'level_3',
            'plus_balance', 'plus_bus', 'plus_fix', 'plus_learn',
            'plus_on_show', 'plus_parents', 'plus_promotion', 'plus_recycled',
            'plus_ride', 'plus_transition', 'plus_family', 'plus_adult'
        ];

        // 1. Pivot places (Value_Count) and funding (Amount) per amendment from Fact_Grant_Amendment_Logs
        $deltasQuery = $db->table('Fact_Grant_Amendment_Logs as al')
            ->select('al.Amendment_Key');

        foreach ($modules as $mod) {
            $deltasQuery->selectRaw("
            SUM(CASE
                WHEN al.Module_Key = '{$mod}' AND LOWER(al.Type_Label) = 'reduce' THEN -al.Value_Count
                WHEN al.Module_Key = '{$mod}' AND LOWER(al.Type_Label) = 'increase' THEN al.Value_Count
                ELSE 0
            END) as {$mod}_places
        ");

            $deltasQuery->selectRaw("
            SUM(CASE
                WHEN al.Module_Key = '{$mod}' AND LOWER(al.Type_Label) = 'reduce' THEN -al.Amount
                WHEN al.Module_Key = '{$mod}' AND LOWER(al.Type_Label) = 'increase' THEN al.Amount
                ELSE 0
            END) as {$mod}_funding
        ");
        }
        $deltasQuery->groupBy('al.Amendment_Key');

        // 2. Build summation expressions for totals
        $totalPlacesSql  = implode(' + ', array_map(fn($m) => "COALESCE(d.{$m}_places, 0)", $modules));
        $totalFundingSql = implode(' + ', array_map(fn($m) => "COALESCE(d.{$m}_funding, 0)", $modules));

        // 3. Main query joining Dim_Grant, Dim_Grant_Recipient, Fact_Grant_Allocation_Dft, and deltas
        $query = $db->table('Fact_Grant_Amendments as a')
            ->join('Dim_Grant as g', 'a.Grant_Key', '=', 'g.Grant_Key')
            ->join('Dim_Grant_Recipient as gr', 'g.Grant_Recipient_Key', '=', 'gr.Recipient_Key')
            ->leftJoin('Fact_Grant_Allocation_Dft as gad', 'g.Grant_Key', '=', 'gad.Grant_Key')
            ->leftJoinSub($deltasQuery, 'd', 'a.Amendment_Key', '=', 'd.Amendment_Key')
            ->where('a.Status_Raw', 1);

        if (!empty($params['date_from'])) {
            $query->whereDate('a.created_at', '>=', $params['date_from']);
        }
        if (!empty($params['date_to'])) {
            $query->whereDate('a.created_at', '<=', $params['date_to']);
        }
        if (!empty($params['grant_source']) && $params['grant_source'] !== 'All') {
            $query->where('g.Grant_Source', '=', $params['grant_source']);
        }

        $query->select([
            'g.Grant_Number',
            'g.Grant_Source',
            'gr.Recipient_Name',
            'a.Amendment_Number',
            'a.created_at',
            'a.Date_Approved',
            'gr.Is_SGO',
            DB::raw("'' as gap_1"),
            // Core modules (Places, Funding)
            DB::raw("COALESCE(d.level_1_places, 0) as level_1_places"),
            DB::raw("COALESCE(d.level_1_funding, 0) as level_1_funding"),
            DB::raw("COALESCE(d.level_1_2_places, 0) as level_1_2_places"),
            DB::raw("COALESCE(d.level_1_2_funding, 0) as level_1_2_funding"),
            DB::raw("COALESCE(d.level_2_places, 0) as level_2_places"),
            DB::raw("COALESCE(d.level_2_funding, 0) as level_2_funding"),
            DB::raw("COALESCE(d.level_3_places, 0) as level_3_places"),
            DB::raw("COALESCE(d.level_3_funding, 0) as level_3_funding"),
            DB::raw("'' as gap_2"),
            // Plus modules (Places, Funding)
            DB::raw("COALESCE(d.plus_balance_places, 0) as plus_balance_places"),
            DB::raw("COALESCE(d.plus_balance_funding, 0) as plus_balance_funding"),
            DB::raw("COALESCE(d.plus_bus_places, 0) as plus_bus_places"),
            DB::raw("COALESCE(d.plus_bus_funding, 0) as plus_bus_funding"),
            DB::raw("COALESCE(d.plus_fix_places, 0) as plus_fix_places"),
            DB::raw("COALESCE(d.plus_fix_funding, 0) as plus_fix_funding"),
            DB::raw("COALESCE(d.plus_learn_places, 0) as plus_learn_places"),
            DB::raw("COALESCE(d.plus_learn_funding, 0) as plus_learn_funding"),
            DB::raw("COALESCE(d.plus_on_show_places, 0) as plus_on_show_places"),
            DB::raw("COALESCE(d.plus_on_show_funding, 0) as plus_on_show_funding"),
            DB::raw("COALESCE(d.plus_parents_places, 0) as plus_parents_places"),
            DB::raw("COALESCE(d.plus_parents_funding, 0) as plus_parents_funding"),
            DB::raw("COALESCE(d.plus_promotion_places, 0) as plus_promotion_places"),
            DB::raw("COALESCE(d.plus_promotion_funding, 0) as plus_promotion_funding"),
            DB::raw("COALESCE(d.plus_recycled_places, 0) as plus_recycled_places"),
            DB::raw("COALESCE(d.plus_recycled_funding, 0) as plus_recycled_funding"),
            DB::raw("COALESCE(d.plus_ride_places, 0) as plus_ride_places"),
            DB::raw("COALESCE(d.plus_ride_funding, 0) as plus_ride_funding"),
            DB::raw("COALESCE(d.plus_transition_places, 0) as plus_transition_places"),
            DB::raw("COALESCE(d.plus_transition_funding, 0) as plus_transition_funding"),
            DB::raw("COALESCE(d.plus_family_places, 0) as plus_family_places"),
            DB::raw("COALESCE(d.plus_family_funding, 0) as plus_family_funding"),
            DB::raw("COALESCE(d.plus_adult_places, 0) as plus_adult_places"),
            DB::raw("COALESCE(d.plus_adult_funding, 0) as plus_adult_funding"),
            DB::raw("'' as gap_3"),
            DB::raw("({$totalPlacesSql}) as total_places_change"),
            DB::raw("({$totalFundingSql}) as total_funding_change"),
            // Original Allocation: (Total_Levels + Total_Plus) - totalFundingDelta
            DB::raw("((COALESCE(gad.Total_Levels, 0) + COALESCE(gad.Total_Plus, 0)) - ({$totalFundingSql})) as original_allocation"),
        ]);

        return $query->orderBy('g.Grant_Number')->orderBy('a.Amendment_Number');
    }
}

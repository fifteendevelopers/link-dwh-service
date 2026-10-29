<?php

namespace App\Reports\Handlers;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Validator;

class GrantClaimsToPayHandler extends AbstractStreamingReportHandler
{
    public function validate(array $parameters): array
    {
        return Validator::make($parameters, [
            'financial_year' => 'nullable|integer',
            'year'           => 'nullable|integer',
            'grant_source'   => 'nullable|string',
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
        // Extract 4-digit start year cleanly
        $fy = $params['financial_year'] ?? $params['year'] ?? null;
        $startYear = null;
        if (!empty($fy)) {
            $startYear = is_numeric($fy) ? (int)$fy : (int)substr((string)$fy, 0, 4);
        }

        // 1. Identify target Claim_Keys first to avoid full-table scans
        $targetClaims = DB::connection('mysql')->table('Fact_Grant_Claims as c')
            ->join('Dim_Grant as g', 'c.Grant_Key', '=', 'g.Grant_Key')
            ->where('c.Status_Raw', 1)
            ->where('c.Pref_Claim_Paid', 0);

        if ($startYear) {
            $targetClaims->where('g.Grant_Period_Start_Year', $startYear);
        }

        if (!empty($params['grant_source'])) {
            $targetClaims->where('g.Grant_Source', $params['grant_source']);
        }

        $claimKeys = $targetClaims->pluck('c.Claim_Key')->all();

        // 2. Pivot Fact_Grant_Claim_Logs ONLY for the target claims
        $claimLogSub = DB::connection('mysql')->table('Fact_Grant_Claim_Logs')
            ->whereIn('Claim_Key', !empty($claimKeys) ? $claimKeys : [-1])
            ->select('Claim_Key')
            ->selectRaw("MAX(CASE WHEN Module_Key = 'level_1' THEN Item_Count ELSE 0 END) as places_level_1")
            ->selectRaw("MAX(CASE WHEN Module_Key = 'level_1_2' THEN Item_Count ELSE 0 END) as places_level_1_2")
            ->selectRaw("MAX(CASE WHEN Module_Key = 'level_2' THEN Item_Count ELSE 0 END) as places_level_2")
            ->selectRaw("MAX(CASE WHEN Module_Key = 'level_3' THEN Item_Count ELSE 0 END) as places_level_3")
            ->selectRaw("MAX(CASE WHEN Module_Key = 'plus_balance' THEN Item_Count ELSE 0 END) as places_plus_balance")
            ->selectRaw("MAX(CASE WHEN Module_Key = 'plus_bus' THEN Item_Count ELSE 0 END) as places_plus_bus")
            ->selectRaw("MAX(CASE WHEN Module_Key = 'plus_fix' THEN Item_Count ELSE 0 END) as places_plus_fix")
            ->selectRaw("MAX(CASE WHEN Module_Key = 'plus_learn' THEN Item_Count ELSE 0 END) as places_plus_learn")
            ->selectRaw("MAX(CASE WHEN Module_Key = 'plus_on_show' THEN Item_Count ELSE 0 END) as places_plus_on_show")
            ->selectRaw("MAX(CASE WHEN Module_Key = 'plus_parents' THEN Item_Count ELSE 0 END) as places_plus_parents")
            ->selectRaw("MAX(CASE WHEN Module_Key = 'plus_promotion' THEN Item_Count ELSE 0 END) as places_plus_promotion")
            ->selectRaw("MAX(CASE WHEN Module_Key = 'plus_recycled' THEN Item_Count ELSE 0 END) as places_plus_recycled")
            ->selectRaw("MAX(CASE WHEN Module_Key = 'plus_ride' THEN Item_Count ELSE 0 END) as places_plus_ride")
            ->selectRaw("MAX(CASE WHEN Module_Key = 'plus_transition' THEN Item_Count ELSE 0 END) as places_plus_transition")
            ->selectRaw("MAX(CASE WHEN Module_Key = 'plus_family' THEN Item_Count ELSE 0 END) as places_plus_family")
            ->selectRaw("MAX(CASE WHEN Module_Key = 'plus_adult' THEN Item_Count ELSE 0 END) as places_plus_adult")
            ->groupBy('Claim_Key');

        // 3. Build main query using LEFT JOINs
        $query = DB::connection('mysql')->table('Fact_Grant_Claims as c')
            ->join('Dim_Grant as g', 'c.Grant_Key', '=', 'g.Grant_Key')
            ->leftJoin('Dim_Grant_Recipient as gr', 'g.Grant_Recipient_Key', '=', 'gr.Recipient_Key')
            ->leftJoin('Fact_Grant_Allocation_Dft as ga', 'g.Grant_Key', '=', 'ga.Grant_Key')
            ->leftJoinSub($claimLogSub, 'cl', 'c.Claim_Key', '=', 'cl.Claim_Key')
            ->select([
                'g.Grant_Number',
                'g.Grant_Source',
                DB::raw("IFNULL(gr.Recipient_Name, 'Unlinked') as Recipient_Name"),
                'c.Claim_Number',
                DB::raw("CASE WHEN gr.Is_SGO = 1 THEN 'SGO' ELSE '' END as Sgo_Status"),

                // Core Totals
                DB::raw("ROUND(IFNULL(cl.places_level_1, 0) * IFNULL(ga.Grant_Level_1, 0), 2) as level1_total"),
                DB::raw("ROUND(IFNULL(cl.places_level_1_2, 0) * IFNULL(ga.Grant_Level_1_2, 0), 2) as level1_2_total"),
                DB::raw("ROUND(IFNULL(cl.places_level_2, 0) * IFNULL(ga.Grant_Level_2, 0), 2) as level2_total"),
                DB::raw("ROUND(IFNULL(cl.places_level_3, 0) * IFNULL(ga.Grant_Level_3, 0), 2) as level3_total"),
                DB::raw("ROUND(
                    (IFNULL(cl.places_level_1, 0) * IFNULL(ga.Grant_Level_1, 0)) +
                    (IFNULL(cl.places_level_1_2, 0) * IFNULL(ga.Grant_Level_1_2, 0)) +
                    (IFNULL(cl.places_level_2, 0) * IFNULL(ga.Grant_Level_2, 0)) +
                    (IFNULL(cl.places_level_3, 0) * IFNULL(ga.Grant_Level_3, 0))
                , 2) as core_subtotal"),

                // Plus Totals
                DB::raw("ROUND(IFNULL(cl.places_plus_balance, 0) * IFNULL(ga.Grant_Plus_Balance, 0), 2) as plus_balance_total"),
                DB::raw("ROUND(IFNULL(cl.places_plus_bus, 0) * IFNULL(ga.Grant_Plus_Bus, 0), 2) as plus_bus_total"),
                DB::raw("ROUND(IFNULL(cl.places_plus_fix, 0) * IFNULL(ga.Grant_Plus_Fix, 0), 2) as plus_fix_total"),
                DB::raw("ROUND(IFNULL(cl.places_plus_learn, 0) * IFNULL(ga.Grant_Plus_Learn, 0), 2) as plus_learn_total"),
                DB::raw("ROUND(IFNULL(cl.places_plus_on_show, 0) * IFNULL(ga.Grant_Plus_On_Show, 0), 2) as plus_on_show_total"),
                DB::raw("ROUND(IFNULL(cl.places_plus_parents, 0) * IFNULL(ga.Grant_Plus_Parents, 0), 2) as plus_parents_total"),
                DB::raw("ROUND(IFNULL(cl.places_plus_promotion, 0) * IFNULL(ga.Grant_Plus_Promotion, 0), 2) as plus_promotion_total"),
                DB::raw("ROUND(IFNULL(cl.places_plus_recycled, 0) * IFNULL(ga.Grant_Plus_Recycled, 0), 2) as plus_recycled_total"),
                DB::raw("ROUND(IFNULL(cl.places_plus_ride, 0) * IFNULL(ga.Grant_Plus_Ride, 0), 2) as plus_ride_total"),
                DB::raw("ROUND(IFNULL(cl.places_plus_transition, 0) * IFNULL(ga.Grant_Plus_Transition, 0), 2) as plus_transition_total"),
                DB::raw("ROUND(IFNULL(cl.places_plus_family, 0) * IFNULL(ga.Grant_Plus_Family, 0), 2) as plus_family_total"),
                DB::raw("ROUND(IFNULL(cl.places_plus_adult, 0) * IFNULL(ga.Grant_Plus_Adult, 0), 2) as plus_adult_total"),
                DB::raw("ROUND(
                    (IFNULL(cl.places_plus_balance, 0) * IFNULL(ga.Grant_Plus_Balance, 0)) +
                    (IFNULL(cl.places_plus_bus, 0) * IFNULL(ga.Grant_Plus_Bus, 0)) +
                    (IFNULL(cl.places_plus_fix, 0) * IFNULL(ga.Grant_Plus_Fix, 0)) +
                    (IFNULL(cl.places_plus_learn, 0) * IFNULL(ga.Grant_Plus_Learn, 0)) +
                    (IFNULL(cl.places_plus_on_show, 0) * IFNULL(ga.Grant_Plus_On_Show, 0)) +
                    (IFNULL(cl.places_plus_parents, 0) * IFNULL(ga.Grant_Plus_Parents, 0)) +
                    (IFNULL(cl.places_plus_promotion, 0) * IFNULL(ga.Grant_Plus_Promotion, 0)) +
                    (IFNULL(cl.places_plus_recycled, 0) * IFNULL(ga.Grant_Plus_Recycled, 0)) +
                    (IFNULL(cl.places_plus_ride, 0) * IFNULL(ga.Grant_Plus_Ride, 0)) +
                    (IFNULL(cl.places_plus_transition, 0) * IFNULL(ga.Grant_Plus_Transition, 0)) +
                    (IFNULL(cl.places_plus_family, 0) * IFNULL(ga.Grant_Plus_Family, 0)) +
                    (IFNULL(cl.places_plus_adult, 0) * IFNULL(ga.Grant_Plus_Adult, 0))
                , 2) as plus_subtotal"),

                // SEND / Inclusion
                DB::raw("ROUND(IFNULL(c.Send_Claimable_Amount, 0), 2) as send_claimed"),
                DB::raw("ROUND(IFNULL(c.Inclusion_Claimable_Amount, 0), 2) as inclusion_claimed"),
                DB::raw("ROUND(IFNULL(c.Send_Claimable_Amount, 0) + IFNULL(c.Inclusion_Claimable_Amount, 0), 2) as send_inclusion_total"),

                // Overall Total Claim
                DB::raw("ROUND(
                    (IFNULL(cl.places_level_1, 0) * IFNULL(ga.Grant_Level_1, 0)) +
                    (IFNULL(cl.places_level_1_2, 0) * IFNULL(ga.Grant_Level_1_2, 0)) +
                    (IFNULL(cl.places_level_2, 0) * IFNULL(ga.Grant_Level_2, 0)) +
                    (IFNULL(cl.places_level_3, 0) * IFNULL(ga.Grant_Level_3, 0)) +
                    (IFNULL(cl.places_plus_balance, 0) * IFNULL(ga.Grant_Plus_Balance, 0)) +
                    (IFNULL(cl.places_plus_bus, 0) * IFNULL(ga.Grant_Plus_Bus, 0)) +
                    (IFNULL(cl.places_plus_fix, 0) * IFNULL(ga.Grant_Plus_Fix, 0)) +
                    (IFNULL(cl.places_plus_learn, 0) * IFNULL(ga.Grant_Plus_Learn, 0)) +
                    (IFNULL(cl.places_plus_on_show, 0) * IFNULL(ga.Grant_Plus_On_Show, 0)) +
                    (IFNULL(cl.places_plus_parents, 0) * IFNULL(ga.Grant_Plus_Parents, 0)) +
                    (IFNULL(cl.places_plus_promotion, 0) * IFNULL(ga.Grant_Plus_Promotion, 0)) +
                    (IFNULL(cl.places_plus_recycled, 0) * IFNULL(ga.Grant_Plus_Recycled, 0)) +
                    (IFNULL(cl.places_plus_ride, 0) * IFNULL(ga.Grant_Plus_Ride, 0)) +
                    (IFNULL(cl.places_plus_transition, 0) * IFNULL(ga.Grant_Plus_Transition, 0)) +
                    (IFNULL(cl.places_plus_family, 0) * IFNULL(ga.Grant_Plus_Family, 0)) +
                    (IFNULL(cl.places_plus_adult, 0) * IFNULL(ga.Grant_Plus_Adult, 0)) +
                    IFNULL(c.Send_Claimable_Amount, 0) +
                    IFNULL(c.Inclusion_Claimable_Amount, 0)
                , 2) as overall_total"),
            ])
            ->where('c.Status_Raw', 1)
            ->where('c.Pref_Claim_Paid', 0);

        if ($startYear) {
            $query->where('g.Grant_Period_Start_Year', $startYear);
        }

        if (!empty($params['grant_source'])) {
            $query->where('g.Grant_Source', $params['grant_source']);
        }

        return $query->orderBy('g.Grant_Number')->orderBy('c.Claim_Number');
    }
}

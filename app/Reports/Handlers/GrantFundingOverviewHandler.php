<?php

namespace App\Reports\Handlers;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Validator;

class GrantFundingOverviewHandler extends AbstractStreamingReportHandler
{
    public function validate(array $parameters): array
    {
        return Validator::make($parameters, [
            'start_date'   => 'required|date_format:Y-m-d',
            'end_date'     => 'required|date_format:Y-m-d',
            'grant_source' => 'nullable|string',
        ])->validate();
    }

    /**
     * Aggregates and streams clean data packages back across the network boundary.
     */
    public function execute(array $params): array
    {
        ini_set('memory_limit', '1024M');
        ini_set('max_execution_time', '900');

        $query = $this->buildQuery($params);

        // Synchronous track fallback if no callback URL is provided
        if (empty($this->callbackUrl)) {
            return $query->get()->map(fn($row) => $this->mapRowToPayload($row))->toArray();
        }

        // Asynchronous Chunk Streaming Size matching the asynchronous pipeline expectation
        $chunkSize = 250;

        $query->chunk($chunkSize, function ($rows) {
            $chunkArray = $rows->map(fn($row) => $this->mapRowToPayload($row))->toArray();

            // Send the chunk back down the pipeline
            $this->transmitBatch($chunkArray, false);
        });

        // Trigger the empty handshake packet with the EOF True header flag to close out the storage stream
        $this->transmitBatch([], true);

        return ['status' => 'async_completed'];
    }

    /**
     * Map a raw database row to your structured flat array matching the layout sequence.
     */
    protected function mapRowToPayload($row): array
    {
        $level1Total       = $row->Places_Level_1 * $row->Grant_Level_1;
        $claimLevel1Total  = $row->claim_level_1_places * $row->Grant_Level_1;

        $level12Total      = $row->Places_Level_1_2 * $row->Grant_Level_1_2;
        $claimLevel12Total = $row->claim_level_1_2_places * $row->Grant_Level_1_2;

        $level2Total       = $row->Places_Level_2 * $row->Grant_Level_2;
        $claimLevel2Total  = $row->claim_level_2_places * $row->Grant_Level_2;

        $level3Total       = $row->Places_Level_3 * $row->Grant_Level_3;
        $claimLevel3Total  = $row->claim_level_3_places * $row->Grant_Level_3;

        $plusBalanceTotal        = $row->Places_Plus_Balance * $row->Grant_Plus_Balance;
        $claimPlusBalanceTotal   = $row->claim_plus_balance_places * $row->Grant_Plus_Balance;

        $plusBusTotal            = $row->Places_Plus_Bus * $row->Grant_Plus_Bus;
        $claimPlusBusTotal       = $row->claim_plus_bus_places * $row->Grant_Plus_Bus;

        $plusFixTotal            = $row->Places_Plus_Fix * $row->Grant_Plus_Fix;
        $claimPlusFixTotal       = $row->claim_plus_fix_places * $row->Grant_Plus_Fix;

        $plusLearnTotal          = $row->Places_Plus_Learn * $row->Grant_Plus_Learn;
        $claimPlusLearnTotal     = $row->claim_plus_learn_places * $row->Grant_Plus_Learn;

        $plusOnShowTotal         = $row->Places_Plus_On_Show * $row->Grant_Plus_On_Show;
        $claimPlusOnShowTotal    = $row->claim_plus_on_show_places * $row->Grant_Plus_On_Show;

        $plusParentsTotal        = $row->Places_Plus_Parents * $row->Grant_Plus_Parents;
        $claimPlusParentsTotal   = $row->claim_plus_parents_places * $row->Grant_Plus_Parents;

        $plusPromotionTotal      = $row->Places_Plus_Promotion * $row->Grant_Plus_Promotion;
        $claimPlusPromotionTotal = $row->claim_plus_promotion_places * $row->Grant_Plus_Promotion;

        $plusRecycledTotal       = $row->Places_Plus_Recycled * $row->Grant_Plus_Recycled;
        $claimPlusRecycledTotal  = $row->claim_plus_recycled_places * $row->Grant_Plus_Recycled;

        $plusRideTotal           = $row->Places_Plus_Ride * $row->Grant_Plus_Ride;
        $claimPlusRideTotal      = $row->claim_plus_ride_places * $row->Grant_Plus_Ride;

        $plusTransitionTotal     = $row->Places_Plus_Transition * $row->Grant_Plus_Transition;
        $claimPlusTransitionTotal= $row->claim_plus_transition_places * $row->Grant_Plus_Transition;

        $plusFamilyTotal         = $row->Places_Plus_Family * $row->Grant_Plus_Family;
        $claimPlusFamilyTotal    = $row->claim_plus_family_places * $row->Grant_Plus_Family;

        $plusAdultTotal          = $row->Places_Plus_Adult * $row->Grant_Plus_Adult;
        $claimPlusAdultTotal     = $row->claim_plus_adult_places * $row->Grant_Plus_Adult;

        $coreSubtotal       = $level1Total + $level12Total + $level2Total + $level3Total;
        $coreClaimsSubtotal = $claimLevel1Total + $claimLevel12Total + $claimLevel2Total + $claimLevel3Total;

        $plusSubtotal = $plusBalanceTotal + $plusBusTotal + $plusFixTotal + $plusLearnTotal + $plusOnShowTotal +
            $plusParentsTotal + $plusPromotionTotal + $plusRecycledTotal + $plusRideTotal +
            $plusTransitionTotal + $plusFamilyTotal + $plusAdultTotal;

        $plusClaimsSubtotal = $claimPlusBalanceTotal + $claimPlusBusTotal + $claimPlusFixTotal + $claimPlusLearnTotal +
            $claimPlusOnShowTotal + $claimPlusParentsTotal + $claimPlusPromotionTotal +
            $claimPlusRecycledTotal + $claimPlusRideTotal + $claimPlusTransitionTotal +
            $claimPlusFamilyTotal + $claimPlusAdultTotal;

        $overallTotal = $coreSubtotal + $plusSubtotal;
        $claimsTotal  = $coreClaimsSubtotal + $plusClaimsSubtotal;

        // Returns a flat indexed array to safely parse sequential string line blocks onto your webhook file storage disk
        return [
            $row->Grant_Number,
            $row->Grant_Source . ' (' . $row->Grant_Period_Start_Year . ')',
            $row->Recipient_Name,
            $row->Is_SGO == 1 ? 'SGO' : '',
            (float) $row->Grant_Level_1,
            (int) $row->Places_Level_1,
            (float) $level1Total,
            (int) $row->claim_level_1_places,
            (float) $claimLevel1Total,
            (float) $row->Grant_Level_1_2,
            (int) $row->Places_Level_1_2,
            (float) $level12Total,
            (int) $row->claim_level_1_2_places,
            (float) $claimLevel12Total,
            (float) $row->Grant_Level_2,
            (int) $row->Places_Level_2,
            (float) $level2Total,
            (int) $row->claim_level_2_places,
            (float) $claimLevel2Total,
            (float) $row->Grant_Level_3,
            (int) $row->Places_Level_3,
            (float) $level3Total,
            (int) $row->claim_level_3_places,
            (float) $claimLevel3Total,
            (float) $coreSubtotal,
            (float) $coreClaimsSubtotal,
            (float) $row->Grant_Plus_Balance,
            (int) $row->Places_Plus_Balance,
            (float) $plusBalanceTotal,
            (int) $row->claim_plus_balance_places,
            (float) $claimPlusBalanceTotal,
            (float) $row->Grant_Plus_Bus,
            (int) $row->Places_Plus_Bus,
            (float) $plusBusTotal,
            (int) $row->claim_plus_bus_places,
            (float) $claimPlusBusTotal,
            (float) $row->Grant_Plus_Fix,
            (int) $row->Places_Plus_Fix,
            (float) $plusFixTotal,
            (int) $row->claim_plus_fix_places,
            (float) $claimPlusFixTotal,
            (float) $row->Grant_Plus_Learn,
            (int) $row->Places_Plus_Learn,
            (float) $plusLearnTotal,
            (int) $row->claim_plus_learn_places,
            (float) $claimPlusLearnTotal,
            (float) $row->Grant_Plus_On_Show,
            (int) $row->Places_Plus_On_Show,
            (float) $plusOnShowTotal,
            (int) $row->claim_plus_on_show_places,
            (float) $claimPlusOnShowTotal,
            (float) $row->Grant_Plus_Parents,
            (int) $row->Places_Plus_Parents,
            (float) $plusParentsTotal,
            (int) $row->claim_plus_parents_places,
            (float) $claimPlusParentsTotal,
            (float) $row->Grant_Plus_Promotion,
            (int) $row->Places_Plus_Promotion,
            (float) $plusPromotionTotal,
            (int) $row->claim_plus_promotion_places,
            (float) $claimPlusPromotionTotal,
            (float) $row->Grant_Plus_Recycled,
            (int) $row->Places_Plus_Recycled,
            (float) $plusRecycledTotal,
            (int) $row->claim_plus_recycled_places,
            (float) $claimPlusRecycledTotal,
            (float) $row->Grant_Plus_Ride,
            (int) $row->Places_Plus_Ride,
            (float) $plusRideTotal,
            (int) $row->claim_plus_ride_places,
            (float) $claimPlusRideTotal,
            (float) $row->Grant_Plus_Transition,
            (int) $row->Places_Plus_Transition,
            (float) $plusTransitionTotal,
            (int) $row->claim_plus_transition_places,
            (float) $claimPlusTransitionTotal,
            (float) $row->Grant_Plus_Family,
            (int) $row->Places_Plus_Family,
            (float) $plusFamilyTotal,
            (int) $row->claim_plus_family_places,
            (float) $claimPlusFamilyTotal,
            (float) $row->Grant_Plus_Adult,
            (int) $row->Places_Plus_Adult,
            (float) $plusAdultTotal,
            (int) $row->claim_plus_adult_places,
            (float) $claimPlusAdultTotal,
            (float) $plusSubtotal,
            (float) $plusClaimsSubtotal,
            (float) $overallTotal,
            (float) $claimsTotal,
            (float) ($overallTotal - $claimsTotal),
            (float) $row->reallocation_release,
            (float) $row->reallocation_increase
        ];
    }

    /**
     * Build the query using true Pascal_Snake_Case attributes for your data tables.
     */
    protected function buildQuery(array $params)
    {
        $startDateTime = $params['start_date'] . ' 00:00:00';
        $endDateTime   = $params['end_date'] . ' 23:59:59';

        $claimsSubquery = DB::connection('mysql')->table('Fact_Grant_Claim_Logs as cl')
            ->join('Fact_Grant_Claims as c', 'cl.Claim_Key', '=', 'c.Claim_Key')
            ->select('c.Grant_Key')
            ->selectRaw("SUM(CASE WHEN c.Pref_Claim_Paid = 1 AND cl.Module_Key = 'level_1' THEN cl.Item_Count ELSE 0 END) as claim_level_1_places")
            ->selectRaw("SUM(CASE WHEN c.Pref_Claim_Paid = 1 AND cl.Module_Key = 'level_1_2' THEN cl.Item_Count ELSE 0 END) as claim_level_1_2_places")
            ->selectRaw("SUM(CASE WHEN c.Pref_Claim_Paid = 1 AND cl.Module_Key = 'level_2' THEN cl.Item_Count ELSE 0 END) as claim_level_2_places")
            ->selectRaw("SUM(CASE WHEN c.Pref_Claim_Paid = 1 AND cl.Module_Key = 'level_3' THEN cl.Item_Count ELSE 0 END) as claim_level_3_places")
            ->selectRaw("SUM(CASE WHEN c.Pref_Claim_Paid = 1 AND cl.Module_Key = 'plus_balance' THEN cl.Item_Count ELSE 0 END) as claim_plus_balance_places")
            ->selectRaw("SUM(CASE WHEN c.Pref_Claim_Paid = 1 AND cl.Module_Key = 'plus_bus' THEN cl.Item_Count ELSE 0 END) as claim_plus_bus_places")
            ->selectRaw("SUM(CASE WHEN c.Pref_Claim_Paid = 1 AND cl.Module_Key = 'plus_fix' THEN cl.Item_Count ELSE 0 END) as claim_plus_fix_places")
            ->selectRaw("SUM(CASE WHEN c.Pref_Claim_Paid = 1 AND cl.Module_Key = 'plus_learn' THEN cl.Item_Count ELSE 0 END) as claim_plus_learn_places")
            ->selectRaw("SUM(CASE WHEN c.Pref_Claim_Paid = 1 AND cl.Module_Key = 'plus_on_show' THEN cl.Item_Count ELSE 0 END) as claim_plus_on_show_places")
            ->selectRaw("SUM(CASE WHEN c.Pref_Claim_Paid = 1 AND cl.Module_Key = 'plus_parents' THEN cl.Item_Count ELSE 0 END) as claim_plus_parents_places")
            ->selectRaw("SUM(CASE WHEN c.Pref_Claim_Paid = 1 AND cl.Module_Key = 'plus_promotion' THEN cl.Item_Count ELSE 0 END) as claim_plus_promotion_places")
            ->selectRaw("SUM(CASE WHEN c.Pref_Claim_Paid = 1 AND cl.Module_Key = 'plus_recycled' THEN cl.Item_Count ELSE 0 END) as claim_plus_recycled_places")
            ->selectRaw("SUM(CASE WHEN c.Pref_Claim_Paid = 1 AND cl.Module_Key = 'plus_ride' THEN cl.Item_Count ELSE 0 END) as claim_plus_ride_places")
            ->selectRaw("SUM(CASE WHEN c.Pref_Claim_Paid = 1 AND cl.Module_Key = 'plus_transition' THEN cl.Item_Count ELSE 0 END) as claim_plus_transition_places")
            ->selectRaw("SUM(CASE WHEN c.Pref_Claim_Paid = 1 AND cl.Module_Key = 'plus_family' THEN cl.Item_Count ELSE 0 END) as claim_plus_family_places")
            ->selectRaw("SUM(CASE WHEN c.Pref_Claim_Paid = 1 AND cl.Module_Key = 'plus_adult' THEN cl.Item_Count ELSE 0 END) as claim_plus_adult_places")
            ->where('c.Status_Raw', 1)
            ->whereBetween('c.Date_Approved', [$startDateTime, $endDateTime])
            ->groupBy('c.Grant_Key');

        $reallocationsSubquery = DB::connection('mysql')->table('Fact_Grant_Reallocation_Logs as rl')
            ->join('Fact_Grant_Reallocations as r', 'rl.Reallocation_Key', '=', 'r.Reallocation_Key')
            ->select('r.Grant_Key')
            ->selectRaw("SUM(CASE WHEN r.Reallocation_Type = 'release' THEN rl.Amount ELSE 0.00 END) as reallocation_release")
            ->selectRaw("SUM(CASE WHEN r.Reallocation_Type = 'increase' THEN rl.Amount ELSE 0.00 END) as reallocation_increase")
            ->where('r.Status_Raw', 1)
            ->whereBetween('r.Date_Approved', [$startDateTime, $endDateTime])
            ->groupBy('r.Grant_Key');

        $query = DB::connection('mysql')->table('Dim_Grant as g')
            ->join('Dim_Grant_Recipient as gr', 'g.Grant_Recipient_Key', '=', 'gr.Recipient_Key')
            ->join('Fact_Grant_Financials as gf', 'g.Grant_Key', '=', 'gf.Grant_Key')
            ->leftJoinSub($claimsSubquery, 'c_sub', 'g.Grant_Key', '=', 'c_sub.Grant_Key')
            ->leftJoinSub($reallocationsSubquery, 'r_sub', 'g.Grant_Key', '=', 'r_sub.Grant_Key')
            ->select([
                'g.Grant_Number',
                'g.Grant_Source',
                'g.Grant_Period_Start_Year',
                'gr.Recipient_Name',
                'gr.Is_SGO',

                // Base Core Rate Rules mapping straight from Fact_Grant_Financials table attributes
                'gf.Places_Level_1', 'gf.Grant_Level_1',
                'gf.Places_Level_1_2', 'gf.Grant_Level_1_2',
                'gf.Places_Level_2', 'gf.Grant_Level_2',
                'gf.Places_Level_3', 'gf.Grant_Level_3',

                'gf.Places_Plus_Balance',    'gf.Grant_Plus_Balance',
                'gf.Places_Plus_Bus',        'gf.Grant_Plus_Bus',
                'gf.Places_Plus_Fix',        'gf.Grant_Plus_Fix',
                'gf.Places_Plus_Learn',      'gf.Grant_Plus_Learn',
                'gf.Places_Plus_On_Show',    'gf.Grant_Plus_On_Show',
                'gf.Places_Plus_Parents',    'gf.Grant_Plus_Parents',
                'gf.Places_Plus_Promotion',  'gf.Grant_Plus_Promotion',
                'gf.Places_Plus_Recycled',   'gf.Grant_Plus_Recycled',
                'gf.Places_Plus_Ride',       'gf.Grant_Plus_Ride',
                'gf.Places_Plus_Transition', 'gf.Grant_Plus_Transition',
                'gf.Places_Plus_Family',     'gf.Grant_Plus_Family',
                'gf.Places_Plus_Adult',      'gf.Grant_Plus_Adult',

                DB::raw("COALESCE(c_sub.claim_level_1_places, 0) as claim_level_1_places"),
                DB::raw("COALESCE(c_sub.claim_level_1_2_places, 0) as claim_level_1_2_places"),
                DB::raw("COALESCE(c_sub.claim_level_2_places, 0) as claim_level_2_places"),
                DB::raw("COALESCE(c_sub.claim_level_3_places, 0) as claim_level_3_places"),
                DB::raw("COALESCE(c_sub.claim_plus_balance_places, 0) as claim_plus_balance_places"),
                DB::raw("COALESCE(c_sub.claim_plus_bus_places, 0) as claim_plus_bus_places"),
                DB::raw("COALESCE(c_sub.claim_plus_fix_places, 0) as claim_plus_fix_places"),
                DB::raw("COALESCE(c_sub.claim_plus_learn_places, 0) as claim_plus_learn_places"),
                DB::raw("COALESCE(c_sub.claim_plus_on_show_places, 0) as claim_plus_on_show_places"),
                DB::raw("COALESCE(c_sub.claim_plus_parents_places, 0) as claim_plus_parents_places"),
                DB::raw("COALESCE(c_sub.claim_plus_promotion_places, 0) as claim_plus_promotion_places"),
                DB::raw("COALESCE(c_sub.claim_plus_recycled_places, 0) as claim_plus_recycled_places"),
                DB::raw("COALESCE(c_sub.claim_plus_ride_places, 0) as claim_plus_ride_places"),
                DB::raw("COALESCE(c_sub.claim_plus_transition_places, 0) as claim_plus_transition_places"),
                DB::raw("COALESCE(c_sub.claim_plus_family_places, 0) as claim_plus_family_places"),
                DB::raw("COALESCE(c_sub.claim_plus_adult_places, 0) as claim_plus_adult_places"),

                DB::raw("COALESCE(r_sub.reallocation_release, 0.00) as reallocation_release"),
                DB::raw("COALESCE(r_sub.reallocation_increase, 0.00) as reallocation_increase")
            ]);

        if (!empty($params['grant_source'])) {
            $query->where('g.Grant_Source', $params['grant_source']);
        }

        $startYear = date('Y', strtotime($params['start_date']));
        $query->where('g.Grant_Period_Start_Year', $startYear);

        return $query->orderBy('g.Grant_Number');
    }
}

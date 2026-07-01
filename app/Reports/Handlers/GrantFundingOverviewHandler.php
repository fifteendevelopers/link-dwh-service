<?php

namespace App\Reports\Handlers;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Validator;

class GrantFundingOverviewHandler extends AbstractStreamingReportHandler
{
    /**
     * Define and validate acceptable parameters.
     */
    public function validate(array $parameters): array
    {
        return Validator::make($parameters, [
            'start_date'   => 'required|date_format:Y-m-d',
            'end_date'     => 'required|date_format:Y-m-d',
            'grant_source' => 'nullable|string',
        ])->validate();
    }

    /**
     * Compile and stream data chunks back across the network boundary.
     */
    public function execute(array $params): array
    {
        ini_set('memory_limit', '1024M');
        ini_set('max_execution_time', '900');

        $query = $this->buildQuery($params);

        if (empty($this->callbackUrl)) {
            return $query->get()->map(fn($row) => (array)$row)->toArray();
        }

        $chunkSize = 500;

        $query->chunk($chunkSize, function ($rows) {
            $chunkArray = $rows->map(function ($row) {

                // 📊 1. Core Levels Allocation Calculations
                $level1Total       = $row->Places_Level_1 * $row->Grant_Level_1;
                $claimLevel1Total  = $row->claim_level_1_places * $row->Grant_Level_1;

                $level12Total      = $row->Places_Level_1_2 * $row->Grant_Level_1_2;
                $claimLevel12Total = $row->claim_level_1_2_places * $row->Grant_Level_1_2;

                $level2Total       = $row->Places_Level_2 * $row->Grant_Level_2;
                $claimLevel2Total  = $row->claim_level_2_places * $row->Grant_Level_2;

                $level3Total       = $row->Places_Level_3 * $row->Grant_Level_3;
                $claimLevel3Total  = $row->claim_level_3_places * $row->Grant_Level_3;

                // 📊 2. Plus Modules Allocation Calculations
                $plusBalanceTotal       = $row->Places_Plus_Balance * $row->Grant_Plus_Balance;
                $claimPlusBalanceTotal  = $row->claim_plus_balance_places * $row->Grant_Plus_Balance;

                $plusBusTotal           = $row->Places_Plus_Bus * $row->Grant_Plus_Bus;
                $claimPlusBusTotal      = $row->claim_plus_bus_places * $row->Grant_Plus_Bus;

                $plusFixTotal           = $row->Places_Plus_Fix * $row->Grant_Plus_Fix;
                $claimPlusFixTotal      = $row->claim_plus_fix_places * $row->Grant_Plus_Fix;

                $plusLearnTotal         = $row->Places_Plus_Learn * $row->Grant_Plus_Learn;
                $claimPlusLearnTotal    = $row->claim_plus_learn_places * $row->Grant_Plus_Learn;

                $plusOnShowTotal        = $row->Places_Plus_On_Show * $row->Grant_Plus_On_Show;
                $claimPlusOnShowTotal   = $row->claim_plus_on_show_places * $row->Grant_Plus_On_Show;

                $plusParentsTotal       = $row->Places_Plus_Parents * $row->Grant_Plus_Parents;
                $claimPlusParentsTotal  = $row->claim_plus_parents_places * $row->Grant_Plus_Parents;

                $plusPromotionTotal     = $row->Places_Plus_Promotion * $row->Grant_Plus_Promotion;
                $claimPlusPromotionTotal= $row->claim_plus_promotion_places * $row->Grant_Plus_Promotion;

                $plusRecycledTotal      = $row->Places_Plus_Recycled * $row->Grant_Plus_Recycled;
                $claimPlusRecycledTotal = $row->claim_plus_recycled_places * $row->Grant_Plus_Recycled;

                $plusRideTotal          = $row->Places_Plus_Ride * $row->Grant_Plus_Ride;
                $claimPlusRideTotal     = $row->claim_plus_ride_places * $row->Grant_Plus_Ride;

                $plusTransitionTotal    = $row->Places_Plus_Transition * $row->Grant_Plus_Transition;
                $claimPlusTransitionTotal= $row->claim_plus_transition_places * $row->Grant_Plus_Transition;

                $plusFamilyTotal        = $row->Places_Plus_Family * $row->Grant_Plus_Family;
                $claimPlusFamilyTotal   = $row->claim_plus_family_places * $row->Grant_Plus_Family;

                $plusAdultTotal         = $row->Places_Plus_Adult * $row->Grant_Plus_Adult;
                $claimPlusAdultTotal    = $row->claim_plus_adult_places * $row->Grant_Plus_Adult;

                // 📊 3. Subtotals & Rollups
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

                return [
                    'Grant Number'                    => $row->Grant_Number,
                    'Grant Source'                    => $row->Grant_Source . ' (' . $row->Grant_Period_Start_Year . ')',
                    'Grant Recipient'                 => $row->Recipient_Name,
                    'SGO Status'                      => $row->Is_SGO,
                    'Level 1 Funding per Place'       => number_format($row->Grant_Level_1, 2, '.', ''),
                    'Level 1 Places'                  => number_format($row->Places_Level_1, 0, '.', ''),
                    'Level 1 Total'                   => number_format($level1Total, 2, '.', ''),
                    'Level 1 Paid Claimed Places'     => number_format($row->claim_level_1_places, 0, '.', ''),
                    'Level 1 Paid Claimed Total'      => number_format($claimLevel1Total, 2, '.', ''),
                    'Level 1 & 2 Funding per Place'   => number_format($row->Grant_Level_1_2, 2, '.', ''),
                    'Level 1 & 2 Places'              => number_format($row->Places_Level_1_2, 0, '.', ''),
                    'Level 1 & 2 Total'               => number_format($level12Total, 2, '.', ''),
                    'Level 1 & 2 Paid Claimed Places' => number_format($row->claim_level_1_2_places, 0, '.', ''),
                    'Level 1 & 2 Paid Claimed Total'  => number_format($claimLevel12Total, 2, '.', ''),
                    'Level 2 Funding per Place'       => number_format($row->Grant_Level_2, 2, '.', ''),
                    'Level 2 Places'                  => number_format($row->Places_Level_2, 0, '.', ''),
                    'Level 2 Total'                   => number_format($level2Total, 2, '.', ''),
                    'Level 2 Paid Claimed Places'     => number_format($row->claim_level_2_places, 0, '.', ''),
                    'Level 2 Paid Claimed Total'      => number_format($claimLevel2Total, 2, '.', ''),
                    'Level 3 Funding per Place'       => number_format($row->Grant_Level_3, 2, '.', ''),
                    'Level 3 Places'                  => number_format($row->Places_Level_3, 0, '.', ''),
                    'Level 3 Total'                   => number_format($level3Total, 2, '.', ''),
                    'Level 3 Paid Claimed Places'     => number_format($row->claim_level_3_places, 0, '.', ''),
                    'Level 3 Paid Claimed Total'      => number_format($claimLevel3Total, 2, '.', ''),
                    'Core award subtotal'             => number_format($coreSubtotal, 2, '.', ''),
                    'Core paid claimed subtotal'      => number_format($coreClaimsSubtotal, 2, '.', ''),
                    'Plus Balance Funding per Place'  => number_format($row->Grant_Plus_Balance, 2, '.', ''),
                    'Plus Balance Places'             => number_format($row->Places_Plus_Balance, 0, '.', ''),
                    'Plus Balance Total'              => number_format($plusBalanceTotal, 2, '.', ''),
                    'Plus Balance Paid Claimed Places'=> number_format($row->claim_plus_balance_places, 0, '.', ''),
                    'Plus Balance Paid Claimed Total' => number_format($claimPlusBalanceTotal, 2, '.', ''),
                    'Plus Bus Funding per Place'      => number_format($row->Grant_Plus_Bus, 2, '.', ''),
                    'Plus Bus Places'                 => number_format($row->Places_Plus_Bus, 0, '.', ''),
                    'Plus Bus Total'                  => number_format($plusBusTotal, 2, '.', ''),
                    'Plus Bus Paid Claimed Places'    => number_format($row->claim_plus_bus_places, 0, '.', ''),
                    'Plus Bus Paid Claimed Total'     => number_format($claimPlusBusTotal, 2, '.', ''),
                    'Plus Fix Funding per Place'      => number_format($row->Grant_Plus_Fix, 2, '.', ''),
                    'Plus Fix Places'                 => number_format($row->Places_Plus_Fix, 0, '.', ''),
                    'Plus Fix Total'                  => number_format($plusFixTotal, 2, '.', ''),
                    'Plus Fix Paid Claimed Places'    => number_format($row->claim_plus_fix_places, 0, '.', ''),
                    'Plus Fix Paid Claimed Total'     => number_format($claimPlusFixTotal, 2, '.', ''),
                    'Plus Learn Funding per Place'    => number_format($row->Grant_Plus_Learn, 2, '.', ''),
                    'Plus Learn Places'               => number_format($row->Places_Plus_Learn, 0, '.', ''),
                    'Plus Learn Total'                => number_format($plusLearnTotal, 2, '.', ''),
                    'Plus Learn Paid Claimed Places'  => number_format($row->claim_plus_learn_places, 0, '.', ''),
                    'Plus Learn Paid Claimed Total'   => number_format($claimPlusLearnTotal, 2, '.', ''),
                    'Plus On Show Funding per Place'  => number_format($row->Grant_Plus_On_Show, 2, '.', ''),
                    'Plus On Show Places'             => number_format($row->Places_Plus_On_Show, 0, '.', ''),
                    'Plus On Show Total'              => number_format($plusOnShowTotal, 2, '.', ''),
                    'Plus On Show Paid Claimed Places'=> number_format($row->claim_plus_on_show_places, 0, '.', ''),
                    'Plus On Show Paid Claimed Total' => number_format($claimPlusOnShowTotal, 2, '.', ''),
                    'Plus Parents Funding per Place'  => number_format($row->Grant_Plus_Parents, 2, '.', ''),
                    'Plus Parents Places'             => number_format($row->Places_Plus_Parents, 0, '.', ''),
                    'Plus Parents Total'              => number_format($plusParentsTotal, 2, '.', ''),
                    'Plus Parents Paid Claimed Places'=> number_format($row->claim_plus_parents_places, 0, '.', ''),
                    'Plus Parents Paid Claimed Total' => number_format($claimPlusParentsTotal, 2, '.', ''),
                    'Plus Promotion Funding per Place'=> number_format($row->Grant_Plus_Promotion, 2, '.', ''),
                    'Plus Promotion Places'           => number_format($row->Places_Plus_Promotion, 0, '.', ''),
                    'Plus Promotion Total'            => number_format($plusPromotionTotal, 2, '.', ''),
                    'Plus Promotion Paid Claimed Places'=> number_format($row->claim_plus_promotion_places, 0, '.', ''),
                    'Plus Promotion Paid Claimed Total' => number_format($claimPlusPromotionTotal, 2, '.', ''),
                    'Plus Recycled Funding per Place' => number_format($row->Grant_Plus_Recycled, 2, '.', ''),
                    'Plus Recycled Places'            => number_format($row->Places_Plus_Recycled, 0, '.', ''),
                    'Plus Recycled Total'             => number_format($plusRecycledTotal, 2, '.', ''),
                    'Plus Recycled Paid Claimed Places'=> number_format($row->claim_plus_recycled_places, 0, '.', ''),
                    'Plus Recycled Paid Claimed Total' => number_format($claimPlusRecycledTotal, 2, '.', ''),
                    'Plus Ride Funding per Place'     => number_format($row->Grant_Plus_Ride, 2, '.', ''),
                    'Plus Ride Places'                => number_format($row->Places_Plus_Ride, 0, '.', ''),
                    'Plus Ride Total'                 => number_format($plusRideTotal, 2, '.', ''),
                    'Plus Ride Paid Claimed Places'   => number_format($row->claim_plus_ride_places, 0, '.', ''),
                    'Plus Ride Paid Claimed Total'    => number_format($claimPlusRideTotal, 2, '.', ''),
                    'Plus Transition Funding per Place'=> number_format($row->Grant_Plus_Transition, 2, '.', ''),
                    'Plus Transition Places'          => number_format($row->Places_Plus_Transition, 0, '.', ''),
                    'Plus Transition Total'           => number_format($plusTransitionTotal, 2, '.', ''),
                    'Plus Transition Paid Claimed Places'=> number_format($row->claim_plus_transition_places, 0, '.', ''),
                    'Plus Transition Paid Claimed Total' => number_format($claimPlusTransitionTotal, 2, '.', ''),
                    'Plus Family Funding per Place'   => number_format($row->Grant_Plus_Family, 2, '.', ''),
                    'Plus Family Places'              => number_format($row->Places_Plus_Family, 0, '.', ''),
                    'Plus Family Total'               => number_format($plusFamilyTotal, 2, '.', ''),
                    'Plus Family Paid Claimed Places' => number_format($row->claim_plus_family_places, 0, '.', ''),
                    'Plus Family Paid Claimed Total'  => number_format($claimPlusFamilyTotal, 2, '.', ''),
                    'Plus Adult Funding per Place'    => number_format($row->Grant_Plus_Adult, 2, '.', ''),
                    'Plus Adult Places'               => number_format($row->Places_Plus_Adult, 0, '.', ''),
                    'Plus Adult Total'                => number_format($plusAdultTotal, 2, '.', ''),
                    'Plus Adult Paid Claimed Places'  => number_format($row->claim_plus_adult_places, 0, '.', ''),
                    'Plus Adult Paid Claimed Total'   => number_format($claimPlusAdultTotal, 2, '.', ''),
                    'Plus award subtotal'             => number_format($plusSubtotal, 2, '.', ''),
                    'Plus paid claimed subtotal'      => number_format($plusClaimsSubtotal, 2, '.', ''),
                    'Grant award total'               => number_format($overallTotal, 2, '.', ''),
                    'Paid claims total'               => number_format($claimsTotal, 2, '.', ''),
                    'Funding remaining'               => number_format(($overallTotal - $claimsTotal), 2, '.', ''),
                    'Funding released'                => number_format($row->reallocation_release, 2, '.', ''),
                    'Additional funding awarded'      => number_format($row->reallocation_increase, 2, '.', '')
                ];
            })->toArray();

            $this->transmitBatch($chunkArray, false);
        });

        $this->transmitBatch([], true);

        return ['status' => 'async_completed'];
    }

    /**
     * Build the normalized query using your correct operational warehouse architecture.
     */
    protected function buildQuery(array $params)
    {
        // Subquery 1: Aggregate approved claims from Fact_Grant_Claim_Logs linked to Fact_Grant_Claims
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
            ->where('c.Status_Raw', 1) // 1 = Approved
            ->whereBetween('c.Date_Approved', [$params['start_date'], $params['end_date']])
            ->groupBy('c.Grant_Key');

        // Subquery 2: Aggregate approved variation values using Fact_Grant_Reallocation_Logs
        $reallocationsSubquery = DB::connection('mysql')->table('Fact_Grant_Reallocation_Logs as rl')
            ->join('Fact_Grant_Reallocations as r', 'rl.Reallocation_Key', '=', 'r.Reallocation_Key')
            ->select('r.Grant_Key')
            ->selectRaw("SUM(CASE WHEN r.Reallocation_Type = 'release' THEN rl.Amount ELSE 0.00 END) as reallocation_release")
            ->selectRaw("SUM(CASE WHEN r.Reallocation_Type = 'increase' THEN rl.Amount ELSE 0.00 END) as reallocation_increase")
            ->where('r.Status_Raw', 1) // 1 = Approved
            ->whereBetween('r.Date_Approved', [$params['start_date'], $params['end_date']])
            ->groupBy('r.Grant_Key');

        // Main Query compilation joining targets and financials allocations
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

                // Plus Module targets map dynamically via your active Fact_Grant_Financials schema keys
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

                // Map aggregated fallback elements cleanly
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
            ]); // 🎯 Removed where('Is_Current', 1) conditions completely

        if (!empty($params['grant_source'])) {
            $query->where('g.Grant_Source', $params['grant_source']);
        }

        $startYear = date('Y', strtotime($params['start_date']));
        $query->where('g.Grant_Period_Start_Year', $startYear);

        return $query->orderBy('g.Grant_Number');
    }
}

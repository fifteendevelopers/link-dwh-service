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
     * Aggregates and returns the clean collection payload instantly.
     */
    public function execute(array $params): array
    {
        ini_set('memory_limit', '1024M');
        ini_set('max_execution_time', '900');

        $rows = $this->buildQuery($params)->get();

        return $rows->map(function ($row) {
            $level1Total       = $row->Places_Level_1 * $row->Grant_Level_1;
            $claimLevel1Total  = $row->claim_level_1_places * $row->Grant_Level_1;

            $level12Total      = $row->Places_Level_1_2 * $row->Grant_Level_1_2;
            $claimLevel12Total = $row->claim_level_1_2_places * $row->Grant_Level_1_2;

            $level2Total       = $row->Places_Level_2 * $row->Grant_Level_2;
            $claimLevel2Total  = $row->claim_level_2_places * $row->Grant_Level_2;

            $level3Total       = $row->Places_Level_3 * $row->Grant_Level_3;
            $claimLevel3Total  = $row->claim_level_3_places * $row->Grant_Level_3;

            // 🎯 FIXED: Properties parsed using exact Pascal_Snake_Case matching the database engine
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

            return (object) [
                'grant_number'                    => $row->Grant_Number,
                'grant_source_display'            => $row->Grant_Source . ' (' . $row->Grant_Period_Start_Year . ')',
                'recipient_name'                  => $row->Recipient_Name,
                'sgo_status'                      => $row->Is_SGO,
                'grant_level_1'                   => (float) $row->Grant_Level_1,
                'places_level_1'                  => (int) $row->Places_Level_1,
                'level_1_total'                   => (float) $level1Total,
                'claim_level_1_places'            => (int) $row->claim_level_1_places,
                'claim_level_1_total'             => (float) $claimLevel1Total,
                'grant_level_1_2'                 => (float) $row->Grant_Level_1_2,
                'places_level_1_2'                => (int) $row->Places_Level_1_2,
                'level_1_2_total'                 => (float) $level12Total,
                'claim_level_1_2_places'          => (int) $row->claim_level_1_2_places,
                'claim_level_1_2_total'           => (float) $claimLevel12Total,
                'grant_level_2'                   => (float) $row->Grant_Level_2,
                'places_level_2'                  => (int) $row->Places_Level_2,
                'level_2_total'                   => (float) $level2Total,
                'claim_level_2_places'            => (int) $row->claim_level_2_places,
                'claim_level_2_total'             => (float) $claimLevel2Total,
                'grant_level_3'                   => (float) $row->Grant_Level_3,
                'places_level_3'                  => (int) $row->Places_Level_3,
                'level_3_total'                   => (float) $level3Total,
                'claim_level_3_places'            => (int) $row->claim_level_3_places,
                'claim_level_3_total'             => (float) $claimLevel3Total,
                'core_subtotal'                   => (float) $coreSubtotal,
                'core_claims_subtotal'            => (float) $coreClaimsSubtotal,
                'grant_plus_balance'              => (float) $row->Grant_Plus_Balance,
                'places_plus_balance'             => (int) $row->Places_Plus_Balance,
                'plus_balance_total'              => (float) $plusBalanceTotal,
                'claim_plus_balance_places'       => (int) $row->claim_plus_balance_places,
                'claim_plus_balance_total'        => (float) $claimPlusBalanceTotal,
                'grant_plus_bus'                  => (float) $row->Grant_Plus_Bus,
                'places_plus_bus'                 => (int) $row->Places_Plus_Bus,
                'plus_bus_total'                  => (float) $plusBusTotal,
                'claim_plus_bus_places'           => (int) $row->claim_plus_bus_places,
                'claim_plus_bus_total'            => (float) $claimPlusBusTotal,
                'grant_plus_fix'                  => (float) $row->Grant_Plus_Fix,
                'places_plus_fix'                 => (int) $row->Places_Plus_Fix,
                'plus_fix_total'                  => (float) $plusFixTotal,
                'claim_plus_fix_places'           => (int) $row->claim_plus_fix_places,
                'claim_plus_fix_total'            => (float) $claimPlusFixTotal,
                'grant_plus_learn'                => (float) $row->Grant_Plus_Learn,
                'places_plus_learn'               => (int) $row->Places_Plus_Learn,
                'plus_learn_total'                => (float) $plusLearnTotal,
                'claim_plus_learn_places'         => (int) $row->claim_plus_learn_places,
                'claim_plus_learn_total'          => (float) $claimPlusLearnTotal,
                'grant_plus_on_show'              => (float) $row->Grant_Plus_On_Show,
                'places_plus_on_show'             => (int) $row->Places_Plus_On_Show,
                'plus_on_show_total'              => (float) $plusOnShowTotal,
                'claim_plus_on_show_places'       => (int) $row->claim_plus_on_show_places,
                'claim_plus_on_show_total'        => (float) $claimPlusOnShowTotal,
                'grant_plus_parents'              => (float) $row->Grant_Plus_Parents,
                'places_plus_parents'             => (int) $row->Places_Plus_Parents,
                'plus_parents_total'              => (float) $plusParentsTotal,
                'claim_plus_parents_places'       => (int) $row->claim_plus_parents_places,
                'claim_plus_parents_total'        => (float) $claimPlusParentsTotal,
                'grant_plus_promotion'            => (float) $row->Grant_Plus_Promotion,
                'places_plus_promotion'           => (int) $row->Places_Plus_Promotion,
                'plus_promotion_total'            => (float) $plusPromotionTotal,
                'claim_plus_promotion_places'     => (int) $row->claim_plus_promotion_places,
                'claim_plus_promotion_total'      => (float) $claimPlusPromotionTotal,
                'grant_plus_recycled'             => (float) $row->Grant_Plus_Recycled,
                'places_plus_recycled'            => (int) $row->Places_Plus_Recycled,
                'plus_recycled_total'             => (float) $plusRecycledTotal,
                'claim_plus_recycled_places'      => (int) $row->claim_plus_recycled_places,
                'claim_plus_recycled_total'       => (float) $claimPlusRecycledTotal,
                'grant_plus_ride'                 => (float) $row->Grant_Plus_Ride,
                'places_plus_ride'                => (int) $row->Places_Plus_Ride,
                'plus_ride_total'                 => (float) $plusRideTotal,
                'claim_plus_ride_places'          => (int) $row->claim_plus_ride_places,
                'claim_plus_ride_total'           => (float) $claimPlusRideTotal,
                'grant_plus_transition'           => (float) $row->Grant_Plus_Transition,
                'places_plus_transition'          => (int) $row->Places_Plus_Transition,
                'plus_transition_total'           => (float) $plusTransitionTotal,
                'claim_plus_transition_places'    => (int) $row->claim_plus_transition_places,
                'claim_plus_transition_total'     => (float) $claimPlusTransitionTotal,
                'grant_plus_family'               => (float) $row->Grant_Plus_Family,
                'places_plus_family'              => (int) $row->Places_Plus_Family,
                'plus_family_total'               => (float) $plusFamilyTotal,
                'claim_plus_family_places'        => (int) $row->claim_plus_family_places,
                'claim_plus_family_total'         => (float) $claimPlusFamilyTotal,
                'grant_plus_adult'                => (float) $row->Grant_Plus_Adult,
                'places_plus_adult'               => (int) $row->Places_Plus_Adult,
                'plus_adult_total'                => (float) $plusAdultTotal,
                'claim_plus_adult_places'         => (int) $row->claim_plus_adult_places,
                'claim_plus_adult_total'          => (float) $claimPlusAdultTotal,
                'plus_subtotal'                   => (float) $plusSubtotal,
                'plus_claims_subtotal'            => (float) $plusClaimsSubtotal,
                'overall_total'                   => (float) $overallTotal,
                'claims_total'                    => (float) $claimsTotal,
                'funding_remaining'               => (float) ($overallTotal - $claimsTotal),
                'reallocation_release'            => (float) $row->reallocation_release,
                'reallocation_increase'           => (float) $row->reallocation_increase
            ];
        })->toArray();
    }

    /**
     * Build the query using true Pascal_Snake_Case attributes for your data tables.
     */
    protected function buildQuery(array $params)
    {
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
            ->whereBetween('c.Date_Approved', [$params['start_date'], $params['end_date']])
            ->groupBy('c.Grant_Key');

        $reallocationsSubquery = DB::connection('mysql')->table('Fact_Grant_Reallocation_Logs as rl')
            ->join('Fact_Grant_Reallocations as r', 'rl.Reallocation_Key', '=', 'r.Reallocation_Key')
            ->select('r.Grant_Key')
            ->selectRaw("SUM(CASE WHEN r.Reallocation_Type = 'release' THEN rl.Amount ELSE 0.00 END) as reallocation_release")
            ->selectRaw("SUM(CASE WHEN r.Reallocation_Type = 'increase' THEN rl.Amount ELSE 0.00 END) as reallocation_increase")
            ->where('r.Status_Raw', 1)
            ->whereBetween('r.Date_Approved', [$params['start_date'], $params['end_date']])
            ->groupBy('r.Grant_Key');

        return DB::connection('mysql')->table('Dim_Grant as g')
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

                // 🎯 FIXED: Both Places and Grant multipliers mapped using Pascal_Snake_Case layout keys
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

<?php

namespace Database\Seeders;

use Illuminate\Database\Seeder;
use Illuminate\Support\Facades\DB;

class ConsentAndDeliveryBackfillSeeder extends Seeder
{
    public function run(): void
    {
        // --- CONSENT DETAILS ---
        $this->command->info("Phase 1: Backfilling Consent Details & Preferences");

        $consentQuery = DB::connection('mysql')->table('Dim_Consent');
        $totalConsents = $consentQuery->count();

        $this->command->getOutput()->progressStart($totalConsents);

        $consentQuery->orderBy('Consent_Key')->chunk(1000, function ($consents) {
            $sourceIds = $consents->pluck('Source_Consent_Id')->toArray();

            $sourceData = DB::connection('mysql_src')->table('consents')
                ->whereIn('id', $sourceIds)
                ->get(['id', 'pref_join_bikeability_club', 'pref_further_research', 'pref_receive_news',
                    'year_group', 'send_details', 'medical_condition_details', 'has_medical_condition'])
                ->keyBy('id');

            foreach ($consents as $consent) {
                $src = $sourceData->get($consent->Source_Consent_Id);

                if ($src) {
                    DB::connection('mysql')->table('Dim_Consent')
                        ->where('Consent_Key', $consent->Consent_Key)
                        ->update([
                            'Pref_Join_Bikeclub'    => (bool)$src->pref_join_bikeability_club,
                            'Pref_Further_Research' => (bool)$src->pref_further_research,
                            'Pref_Receive_News'     => (bool)$src->pref_receive_news,
                            'Year_Group'            => $src->year_group,
                            'SEND_Details'          => $src->send_details,
                            'Medical_Details'       => $src->medical_condition_details,
                            'Has_Medical_Condition' => (bool)$src->has_medical_condition,
                        ]);
                }
                $this->command->getOutput()->progressAdvance();
            }
        });

        $this->command->getOutput()->progressFinish();
        $this->command->newLine();


        // --- DELIVERY HEADER CUTOFF DATES ---
        $this->command->info("Phase 2: Backfilling Delivery Header Cutoff Dates");

        $headerQuery = DB::connection('mysql')->table('Dim_Delivery_Header');
        $totalHeaders = $headerQuery->count();

        $this->command->getOutput()->progressStart($totalHeaders);

        $headerQuery->orderBy('Delivery_Key')->chunk(1000, function ($headers) {
            $sourceIds = $headers->pluck('Source_Delivery_Id')->toArray();

            $sourceData = DB::connection('mysql_src')->table('deliveries')
                ->whereIn('id', $sourceIds)
                ->pluck('consent_cutoff_date', 'id');

            foreach ($headers as $header) {
                if (isset($sourceData[$header->Source_Delivery_Id])) {
                    DB::connection('mysql')->table('Dim_Delivery_Header')
                        ->where('Delivery_Key', $header->Delivery_Key)
                        ->update([
                            'Consent_Cutoff_Date' => $sourceData[$header->Source_Delivery_Id]
                        ]);
                }
                $this->command->getOutput()->progressAdvance();
            }
        });

        $this->command->getOutput()->progressFinish();
        $this->command->info("All backfills completed successfully.");
    }
}

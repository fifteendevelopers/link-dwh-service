<?php

namespace Database\Seeders;

use Illuminate\Database\Seeder;
use Illuminate\Support\Facades\DB;

class ConsentFrequencyBackfillSeeder extends Seeder
{
    public function run(): void
    {
        $this->command->info("Starting backfill...");

        DB::connection('mysql')->table('Dim_Consent')
            ->select('Consent_Key', 'Source_Consent_Id')
            ->orderBy('Consent_Key')
            ->chunk(1000, function ($consents) {
                $sourceIds = $consents->pluck('Source_Consent_Id')->toArray();

                $allFreqs = DB::connection('mysql_src')->table('cycle_frequency')
                    ->whereIn('consent_id', $sourceIds)
                    ->get()
                    ->groupBy('consent_id');

                foreach ($consents as $consent) {
                    $f = $allFreqs->get($consent->Source_Consent_Id) ?? collect();

                    DB::connection('mysql')->table('Dim_Consent')
                        ->where('Consent_Key', $consent->Consent_Key)
                        ->update([
                            'Pre_Freq_To_School' => $f->where('label_lookup_id', 1)->first()->grading_lookup_id ?? null,
                            'Pre_Freq_Leisure' => $f->where('label_lookup_id', 2)->first()->grading_lookup_id ?? null,
                            'Pre_Freq_Exercise' => $f->where('label_lookup_id', 3)->first()->grading_lookup_id ?? null,
                            'Pre_Freq_Other' => $f->where('label_lookup_id', 4)->first()->grading_lookup_id ?? null,
                        ]);
                }
                $this->command->getOutput()->write('.'); // Progress indicator
            });
    }
}

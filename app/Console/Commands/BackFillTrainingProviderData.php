<?php

namespace App\Console\Commands\DataWarehouse;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;

class BackFillTrainingProviderData extends Command
{
    protected $signature = 'dwh:backfill-dim-training-providers';
    protected $description = 'Backfills newly added columns into existing Is_Current = 1 Dim_Training_Provider records in-place without triggering new SCD versions';

    public function handle(): int
    {
        $this->info('Starting in-place backfill for Dim_Training_Provider (Is_Current = 1)...');

        // Resolve connection references matching your ETL configuration
        $sourceConn = DB::connection('mysql_src');
        $dwhConn    = DB::connection('mysql');

        // 1. Fetch current active records from DWH
        $activeDwhProviders = $dwhConn->table('Dim_Training_Provider')
            ->where('Is_Current', 1)
            ->get(['Provider_Key', 'Source_Provider_Id']);

        if ($activeDwhProviders->isEmpty()) {
            $this->warn('No active Dim_Training_Provider records found to backfill.');
            return Command::SUCCESS;
        }

        $this->info("Found {$activeDwhProviders->count()} active providers in DWH.");

        // 2. Fetch corresponding source records in chunks
        $sourceIds = $activeDwhProviders->pluck('Source_Provider_Id')->toArray();
        $sourceRecords = $sourceConn->table('training_providers')
            ->whereIn('id', $sourceIds)
            ->get()
            ->keyBy('id');

        $bar = $this->output->createProgressBar($activeDwhProviders->count());
        $bar->start();

        $updatedCount = 0;

        foreach ($activeDwhProviders as $dwhRecord) {
            $source = $sourceRecords->get($dwhRecord->Source_Provider_Id);

            if (!$source) {
                $bar->advance();
                continue;
            }

            // In-place update of all newly added attributes
            $dwhConn->table('Dim_Training_Provider')
                ->where('Provider_Key', $dwhRecord->Provider_Key)
                ->update([
                    'Primary_Email'                           => $source->primary_email ?? null,
                    'Secondary_Email'                         => $source->secondary_email ?? null,
                    'Landline'                                => $source->landline ?? null,
                    'Status'                                  => (int) ($source->status ?? 0),
                    'Date_Inception'                          => $source->date_inception ?? null,
                    'Date_Renewal'                            => $source->date_renewal ?? null,
                    'Date_Deregistered'                       => $source->date_deregistered ?? null,
                    'Deregistration_Reason'                   => $source->deregistration_reason ?? null,
                    'Date_Eqa_Visit'                          => $source->date_eqa_visit ?? null,
                    'Date_Insurance_Expiry'                   => $source->date_insurance_expiry ?? null,
                    'Renewal_Blocked'                         => (bool) ($source->renewal_blocked ?? false),

                    // Course Level Preferences
                    'Pref_Level_1'                            => (bool) ($source->pref_level_1 ?? false),
                    'Pref_Level_2'                            => (bool) ($source->pref_level_2 ?? false),
                    'Pref_Level_3'                            => (bool) ($source->pref_level_3 ?? false),
                    'Pref_Plus_Balance'                       => (bool) ($source->pref_plus_balance ?? false),
                    'Pref_Plus_Bus'                           => (bool) ($source->pref_plus_bus ?? false),
                    'Pref_Plus_Fix'                           => (bool) ($source->pref_plus_fix ?? false),
                    'Pref_Plus_Learn'                         => (bool) ($source->pref_plus_learn ?? false),
                    'Pref_Plus_On_Show'                       => (bool) ($source->pref_plus_on_show ?? false),
                    'Pref_Plus_Parents'                       => (bool) ($source->pref_plus_parents ?? false),
                    'Pref_Plus_Promotion'                     => (bool) ($source->pref_plus_promotion ?? false),
                    'Pref_Plus_Recycled'                      => (bool) ($source->pref_plus_recycled ?? false),
                    'Pref_Plus_Ride'                          => (bool) ($source->pref_plus_ride ?? false),
                    'Pref_Plus_Transition'                    => (bool) ($source->pref_plus_transition ?? false),
                    'Pref_Plus_Family'                        => (bool) ($source->pref_plus_family ?? false),
                    'Pref_Plus_Adult'                         => (bool) ($source->pref_plus_adult ?? false),

                    // Operational & Areas
                    'Legacy_Areas_Of_Operation'               => $source->legacy_areas_of_operation ?? null,
                    'Delivery_Areas'                          => $source->delivery_areas ?? null,
                    'Account_Notes'                           => $source->account_notes ?? null,
                    'Terms_Url'                               => $source->terms_url ?? null,

                    // Booking Questions
                    'Booking_Question_1'                      => $source->booking_question_1 ?? null,
                    'Booking_Question_2'                      => $source->booking_question_2 ?? null,
                    'Booking_Question_3'                      => $source->booking_question_3 ?? null,
                    'Booking_Question_4'                      => $source->booking_question_4 ?? null,
                    'Booking_Question_5'                      => $source->booking_question_5 ?? null,

                    // Consent & Digi Configs
                    'Pref_Collect_Characteristics_In_Consent' => (bool) ($source->pref_collect_characteristics_in_consent ?? false),
                    'Pref_Allow_Non_Riders'                   => (bool) ($source->pref_allow_non_riders ?? false),
                    'Pref_Enable_Tp_Digi_Delivery_Access'     => (bool) ($source->pref_enable_tp_digi_delivery_access ?? false),
                    'Pref_Enable_Tp_Digi_Consent_Upload'      => (bool) ($source->pref_enable_tp_digi_consent_upload ?? false),
                    'Has_Fleet_Cycles'                        => (bool) ($source->has_fleet_cycles ?? false),
                    'Provide_A_Cycle_Question_Optional'       => (bool) ($source->provide_a_cycle_question_optional ?? false),
                    'External_System_Id'                      => !empty($source->external_system_id) ? (int) $source->external_system_id : null,
                    'Source_Deleted_At'                       => $source->deleted_at ?? null,
                ]);

            $updatedCount++;
            $bar->advance();
        }

        $bar->finish();
        $this->newLine(2);
        $this->info("Successfully backfilled {$updatedCount} active records in Dim_Training_Provider.");

        return Command::SUCCESS;
    }
}

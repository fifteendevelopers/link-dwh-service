<?php

namespace Database\Seeders;

use Illuminate\Database\Seeder;
use Illuminate\Support\Facades\DB;

class DeliveryHeaderCatchUpSeeder extends Seeder
{
    /**
     * Run the one-off catch up operation for Dim_Delivery_Header tracking properties.
     * Assumes Dim_External_Systems has already been completely synchronized.
     */
    public function run(): void
    {
        $this->command->info("Processing Delivery Header extended configurations backfill...");

        // 1. Establish query tracking on the DWH side
        $headerQuery = DB::connection('mysql')->table('Dim_Delivery_Header');
        $totalHeaders = $headerQuery->count();

        // 2. Cache the active lookup dictionary from your synchronized dimension in memory
        $systemsLookup = DB::connection('mysql')->table('Dim_External_Systems')
            ->pluck('External_System_Key', 'Source_External_System_Id')
            ->toArray();

        // 3. Initialize the visual terminal progress tracker bar
        $this->command->getOutput()->progressStart($totalHeaders);

        // 4. Chunk through the DWH data entries using its unique primary key
        $headerQuery->orderBy('Delivery_Key')->chunk(1000, function ($headers) use ($systemsLookup) {
            // Pluck out the source reference identifiers for the current block
            $sourceIds = $headers->pluck('Source_Delivery_Id')->toArray();

            // Fetch matching descriptive metadata logs from the live source application connection
            $sourceData = DB::connection('mysql_src')->table('deliveries')
                ->whereIn('id', $sourceIds)
                ->get([
                    'id', 'pref_alt_delivery_location', 'alt_delivery_location', 'notes',
                    'pref_link_managed_consent', 'instructor_general_notes', 'teacher_notes',
                    'school_contacts', 'venue', 'provider_additional_questions', 'comms_start_date',
                    'include_tp_terms_in_consent', 'consent_src_characteristics', 'date_completed',
                    'max_consents', 'waiting_list_enabled', 'external_system_id'
                ])
                ->keyBy('id');

            // 5. Update the DWH matrix record fields sequentially
            foreach ($headers as $header) {
                $src = $sourceData->get($header->Source_Delivery_Id);

                if ($src) {
                    // Pull the pre-mapped relational key from our in-memory cache dictionary
                    $systemKey = null;
                    if (!empty($src->external_system_id) && isset($systemsLookup[$src->external_system_id])) {
                        $systemKey = $systemsLookup[$src->external_system_id];
                    }

                    DB::connection('mysql')->table('Dim_Delivery_Header')
                        ->where('Delivery_Key', $header->Delivery_Key)
                        ->update([
                            'External_System_Key'           => $systemKey,
                            'Pref_Alt_Delivery_Location'    => $src->pref_alt_delivery_location,
                            'Alt_Delivery_Location'         => $src->alt_delivery_location,
                            'Notes'                         => $src->notes,
                            'Instructor_General_Notes'      => $src->instructor_general_notes,
                            'Teacher_Notes'                 => $src->teacher_notes,
                            'School_Contacts'               => $src->school_contacts,
                            'Venue'                         => $src->venue,
                            'Provider_Additional_Questions' => $src->provider_additional_questions,
                            'Comms_Start_Date'              => $src->comms_start_date,
                            'Date_Completed'                => $src->date_completed,
                            'Pref_Link_Managed_Consent'     => ($src->pref_link_managed_consent??false)?:0,
                            'Include_Tp_Terms_In_Consent'   => ($src->include_tp_terms_in_consent??false)?:0,
                            'Consent_Src_Characteristics'   => ($src->consent_src_characteristics??false)?:0,
                            'Max_Consents'                  => $src->max_consents,
                            'Waiting_List_Enabled'          => $src->waiting_list_enabled,
                        ]);
                }

                // Tick the progress bar for every processed record
                $this->command->getOutput()->progressAdvance();
            }
        });

        // 6. Clean up console session markers
        $this->command->getOutput()->progressFinish();
        $this->command->info("All historical system data features successfully updated.");
    }
}

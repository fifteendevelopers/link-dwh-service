<?php

namespace Database\Seeders;

use Illuminate\Database\Seeder;
use Illuminate\Support\Facades\DB;

class BackfillGrantReallocationsSeeder extends Seeder
{
    /**
     * Run the one-off historical columns backfill for Grant Reallocations.
     */
    public function run(): void
    {
        $this->command->info("Backfilling Grant Reallocation Details & Notes");

        // Establish query tracking on the DWH side
        $reallocationQuery = DB::connection('mysql')->table('Fact_Grant_Reallocations');
        $totalReallocations = $reallocationQuery->count();

        // Initialize the visual terminal progress tracker bar
        $this->command->getOutput()->progressStart($totalReallocations);

        // Chunk through the DWH data using its unique primary key
        $reallocationQuery->orderBy('Reallocation_Key')->chunk(1000, function ($reallocations) {
            // Pluck out the source reference identifiers for the current block
            $sourceIds = $reallocations->pluck('Source_Reallocation_Id')->toArray();

            // Fetch matching descriptive logs from the live source application connection in a single query
            $sourceData = DB::connection('mysql_src')->table('grant_reallocations')
                ->whereIn('id', $sourceIds)
                ->get([
                    'id',
                    'reallocation_type',
                    'reallocation_notes',
                    'reallocation_increase_reasons'
                ])
                ->keyBy('id');

            // Update the DWH matrix record fields sequentially
            foreach ($reallocations as $reallocation) {
                $src = $sourceData->get($reallocation->Source_Reallocation_Id);

                if ($src) {
                    DB::connection('mysql')->table('Fact_Grant_Reallocations')
                        ->where('Reallocation_Key', $reallocation->Reallocation_Key)
                        ->update([
                            'Reallocation_Type'             => $src->reallocation_type,
                            'Reallocation_Notes'            => $src->reallocation_notes,
                            'Reallocation_Increase_Reasons' => $src->reallocation_increase_reasons,
                        ]);
                }

                // Tick the progress bar for every processed record
                $this->command->getOutput()->progressAdvance();
            }
        });

        // Terminate the progress sequence cleanly
        $this->command->getOutput()->progressFinish();
        $this->command->newLine();
        $this->command->info("Grant Reallocation backfill completed successfully.");
    }
}

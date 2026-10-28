<?php


namespace App\Console\Commands\DataWarehouse\Support;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use App\Services\DataWarehouseSyncService;

class BackfillNonDigitisedDwhCourses extends Command
{
    protected $signature = 'dwh:backfill-non-digitised-courses
                            {--chunk=250 : Number of deliveries to process per database chunk}
                            {--delivery= : Target a single delivery ID for testing}';

    protected $description = 'Backfills multi-module non-digitised delivery_details JSON into Dim_Course and Fact_Course_Delivery';

    public function handle()
    {
        $src = DB::connection('mysql_src');
        $dwh = DB::connection('mysql');

        $sourceSystemKey = (int)($dwh->table('Dim_Source_System')
            ->where('System_Name', 'link')
            ->value('Source_System_Key') ?? 1);

        $specificDeliveryId = $this->option('delivery');
        $chunkSize = (int)$this->option('chunk');

        $this->info("Initialising Non-Digitised Course Backfill...");

        // Build base query
        $query = $src->table('deliveries')
            ->where('digitisation_booking', 0)
            ->whereNull('deleted_at')
            ->whereNotNull('delivery_details')
            ->where('delivery_details', '<>', '[]')
            ->select(['id', 'date_delivery_start', 'delivery_details']);

        if ($specificDeliveryId) {
            $query->where('id', $specificDeliveryId);
        }

        $totalCount = $query->count();

        if ($totalCount === 0) {
            $this->warn("No eligible non-digitised deliveries found.");
            return Command::SUCCESS;
        }

        $this->info("Found {$totalCount} delivery records to evaluate.");
        $bar = $this->output->createProgressBar($totalCount);
        $bar->start();

        // Reflection to access DataWarehouseSyncService base metrics layout
        $syncService = app(DataWarehouseSyncService::class);
        $reflection = new \ReflectionClass($syncService);
        $initMethod = $reflection->getMethod('initializeExtendedMetricArray');
        $initMethod->setAccessible(true);
        $baseMetrics = $initMethod->invoke($syncService);

        $coursesCreated = 0;
        $factsUpserted = 0;

        $query->orderBy('id')->chunk($chunkSize, function ($deliveries) use (
            $dwh, $sourceSystemKey, $baseMetrics, &$coursesCreated, &$factsUpserted, $bar
        ) {
            $deliveryIds = $deliveries->pluck('id')->toArray();

            // Bulk load Dim_Delivery_Header lookup map for this chunk
            $headers = $dwh->table('Dim_Delivery_Header')
                ->whereIn('Source_Delivery_Id', $deliveryIds)
                ->where('Source_System_Key', $sourceSystemKey)
                ->get()
                ->keyBy('Source_Delivery_Id');

            foreach ($deliveries as $deliv) {
                $bar->advance();

                $header = $headers->get($deliv->id);
                if (!$header) {
                    continue; // Skip if delivery header has not been synced yet
                }

                $hasRelationalCourses = $dwh->table('Dim_Course')
                    ->where('Delivery_Key', $header->Delivery_Key)
                    ->whereNotNull('Source_Course_Id')
                    ->where('Source_System_Key', $sourceSystemKey)
                    ->exists();

                if ($hasRelationalCourses) {
                    continue;
                }

                $details = json_decode($deliv->delivery_details, true);
                if (!is_array($details) || empty($details)) {
                    continue;
                }

                $factDate = $deliv->date_delivery_start;
                $dateKey = $factDate ? str_replace('-', '', substr($factDate, 0, 10)) : null;

                foreach ($details as $entry) {
                    $courseLevel = $entry['module']['id'] ?? null;
                    $yearGroup = $entry['delivery']['year_group'] ?? 'Unspecified';

                    if (empty($courseLevel)) {
                        continue;
                    }

                    // 1. Upsert Dim_Course (Composite on Delivery_Key + Course_Level + Year_Group)
                    $dwh->table('Dim_Course')->updateOrInsert(
                        [
                            'Delivery_Key' => $header->Delivery_Key,
                            'Course_Level' => $courseLevel,
                            'Year_Group' => $yearGroup,
                            'Source_Course_Id' => null,
                            'Source_System_Key' => $sourceSystemKey,
                        ],
                        [
                            'Status' => 1,
                            'Start_Date' => $factDate,
                            'Date_Complete' => $factDate,
                        ]
                    );

                    $courseKey = $dwh->table('Dim_Course')
                        ->where('Delivery_Key', $header->Delivery_Key)
                        ->where('Course_Level', $courseLevel)
                        ->where('Year_Group', $yearGroup)
                        ->whereNull('Source_Course_Id')
                        ->where('Source_System_Key', $sourceSystemKey)
                        ->value('Course_Key');

                    if (!$courseKey) {
                        continue;
                    }
                    $coursesCreated++;

                    // 2. Extract metrics for this module
                    $delivBlock = $entry['delivery'] ?? [];
                    $booked = $delivBlock['booked'] ?? [];
                    $isConfirmed = (isset($delivBlock['confirmed']) && (int)$delivBlock['confirmed'] === 1);

                    $moduleMetrics = array_merge($baseMetrics, [
                        'Count_Booked_Provisional' => (int)($booked['provisional'] ?? 0),
                        'Count_Booked_Confirmed' => $isConfirmed ? (int)($booked['total'] ?? 0) : 0,
                        'Count_Attended_Confirmed' => $isConfirmed ? (int)($delivBlock['attended']['total'] ?? 0) : 0,
                        'Count_Male' => (int)($booked['gender']['male'] ?? 0),
                        'Count_Female' => (int)($booked['gender']['female'] ?? 0),
                        'Count_Gender_Other' => (int)($booked['gender']['other'] ?? 0),
                        'Count_Gender_Not_Stated' => (int)($booked['gender']['na'] ?? 0),
                        'Count_Ethnicity_White_British' => (int)($booked['ethnicity']['white_british'] ?? 0),
                        'Count_Ethnicity_White_Other' => (int)($booked['ethnicity']['white'] ?? 0),
                        'Count_Ethnicity_Asian_Bangladeshi' => (int)($booked['ethnicity']['asian_bangladeshi'] ?? 0),
                        'Count_Ethnicity_Asian_Indian' => (int)($booked['ethnicity']['asian_indian'] ?? 0),
                        'Count_Ethnicity_Asian_Pakistani' => (int)($booked['ethnicity']['asian_pakistani'] ?? 0),
                        'Count_Ethnicity_Asian_Chinese' => (int)($booked['ethnicity']['asian_chinese'] ?? 0),
                        'Count_Ethnicity_Black_African' => (int)($booked['ethnicity']['black_african'] ?? 0),
                        'Count_Ethnicity_Black_Caribbean' => (int)($booked['ethnicity']['black_caribbean'] ?? 0),
                        'Count_Ethnicity_Not_Stated' => (int)($booked['ethnicity']['na'] ?? 0),
                        'Count_SEND' => (int)($booked['send'] ?? 0),
                        'Count_SEND_Not_Stated' => (int)($booked['send_na'] ?? 0),
                        'Count_Pupil_Premium' => (int)($booked['free_school_meals'] ?? 0),
                        'Count_Pupil_Premium_Not_Stated' => (int)($booked['free_school_meals_na'] ?? 0),
                        'Count_Bikes_Swapped' => (int)($delivBlock['bikes_swapped'] ?? 0),
                        'Count_Bikes_Recycled' => (int)($delivBlock['bikes_recycled'] ?? 0),
                    ]);

                    if ($courseLevel === 'plus_family') {
                        $moduleMetrics['Count_Adults'] = (int)($delivBlock['adults'] ?? 0);
                        $moduleMetrics['Count_Teens'] = (int)($delivBlock['teens'] ?? 0);
                        $moduleMetrics['Count_Children'] = (int)($delivBlock['children'] ?? 0);
                    }

                    // 3. Upsert into Fact_Course_Delivery
                    $dwh->table('Fact_Course_Delivery')->updateOrInsert(
                        [
                            'Delivery_Key' => $header->Delivery_Key,
                            'Course_Key' => $courseKey,
                        ],
                        array_merge($moduleMetrics, [
                            'Riders_Enrolled_Count' => 0,
                            'Riders_Completed_Count' => 0,
                            'Date_Key' => $dateKey,
                            'School_Key' => $header->School_Key,
                            'Organisation_Key' => $header->Organisation_Key,
                            'Provider_Key' => $header->Training_Provider_Key,
                            'Grant_Key' => $header->Grant_Key,
                        ])
                    );
                    $factsUpserted++;
                }
            }
        });

        $bar->finish();
        $this->newLine(2);
        $this->info("Backfill complete! Processed {$coursesCreated} Dim_Course records and {$factsUpserted} Fact rows.");

        return Command::SUCCESS;
    }
}

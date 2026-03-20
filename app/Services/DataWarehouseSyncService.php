<?php

namespace App\Services;

use App\Models\Rider;
use Illuminate\Support\Facades\DB;
use Carbon\Carbon;

class DataWarehouseSyncService
{
    protected $dwh;
    protected $source;

    public function __construct()
    {
        $this->dwh = DB::connection('mysql_dwh');
        $this->source = DB::connection('mysql');
    }

    public function syncTrainingProviders($command = null)
    {
        $sourceSystemKey = $this->getSourceSystemKey();
        $sourceProviders = $this->source->table('training_providers')->get();

        $bar = $command ? $command->getOutput()->createProgressBar(count($sourceProviders)) : null;
        if ($bar) $bar->start();

        $today = Carbon::now()->toDateString();
        $newRecords = 0;
        $updatedVersions = 0;

        foreach ($sourceProviders as $provider) {
            // 1. Prepare the incoming data
            $incomingData = [
                'Provider_Name'   => $provider->provider_name,
                'Provider_Number' => $provider->provider_number,
                'Is_Active'       => is_null($provider->deleted_at) ? 'Y' : 'N'
            ];

            // 2. Fetch the current active version in DWH
            $current = $this->dwh->table('Dim_Training_Provider')
                ->where('Source_Provider_Id', $provider->id)
                ->where('Source_System_Key', $sourceSystemKey)
                ->where('Is_Current', 1)
                ->first();

            if (!$current) {
                // New record: Create first version
                $this->createNewVersion('Dim_Training_Provider', 'Source_Provider_Id', $provider->id, $incomingData, $sourceSystemKey, $today);
                $newRecords++;
            } elseif ($this->hasChanged($current, $incomingData)) {
                // Change detected: Close current version and create new one
                $this->dwh->table('Dim_Training_Provider')
                    ->where('Provider_Key', $current->Provider_Key)
                    ->update([
                        'Is_Current' => 0,
                        'Valid_To_Date' => $today
                    ]);

                $this->createNewVersion('Dim_Training_Provider', 'Source_Provider_Id', $provider->id, $incomingData, $sourceSystemKey, $today);
                $updatedVersions++;
            }

            if ($bar) $bar->advance();
        }

        if ($bar) { $bar->finish(); $command->newLine(); }

        return "Successfully synced Training Providers. (New: $newRecords, Updated: $updatedVersions)";
    }

    public function syncSchools($command = null)
    {
        $sourceSystemKey = $this->getSourceSystemKey();

        // 1. Get total count for the progress bar
        $total = $this->source->table('vendor_edubase')->count();
        $bar = $command ? $command->getOutput()->createProgressBar($total) : null;
        if ($bar) $bar->start();

        $syncCount = 0;

        // 2. Chunking to avoid the memory exhaustion error
        $this->source->table('vendor_edubase')
            ->select('id', 'urn', 'establishment_name', 'la_code', 'la_name')
            ->orderBy('id')
            ->chunk(1000, function ($schools) use (&$syncCount, $sourceSystemKey, $bar) {
                foreach ($schools as $school) {

                    // Using updateOrInsert (SCD Type 1)
                    // This will update the existing record if the ID + System Key matches
                    $this->dwh->table('Dim_School')->updateOrInsert(
                        [
                            'Source_School_Id'  => $school->id,
                            'Source_System_Key' => $sourceSystemKey
                        ],
                        [
                            'School_Urn'  => $school->urn,
                            'School_Name' => $school->establishment_name,
                            'La_Code'     => $school->la_code,
                            'La_Name'     => $school->la_name
                        ]
                    );

                    if ($bar) $bar->advance();
                    $syncCount++;
                }
            });

        if ($bar) { $bar->finish(); $command->newLine(); }

        return "Successfully synced {$syncCount} Schools (Overwritten/Updated).";
    }
    public function syncOrganisations($command = null)
    {
        $sourceSystemKey = $this->getSourceSystemKey();

        // Fetch all organisations
        $sourceOrgs = $this->source->table('organisations')
            ->select('id', 'training_provider_id','name')
            ->get();

        $bar = $command ? $command->getOutput()->createProgressBar(count($sourceOrgs)) : null;
        if ($bar) $bar->start();

        $syncCount = 0;

        foreach ($sourceOrgs as $org) {
            $providerKey = $this->dwh->table('Dim_Training_Provider')
                ->where('Source_Provider_Id', $org->training_provider_id)
                ->where('Source_System_Key', $sourceSystemKey)
                ->value('Provider_Key');

            $this->dwh->table('Dim_Organisation')->updateOrInsert(
                [
                    'Source_Organisation_Id' => $org->id,
                    'Source_System_Key'      => $sourceSystemKey
                ],
                [
                    'Provider_Key'       => $providerKey,
                    'Organisation_Name'  => $org->name,
                ]
            );

            if ($bar) $bar->advance();
            $syncCount++;
        }

        if ($bar) { $bar->finish(); $command->newLine(); }
        return "Successfully synced {$syncCount} Organisations.";
    }

    public function syncGrantRecipients($command = null)
    {
        $sourceSystemKey = $this->getSourceSystemKey();
        $sourceRecipients = $this->source->table('grant_recipients')->get();

        $bar = $command ? $command->getOutput()->createProgressBar(count($sourceRecipients)) : null;
        if ($bar) $bar->start();

        $today = Carbon::now()->toDateString();
        $newRecords = 0;
        $updatedVersions = 0;

        foreach ($sourceRecipients as $recipient) {
            $incomingData = [
                'Recipient_Name'   => $recipient->recipient_name,
                'Recipient_Number' => $recipient->recipient_number,
                'LA_Id'            => $recipient->local_authority_id,
                'Is_Active'        => is_null($recipient->deleted_at) ? 'Y' : 'N'
            ];

            $current = $this->dwh->table('Dim_Grant_Recipient')
                ->where('Source_Recipient_Id', $recipient->id)
                ->where('Source_System_Key', $sourceSystemKey)
                ->where('Is_Current', 1)
                ->first();

            if (!$current) {
                $this->createNewVersion('Dim_Grant_Recipient', 'Source_Recipient_Id', $recipient->id, $incomingData, $sourceSystemKey, $today);
                $newRecords++;
            } elseif ($this->hasChanged($current, $incomingData)) {
                $this->dwh->table('Dim_Grant_Recipient')
                    ->where('Recipient_Key', $current->Recipient_Key)
                    ->update([
                        'Is_Current' => 0,
                        'Valid_To_Date' => $today
                    ]);

                $this->createNewVersion('Dim_Grant_Recipient', 'Source_Recipient_Id', $recipient->id, $incomingData, $sourceSystemKey, $today);
                $updatedVersions++;
            }

            if ($bar) $bar->advance();
        }

        if ($bar) { $bar->finish(); $command->newLine(); }

        return "Successfully synced Grant Recipients. (New: $newRecords, Updated: $updatedVersions)";
    }

    public function syncGrants($command = null)
    {
        $sourceSystemKey = $this->getSourceSystemKey();

        // Fetch the source grants
        $sourceGrants = $this->source->table('grants')->get();

        $bar = $command ? $command->getOutput()->createProgressBar(count($sourceGrants)) : null;
        if ($bar) $bar->start();

        $syncCount = 0;
        $errorCount = 0;

        foreach ($sourceGrants as $grant) {
            // Lookup the Recipient_Key - this is the DWH "Key" value
            // ensuring we find the current active version of this recipient
            $recipientKey = $this->dwh->table('Dim_Grant_Recipient')
                ->where('Source_Recipient_Id', $grant->grant_recipient_id)
                ->where('Source_System_Key', $sourceSystemKey)
                ->where('Is_Current', 1)
                ->value('Recipient_Key');

            if (!$recipientKey) {
                // Log an error or handle "Orphaned" grants
                if ($command) $command->error("Recipient ID {$grant->grant_recipient_id} not found for Grant ID {$grant->id}");
                $errorCount++;
                continue;
            }

            // 3. Perform SCD Type 1 Sync for the Grant itself
            // (Grants are typically unique by their Source ID)
            $this->dwh->table('Dim_Grant')->updateOrInsert(
                [
                    'Source_Grant_Id'   => $grant->id,
                    'Source_System_Key' => $sourceSystemKey
                ],
                [
                    'Grant_Recipient_Key'     => $recipientKey,
                    'Grant_Number'            => $grant->grant_number,
                    'Grant_Label'             => $grant->grant_label,
                    'Grant_Period_Start_Year' => $grant->grant_period_start_year,
                    'Grant_Source'            => $grant->grant_source,
                ]
            );

            if ($bar) $bar->advance();
            $syncCount++;
        }

        if ($bar) { $bar->finish(); $command->newLine(); }

        return "Successfully synced {$syncCount} Grants. ({$errorCount} orphans skipped).";
    }

    public function syncDeliveryHeaders($command = null)
    {
        $sourceSystemKey = $this->getSourceSystemKey();

        $watermark = $this->dwh->table('Sync_Log')
            ->where('Table_Name', 'Dim_Delivery_Header')
            ->value('Last_Synced_At') ?? '1900-01-01 00:00:00';
        $watermark = Carbon::parse($watermark)->subSeconds(5)->toDateTimeString();

        $highestTimestampSeen = $watermark;

        $query = $this->source->table('deliveries')
            ->where('updated_at', '>', $watermark);

        $total = $query->count();

        if ($total === 0) {
            return "No new delivery changes detected.";
        }

        $bar = $command ? $command->getOutput()->createProgressBar($total) : null;

        if ($bar) $bar->start();

        $this->source->table('deliveries')
            ->select('id', 'grant_id', 'school_urn', 'training_provider_id', 'status', 'date_delivery_start', 'date_delivery_end','digitisation_booking','organisation_id', 'updated_at')
            ->where('updated_at', '>', $watermark)
            ->orderBy('id')
            ->chunk(1000, function ($deliveries) use ($sourceSystemKey, $bar, $highestTimestampSeen) {
                foreach ($deliveries as $delivery) {

                    // Get Grant Key - if the delivery has a grant id
                    $grantKey = $delivery->grant_id ? $this->dwh->table('Dim_Grant')
                        ->where('Source_Grant_Id', $delivery->grant_id)
                        ->where('Source_System_Key', $sourceSystemKey)
                        ->value('Grant_Key') : null;

                    // Get School Key - if the delivery has a school_urn
                    $schoolKey = $delivery->school_urn ? $this->dwh->table('Dim_School')
                        ->where('School_Urn', $delivery->school_urn)
                        ->where('Source_System_Key', $sourceSystemKey)
                        ->value('School_Key') : null;

                    // Get Organisation Key - if the delivery has a organisation_id
                    $organisationKey = $delivery->organisation_id ? $this->dwh->table('Dim_Organisation')
                        ->where('Source_Organisation_Id', $delivery->organisation_id)
                        ->where('Source_System_Key', $sourceSystemKey)
                        ->value('Organisation_Key') : null;

                    // Get Training Provider key
                    $providerKey = $this->dwh->table('Dim_Training_Provider')
                        ->where('Source_Provider_Id', $delivery->training_provider_id)
                        ->where('Source_System_Key', $sourceSystemKey)
                        ->where('Is_Current', 1)
                        ->value('Provider_Key');

                    // Upsert the Delivery Header data
                    $this->dwh->table('Dim_Delivery_Header')->updateOrInsert(
                        [
                            'Source_Delivery_Id' => $delivery->id,
                            'Source_System_Key'  => $sourceSystemKey
                        ],
                        [
                            'Grant_Key'       => $grantKey,
                            'School_Key'      => $schoolKey,
                            'Organisation_Key'=> $organisationKey,
                            'Training_Provider_Key'    => $providerKey,
                            'Delivery_Status' => $this->mapDeliveryStatus($delivery->status),
                            'Date_Delivery_Start' => $delivery->date_delivery_start,
                            'Date_Delivery_End' => $delivery->date_delivery_end,
                            'Digitisation_Booking' => $delivery->digitisation_booking,
                        ]
                    );

                    if ($delivery->updated_at > $highestTimestampSeen) {
                        $highestTimestampSeen = $delivery->updated_at;
                    }

                    if ($bar) $bar->advance();
                }
            });

        $this->dwh->table('Sync_Log')->updateOrInsert(
            ['Table_Name' => 'Dim_Delivery_Header'],
            [
                'Last_Synced_At' => $highestTimestampSeen,
                'Records_Processed' => $total
            ]
        );

        if ($bar) { $bar->finish(); $command->newLine(); }
        return "Delivery Headers synced with relational keys.";
    }

    public function syncCourseDimensions($command = null)
    {
        $sourceSystemKey = $this->getSourceSystemKey();

        $watermark = $this->dwh->table('Sync_Log')
            ->where('Table_Name', 'Dim_Course')
            ->value('Last_Synced_At') ?? '1900-01-01 00:00:00';
        $watermark = Carbon::parse($watermark)->subSeconds(5)->toDateTimeString();

        $query = $this->source->table('courses')
            ->where('updated_at', '>', $watermark);

        $total = $query->count();

        if ($total === 0) {
            return "No new delivery changes detected.";
        }

        $highestTimestampSeen = $watermark;

        // Fetch courses with their delivery relationship
        $sourceCourses = $this->source->table('courses')
            ->select('id', 'course_id', 'parent_course_id', 'delivery_id', 'status','start_date','date_complete','year_group')
            ->where('updated_at', '>', $watermark)
            ->get();

        $bar = $command ? $command->getOutput()->createProgressBar($total) : null;
        if ($bar) $bar->start();

        // Upsert Basic Info & Delivery Link
        foreach ($sourceCourses as $course) {
            // Find the Delivery_Key from the Header dimension
            $deliveryKey = $this->dwh->table('Dim_Delivery_Header')
                ->where('Source_Delivery_Id', $course->delivery_id)
                ->where('Source_System_Key', $sourceSystemKey)
                ->value('Delivery_Key');

            $this->dwh->table('Dim_Course')->updateOrInsert(
                [
                    'Source_Course_Id'  => $course->id,
                    'Source_System_Key' => $sourceSystemKey
                ],
                [
                    'Delivery_Key' => $deliveryKey,
                    'Course_Level' => $course->course_id,
                    'Status'  => $course->status,
                    'Start_Date'  => $course->start_date,
                    'Date_Complete'  => $course->date_complete,
                    'Year_Group' => $course->year_group,
                ]
            );

            if ($course->updated_at > $highestTimestampSeen) {
                $highestTimestampSeen = $course->updated_at;
            }

            if ($bar) $bar->advance();
        }

        // Connect child courses to parent
        if ($bar) {
            $bar->finish();
            $command->newLine();
            $command->info("Finalizing Course parent-child relationships...");
            $bar->start();
        }

        foreach ($sourceCourses as $course) {
            if ($course->parent_course_id) {
                $parentKey = $this->dwh->table('Dim_Course')
                    ->where('Source_Course_Id', $course->parent_course_id)
                    ->where('Source_System_Key', $sourceSystemKey)
                    ->value('Course_Key');

                if ($parentKey) {
                    $this->dwh->table('Dim_Course')
                        ->where('Source_Course_Id', $course->id)
                        ->update(['Parent_Course_Key' => $parentKey]);
                }
            }
            if ($bar) $bar->advance();
        }

        $this->dwh->table('Sync_Log')->updateOrInsert(
            ['Table_Name' => 'Dim_Course'],
            [
                'Last_Synced_At' => $highestTimestampSeen,
                'Records_Processed' => $total
            ]
        );

        if ($bar) { $bar->finish(); $command->newLine(); }
        return "Course Dimension synced with Delivery links and hierarchy.";
    }

    public function syncRiders($command = null)
    {
        $sourceSystemKey = $this->getSourceSystemKey();

        // Get the update Watermark for riders
        $watermark = $this->dwh->table('Sync_Log')
            ->where('Table_Name', 'Dim_Rider')
            ->value('Last_Synced_At') ?? '1900-01-01 00:00:00';

        // Safety buffer for overlapping transactions
        $watermark = Carbon::parse($watermark)->subSeconds(5)->toDateTimeString();
        $highestTimestampSeen = $watermark;

        // Get Rider records which have been updated after the watermark
        $query = Rider::select([
            'id',
            'school_urn',
            'ethnicity',
            'gender',
            'free_school_meals',
            'send_code',
            'updated_at'
        ])->where('updated_at', '>', $watermark)
            ->orderBy('updated_at', 'asc');

        $total = $query->count();
        if ($total === 0) return "No new rider changes detected.";

        $bar = $command ? $command->getOutput()->createProgressBar($total) : null;
        if ($bar) $bar->start();

        // Process in Chunks
        $query->chunk(1000, function ($riders) use ($sourceSystemKey, $bar, &$highestTimestampSeen) {
            foreach ($riders as $rider) {

                // Resolve School Key
                $schoolKey = $this->dwh->table('Dim_School')
                    ->where('School_Urn', $rider->school_urn)
                    ->value('School_Key');

                // Upsert the Main Rider Record (SCD Type 1)
                $this->dwh->table('Dim_Rider')->updateOrInsert(
                    ['Source_Rider_Id' => $rider->id, 'Source_System_Key' => $sourceSystemKey],
                    [
                        'School_Key'     => $schoolKey,
                        'Ethnicity'      => $rider->ethnicity,
                        'Gender'         => $rider->gender,
                        'Pupil_Premium'  => ($rider->free_school_meals === 'Yes') ? 1 : 0,
                        'Has_SEND'       => !empty($rider->send_code) ? 1 : 0,
                    ]
                );

                // Get the Rider_Key
                $riderKey = $this->dwh->table('Dim_Rider')
                    ->where('Source_Rider_Id', $rider->id)
                    ->value('Rider_Key');

                // Handle SEND Codes - these are created as required
                $sendCodesRaw = is_array($rider->send_code) ? $rider->send_code : [];
                $sendCodes = array_unique(array_filter($sendCodesRaw));

                // Clear existing map for this rider (re-map fresh for updates)
                $this->dwh->table('Map_Rider_Send')->where('Rider_Key', $riderKey)->delete();

                foreach ($sendCodes as $code) {
                    if (empty($code)) continue;

                    // Ensure code exists in Dimension table
                    $this->dwh->table('Dim_Send_Code')->updateOrInsert(
                        ['Send_Code' => $code],
                        ['Send_Code' => $code]
                    );

                    $sendCodeKey = $this->dwh->table('Dim_Send_Code')
                        ->where('Send_Code', $code)
                        ->value('Send_Code_Key');

                    // Insert Mapping
                    $this->dwh->table('Map_Rider_Send')->insert([
                        'Rider_Key'     => $riderKey,
                        'Send_Code_Key' => $sendCodeKey
                    ]);
                }

                // Track highest timestamp for the watermark update
                if ($rider->updated_at->gt($highestTimestampSeen)) {
                    $highestTimestampSeen = $rider->updated_at->toDateTimeString();
                }

                if ($bar) $bar->advance();
            }
        });

        // Update the Watermark
        $this->dwh->table('Sync_Log')->updateOrInsert(
            ['Table_Name' => 'Dim_Rider'],
            [
                'Last_Synced_At' => $highestTimestampSeen,
                'Records_Processed' => $total
            ]
        );

        if ($bar) { $bar->finish(); $command->newLine(); }
        return "Synced {$total} Riders (Incremental).";
    }

    /**
     * Optional Helper to turn status IDs into readable strings for the DWH
     */
    private function mapDeliveryStatus($statusId) {
        return match($statusId) {
            1 => 'Draft',
            2 => 'Confirmed',
            3 => 'Completed',
            4 => 'Cancelled',
            default => 'Unknown'
        };
    }

    /**
     * Helper to insert a new version of a record
     */
    protected function createNewVersion($table, $idCol, $idVal, $data, $sysKey, $startDate)
    {
        $data[$idCol] = $idVal;
        $data['Source_System_Key'] = $sysKey;
        $data['Valid_From_Date'] = $startDate;
        $data['Valid_To_Date'] = '9999-12-31';
        $data['Is_Current'] = 1;

        $this->dwh->table($table)->insert($data);
    }

    /**
     * Comparison helper to detect changes
     */
    protected function hasChanged($existing, $new)
    {
        foreach ($new as $key => $value) {
            // Loose comparison to handle numeric strings vs integers
            if ($existing->$key != $value) return true;
        }
        return false;
    }

    /**
     * Helper to get the Source System Key for link
     * Link will be the first and possibly only source system but this allows for
     * any future that may involve other source systems
     */
    protected function getSourceSystemKey()
    {
        return $this->dwh->table('Dim_Source_System')->where('System_Name', 'link')->value('Source_System_Key');
    }
}

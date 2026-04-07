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
        $this->dwh = DB::connection('mysql');
        $this->source = DB::connection('mysql_src');
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
            ->chunk(1000, function ($deliveries) use ($sourceSystemKey, $bar, &$highestTimestampSeen) {
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
    public function syncCourses($command = null)
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
            ->select('id', 'course_id', 'parent_course_id', 'delivery_id', 'status','start_date','date_complete','year_group', 'updated_at')
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
    public function syncConsents($command = null)
    {
        $sourceSystemKey = $this->getSourceSystemKey();

        // 1. Get the Watermark for Consents
        $watermark = $this->dwh->table('Sync_Log')
            ->where('Table_Name', 'Dim_Consent')
            ->value('Last_Synced_At') ?? '1900-01-01 00:00:00';

        // Put the timestamp back by 5-seconds so we overlap for potential updates in source
        // which occur during the Sync process
        $watermark = Carbon::parse($watermark)->subSeconds(5)->toDateTimeString();
        $highestTimestampSeen = $watermark;

        $query = $this->source->table('consents')
            ->select([
                'id', 'rider_id', 'delivery_id', 'consent_status', 'has_bike',
                'cycle_ability', 'is_fsm', 'is_SEND', 'has_medical_condition',
                'attended', 'gender', 'ethnicity', 'updated_at'
            ])
            ->where('updated_at', '>', $watermark)
            ->orderBy('updated_at', 'asc');

        $total = $query->count();
        if ($total === 0) return "No new consent changes detected.";

        $bar = $command ? $command->getOutput()->createProgressBar($total) : null;
        if ($bar) $bar->start();

        // 3. Process in Chunks
        $query->chunk(1000, function ($consents) use ($sourceSystemKey, $bar, &$highestTimestampSeen) {
            foreach ($consents as $consent) {

                // Resolve Rider Key from DWH
                $riderKey = $this->dwh->table('Dim_Rider')
                    ->where('Source_Rider_Id', $consent->rider_id)
                    ->where('Source_System_Key', $sourceSystemKey)
                    ->value('Rider_Key');

                // Resolve Delivery Key from DWH
                $deliveryKey = $this->dwh->table('Dim_Delivery_Header')
                    ->where('Source_Delivery_Id', $consent->delivery_id)
                    ->where('Source_System_Key', $sourceSystemKey)
                    ->value('Delivery_Key');

                // If keys aren't found, we skip (Ensures Referential Integrity)
                if (!$riderKey || !$deliveryKey) {
                    if ($bar) $bar->advance();
                    continue;
                }

                //Get the array of Abilities
                $abilities = is_array($consent->cycle_ability)
                    ? $consent->cycle_ability
                    : json_decode($consent->cycle_ability, true) ?? [];

                $this->dwh->table('Dim_Consent')->updateOrInsert(
                    [
                        'Source_Consent_Id' => $consent->id,
                        'Source_System_Key' => $sourceSystemKey
                    ],
                    [
                        'Rider_Key'             => $riderKey,
                        'Delivery_Key'          => $deliveryKey,
                        'Consent_Status'        => $consent->consent_status,

                        // Flatten Ability Flags
                        'Ability_Cannot_Cycle'          => in_array("1", $abilities) ? 1 : 0,
                        'Ability_Can_Look_Over_Shoulder' => in_array("2", $abilities) ? 1 : 0,
                        'Ability_Can_One_Hand_Signal'   => in_array("3", $abilities) ? 1 : 0,
                        'Ability_Has_Level_2'           => in_array("4", $abilities) ? 1 : 0,

                        'Cycle_Ability_Raw'     => json_encode($abilities), // Normalize to JSON string
                        'Is_FSM'                => $consent->is_fsm,
                        'Is_SEND'               => $consent->is_SEND,
                        'Attended'              => $consent->attended,
                        'Gender'                => $consent->gender,
                        'Ethnicity'             => $consent->ethnicity,
                    ]
                );

                // Update Watermark tracker
                if ($consent->updated_at > $highestTimestampSeen) {
                    $highestTimestampSeen = $consent->updated_at;
                }

                if ($bar) $bar->advance();
            }
        });

        // 5. Finalize Watermark
        $this->dwh->table('Sync_Log')->updateOrInsert(
            ['Table_Name' => 'Dim_Consent'],
            [
                'Last_Synced_At' => $highestTimestampSeen,
                'Records_Processed' => $total
            ]
        );

        if ($bar) { $bar->finish(); $command->newLine(); }
        return "Synced {$total} Consents.";
    }

    /**
     * Synchronise the "Facts"
     */
    public function syncFactCourseDelivery($command = null)
    {
        $sourceSystemKey = $this->getSourceSystemKey();

        // Get Courses updated sunce last sync
        $watermark = $this->dwh->table('Sync_Log')
            ->where('Table_Name', 'Fact_Course_Delivery')
            ->value('Last_Synced_At') ?? '1900-01-01 00:00:00';

        $query = $this->source->table('courses')
            ->join('deliveries', 'courses.delivery_id', '=', 'deliveries.id')
            ->select([
                'courses.id',
                'courses.delivery_id',
                'courses.start_date',
                'courses.updated_at',
                'deliveries.consent_src_characteristics'
            ])
            ->where('courses.updated_at', '>', $watermark)
            ->orderBy('courses.updated_at', 'asc');

        $total = $query->count();
        if ($total === 0) return "Fact table is up to date.";

        $bar = $command ? $command->getOutput()->createProgressBar($total) : null;
        if ($bar) $bar->start();

        $highestTimestampSeen = $watermark;

        $query->chunk(500, function ($courses) use ($sourceSystemKey, $bar, &$highestTimestampSeen) {
            foreach ($courses as $course) {

                // Resolve Keys
                $courseKey = $this->dwh->table('Dim_Course')->where('Source_Course_Id', $course->id)->value('Course_Key');
                $delivery = $this->dwh->table('Dim_Delivery_Header')->where('Source_Delivery_Id', $course->delivery_id)->first();

                if (!$courseKey || !$delivery) continue;

                // Check where demographic data is sourced from
                // Course data from the query above joins the deliveries table
                // to get the consent_src_characteristics value
                if ($course->consent_src_characteristics == 1) {
                    // Get from Rider's Consent data
                    $metrics = $this->aggregateFromDWHConsents($delivery->Delivery_Key);
                } else {
                    // Get from the Course Characteristics table
                    $metrics = $this->aggregateFromSourceTable('course_characteristics', $course->id);
                }

                // Upsert into Fact Table
                $this->dwh->table('Fact_Course_Delivery')->updateOrInsert(
                    ['Course_Key' => $courseKey],
                    array_merge($metrics, [
                        'Date_Key'     => $course->start_date ? str_replace('-', '', substr($course->start_date, 0, 10)) : null,
                        'Delivery_Key' => $delivery->Delivery_Key,
                        'School_Key'   => $delivery->School_Key,
                        'Organisation_Key'   => $delivery->Organisation_Key,
                        'Provider_Key' => $delivery->Training_Provider_Key,
                        'Grant_Key'    => $delivery->Grant_Key,
                    ])
                );

                if ($course->updated_at > $highestTimestampSeen) {
                    $highestTimestampSeen = $course->updated_at;
                }
                if ($bar) $bar->advance();
            }
        });

        // Update Watermark
        $this->dwh->table('Sync_Log')->updateOrInsert(['Table_Name' => 'Fact_Course_Delivery'], ['Last_Synced_At' => $highestTimestampSeen]);

        if ($bar) { $bar->finish(); $command->newLine(); }
        return "Fact_Course_Delivery synced.";
    }

    private function aggregateFromDWHConsents($deliveryKey)
    {
        // Filter only for riders who actually attended
        $consents = $this->dwh->table('Dim_Consent')
            ->where('Delivery_Key', $deliveryKey)
            ->where('Attended', 1)
            ->get();

        //TODO: Handle All genders, Ethnicities, Pupil Premium and Send
        $m = [
            'Count_Female' => 0, 'Count_Male' => 0,
            'Count_Ethnicity_White' => 0, 'Count_Ethnicity_Asian' => 0,
            'Count_Ethnicity_Black' => 0, 'Count_Ethnicity_Mixed' => 0,
            'Count_Ethnicity_Other' => 0
        ];

        foreach ($consents as $consent) {
            // Gender logic
            if ($consent->Gender === 'Female') $m['Count_Female']++;
            if ($consent->Gender === 'Male')   $m['Count_Male']++;

            // Ethnicity logic (Mapping string values to the Fact columns)
            $eth = strtolower($consent->Ethnicity);
            if (str_contains($eth, 'white')) $m['Count_Ethnicity_White']++;
            elseif (str_contains($eth, 'asian')) $m['Count_Ethnicity_Asian']++;
            elseif (str_contains($eth, 'black')) $m['Count_Ethnicity_Black']++;
            elseif (str_contains($eth, 'mixed')) $m['Count_Ethnicity_Mixed']++;
            else $m['Count_Ethnicity_Other']++;
        }

        return $m;
    }

    private function aggregateFromSourceTable($tableName, $courseId)
    {
        $rows = $this->source->table($tableName)
            ->where('course_id', $courseId)
            ->get();

        //TODO: Handle All genders, Ethnicities, Pupil Premium and Send

        $m = [
            'Count_Female' => 0, 'Count_Male' => 0,
            'Count_Ethnicity_White' => 0, 'Count_Ethnicity_Asian' => 0,
            'Count_Ethnicity_Black' => 0, 'Count_Ethnicity_Mixed' => 0,
            'Count_Ethnicity_Other' => 0
        ];

        foreach ($rows as $row) {
            $sub = strtolower($row->sub_category);

            if ($row->category === 'gender') {
                if ($sub === 'female') $m['Count_Female'] += $row->value;
                if ($sub === 'male')   $m['Count_Male']   += $row->value;
            }

            if ($row->category === 'ethnicity') {
                $key = 'Count_Ethnicity_' . ucfirst($sub);
                if (array_key_exists($key, $m)) {
                    $m[$key] += $row->value;
                } else {
                    $m['Count_Ethnicity_Other'] += $row->value;
                }
            }
        }
        return $m;
    }

    public function syncFactParentSurvey($command = null)
    {
        $sourceSystemKey = $this->getSourceSystemKey();

        $query = $this->source->table('parent_survey')
            ->where('status', 1)
            ->orderBy('updated_at', 'asc');

        $total = $query->count();
        $bar = $command ? $command->getOutput()->createProgressBar($total) : null;
        if ($bar) $bar->start();

        $query->chunk(500, function ($surveys) use ($sourceSystemKey, $bar) {
            foreach ($surveys as $survey) {

                // Resolve Keys
                $riderKey = $this->dwh->table('Dim_Rider')->where('Source_Rider_Id', $survey->rider_id)->value('Rider_Key');
                $courseKey = $this->dwh->table('Dim_Course')->where('Source_Course_Id', $survey->course_id)->value('Course_Key');
                $delivery = $this->dwh->table('Dim_Delivery_Header')->where('Source_Delivery_Id', $survey->delivery_id)->first();

                if (!$riderKey || !$courseKey || !$delivery) continue;

                // Resolve Multi-select Feedback (Pivoted)
                $feedbackKeys = $this->source->table('parent_survey_feedback')
                    ->where('parent_survey_id', $survey->id)
                    ->pluck('feedback_key')
                    ->toArray();

                // 1. Handle OQ1 Multi-choice (Encouragers)
                $oq1_data = json_decode($survey->optional_questions, true) ?? [];
                $encouragers = [
                    'Encourage_More_Direct_Routes'     => in_array('oq1_o1', $oq1_data) ? 1 : 0,
                    'Encourage_Local_Route_Awareness'     => in_array('oq1_o2', $oq1_data) ? 1 : 0,
                    'Encourage_Storage'    => in_array('oq1_o3', $oq1_data) ? 1 : 0,
                    'Encourage_Road_Surfaces'    => in_array('oq1_o4', $oq1_data) ? 1 : 0,
                    'Encourage_Confidence' => in_array('oq1_o5', $oq1_data) ? 1 : 0,
                    'Encourage_Cycle_Maintenance' => in_array('oq1_o6', $oq1_data) ? 1 : 0,
                    'Encourage_Local_Initiatives' => in_array('oq1_o7', $oq1_data) ? 1 : 0,
                    'Encourage_Purchase_Ability' => in_array('oq1_o8', $oq1_data) ? 1 : 0,
                    'Encourage_Doesnt_Want_To_Cycle_More' => in_array('oq1_o9', $oq1_data) ? 1 : 0,
                    'Encourage_None'       => in_array('oq1_null', $oq1_data) ? 1 : 0,
                    'Encourage_Other_Reason' => $survey->optional_questions_input,
                ];

                // 2. Handle OQ2-OQ19 (Agree/Disagree Array)
                $agree_disagree_data = json_decode($survey->agree_disagree_options, true) ?? [];

                // Convert the flat array ["oq2_o5", "oq3_o4"] into a keyed lookup for easy extraction
                $pivoted = [];
                foreach ($agree_disagree_data as $item) {
                    // e.g., "oq2_o5" -> key: "oq2", value: 5
                    if (preg_match('/(oq\d+)_o(\d+)/', $item, $matches)) {
                        $pivoted[$matches[1]] = (int)$matches[2];
                    }
                }

                $likertMetrics = [
                    'Likert_Life_Skill'  => $pivoted['oq2'] ?? null, // oq2
                    'Likert_Self_Esteem' => $pivoted['oq3'] ?? null, // oq3
                    'Likert_Fitness'     => $pivoted['oq4'] ?? null, // oq4
                    'Likert_Active'      => $pivoted['oq5'] ?? null, // oq5
                    'Likert_Mindfulness' => $pivoted['oq6'] ?? null, // oq6
                    'Likert_Improve_Self_Regulate' => $pivoted['oq7'] ?? null, // oq7
                    'Likert_Improve_Concentration' => $pivoted['oq8'] ?? null, // oq8
                    'Likert_Improve_Academic_Performance' => $pivoted['oq9'] ?? null, // oq9
                    'Likert_Independence' => $pivoted['oq10'] ?? null, // oq10
                    'Likert_Improve_Road_Awareness' => $pivoted['oq11'] ?? null, // oq11
                    'Likert_Improve_Environment_Awareness' => $pivoted['oq12'] ?? null, // oq12
                    'Likert_Help_Socialise' => $pivoted['oq13'] ?? null, // oq13
                    'Likert_Make_Children_Happy' => $pivoted['oq14'] ?? null, // oq14
                    'Likert_Keep_Children_Occupied' => $pivoted['oq15'] ?? null, // oq15
                    'Likert_Encourage_Children_Outside' => $pivoted['oq16'] ?? null, // oq16
                    'Likert_Children_Less_Dependent' => $pivoted['oq17'] ?? null, // oq17
                    'Likert_Reduce_Other_Transport_Expense' => $pivoted['oq18'] ?? null, // oq18
                    'Likert_Enable_Cycle_As_Family' => $pivoted['oq19'] ?? null //oq19
                ];

                $this->dwh->table('Fact_Parent_Survey')->updateOrInsert(
                    ['Source_Survey_Id' => $survey->id],
                    array_merge($encouragers, $likertMetrics,[
                        'Date_Key'     => str_replace('-', '', substr($survey->created_at, 0, 10)),
                        'Rider_Key'    => $riderKey,
                        'Course_Key'   => $courseKey,
                        'Delivery_Key' => $delivery->Delivery_Key,
                        'Grant_Key'    => $delivery->Grant_Key,

                        'Rider_Emotion' => $survey->rider_emotion,
                        'Pref_More_Training' => $survey->pref_more_training,
                        'Pref_Interest_In_Training' => $survey->pref_interest_in_training,
                        // Store as Clean Integers (1-6)
                        'Confidence_Bike_General' => $this->extractOptionInt('co',$survey->confidence_to_use_a_bike),
                        'Confidence_Road'         => $this->extractOptionInt('co',$survey->confidence_to_use_bike_on_road),
                        'Confidence_Independent'  => $this->extractOptionInt('co',$survey->confidence_to_use_bike_independently),

                        // Feedback Flags
                        'Feedback_Is_Fun'      => in_array('rfq1_rf1', $feedbackKeys) ? 1 : 0,
                        'Feedback_Is_Hard'      => in_array('rfq1_rf2', $feedbackKeys) ? 1 : 0,
                        'Feedback_Is_Healthy'  => in_array('rfq1_rf3', $feedbackKeys) ? 1 : 0,
                        'Feedback_Still_New'  => in_array('rfq1_rf4', $feedbackKeys) ? 1 : 0,
                        'Feedback_Family_Friends' => in_array('rfq1_rf5', $feedbackKeys) ? 1 : 0,
                        'Feedback_Dont_See_Others_Like_Me' => in_array('rfq1_rf6', $feedbackKeys) ? 1 : 0,
                        'Feedback_On_Own'      => in_array('rfq1_rf7', $feedbackKeys) ? 1 : 0,
                        'Feedback_Not_Enjoy'   => in_array('rfq1_rf8', $feedbackKeys) ? 1 : 0,
                        'Feedback_None_Apply'   => in_array('rfq1_null', $feedbackKeys) ? 1 : 0,
                        'Feedback_None_Apply_Input' => $survey->rider_feedback_input,

                        //Encouragment
                        'Encouragement_Use_Bike' => $this->extractOptionInt('eo',$survey->encouragement_to_use_a_bike),
                        'Encouragement_Use_Bike_On_Road' => $this->extractOptionInt('eo',$survey->encouragement_to_use_bike_on_road),

                        //Recommend
                        'Likely_To_Recommend' => $survey->like_to_recommend,
                    ])
                );

                if ($bar) $bar->advance();
            }
        });

        if ($bar) { $bar->finish(); $command->newLine(); }
        return "Parent Survey Fact synced with integer-based Likert scores.";
    }

    public function syncFactHandsupSurvey($command = null)
    {
        $courseIds = $this->source->table('course_survey_results')->distinct()->pluck('course_id');

        $bar = $command ? $command->getOutput()->createProgressBar(count($courseIds)) : null;
        if ($bar) $bar->start();

        foreach ($courseIds as $sourceCourseId) {
            $course = $this->dwh->table('Dim_Course')->where('Source_Course_Id', $sourceCourseId)->first();
            if (!$course) continue;

            // Fetch all the "Tall" results for this course
            $results = $this->source->table('course_survey_results')->where('course_id', $sourceCourseId)->get();

            // Initialize the Wide Row with 0s
            $m = [
                'Exp_Enjoyed' => 0, 'Exp_Did_Not_Enjoy' => 0, 'Exp_Not_Sure' => 0, 'Exp_Absent' => 0,
                'Base_Yes' => 0, 'Base_No' => 0, 'Base_Not_Sure' => 0,
                'Safe_More' => 0, 'Safe_Less' => 0, 'Safe_No_Diff' => 0, 'Safe_Not_Sure' => 0,
                'Conf_More' => 0, 'Conf_Less' => 0, 'Conf_No_Diff' => 0, 'Conf_Not_Sure' => 0,
            ];

            foreach ($results as $res) {
                $qid = $res->question_id;
                $oid = $res->option_id;
                $val = (int)$res->total; // This is the pre-aggregated sum from source

                // BUCKET 1: Experience (IDs 1, 4, 8, 12, 14, 16)
                if (in_array($qid, [1, 4, 8, 12, 14, 16])) {
                    // Using Modulo 4 because the pattern is: 1:Enjoy, 2:Not, 3:Sure, 0:Absent
                    match($oid % 4) {
                        1 => $m['Exp_Enjoyed'] += $val,
                        2 => $m['Exp_Did_Not_Enjoy'] += $val,
                        3 => $m['Exp_Not_Sure'] += $val,
                        0 => $m['Exp_Absent'] += $val,
                    };
                }

                // BUCKET 2: Baseline (IDs 5, 9)
                if (in_array($qid, [5, 9])) {
                    // Pattern: Yes, No, Not Sure (Assuming IDs are 1, 2, 3 in their group)
                    match($oid % 3) {
                        1 => $m['Base_Yes'] += $val,
                        2 => $m['Base_No'] += $val,
                        0 => $m['Base_Not_Sure'] += $val,
                    };
                }

                // BUCKET 3: Safety (IDs 2, 6, 10)
                if (in_array($qid, [2, 6, 10])) {
                    match($oid % 5) { // Assuming 5 options: More, Less, No Diff, Not Sure, Absent
                        1 => $m['Safe_More'] += $val,
                        2 => $m['Safe_Less'] += $val,
                        3 => $m['Safe_No_Diff'] += $val,
                        4 => $m['Safe_Not_Sure'] += $val,
                        0 => null, // Ignore Absent for safety if not needed
                    };
                }

                // BUCKET 4: Confidence (IDs 3, 7, 11, 13, 15)
                if (in_array($qid, [3, 7, 11, 13, 15])) {
                    match($oid % 4) {
                        1 => $m['Conf_More'] += $val,
                        2 => $m['Conf_Less'] += $val,
                        3 => $m['Conf_No_Diff'] += $val,
                        0 => $m['Conf_Not_Sure'] += $val,
                    };
                }
            }

            // Upsert the single wide row for this Course
            $this->dwh->table('Fact_HandsUp_Survey')->updateOrInsert(
                ['Course_Key' => $course->Course_Key],
                array_merge($m, [
                    'Date_Key'     => $course->Start_Date?str_replace('-', '', substr($course->Start_Date, 0, 10)):null,
                    'Delivery_Key' => $course->Delivery_Key,
                ])
            );

            if ($bar) $bar->advance();
        }

        if ($bar) { $bar->finish(); $command->newLine(); }
        return "Hands-up Facts synced (Mapped from pre-aggregated source).";
    }

    /**
     * Extracts the integer from strings like "cq1_co1", "cq3_co6" or "eq3_eo6"
     * Returns null if no valid code is found.
     */
    private function extractOptionInt($prefix, $code)
    {
        if (empty($code)) return null;

        // Pattern looks for the "prefix" followed by digits
        $pattern = '/' . preg_quote($prefix, '/') . '(\d+)/';

        if (preg_match($pattern, $code, $matches)) {
            return (int) $matches[1];
        }

        return null;
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

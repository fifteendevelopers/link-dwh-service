<?php

namespace App\Services;

use App\Models\Rider;
use Illuminate\Support\Facades\DB;
use Carbon\Carbon;
use Illuminate\Support\Facades\Schema;

class DataWarehouseSyncService
{
    protected $dwh;
    protected $source;

    public function __construct()
    {
        $this->dwh = DB::connection('mysql');
        $this->source = DB::connection('mysql_src');
    }

    public function syncExternalSystems($command = null)
    {
        $sourceSystemKey = $this->getSourceSystemKey();
        $sourceExtSystems = $this->source->table('external_systems')->get();

        $bar = $command ? $command->getOutput()->createProgressBar(count($sourceExtSystems)) : null;
        if ($bar) $bar->start();

        foreach ($sourceExtSystems as $sys) {
            // Prepare the incoming data
            $incomingData = [
                'Username' => $sys->username,
                'Name' => $sys->name,
                'Pref_Link_Managed_Consent' => $sys->pref_link_managed_consent,
                'Pref_Use_Instructor_App' => $sys->pref_use_instructor_app,
                'Pref_Link_Managed_Comms' => $sys->pref_link_managed_comms,
                'created_at' => $sys->created_at ?? now(),
                'updated_at' => $sys->updated_at ?? now(),
            ];

            $this->dwh->table('Dim_External_Systems')->updateOrInsert(
                [
                    'Source_External_System_Id' => $sys->id,
                    'Source_System_Key' => $sourceSystemKey
                ],
                $incomingData
            );

            if ($bar) $bar->advance();
        }

        if ($bar) {
            $bar->finish();
            $command->newLine();
        }

        return "Successfully synced External Systems.";
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
                'Provider_Name' => $provider->provider_name,
                'Provider_Number' => $provider->provider_number,
                'Is_Active' => is_null($provider->deleted_at) ? 'Y' : 'N',
                'Address_Line_1' => $provider->address_01,
                'Address_Line_2' => $provider->address_02,
                'City' => $provider->city,
                'Postcode' => $provider->postcode,
                'Website' => $provider->website,
                'Telephone' => $provider->telephone,
                'Public_Email' => $provider->public_email,
                'Public_Telephone' => $provider->public_telephone,
                'Provider_Type' => $provider->provider_type,
                'Source_Created_At' => $provider->created_at,
                'Source_Updated_At' => $provider->updated_at,
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

        if ($bar) {
            $bar->finish();
            $command->newLine();
        }

        return "Successfully synced Training Providers. (New: $newRecords, Updated: $updatedVersions)";
    }

    public function syncSchools($command = null)
    {
        $sourceSystemKey = $this->getSourceSystemKey();

        $watermark = $this->dwh->table('Sync_Log')
            ->where('Table_Name', 'Dim_School')
            ->value('Last_Synced_At') ?? '1900-01-01 00:00:00';

        // 2. Count only new or changed schools
        $query = $this->source->table('vendor_edubase')
            ->where('updated_at', '>', $watermark);

        $total = $query->count();

        if ($total === 0) {
            return "Schools are already up to date.";
        }

        $bar = $command ? $command->getOutput()->createProgressBar($total) : null;
        if ($bar) $bar->start();

        $highestTimestampSeen = $watermark;
        $syncCount = 0;

        // 2. Chunking to avoid the memory exhaustion error
        $this->source->table('vendor_edubase')
            ->select('id', 'urn', 'establishment_name', 'la_code', 'la_name', 'updated_at')
            ->orderBy('id')
            ->chunk(1000, function ($schools) use (&$syncCount, &$highestTimestampSeen, $sourceSystemKey, $bar) {
                foreach ($schools as $school) {

                    // Using updateOrInsert (SCD Type 1)
                    // This will update the existing record if the ID + System Key matches
                    $this->dwh->table('Dim_School')->updateOrInsert(
                        [
                            'Source_School_Id' => $school->id,
                            'Source_System_Key' => $sourceSystemKey
                        ],
                        [
                            'School_Urn' => $school->urn,
                            'School_Name' => $school->establishment_name,
                            'La_Code' => $school->la_code,
                            'La_Name' => $school->la_name
                        ]
                    );

                    // Track the latest timestamp processed
                    if ($school->updated_at > $highestTimestampSeen) {
                        $highestTimestampSeen = $school->updated_at;
                    }

                    if ($bar) $bar->advance();
                    $syncCount++;
                }
            });

        $this->dwh->table('Sync_Log')->updateOrInsert(
            ['Table_Name' => 'Dim_School'],
            [
                'Last_Synced_At' => $highestTimestampSeen,
                'Records_Processed' => $total
            ]
        );

        if ($bar) {
            $bar->finish();
            $command->newLine();
        }

        return "Successfully synced {$syncCount} Schools (Overwritten/Updated).";
    }

    public function syncOrganisations($command = null)
    {
        $sourceSystemKey = $this->getSourceSystemKey();

        // Fetch all organisations
        $sourceOrgs = $this->source->table('organisations')
            ->select('id', 'training_provider_id', 'name')
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
                    'Source_System_Key' => $sourceSystemKey
                ],
                [
                    'Provider_Key' => $providerKey,
                    'Organisation_Name' => $org->name,
                ]
            );

            if ($bar) $bar->advance();
            $syncCount++;
        }

        if ($bar) {
            $bar->finish();
            $command->newLine();
        }
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
                'Recipient_Name' => $recipient->recipient_name,
                'Recipient_Number' => $recipient->recipient_number,
                'LA_Id' => $recipient->local_authority_id,
                'Is_Active' => is_null($recipient->deleted_at) ? 'Y' : 'N',
                'Address_Line_1' => $recipient->address_01,
                'Address_Line_2' => $recipient->address_02,
                'City' => $recipient->city,
                'Postcode' => $recipient->postcode,
                'Website' => $recipient->website,

                // Dates
                'Inception_Date' => $recipient->date_inception,
                'Renewal_Date' => $recipient->date_renewal,
                'Deregistered_Date' => $recipient->date_deregistered,

                // Derived Field: Is_SGO
                'Is_SGO' => ($recipient->pref_sgoh == 1) ? 1 : 0,

                // Source Metadata
                'Source_Created_At' => $recipient->created_at,
                'Source_Updated_At' => $recipient->updated_at,
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

        if ($bar) {
            $bar->finish();
            $command->newLine();
        }

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
                    'Source_Grant_Id' => $grant->id,
                    'Source_System_Key' => $sourceSystemKey
                ],
                [
                    'Grant_Recipient_Key' => $recipientKey,
                    'Grant_Number' => $grant->grant_number,
                    'Grant_Label' => $grant->grant_label,
                    'Grant_Period_Start_Year' => $grant->grant_period_start_year,
                    'Grant_Source' => $grant->grant_source,
                ]
            );

            if ($bar) $bar->advance();
            $syncCount++;
        }

        if ($bar) {
            $bar->finish();
            $command->newLine();
        }

        return "Successfully synced {$syncCount} Grants. ({$errorCount} orphans skipped).";
    }

    public function syncInstructors($command = null)
    {
        $sourceSystemKey = $this->getSourceSystemKey();

        if ($command) $command->info("[" . now()->format('Y-m-d H:i:s') . "] Starting Instructors Sync...");

        // 1. Resolve Watermark tracking
        $watermark = $this->dwh->table('Sync_Log')
            ->where('Table_Name', 'Dim_Instructor')
            ->value('Last_Synced_At') ?? '1900-01-01 00:00:00';

        if ($command) $command->comment("Watermark: $watermark");

        // 2. Query Source delta (including rows where updated_at might be missing)
        $query = $this->source->table('instructors')
            ->where(function ($q) use ($watermark) {
                $q->where('updated_at', '>', $watermark)
                    ->orWhereNull('updated_at');
            });

        $total = $query->count();

        if ($total === 0) {
            if ($command) $command->info("[" . now()->format('Y-m-d H:i:s') . "] Instructors are already up to date.");
            return;
        }

        $bar = $command ? $command->getOutput()->createProgressBar($total) : null;
        if ($bar) $bar->start();

        $highestTimestampSeen = $watermark;
        $syncCount = 0;

        // 3. Process records via stable chunks
        $query->orderBy('updated_at', 'asc')->chunk(500, function ($instructors) use (&$syncCount, &$highestTimestampSeen, $sourceSystemKey, $bar) {
            foreach ($instructors as $instructor) {

                $this->dwh->table('Dim_Instructor')->updateOrInsert(
                    [
                        'Source_Instructor_Id' => $instructor->id,
                        'Source_System_Key' => $sourceSystemKey
                    ],
                    [
                        'Instructor_Number' => $instructor->instructor_number,
                        'Instructor_Type' => $instructor->instructor_type,
                        'First_Name' => $instructor->first_name,
                        'Last_Name' => $instructor->last_name,
                        'Email' => $instructor->email,
                        'Telephone' => $instructor->telephone,
                        'Landline' => $instructor->landline,

                        'Age_Range_Id' => $instructor->age_range_id,
                        'Ethnicity_Id' => $instructor->ethnicity_id,
                        'Gender_Id' => $instructor->gender_id,
                        'Title_Id' => $instructor->title_id,

                        'Address_01' => $instructor->address_01,
                        'Address_02' => $instructor->address_02,
                        'City' => $instructor->city,
                        'Postcode' => $instructor->postcode,

                        'Status_Raw' => $instructor->status,
                        'Is_Pending' => (bool)$instructor->is_pending,
                        'Flag_Nsi_Migrated' => (bool)$instructor->flag_nsi_migrated,

                        'Pref_Receive_News' => (bool)$instructor->pref_receive_news,
                        'Pref_Delivering_Bikeability' => (bool)$instructor->pref_delivering_bikeability,
                        'Pref_Delivering_Other' => (bool)$instructor->pref_delivering_other,
                        'Pref_Bursary_Eligibility' => (bool)$instructor->pref_bursary_eligibility,
                        'Has_Received_Bursary' => (bool)$instructor->has_received_bursary,

                        'Date_Registered' => $instructor->date_registered,
                        'Date_Renewal' => $instructor->date_renewal,
                        'Date_Deregistered' => $instructor->date_deregistered,
                        'Deregistration_Reason' => $instructor->deregistration_reason,

                        'First_Aid_Training_Complete_Date' => $instructor->first_aid_training_complete_date,
                        'Safeguarding_Training_Complete_Date' => $instructor->safeguarding_training_complete_date,
                        'Send_Training_Complete_Date' => $instructor->send_training_complete_date,
                        'Send_Training_Overridden' => (bool)$instructor->send_training_overridden,
                        'Send_Training_Certificate_Download_Date' => $instructor->send_training_certificate_download_date,

                        'Account_Notes' => $instructor->account_notes,
                        'Source_Created_At' => $instructor->created_at,
                        'Source_Updated_At' => $instructor->updated_at,
                        'updated_at' => now()
                    ]
                );

                if ($instructor->updated_at > $highestTimestampSeen) {
                    $highestTimestampSeen = $instructor->updated_at;
                }

                if ($bar) $bar->advance();
                $syncCount++;
            }
        });

        // 4. Record the final Watermark state
        $this->dwh->table('Sync_Log')->updateOrInsert(
            ['Table_Name' => 'Dim_Instructor'],
            [
                'Last_Synced_At' => $highestTimestampSeen,
                'Records_Processed' => $syncCount
            ]
        );

        if ($bar) {
            $bar->finish();
            $command->newLine();
        }

        if ($command) $command->info("[" . now()->format('Y-m-d H:i:s') . "] Successfully synced {$syncCount} Instructors.");
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
            ->where(function ($q) use ($watermark) {
                $q->where('deliveries.updated_at', '>', $watermark)
                    ->orWhereNull('deliveries.updated_at');
            });

        $total = $query->count();

        if ($total === 0) {
            return "No new delivery changes detected.";
        }

        $bar = $command ? $command->getOutput()->createProgressBar($total) : null;

        if ($bar) $bar->start();

        $this->source->table('deliveries')
            ->select('id', 'grant_id', 'school_urn', 'training_provider_id',
                'status', 'date_delivery_start', 'date_delivery_end', 'digitisation_booking', 'pref_alt_delivery_location','alt_delivery_location',
                'notes','instructor_general_notes','teacher_notes','school_contacts','venue','provider_additional_questions',
                'comms_start_date','date_completed','pref_link_managed_consent','include_tp_terms_in_consent','consent_src_characteristics',
                'max_consents','waiting_list_enabled', 'organisation_id', 'fleet_cycles_used', 'consent_cutoff_date','updated_at')
            ->where(function ($q) use ($watermark) {
                $q->where('deliveries.updated_at', '>', $watermark)
                    ->orWhereNull('deliveries.updated_at');
            })
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

                    $externalSystemKey = null;
                    if (!empty($delivery->external_system_id)) {
                        $externalSystemKey = DB::connection('mysql')->table('Dim_External_Systems')
                            ->where('Source_External_System_Id', $delivery->external_system_id)
                            ->value('External_System_Key');
                    }

                    // Upsert the Delivery Header data
                    $this->dwh->table('Dim_Delivery_Header')->updateOrInsert(
                        [
                            'Source_Delivery_Id' => $delivery->id,
                            'Source_System_Key' => $sourceSystemKey
                        ],
                        [
                            'Grant_Key' => $grantKey,
                            'School_Key' => $schoolKey,
                            'Organisation_Key' => $organisationKey,
                            'Training_Provider_Key' => $providerKey,
                            'External_System_Key' => $externalSystemKey,
                            'Delivery_Status' => $this->mapDeliveryStatus($delivery->status),
                            'Date_Delivery_Start' => $delivery->date_delivery_start,
                            'Date_Delivery_End' => $delivery->date_delivery_end,
                            'Digitisation_Booking' => $delivery->digitisation_booking,
                            'Fleet_Cycles_Used' => $delivery->fleet_cycles_used,
                            'Consent_Cutoff_Date' => $delivery->consent_cutoff_date,
                            'Pref_Alt_Delivery_Location' => $delivery->pref_alt_delivery_location,
                            'Alt_Delivery_Location' => $delivery->alt_delivery_location,
                            'Notes' => $delivery->notes,
                            'Instructor_General_Notes' => $delivery->instructor_general_notes,
                            'Teacher_Notes' => $delivery->teacher_notes,
                            'School_Contacts' => $delivery->school_contacts, // Laravel passes as string, MySQL writes to JSON
                            'Venue' => $delivery->venue,
                            'Provider_Additional_Questions' => $delivery->provider_additional_questions,
                            'Comms_Start_Date' => $delivery->comms_start_date,
                            'Date_Completed' => $delivery->date_completed,
                            'Pref_Link_Managed_Consent' => $delivery->pref_link_managed_consent,
                            'Include_Tp_Terms_In_Consent' => $delivery->include_tp_terms_in_consent,
                            'Consent_Src_Characteristics' => $delivery->consent_src_characteristics,
                            'Max_Consents' => $delivery->max_consents,
                            'Waiting_List_Enabled' => $delivery->waiting_list_enabled
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

        if ($bar) {
            $bar->finish();
            $command->newLine();
        }
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
            ->where(function ($q) use ($watermark) {
                $q->where('courses.updated_at', '>', $watermark)
                    ->orWhereNull('courses.updated_at');
            });

        $total = $query->count();

        if ($total === 0) {
            return "No new delivery changes detected.";
        }

        $highestTimestampSeen = $watermark;

        // Fetch courses with their delivery relationship
        $sourceCourses = $this->source->table('courses')
            ->select('id', 'course_id', 'parent_course_id', 'delivery_id', 'status', 'start_date', 'date_complete', 'year_group', 'updated_at')
            ->where(function ($q) use ($watermark) {
                $q->where('courses.updated_at', '>', $watermark)
                    ->orWhereNull('courses.updated_at');
            })
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
                    'Source_Course_Id' => $course->id,
                    'Source_System_Key' => $sourceSystemKey
                ],
                [
                    'Delivery_Key' => $deliveryKey,
                    'Course_Level' => $course->course_id,
                    'Status' => $course->status,
                    'Start_Date' => $course->start_date,
                    'Date_Complete' => $course->date_complete,
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

        if ($bar) {
            $bar->finish();
            $command->newLine();
        }
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
        ])->where(function ($q) use ($watermark) {
            $q->where('updated_at', '>', $watermark)
                ->orWhereNull('updated_at');
        })->orderBy('updated_at', 'asc');

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
                        'School_Key' => $schoolKey,
                        'Ethnicity' => $rider->ethnicity,
                        'Gender' => $rider->gender,
                        'Pupil_Premium' => ($rider->free_school_meals === 'Yes') ? 1 : 0,
                        'Has_SEND' => !empty($rider->send_code) ? 1 : 0,
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
                        'Rider_Key' => $riderKey,
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

        if ($bar) {
            $bar->finish();
            $command->newLine();
        }
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
                'id', 'rider_id', 'delivery_id', 'consent_status',
                'pref_join_bikeability_club', 'pref_further_research', 'pref_receive_news',
                'has_bike', 'cycle_ability', 'is_fsm',
                'is_SEND', 'send_details', 'has_medical_condition', 'medical_details',
                'attended', 'year_group', 'gender', 'ethnicity', 'updated_at'
            ])
            ->where(function ($q) use ($watermark) {
                $q->where('updated_at', '>', $watermark)
                    ->orWhereNull('updated_at');
            })->orderBy('updated_at', 'asc');

        $total = $query->count();
        if ($total === 0) return "No new consent changes detected.";

        $bar = $command ? $command->getOutput()->createProgressBar($total) : null;
        if ($bar) $bar->start();

        // 3. Process in Chunks
        $query->chunk(1000, function ($consents) use ($sourceSystemKey, $bar, &$highestTimestampSeen) {

            $consentIds = $consents->pluck('id')->toArray();

            $allFrequencies = $this->source->table('cycle_frequency')
                ->whereIn('consent_id', $consentIds)
                ->get()
                ->groupBy('consent_id');


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

                $freqs = $allFrequencies->get($consent->id) ?? collect();

                // Map based on the label_lookup_id (1: School, 2: Leisure, 3: Exercise, 4: Other)
                $freqData = [
                    'Pre_Freq_To_School' => $freqs->where('label_lookup_id', 1)->first()->grading_lookup_id ?? null,
                    'Pre_Freq_Leisure' => $freqs->where('label_lookup_id', 2)->first()->grading_lookup_id ?? null,
                    'Pre_Freq_Exercise' => $freqs->where('label_lookup_id', 3)->first()->grading_lookup_id ?? null,
                    'Pre_Freq_Other' => $freqs->where('label_lookup_id', 4)->first()->grading_lookup_id ?? null,
                ];


                $this->dwh->table('Dim_Consent')->updateOrInsert(
                    [
                        'Source_Consent_Id' => $consent->id,
                        'Source_System_Key' => $sourceSystemKey
                    ],
                    array_merge($freqData, [
                        'Rider_Key' => $riderKey,
                        'Delivery_Key' => $deliveryKey,
                        'Consent_Status' => $consent->consent_status,
                        'Pref_Join_Bikeclub' => $consent->pref_join_bikeability_club ?: 0,
                        'Pref_Further_Research' => $consent->pref_further_research ?: 0,
                        'Pref_Receive_News' => $consent->pref_receive_news ?: 0,

                        // Flatten Ability Flags
                        'Ability_Cannot_Cycle' => in_array("1", $abilities) ? 1 : 0,
                        'Ability_Can_Look_Over_Shoulder' => in_array("2", $abilities) ? 1 : 0,
                        'Ability_Can_One_Hand_Signal' => in_array("3", $abilities) ? 1 : 0,
                        'Ability_Has_Level_2' => in_array("4", $abilities) ? 1 : 0,
                        'Cycle_Ability_Raw' => json_encode($abilities), // Normalize to JSON string
                        'Is_Pupil_premium' => $consent->is_fsm ?: 0,
                        'Is_SEND' => $consent->is_SEND ?: 0,
                        'SEND_Details' => $consent->send_details,
                        'Has_Medical_Condition' => $consent->has_medical_condition ?: 0,
                        'Medical_Details' => $consent->medical_details,
                        'Attended' => $consent->attended,
                        'Year_Group' => $consent->year_group,
                        'Gender' => $consent->gender,
                        'Ethnicity' => $consent->ethnicity,
                    ])
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

        if ($bar) {
            $bar->finish();
            $command->newLine();
        }
        return "Synced {$total} Consents.";
    }

    /**
     * Synchronise the "Facts"
     */
    private function mapEthnicityToColumn($code)
    {
        $code = strtoupper($code);

        return match ($code) {
            'WBRI' => 'Count_Ethnicity_White_British',
            'WIRI' => 'Count_Ethnicity_White_Irish',
            'WIRT' => 'Count_Ethnicity_Irish_Traveller',
            'WOTH' => 'Count_Ethnicity_White_Other',
            'WROM' => 'Count_Ethnicity_Gypsy_Romany',
            'MWBC' => 'Count_Ethnicity_Mixed_White_Black_Carib',
            'MWBA' => 'Count_Ethnicity_Mixed_White_Black_African',
            'MWAS' => 'Count_Ethnicity_Mixed_White_Asian',
            'MOTH' => 'Count_Ethnicity_Mixed_Other',
            'AIND' => 'Count_Ethnicity_Asian_Indian',
            'APKN' => 'Count_Ethnicity_Asian_Pakistani',
            'ABAN' => 'Count_Ethnicity_Asian_Bangladeshi',
            'AOTH' => 'Count_Ethnicity_Asian_Other',
            'BCRB' => 'Count_Ethnicity_Black_Caribbean',
            'BAFR' => 'Count_Ethnicity_Black_African',
            'BOTH' => 'Count_Ethnicity_Black_Other',
            'CHNE' => 'Count_Ethnicity_Asian_Chinese',
            'OOTH' => 'Count_Ethnicity_Other_Any',
            'NA', 'N' => 'Count_Ethnicity_Refused',
            default => 'Count_Ethnicity_Refused',
        };

    }

    private function initializeExtendedMetricArray()
    {
        return [
            'Count_Female' => 0, 'Count_Male' => 0,
            'Count_Ethnicity_White_British' => 0, 'Count_Ethnicity_White_Irish' => 0, 'Count_Ethnicity_White_Other' => 0,
            'Count_Ethnicity_Mixed_White_Black_Carib' => 0, 'Count_Ethnicity_Mixed_White_Black_African' => 0,
            'Count_Ethnicity_Mixed_White_Asian' => 0, 'Count_Ethnicity_Mixed_Other' => 0,
            'Count_Ethnicity_Asian_Indian' => 0, 'Count_Ethnicity_Asian_Pakistani' => 0,
            'Count_Ethnicity_Asian_Bangladeshi' => 0, 'Count_Ethnicity_Asian_Chinese' => 0, 'Count_Ethnicity_Asian_Other' => 0,
            'Count_Ethnicity_Black_African' => 0, 'Count_Ethnicity_Black_Caribbean' => 0, 'Count_Ethnicity_Black_Other' => 0,
            'Count_Ethnicity_Other_Arab' => 0, 'Count_Ethnicity_Other_Any' => 0, 'Count_Ethnicity_Not_Stated' => 0,
            'Count_Pupil_Premium' => 0, 'Count_SEND' => 0
        ];
    }

    public function syncFactCourseDelivery($command = null)
    {
        $sourceSystemKey = $this->getSourceSystemKey();

        $useLegacy = \Illuminate\Support\Facades\Schema::connection('mysql_src')
            ->hasColumn('deliveries', 'delivery_details');

        $useLegacyCourseCharacteristicss = \Illuminate\Support\Facades\Schema::connection('mysql_src')
            ->hasColumn('courses', 'characteristics');

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
                'deliveries.date_delivery_start',
                'deliveries.consent_src_characteristics'
            ]);

        if ($useLegacy) {
            $query->addSelect('deliveries.delivery_details');
        }

        if ($useLegacyCourseCharacteristicss) {
            $query->addSelect('courses.characteristics');
        }

        $query->where('deliveries.digitisation_booking', 1)
            ->where(function ($q) use ($watermark) {
                $q->where('courses.updated_at', '>', $watermark)
                    ->orWhereNull('courses.updated_at');
            })
            ->orderBy('courses.updated_at', 'asc');

        $total = $query->count();
        if ($total === 0) return "Fact table is up to date.";

        $bar = $command ? $command->getOutput()->createProgressBar($total) : null;
        if ($bar) $bar->start();

        $highestTimestampSeen = $watermark;

        $query->chunk(500, function ($courses) use ($sourceSystemKey, $bar, &$highestTimestampSeen, $useLegacy, $useLegacyCourseCharacteristicss) {
            foreach ($courses as $course) {

                // Resolve Keys
                $courseKey = $this->dwh->table('Dim_Course')->where('Source_Course_Id', $course->id)->value('Course_Key');
                $delivery = $this->dwh->table('Dim_Delivery_Header')->where('Source_Delivery_Id', $course->delivery_id)->first();

                if (!$courseKey || !$delivery) continue;

                $enrolledCount = $this->source->table('join_riders_courses')
                    ->where('course_id', $course->id)
                    ->count();

                $completedCount = $this->source->table('join_riders_courses')
                    ->where('course_id', $course->id)
                    ->where('has_completed_course', 1)
                    ->count();

                $deliveryDetailMetrics = $this->getCourseDeliveryMetrics($course, $useLegacy);

                // Check where demographic data is sourced from
                // Course data from the query above joins the deliveries table
                // to get the consent_src_characteristics value
                if ($course->consent_src_characteristics == 1) {
                    // Get from Rider's Consent data
                    $metrics = $this->aggregateFromDWHConsents($delivery->Delivery_Key);
                } else {
                    if ($useLegacyCourseCharacteristicss) {
                        // Priority 2: Legacy JSON Column
                        $metrics = $this->aggregateFromLegacyCharacteristics($course->characteristics);
                    } else {
                        // Priority 3: Normalized Tables
                        $metrics = $this->aggregateFromSourceTable('course_characteristics', $course->id);
                    }
                }

                //Determine which date to use - if there is a course start date use that otherwise drop to delivery start date
                $fact_date = $course->start_date ?: $course->date_delivery_start;

                // Upsert into Fact Table
                $this->dwh->table('Fact_Course_Delivery')->updateOrInsert(
                    ['Course_Key' => $courseKey],
                    array_merge($metrics, $deliveryDetailMetrics, [
                        'Riders_Enrolled_Count' => $enrolledCount,
                        'Riders_Completed_Count' => $completedCount,
                        'Date_Key' => $fact_date ? str_replace('-', '', substr($fact_date, 0, 10)) : null,
                        'Delivery_Key' => $delivery->Delivery_Key,
                        'School_Key' => $delivery->School_Key,
                        'Organisation_Key' => $delivery->Organisation_Key,
                        'Provider_Key' => $delivery->Training_Provider_Key,
                        'Grant_Key' => $delivery->Grant_Key,
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

        if ($bar) {
            $bar->finish();
            $command->newLine();
        }
        return "Fact_Course_Delivery synced.";
    }

    private function getCourseDeliveryMetrics($course, $useLegacy)
    {
        // Check if the 'delivery_details' column exists in the source 'deliveries' table
        // If it doesn't exist, it means the JSON normalization has happened so we will use
        // the delivery_modules and delivery_metrics

        if (is_null($useLegacy)) {
            $useLegacy = \Illuminate\Support\Facades\Schema::connection('mysql_src')
                ->hasColumn('deliveries', 'delivery_details');
        }

        if ($useLegacy) {
            return $this->extractFromLegacyJson($course);
        }

        return $this->extractFromNormalizedTables($course);
    }

    private function extractFromLegacyJson($course)
    {
        $details = json_decode($course->delivery_details ?? '[]', true);
        $metrics = ['Count_Booked_Provisional' => 0, 'Count_Booked_Confirmed' => 0, 'Count_Attended_Confirmed' => 0];

        foreach ($details as $entry) {
            if (isset($entry['module']['entity_id']) && $entry['module']['entity_id'] == $course->id) {
                $delivery = $entry['delivery'] ?? [];
                $metrics['Count_Booked_Provisional'] = (int)($delivery['booked']['provisional'] ?? 0);

                if (isset($delivery['confirmed']) && $delivery['confirmed'] == 1) {
                    $metrics['Count_Booked_Confirmed'] = (int)($delivery['booked']['total'] ?? 0);
                    $metrics['Count_Attended_Confirmed'] = (int)($delivery['attended']['total'] ?? 0);
                }
                break;
            }
        }
        return $metrics;
    }

    private function extractFromNormalizedTables($course)
    {
        // Query the new delivery_modules table
        $module = $this->source->table('delivery_modules')
            ->where('entity_id', $course->id)
            ->first();

        if (!$module) {
            return ['Count_Booked_Provisional' => 0, 'Count_Booked_Confirmed' => 0, 'Count_Attended_Confirmed' => 0];
        }

        return [
            'Count_Booked_Provisional' => $module->booked_provisional,
            'Count_Booked_Confirmed' => ($module->confirmed) ? $module->booked_total : 0,
            'Count_Attended_Confirmed' => ($module->confirmed) ? $module->attended_total : 0,
        ];
    }


    private function aggregateFromDWHConsents($deliveryKey)
    {
        $consents = $this->dwh->table('Dim_Consent')
            ->where('Delivery_Key', $deliveryKey)
            ->where('Attended', 1)
            ->get();

        $m = $this->initializeExtendedMetricArray();

        foreach ($consents as $consent) {
            // Gender
            if ($consent->Gender === 'Female') $m['Count_Female']++;
            elseif ($consent->Gender === 'Male') $m['Count_Male']++;

            // Detailed Ethnicity Mapping
            $eth = strtolower($consent->Ethnicity);

            // Use a helper or match to find the specific column
            $column = $this->mapEthnicityToColumn($eth);
            if (isset($m[$column])) {
                $m[$column]++;
            } else {
                $m['Count_Ethnicity_Not_Stated']++;
            }

            // Pupil Premium & SEND
            if ($consent->Is_Pupil_Premium) $m['Count_Pupil_Premium']++;
            if ($consent->Is_SEND) $m['Count_SEND']++;
        }

        return $m;
    }

    private function aggregateFromLegacyCharacteristics($jsonString)
    {
        $m = $this->initializeExtendedMetricArray();
        $data = json_decode($jsonString ?? '[]', true);

        if (!is_array($data) || empty($data)) return $m;

        // Course characteristics is typically an array with a single block object at index 0
        $block = $data[0] ?? [];

        // Map Gender
        $gender = $block['gender'] ?? [];
        foreach ($gender as $sub => $val) {
            if ($sub === 'female') $m['Count_Female'] += (int)$val;
            elseif ($sub === 'male') $m['Count_Male'] += (int)$val;
        }

        // Map Ethnicity
        $ethnicity = $block['ethnicity'] ?? [];
        foreach ($ethnicity as $sub => $val) {
            // We use the same column mapping logic as the DWH/Normalized tables
            $column = 'Count_Ethnicity_' . str_replace(' ', '_', strtolower($sub));
            if (isset($m[$column])) {
                $m[$column] += (int)$val;
            } else {
                $m['Count_Ethnicity_Other_Any'] += (int)$val;
            }
        }

        // Map Standalone Demographics
        // Note: Use the keys from the legacy JSON structure
        $m['Count_SEND'] += (int)($block['send'] ?? 0);
        $m['Count_Pupil_Premium'] += (int)($block['free_school_meals'] ?? 0);

        return $m;
    }

    private function aggregateFromSourceTable($tableName, $courseId)
    {
        $rows = $this->source->table($tableName)->where('course_id', $courseId)->get();
        $m = $this->initializeExtendedMetricArray();

        foreach ($rows as $row) {
            $cat = strtolower($row->category);
            $sub = strtolower($row->sub_category);
            $val = (int)$row->value;

            if ($cat === 'gender') {
                if ($sub === 'female') $m['Count_Female'] += $val;
                elseif ($sub === 'male') $m['Count_Male'] += $val;
            }

            if ($cat === 'ethnicity') {
                // We expect the source 'sub_category' to match our grouping names (e.g., 'white_british')
                $column = 'Count_Ethnicity_' . str_replace(' ', '_', $sub);
                if (isset($m[$column])) {
                    $m[$column] += $val;
                } else {
                    $m['Count_Ethnicity_Other_Any'] += $val;
                }
            }

            if ($cat === 'pupil_premium') $m['Count_Pupil_Premium'] += $val;
            if ($cat === 'send') $m['Count_SEND'] += $val;
        }
        return $m;
    }

    /**
     * Synchronize Rider - Course Fact
     */
    public function syncFactRiderCourse($command = null)
    {
        $sourceSystemKey = $this->getSourceSystemKey();
        if ($command) $command->info("[" . now()->format('Y-m-d H:i:s') . "] Starting Facts - Rider Course Sync...");

        // 1. Fetch our high-watermark checkpoint using your standard tracker helper
        $watermark = $this->getWatermark('Fact_Rider_Course');

        // 2. Query against the transactional source table looking for modified rows
        $query = $this->source->table('join_riders_courses')
            ->where('updated_at', '>', $watermark)
            ->orderBy('updated_at', 'asc');

        $total = $query->count();
        if ($total === 0) {
            if ($command) $command->info("[" . now()->format('Y-m-d H:i:s') . "] Facts - Rider Courses are already up to date.");
            return "Fact_Rider_Course up to date.";
        }

        // 3. Optional interactive CLI Progress Bar initialization
        $bar = $command ? $command->getOutput()->createProgressBar($total) : null;
        if ($bar) $bar->start();

        $highestTimestamp = $watermark;

        // 4. Process in chunks to maintain low memory footprints
        $query->chunk(250, function ($riderCourses) use ($sourceSystemKey, $bar, &$highestTimestamp) {
            foreach ($riderCourses as $rc) {

                $finalAttendance = (!empty($rc->attended_override) && $rc->attended_override == 1)
                    ? $rc->attended_override
                    : ($rc->attended ?? 1);

                $this->dwh->table('Fact_Rider_Course')->updateOrInsert(
                    [
                        'Source_Rider_Id' => $rc->rider_id,
                        'Source_Course_Id' => $rc->course_id,
                        'Source_System_Key' => $sourceSystemKey
                    ],
                    [
                        'Status' => $rc->status ?? 0,
                        'Attended' => $finalAttendance,
                        'Withdrawn' => $rc->withdrawn ?? 0,
                        'Withdrawal_Reason' => $rc->withdrawal_reason,
                        'Has_Completed_Course' => $rc->has_completed_course ?? 0,
                        'Has_Survey_Completed' => $rc->has_survey_completed ?? 0,
                        'Course_Complete_Date' => $rc->course_complete_date,
                        'Source_Created_At' => $rc->created_at,
                        'Source_Updated_At' => $rc->updated_at,
                        'Updated_At' => now()
                    ]
                );

                //Track the watermark
                if ($rc->updated_at > $highestTimestamp) {
                    $highestTimestamp = $rc->updated_at;
                }

                if ($bar) $bar->advance();
            }
        });

        // 5. Commit the watermark back down into your tracking state database matrix table row
        $this->updateWatermark('Fact_Rider_Course', $highestTimestamp);

        if ($bar) {
            $bar->finish();
            $command->newLine();
        }

        return "Fact_Rider_Course synced.";
    }

    /**
     * Synchronize Core Allocation Matrix
     */
    public function syncFactGrantFinancials($command = null)
    {
        $sourceSystemKey = $this->getSourceSystemKey();
        if ($command) $command->info("[" . now()->format('Y-m-d H:i:s') . "] Starting Facts - Grant Financials Sync...");

        $watermark = $this->getWatermark('Fact_Grant_Financials');

        $query = $this->source->table('grant_format_dft')
            ->where('updated_at', '>', $watermark)
            ->orderBy('updated_at', 'asc');

        $total = $query->count();
        if ($total === 0) {
            if ($command) $command->info("[" . now()->format('Y-m-d H:i:s') . "] Facts - Grant Financials are already up to date.");
            return;
        }

        $bar = $command ? $command->getOutput()->createProgressBar($total) : null;
        if ($bar) $bar->start();

        $highestTimestamp = $watermark;

        $query->chunk(250, function ($formats) use ($sourceSystemKey, $bar, &$highestTimestamp) {
            foreach ($formats as $fmt) {
                $grantKey = $this->resolveGrantKey($fmt->grant_id, $sourceSystemKey);
                if (!$grantKey) {
                    if ($bar) $bar->advance();
                    continue;
                }

                $this->dwh->table('Fact_Grant_Financials')->updateOrInsert(
                    ['Source_Format_Id' => $fmt->id, 'Source_System_Key' => $sourceSystemKey],
                    [
                        'Grant_Key' => $grantKey,
                        'Max_Allocation' => $fmt->max_allocation ?? 0.00,
                        'Total_Levels' => $fmt->total_levels ?? 0,
                        'Total_Plus' => $fmt->total_plus ?? 0,
                        'Places_Level_1' => $fmt->places_level_1 ?? 0,
                        'Grant_Level_1' => $fmt->grant_level_1 ?? 0.00,
                        'Places_Level_1_2' => $fmt->places_level_1_2 ?? 0,
                        'Grant_Level_1_2' => $fmt->grant_level_1_2 ?? 0.00,
                        'Places_Level_2' => $fmt->places_level_2 ?? 0,
                        'Grant_Level_2' => $fmt->grant_level_2 ?? 0.00,
                        'Places_Level_3' => $fmt->places_level_3 ?? 0,
                        'Grant_Level_3' => $fmt->grant_level_3 ?? 0.00,
                        'Places_Plus_Balance' => $fmt->places_plus_balance ?? 0,
                        'Grant_Plus_Balance' => $fmt->grant_plus_balance ?? 0.00,
                        'Places_Plus_Bus' => $fmt->places_plus_bus ?? 0,
                        'Grant_Plus_Bus' => $fmt->grant_plus_bus ?? 0.00,
                        'Places_Plus_Fix' => $fmt->places_plus_fix ?? 0,
                        'Grant_Plus_Fix' => $fmt->grant_plus_fix ?? 0.00,
                        'Places_Plus_Learn' => $fmt->places_plus_learn ?? 0,
                        'Grant_Plus_Learn' => $fmt->grant_plus_learn ?? 0.00,
                        'Places_Plus_On_Show' => $fmt->places_plus_on_show ?? 0,
                        'Grant_Plus_On_Show' => $fmt->grant_plus_on_show ?? 0.00,
                        'Places_Plus_Parents' => $fmt->places_plus_parents ?? 0,
                        'Grant_Plus_Parents' => $fmt->grant_plus_parents ?? 0.00,
                        'Places_Plus_Promotion' => $fmt->places_plus_promotion ?? 0,
                        'Grant_Plus_Promotion' => $fmt->grant_plus_promotion ?? 0.00,
                        'Places_Plus_Recycled' => $fmt->places_plus_recycled ?? 0,
                        'Grant_Plus_Recycled' => $fmt->grant_plus_recycled ?? 0.00,
                        'Places_Plus_Ride' => $fmt->places_plus_ride ?? 0,
                        'Grant_Plus_Ride' => $fmt->grant_plus_ride ?? 0.00,
                        'Places_Plus_Transition' => $fmt->places_plus_transition ?? 0,
                        'Grant_Plus_Transition' => $fmt->grant_plus_transition ?? 0.00,
                        'Places_Plus_Family' => $fmt->places_plus_family ?? 0,
                        'Grant_Plus_Family' => $fmt->grant_plus_family ?? 0.00,
                        'Places_Plus_Adult' => $fmt->places_plus_adult ?? 0,
                        'Grant_Plus_Adult' => $fmt->grant_plus_adult ?? 0.00,
                        'Grant_Send' => $fmt->grant_send ?? 0.00,
                        'Places_Send' => $fmt->places_send ?? 0,
                        'Grant_Inclusion' => $fmt->grant_inclusion ?? 0.00,
                        'Places_Inclusion' => $fmt->places_inclusion ?? 0,
                        'updated_at' => now()
                    ]
                );

                if ($fmt->updated_at > $highestTimestamp) $highestTimestamp = $fmt->updated_at;
                if ($bar) $bar->advance();
            }
        });

        $this->updateWatermark('Fact_Grant_Financials', $highestTimestamp);

        if ($bar) {
            $bar->finish();
            $command->newLine();
        }

        return "Fact_Grant_Financials synced.";
    }

    /**
     * Sync Reallocations (Header + Child Logs Split)
     */
    public function syncFactGrantReallocations($command = null)
    {
        $sourceSystemKey = $this->getSourceSystemKey();
        if ($command) $command->info("[" . now()->format('Y-m-d H:i:s') . "] Starting Grant Reallocations Sync...");

        $watermark = $this->getWatermark('Fact_Grant_Reallocations');
        $query = $this->source->table('grant_reallocations')
            ->where('updated_at', '>', $watermark)
            ->orderBy('updated_at', 'asc');

        $total = $query->count();
        if ($total === 0) {
            if ($command) $command->info("[" . now()->format('Y-m-d H:i:s') . "] Grant Reallocations are already up to date.");
            return;
        }

        $bar = $command ? $command->getOutput()->createProgressBar($total) : null;
        if ($bar) $bar->start();

        $highestTimestamp = $watermark;

        $query->chunk(250, function ($reallocs) use ($sourceSystemKey, $bar, &$highestTimestamp) {
            foreach ($reallocs as $ra) {
                $grantKey = $this->resolveGrantKey($ra->grant_id, $sourceSystemKey);
                if (!$grantKey) {
                    if ($bar) $bar->advance();
                    continue;
                }

                $existingKeys = $this->dwh->table('Fact_Grant_Reallocations')
                    ->where('Source_Reallocation_Id', $ra->id)
                    ->where('Source_System_Key', $sourceSystemKey)
                    ->pluck('Reallocation_Key')
                    ->toArray();

                if (count($existingKeys) > 0) {
                    $this->dwh->table('Fact_Grant_Reallocation_Logs')->whereIn('Reallocation_Key', $existingKeys)->delete();
                    $this->dwh->table('Fact_Grant_Reallocations')->whereIn('Reallocation_Key', $existingKeys)->delete();
                }

                $reallocationKey = $this->dwh->table('Fact_Grant_Reallocations')->insertGetId([
                    'Grant_Key' => $grantKey,
                    'Source_System_Key' => $sourceSystemKey,
                    'Source_Reallocation_Id' => $ra->id,
                    'Reallocation_Number' => $ra->reallocation_number,
                    'Status_Raw' => $ra->status,
                    'Date_Approved' => $ra->date_approved,
                    'Reallocation_Type' => $ra->reallocation_type,
                    'Reallocation_Notes' => $ra->reallocation_notes,
                    'Reallocation_Increase_Reasons' => $ra->reallocation_increase_reasons,
                    'created_at' => now(), 'updated_at' => now()
                ]);

                if (!$this->sourceHasColumn('grant_reallocations', 'reallocation_log')) {
                    $logs = $this->source->table('grant_reallocation_logs')->where('grant_reallocation_id', $ra->id)->get();
                    foreach ($logs as $log) {
                        $this->dwh->table('Fact_Grant_Reallocation_Logs')->insert([
                            'Reallocation_Key' => $reallocationKey,
                            'Source_System_Key' => $sourceSystemKey,
                            'Module_Key' => $log->module_key,
                            'Value_Count' => $log->value,
                            'Amount' => $log->amount,
                            'created_at' => now(), 'updated_at' => now()
                        ]);
                    }
                } else {
                    $logs = json_decode($ra->reallocation_log, true) ?? [];
                    foreach ($logs as $key => $node) {
                        $this->dwh->table('Fact_Grant_Reallocation_Logs')->insert([
                            'Reallocation_Key' => $reallocationKey,
                            'Source_System_Key' => $sourceSystemKey,
                            'Module_Key' => $key,
                            'Value_Count' => $node['value'] ?? 0,
                            'Amount' => $node['amount'] ?? 0.00,
                            'created_at' => now(), 'updated_at' => now()
                        ]);
                    }
                }

                if ($ra->updated_at > $highestTimestamp) $highestTimestamp = $ra->updated_at;
                if ($bar) $bar->advance();
            }
        });

        $this->updateWatermark('Fact_Grant_Reallocations', $highestTimestamp);

        if ($bar) {
            $bar->finish();
            $command->newLine();
        }
        return "Fact_Grant_Reallocations synced.";
    }

    /**
     * Sync Amendments (Header + Child Logs Split)
     */
    public function syncFactGrantAmendments($command = null)
    {
        $sourceSystemKey = $this->getSourceSystemKey();
        if ($command) $command->info("[" . now()->format('Y-m-d H:i:s') . "] Starting Grant Amendments Sync...");

        $watermark = $this->getWatermark('Fact_Grant_Amendments');
        $query = $this->source->table('grant_amendments')
            ->where('updated_at', '>', $watermark)
            ->orderBy('updated_at', 'asc');

        $total = $query->count();
        if ($total === 0) {
            if ($command) $command->info("[" . now()->format('Y-m-d H:i:s') . "] Grant Amendments are already up to date.");
            return;
        }

        $bar = $command ? $command->getOutput()->createProgressBar($total) : null;
        if ($bar) $bar->start();

        $highestTimestamp = $watermark;

        $query->chunk(250, function ($amends) use ($sourceSystemKey, $bar, &$highestTimestamp) {
            foreach ($amends as $am) {
                $grantKey = $this->resolveGrantKey($am->grant_id, $sourceSystemKey);
                if (!$grantKey) {
                    if ($bar) $bar->advance();
                    continue;
                }

                $existingKeys = $this->dwh->table('Fact_Grant_Amendments')
                    ->where('Source_Amendment_Id', $am->id)
                    ->where('Source_System_Key', $sourceSystemKey)
                    ->pluck('Amendment_Key')
                    ->toArray();

                if (count($existingKeys) > 0) {
                    $this->dwh->table('Fact_Grant_Amendment_Logs')->whereIn('Amendment_Key', $existingKeys)->delete();
                    $this->dwh->table('Fact_Grant_Amendments')->whereIn('Amendment_Key', $existingKeys)->delete();
                }

                $amendmentKey = $this->dwh->table('Fact_Grant_Amendments')->insertGetId([
                    'Grant_Key' => $grantKey,
                    'Source_System_Key' => $sourceSystemKey,
                    'Source_Amendment_Id' => $am->id,
                    'Amendment_Number' => $am->amendment_number,
                    'Status_Raw' => $am->status,
                    'Date_Approved' => $am->date_approved,
                    'created_at' => now(), 'updated_at' => now()
                ]);

                if (!$this->sourceHasColumn('grant_amendments', 'amendment_log')) {
                    $logs = $this->source->table('grant_amendment_logs')->where('grant_amendment_id', $am->id)->get();
                    foreach ($logs as $log) {
                        $this->dwh->table('Fact_Grant_Amendment_Logs')->insert([
                            'Amendment_Key' => $amendmentKey,
                            'Source_System_Key' => $sourceSystemKey,
                            'Type_Label' => $log->type,
                            'Module_Key' => $log->module,
                            'Value_Count' => $log->value,
                            'Amount' => $log->amount,
                            'created_at' => now(), 'updated_at' => now()
                        ]);
                    }
                } else {
                    $logs = json_decode($am->amendment_log, true) ?? [];
                    foreach ($logs as $key => $node) {
                        $this->dwh->table('Fact_Grant_Amendment_Logs')->insert([
                            'Amendment_Key' => $amendmentKey,
                            'Source_System_Key' => $sourceSystemKey,
                            'Type_Label' => $node['type'] ?? null,
                            'Module_Key' => $node['module'] ?? $key,
                            'Value_Count' => $node['value'] ?? 0,
                            'Amount' => $node['amount'] ?? 0.00,
                            'created_at' => now(), 'updated_at' => now()
                        ]);
                    }
                }

                if ($am->updated_at > $highestTimestamp) $highestTimestamp = $am->updated_at;
                if ($bar) $bar->advance();
            }
        });

        $this->updateWatermark('Fact_Grant_Amendments', $highestTimestamp);

        if ($bar) {
            $bar->finish();
            $command->newLine();
        }
        return "Fact_Grant_Amendments synced.";
    }

    /**
     * Sync Claims and all their downstream child metrics into separate dedicated buckets
     */
    public function syncFactGrantClaims($command = null)
    {
        $sourceSystemKey = $this->getSourceSystemKey();
        if ($command) $command->info("[" . now()->format('Y-m-d H:i:s') . "] Starting Grant Claims Sync...");

        $watermark = $this->getWatermark('Fact_Grant_Claims');
        $query = $this->source->table('grant_claims')
            ->where('updated_at', '>', $watermark)
            ->orderBy('updated_at', 'asc');

        $total = $query->count();
        if ($total === 0) {
            if ($command) $command->info("[" . now()->format('Y-m-d H:i:s') . "] Grant Claims are already up to date.");
            return;
        }

        $bar = $command ? $command->getOutput()->createProgressBar($total) : null;
        if ($bar) $bar->start();

        $highestTimestamp = $watermark;

        $query->chunk(250, function ($claims) use ($sourceSystemKey, $bar, &$highestTimestamp) {
            foreach ($claims as $cl) {
                $grantKey = $this->resolveGrantKey($cl->grant_id, $sourceSystemKey);
                if (!$grantKey) {
                    if ($bar) $bar->advance();
                    continue;
                }

                $existingClaimKeys = $this->dwh->table('Fact_Grant_Claims')
                    ->where('Source_Claim_Id', $cl->id)
                    ->where('Source_System_Key', $sourceSystemKey)
                    ->pluck('Claim_Key')
                    ->toArray();

                if (count($existingClaimKeys) > 0) {
                    $this->dwh->table('Fact_Grant_Claim_Inclusions')->whereIn('Claim_Key', $existingClaimKeys)->delete();
                    $this->dwh->table('Fact_Grant_Claim_Send_Records')->whereIn('Claim_Key', $existingClaimKeys)->delete();
                    $this->dwh->table('Fact_Grant_Claim_Logs')->whereIn('Claim_Key', $existingClaimKeys)->delete();
                    $this->dwh->table('Fact_Grant_Claims')->whereIn('Claim_Key', $existingClaimKeys)->delete();
                }

                $claimKey = $this->dwh->table('Fact_Grant_Claims')->insertGetId([
                    'Grant_Key' => $grantKey,
                    'Source_System_Key' => $sourceSystemKey,
                    'Source_Claim_Id' => $cl->id,
                    'Claim_Number' => $cl->claim_number,
                    'Status_Raw' => $cl->status,
                    'Pref_Authority_Given' => (bool)$cl->pref_authority_given,
                    'Pref_Claim_Paid' => (bool)$cl->pref_claim_paid,
                    'Date_Approved' => $cl->date_approved,
                    'Delivery_On_Track_Prediction' => $cl->delivery_on_track_prediction,
                    'Send_Claimable_Amount' => $cl->send_claimable_amount,
                    'Inclusion_Claimable_Amount' => $cl->inclusion_claimable_amount,
                    'created_at' => now(), 'updated_at' => now()
                ]);

                if (!$this->sourceHasColumn('grant_claims', 'claim_log')) {
                    $logs = $this->source->table('grant_claim_logs')->where('grant_claim_id', $cl->id)->get();
                    foreach ($logs as $log) {
                        $this->dwh->table('Fact_Grant_Claim_Logs')->insert([
                            'Claim_Key' => $claimKey, 'Source_System_Key' => $sourceSystemKey,
                            'Module_Key' => $log->module_key, 'Item_Count' => $log->item_count,
                            'created_at' => now(), 'updated_at' => now()
                        ]);
                    }
                } else {
                    $logs = json_decode($cl->claim_log, true) ?? [];
                    foreach ($logs as $key => $count) {
                        $this->dwh->table('Fact_Grant_Claim_Logs')->insert([
                            'Claim_Key' => $claimKey, 'Source_System_Key' => $sourceSystemKey,
                            'Module_Key' => $key, 'Item_Count' => $count,
                            'created_at' => now(), 'updated_at' => now()
                        ]);
                    }
                }

                if (!$this->sourceHasColumn('grant_claims', 'send_records')) {
                    $sends = $this->source->table('grant_claim_send_records')->where('grant_claim_id', $cl->id)->get();
                    foreach ($sends as $s) {
                        $this->dwh->table('Fact_Grant_Claim_Send_Records')->insert([
                            'Claim_Key' => $claimKey, 'Source_System_Key' => $sourceSystemKey,
                            'Send_Id_String' => $s->send_id_string,
                            'Send_Riders_Count' => $s->send_riders,
                            'Send_Amount' => $s->send_amount ?? 0.00,
                            'created_at' => now(), 'updated_at' => now()
                        ]);
                    }
                } else {
                    $sends = json_decode($cl->send_records, true) ?? [];
                    foreach ($sends as $node) {
                        $this->dwh->table('Fact_Grant_Claim_Send_Records')->insert([
                            'Claim_Key' => $claimKey, 'Source_System_Key' => $sourceSystemKey,
                            'Send_Id_String' => $node['id_string'] ?? 'raw',
                            'Send_Riders_Count' => $node['riders'] ?? 0,
                            'Send_Amount' => $node['amount'] ?? 0.00,
                            'created_at' => now(), 'updated_at' => now()
                        ]);
                    }
                }

                if (!$this->sourceHasColumn('grant_claims', 'inclusion_records')) {
                    $inclusions = $this->source->table('grant_claim_inclusions')->where('grant_claim_id', $cl->id)->get();
                    foreach ($inclusions as $inc) {
                        $this->dwh->table('Fact_Grant_Claim_Inclusions')->insert([
                            'Claim_Key' => $claimKey, 'Source_System_Key' => $sourceSystemKey,
                            'Inclusion_Id_String' => $inc->inclusion_id_string,
                            'Inclusion_Category' => $inc->inclusion_category,
                            'Inclusion_Delivery' => $inc->inclusion_delivery,
                            'Inclusion_Amount' => $inc->inclusion_amount ?? 0.00,
                            'created_at' => now(), 'updated_at' => now()
                        ]);
                    }
                } else {
                    $inclusions = json_decode($cl->inclusion_records, true) ?? [];
                    foreach ($inclusions as $node) {
                        $this->dwh->table('Fact_Grant_Claim_Inclusions')->insert([
                            'Claim_Key' => $claimKey, 'Source_System_Key' => $sourceSystemKey,
                            'Inclusion_Id_String' => $node['id_string'] ?? 'raw',
                            'Inclusion_Category' => $node['category'] ?? 'N/A',
                            'Inclusion_Delivery' => $node['delivery'] ?? null,
                            'Inclusion_Amount' => $node['amount'] ?? 0.00,
                            'created_at' => now(), 'updated_at' => now()
                        ]);
                    }
                }

                if ($cl->updated_at > $highestTimestamp) $highestTimestamp = $cl->updated_at;
                if ($bar) $bar->advance();
            }
        });

        $this->updateWatermark('Fact_Grant_Claims', $highestTimestamp);

        if ($bar) {
            $bar->finish();
            $command->newLine();
        }
        return "Fact_Grant_Claims synced.";
    }

    public function syncFactInstructorDeliveries($command = null)
    {
        $sourceSystemKey = $this->getSourceSystemKey();
        if ($command) $command->info("[" . now()->format('Y-m-d H:i:s') . "] Starting Instructor-Deliveries Sync...");

        // 1. Resolve Watermark
        $watermark = $this->getWatermark('Fact_Instructor_Delivery');

        // 2. Query Source delta (including soft deleted rows so we sync the deletion status)
        $query = $this->source->table('join_deliveries_instructors')
            ->where('updated_at', '>', $watermark)
            ->orderBy('updated_at', 'asc');

        $total = $query->count();
        if ($total === 0) {
            if ($command) $command->info("[" . now()->format('Y-m-d H:i:s') . "] Instructor-Deliveries are up to date.");
            return;
        }

        $bar = $command ? $command->getOutput()->createProgressBar($total) : null;
        if ($bar) $bar->start();

        $highestTimestampSeen = $watermark;

        // 3. Process Chunk Loop
        $query->chunk(500, function ($allocations) use ($sourceSystemKey, $bar, &$highestTimestampSeen) {
            foreach ($allocations as $alloc) {

                // Resolve DWH Keys securely from parent dimensions
                $deliveryKey = $this->dwh->table('Dim_Delivery_Header')
                    ->where('Source_Delivery_Id', $alloc->delivery_id)
                    ->where('Source_System_Key', $sourceSystemKey)
                    ->value('Delivery_Key');

                $instructorKey = $this->dwh->table('Dim_Instructor')
                    ->where('Source_Instructor_Id', $alloc->instructor_id)
                    ->where('Source_System_Key', $sourceSystemKey)
                    ->value('Instructor_Key');

                // Enforce referential data integrity
                if (!$deliveryKey || !$instructorKey) {
                    if ($bar) $bar->advance();
                    continue;
                }

                $this->dwh->table('Fact_Instructor_Delivery')->updateOrInsert(
                    [
                        'Delivery_Key' => $deliveryKey,
                        'Instructor_Key' => $instructorKey,
                        'Source_System_Key' => $sourceSystemKey
                    ],
                    [
                        'Instructor_Notified' => (bool)$alloc->instructor_notified,
                        'Source_Created_At' => $alloc->created_at,
                        'Source_Updated_At' => $alloc->updated_at,
                        'Source_Deleted_At' => $alloc->deleted_at,
                        'updated_at' => now()
                    ]
                );

                if ($alloc->updated_at > $highestTimestampSeen) {
                    $highestTimestampSeen = $alloc->updated_at;
                }

                if ($bar) $bar->advance();
            }
        });

        // 4. Record safe watermark state
        $this->updateWatermark('Fact_Instructor_Delivery', $highestTimestampSeen);

        if ($bar) {
            $bar->finish();
            $command->newLine();
        }
        return "Fact_Instructor_Delivery synced.";

    }

    public function syncFactInstructorCourse($command = null)
    {
        $sourceSystemKey = $this->getSourceSystemKey();
        $now = now()->toDateTimeString();

        if ($command) $command->info("[{$now}] Starting Instructor-Course Pivot Sync...");

        // 1. Fetch all currently active allocations from the SOURCE database
        // Pluck as "instructor_id-course_id" strings for instant O(1) lookups in PHP memory
        $sourcePairs = $this->source->table('join_instructors_courses') // replace with actual source table name
        ->select('instructor_id', 'course_id')
            ->get()
            ->map(function ($row) {
                return "{$row->instructor_id}-{$row->course_id}";
            })
            ->toArray();

        // 2. Fetch all rows the DWH currently considers active
        $dwhActiveRows = $this->dwh->table('Fact_Instructor_Course')
            ->join('Dim_Instructor', 'Fact_Instructor_Course.Instructor_Key', '=', 'Dim_Instructor.Instructor_Key')
            ->join('Dim_Course', 'Fact_Instructor_Course.Course_Key', '=', 'Dim_Course.Course_Key')
            ->where('Fact_Instructor_Course.Is_Current', 1)
            ->where('Fact_Instructor_Course.Source_System_Key', $sourceSystemKey)
            ->select([
                'Fact_Instructor_Course.Instructor_Course_Key',
                'Dim_Instructor.Source_Instructor_Id',
                'Dim_Course.Source_Course_Id'
            ])
            ->get();

        $dwhActivePairs = [];
        foreach ($dwhActiveRows as $row) {
            $dwhActivePairs["{$row->Source_Instructor_Id}-{$row->Source_Course_Id}"] = $row->Instructor_Course_Key;
        }

        // Detect Removals (Relationship disappeared) ---
        // If it's active in DWH but missing from Source, it was deleted. Close it off.
        $closedCount = 0;
        foreach ($dwhActivePairs as $pairStr => $dwhKey) {
            if (!in_array($pairStr, $sourcePairs)) {
                $this->dwh->table('Fact_Instructor_Course')
                    ->where('Instructor_Course_Key', $dwhKey)
                    ->update([
                        'Active_To' => $now,
                        'Is_Current' => 0
                    ]);
                $closedCount++;
            }
        }

        // Detect additions (New Relationship)
        // If it's in Source but missing from DWH active list, it's brand new.
        $newCount = 0;

        // Process additions in chunks of source array keys to handle memory safely
        $pairsToInsert = array_diff($sourcePairs, array_keys($dwhActivePairs));

        if ($command && count($pairsToInsert) > 0) {
            $bar = $command->getOutput()->createProgressBar(count($pairsToInsert));
            $bar->start();
        }

        foreach ($pairsToInsert as $pairStr) {
            [$sourceInstructorId, $sourceCourseId] = explode('-', $pairStr);

            // Resolve DWH Dimension keys safely
            $instructorKey = $this->dwh->table('Dim_Instructor')
                ->where('Source_Instructor_Id', $sourceInstructorId)
                ->where('Source_System_Key', $sourceSystemKey)
                ->value('Instructor_Key');

            $courseKey = $this->dwh->table('Dim_Course')
                ->where('Source_Course_Id', $sourceCourseId)
                ->where('Source_System_Key', $sourceSystemKey)
                ->value('Course_Key');

            // If dimensions match, write the open-ended active row
            if ($instructorKey && $courseKey) {
                $this->dwh->table('Fact_Instructor_Course')->insert([
                    'Instructor_Key' => $instructorKey,
                    'Course_Key' => $courseKey,
                    'Source_System_Key' => $sourceSystemKey,
                    'Active_From' => $now,
                    'Active_To' => null,
                    'Is_Current' => 1
                ]);
                $newCount++;
            }

            if (isset($bar)) $bar->advance();
        }

        if (isset($bar)) $bar->finish();

        return "Fact_Instructor_Course synced.";

    }

    public function syncFactParentSurvey($command = null)
    {
        $sourceSystemKey = $this->getSourceSystemKey();

        $useLegacyFeedback = \Illuminate\Support\Facades\Schema::connection('mysql_src')
            ->hasColumn('parent_survey', 'rider_feedback');

        $watermark = $this->dwh->table('Sync_Log')
            ->where('Table_Name', 'Fact_Parent_Survey')
            ->value('Last_Synced_At') ?? '1900-01-01 00:00:00';

        $highestTimestampSeen = $watermark;

        $surveyData = $this->source->table('parent_survey')
            ->where('status', 1)
            ->where('updated_at', '>', $watermark)
            ->select('id', 'updated_at')
            ->get();

        $surveyIds = $surveyData->pluck('id');
        $maxTimestamp = $surveyData->max('updated_at');

        if ($surveyIds->isEmpty()) {
            return "Parent Survey Facts are up to date.";
        }

        $total = count($surveyIds);
        $bar = $command ? $command->getOutput()->createProgressBar($total) : null;
        if ($bar) $bar->start();

        $this->source->table('parent_survey')
            ->whereIn('id', $surveyIds)
            ->orderBy('updated_at', 'asc')
            ->chunk(200, function ($surveys) use ($sourceSystemKey, $bar, $useLegacyFeedback) {
                foreach ($surveys as $survey) {
                    //Logic to Resolve Keys: $riderKey, $courseKey, $delivery
                    $riderKey = $this->dwh->table('Dim_Rider')->where('Source_Rider_Id', $survey->rider_id)->value('Rider_Key');
                    $courseKey = $this->dwh->table('Dim_Course')->where('Source_Course_Id', $survey->course_id)->value('Course_Key');
                    $delivery = $this->dwh->table('Dim_Delivery_Header')->where('Source_Delivery_Id', $survey->delivery_id)->first();

                    if (!$riderKey || !$courseKey || !$delivery) continue;

                    // Resolve Multi-select Feedback
                    if ($useLegacyFeedback) {
                        $feedbackKeys = json_decode($survey->rider_feedback ?? '[]', true);
                    } else {
                        $feedbackKeys = $this->source->table('parent_survey_feedback')
                            ->where('parent_survey_id', $survey->id)
                            ->pluck('feedback_key')
                            ->toArray();
                    }

                    // 1. Handle OQ1 Multi-choice (Encouragers)
                    $oq1_data = json_decode($survey->optional_questions, true) ?? [];
                    $encouragers = [
                        'Encourage_More_Direct_Routes' => in_array('oq1_o1', $oq1_data) ? 1 : 0,
                        'Encourage_Local_Route_Awareness' => in_array('oq1_o2', $oq1_data) ? 1 : 0,
                        'Encourage_Storage' => in_array('oq1_o3', $oq1_data) ? 1 : 0,
                        'Encourage_Road_Surfaces' => in_array('oq1_o4', $oq1_data) ? 1 : 0,
                        'Encourage_Confidence' => in_array('oq1_o5', $oq1_data) ? 1 : 0,
                        'Encourage_Cycle_Maintenance' => in_array('oq1_o6', $oq1_data) ? 1 : 0,
                        'Encourage_Local_Initiatives' => in_array('oq1_o7', $oq1_data) ? 1 : 0,
                        'Encourage_Purchase_Ability' => in_array('oq1_o8', $oq1_data) ? 1 : 0,
                        'Encourage_Doesnt_Want_To_Cycle_More' => in_array('oq1_o9', $oq1_data) ? 1 : 0,
                        'Encourage_None' => in_array('oq1_null', $oq1_data) ? 1 : 0,
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
                        'Likert_Life_Skill' => $pivoted['oq2'] ?? null, // oq2
                        'Likert_Self_Esteem' => $pivoted['oq3'] ?? null, // oq3
                        'Likert_Fitness' => $pivoted['oq4'] ?? null, // oq4
                        'Likert_Active' => $pivoted['oq5'] ?? null, // oq5
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
                        array_merge($encouragers, $likertMetrics, [
                            'Date_Key' => str_replace('-', '', substr($survey->created_at, 0, 10)),
                            'Rider_Key' => $riderKey,
                            'Course_Key' => $courseKey,
                            'Delivery_Key' => $delivery->Delivery_Key,
                            'Grant_Key' => $delivery->Grant_Key,

                            'Like_To_Participate' => $survey->like_to_participation,
                            'Like_To_Answer_Survey' => $survey->like_to_answer_survey,
                            'Pref_Join_Bikeability' => $survey->pref_join_bikeability,
                            'Rider_Emotion' => $survey->rider_emotion,
                            'Pref_More_Training' => $survey->pref_more_training,
                            'Pref_Interest_In_Training' => $survey->pref_interest_in_training,

                            // Feedback Flags
                            'Feedback_Is_Fun' => in_array('rfq1_rf1', $feedbackKeys) ? 1 : 0,
                            'Feedback_Is_Hard' => in_array('rfq1_rf2', $feedbackKeys) ? 1 : 0,
                            'Feedback_Is_Healthy' => in_array('rfq1_rf3', $feedbackKeys) ? 1 : 0,
                            'Feedback_Still_New' => in_array('rfq1_rf4', $feedbackKeys) ? 1 : 0,
                            'Feedback_Family_Friends' => in_array('rfq1_rf5', $feedbackKeys) ? 1 : 0,
                            'Feedback_Dont_See_Others_Like_Me' => in_array('rfq1_rf6', $feedbackKeys) ? 1 : 0,
                            'Feedback_On_Own' => in_array('rfq1_rf7', $feedbackKeys) ? 1 : 0,
                            'Feedback_Not_Enjoy' => in_array('rfq1_rf8', $feedbackKeys) ? 1 : 0,
                            'Feedback_None_Apply' => in_array('rfq1_null', $feedbackKeys) ? 1 : 0,
                            'Feedback_None_Apply_Input' => $survey->rider_feedback_input,

                            // Store as Clean Integers (1-6)
                            'Confidence_Bike_General' => $this->extractOptionInt('co', $survey->confidence_to_use_a_bike),
                            'Confidence_Road' => $this->extractOptionInt('co', $survey->confidence_to_use_bike_on_road),
                            'Confidence_Independent' => $this->extractOptionInt('co', $survey->confidence_to_use_bike_independently),

                            //Frequency Flags
                            'Frequency_School' => $this->extractOptionInt('fo', $survey->frequency_usable_to_and_from_school),
                            'Frequency_Leisure' => $this->extractOptionInt('fo', $survey->frequency_usable_leisure_and_social),
                            'Frequency_Exercise' => $this->extractOptionInt('fo', $survey->frequency_usable_exercise),

                            //Encouragment
                            'Encouragement_Use_Bike' => $this->extractOptionInt('eo', $survey->encouragement_to_use_a_bike),
                            'Encouragement_Use_Bike_On_Road' => $this->extractOptionInt('eo', $survey->encouragement_to_use_bike_on_road),

                            //Recommend
                            'Likely_To_Recommend' => $survey->like_to_recommend,

                            //Timestamps
                            'Source_Created_At' => $survey->created_at,
                            'Source_Updated_At' => $survey->updated_at
                        ])
                    );

                    if ($bar) $bar->advance();
                }
            });

        if ($maxTimestamp && $maxTimestamp > $highestTimestampSeen) {
            $highestTimestampSeen = $maxTimestamp;
        }

        $this->dwh->table('Sync_Log')->updateOrInsert(
            ['Table_Name' => 'Fact_Parent_Survey'],
            ['Last_Synced_At' => $highestTimestampSeen]
        );

        if ($bar) {
            $bar->finish();
            $command->newLine();
        }
        return "Parent Survey Fact synced with integer-based Likert scores.";
    }

    public function syncFactHandsupSurvey($command = null)
    {
        $useLegacySurvey = \Illuminate\Support\Facades\Schema::connection('mysql_src')
            ->hasColumn('courses', 'survey_details');

        $watermark = $this->dwh->table('Sync_Log')
            ->where('Table_Name', 'Fact_HandsUp_Survey')
            ->value('Last_Synced_At') ?? '1900-01-01 00:00:00';

        $highestTimestampSeen = $watermark;


        if ($useLegacySurvey) {
            $courseData = $this->source->table('courses')
                ->whereNotNull('survey_details')
                ->where('updated_at', '>', $watermark)
                ->select('id', 'updated_at')
                ->get();

            $courseIds = $courseData->pluck('id');
            $maxTimestamp = $courseData->max('updated_at');
        } else {
            $resultData = $this->source->table('course_survey_results')
                ->where('updated_at', '>', $watermark)
                ->select('course_id', 'updated_at')
                ->get();

            $courseIds = $resultData->pluck('course_id')->distinct();
            $maxTimestamp = $resultData->max('updated_at');
        }

        if ($courseIds->isEmpty()) {
            return "Hands-up Facts are up to date.";
        }

        $bar = $command ? $command->getOutput()->createProgressBar(count($courseIds)) : null;
        if ($bar) $bar->start();

        foreach ($courseIds as $sourceCourseId) {
            $course = $this->dwh->table('Dim_Course')->where('Source_Course_Id', $sourceCourseId)->first();
            if (!$course) continue;

            // Initialize the metrics array
            $m = $this->initializeHandsupMetrics();

            if ($useLegacySurvey) {
                // Parse from JSON - data hasnt been normalised yet
                $legacyData = $this->source->table('courses')->where('id', $sourceCourseId)->value('survey_details');
                $m = $this->aggregateHandsupFromLegacyJson($legacyData, $m);
            } else {
                // Aggregated from Normalized table
                $results = $this->source->table('course_survey_results')->where('course_id', $sourceCourseId)->get();
                $m = $this->aggregateHandsupFromSourceRows($results, $m);
            }

            // Upsert the single row for this Course
            $this->dwh->table('Fact_HandsUp_Survey')->updateOrInsert(
                ['Course_Key' => $course->Course_Key],
                array_merge($m, [
                    'Date_Key' => $course->Start_Date ? str_replace('-', '', substr($course->Start_Date, 0, 10)) : null,
                    'Delivery_Key' => $course->Delivery_Key,
                ])
            );

            if ($bar) $bar->advance();
        }

        if ($maxTimestamp && $maxTimestamp > $highestTimestampSeen) {
            $highestTimestampSeen = $maxTimestamp;
        }

        $this->dwh->table('Sync_Log')->updateOrInsert(
            ['Table_Name' => 'Fact_HandsUp_Survey'],
            ['Last_Synced_At' => $highestTimestampSeen]
        );

        if ($bar) {
            $bar->finish();
            $command->newLine();
        }
        return "Hands-up Facts synced (Mapped from pre-aggregated source).";
    }

    private function aggregateHandsupFromLegacyJson($jsonString, $m)
    {
        $questions = json_decode($jsonString ?? '[]', true);

        foreach ($questions as $q) {
            $qid = $q['question_id'] ?? null;
            $options = $q['option'] ?? [];

            foreach ($options as $opt) {
                // Reuse the shared mapping logic
                $m = $this->mapHandsupToBucket($qid, $opt['id'], $opt['total'], $m);
            }
        }
        return $m;
    }

    private function aggregateHandsupFromSourceRows($results, $m)
    {
        foreach ($results as $res) {
            // Reuse the shared mapping logic
            $m = $this->mapHandsupToBucket($res->question_id, $res->option_id, $res->total, $m);
        }
        return $m;
    }

    /**
     * Helper function to set the array up
     */
    private function initializeHandsupMetrics()
    {
        return [
            'Exp_Enjoyed' => 0, 'Exp_Did_Not_Enjoy' => 0, 'Exp_Not_Sure' => 0, 'Exp_Absent' => 0,
            'Base_Yes' => 0, 'Base_No' => 0, 'Base_Not_Sure' => 0,
            'Safe_More' => 0, 'Safe_Less' => 0, 'Safe_No_Diff' => 0, 'Safe_Not_Sure' => 0,
            'Conf_More' => 0, 'Conf_Less' => 0, 'Conf_No_Diff' => 0, 'Conf_Not_Sure' => 0,
        ];
    }

    /**
     * Core Mapping for Hands Up metrics
     */
    private function mapHandsupToBucket($qid, $oid, $val, $m)
    {
        $val = (int)$val;

        // BUCKET 1: Experience (Enjoyed / Not Enjoyed)
        // Questions: 1, 4, 8, 12, 14, 16
        if (in_array($oid, [1, 15, 33, 51, 60, 69])) $m['Exp_Enjoyed'] += $val;
        if (in_array($oid, [2, 16, 34, 52, 61, 70])) $m['Exp_Did_Not_Enjoy'] += $val;
        if (in_array($oid, [3, 17, 35, 53, 62, 71])) $m['Exp_Not_Sure'] += $val;
        if (in_array($oid, [4, 18, 36, 54, 63, 72])) $m['Exp_Absent'] += $val;

        // BUCKET 2: Baseline (Cycled on roads before?)
        // Questions: 5, 9
        if (in_array($oid, [19, 37])) $m['Base_Yes'] += $val;
        if (in_array($oid, [20, 38])) $m['Base_No'] += $val;
        if (in_array($oid, [21, 39])) $m['Base_Not_Sure'] += $val;
        // (Note: Absent IDs 22/40 are ignored here as they don't fit the Y/N/NotSure columns)

        // BUCKET 3: Safety (How safe do you feel?)
        // Questions: 2, 6, 10
        if (in_array($oid, [5, 23, 41])) $m['Safe_More'] += $val;
        if (in_array($oid, [6, 24, 42])) $m['Safe_Less'] += $val;
        if (in_array($oid, [7, 25, 43])) $m['Safe_No_Diff'] += $val;
        if (in_array($oid, [8, 26, 44])) $m['Safe_Not_Sure'] += $val;

        // BUCKET 4: Confidence (How confident do you feel?)
        // Questions: 3, 7, 11, 13, 15
        if (in_array($oid, [10, 28, 46, 55, 64])) $m['Conf_More'] += $val;
        if (in_array($oid, [11, 29, 47, 56, 65])) $m['Conf_Less'] += $val;
        if (in_array($oid, [12, 30, 48, 57, 66])) $m['Conf_No_Diff'] += $val;
        if (in_array($oid, [13, 31, 49, 58, 67])) $m['Conf_Not_Sure'] += $val;

        return $m;
    }

    public function syncFactParentFollowUpSurveys($command = null)
    {
        $sourceSystemKey = $this->getSourceSystemKey();

        $watermark = $this->dwh->table('Sync_Log')
            ->where('Table_Name', 'Fact_Follow_Up_Survey')
            ->value('Last_Synced_At') ?? '1900-01-01 00:00:00';

        $highestTimestampSeen = $watermark;

        $surveys = $this->source->table('parent_follow_up_surveys')
            ->where('updated_at', '>', $watermark)
            ->orderBy('updated_at', 'asc')->get();

        $maxTimestamp = $surveys->max('updated_at');

        $total = $surveys->count();
        if ($total === 0) return 'No parent follow up surveys found.';

        $bar = $command ? $command->getOutput()->createProgressBar($total) : null;

        foreach ($surveys as $survey) {

            $data = json_decode($survey->parent_follow_up_survey, true) ?? [];
            $label = strtolower($survey->course_label);

            $riderKey = $this->dwh->table('Dim_Rider')->where('Source_Rider_Id', $survey->rider_id)->value('Rider_Key');
            $courseKey = $this->dwh->table('Dim_Course')->where('Source_Course_Id', $survey->course_id)->value('Course_Key');
            $deliveryKey = $this->dwh->table('Dim_Delivery_Header')->where('Source_Delivery_Id', $survey->delivery_id)->value('Delivery_Key');

            // Mapping logic based on array position in your examples
            $mapped = $this->mapSurveyByCourse($label, $data);

            $this->dwh->table('Fact_Follow_Up_Survey')->updateOrInsert(
                ['Source_Survey_Id' => $survey->id, 'Source_System_Key' => $sourceSystemKey],
                array_merge($mapped, [
                    'Delivery_Key' => $deliveryKey,
                    'Rider_Key' => $riderKey,
                    'Course_Key' => $courseKey,
                    'Course_Label_Raw' => $survey->course_label,
                    'Invitation_Month' => $survey->invitation_month,
                    'Source_Created_At' => $survey->created_at,
                    'updated_at' => now()
                ])
            );

            if ($bar) $bar->advance();
        }

        if ($maxTimestamp && $maxTimestamp > $highestTimestampSeen) {
            $highestTimestampSeen = $maxTimestamp;
        }

        $this->dwh->table('Sync_Log')->updateOrInsert(
            ['Table_Name' => 'Fact_Follow_Up_Survey'],
            ['Last_Synced_At' => $highestTimestampSeen]
        );

        if ($bar) {
            $bar->finish();
            $command->newLine();
        }

        return "Parent Follow Up survey Facts synced.";

    }

    private function mapSurveyByCourse($label, $data)
    {
        $mapped = [
            'q1a_freq_school' => null,
            'q1b_freq_leisure' => null,
            'q1c_freq_exercise' => null,
            'q2a_conf_use_cycle' => null,
            'q2b_conf_cycle_roads' => null,
            'q3a_enc_use_cycle' => null,
            'q3b_enc_cycle_roads' => null,
            'q4_safety_roads' => null,
            'q5_child_desire' => null,
            'q6_encouragement_factors' => null,
            'q7_conf_change' => null,
            'q8_physical_activity' => null,
        ];

        foreach ($data as $value) {
            // Handle Multi-select (Arrays) - Usually q5 or q6 depending on level
            if (is_array($value)) {
                $mapped['q6_encouragement_factors'] = json_encode($value);
                continue;
            }

            // Logic-based mapping by searching for question keys in the string
            // We extract the "Option Code" (e.g., o1, o2) to store in the Fact table

            // Frequencies (Universal q1)
            if (str_contains($value, '_q1a_')) $mapped['q1a_freq_school'] = $this->extractOption($value);
            if (str_contains($value, '_q1b_')) $mapped['q1b_freq_leisure'] = $this->extractOption($value);
            if (str_contains($value, '_q1c_')) $mapped['q1c_freq_exercise'] = $this->extractOption($value);

            // Confidence & Likelihood (Keys shift by Level)
            if (str_contains($value, '_q2a_')) $mapped['q2a_conf_use_cycle'] = $this->extractOption($value);
            if (str_contains($value, '_q2b_')) $mapped['q2b_conf_cycle_roads'] = $this->extractOption($value);
            if (str_contains($value, '_q3a_')) $mapped['q3a_enc_use_cycle'] = $this->extractOption($value);
            if (str_contains($value, '_q3b_')) $mapped['q3b_enc_cycle_roads'] = $this->extractOption($value);

            // Safety (Level 2/3 q4)
            if (str_contains($value, '_q4_') && !str_contains($value, '_q4a_')) {
                $mapped['q4_safety_roads'] = $this->extractOption($value);
            }

            // Desire to use cycle (q4 for Level 1, q5 for Level 2/3)
            if (str_contains($value, '_q4_o') || str_contains($value, '_q4a_o') || str_contains($value, '_q5_o')) {
                // Note: We only map this if it's not the safety question
                if (!str_contains($value, 'level_2_q4') && !str_contains($value, 'level_3_q4')) {
                    $mapped['q5_child_desire'] = $this->extractOption($value);
                }
            }

            // Change in Confidence (Level 2/3 q7)
            if (str_contains($value, '_q7_')) $mapped['q7_conf_change'] = $this->extractOption($value);

            // Physical Activity (q6 for Balance/L1, q8 for L2/L3)
            if (str_contains($value, '_q6_o') || str_contains($value, '_q6a_o') || str_contains($value, '_q8_o')) {
                $mapped['q8_physical_activity'] = $this->extractOption($value);
            }
        }

        return $mapped;
    }

    private function extractOption($string)
    {
        $parts = explode('_', $string);
        return end($parts);
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
            return (int)$matches[1];
        }

        return null;
    }

    /**
     * Optional Helper to turn status IDs into readable strings for the DWH
     */
    private function mapDeliveryStatus($statusId)
    {
        return match ($statusId) {
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

    /**
     * Resolves the last processed timestamp for a specific Data Warehouse table.
     * Falls back to a safe historical date if no record exists.
     *
     * @param string $tableName
     * @return string
     */
    private function getWatermark(string $tableName): string
    {
        return $this->dwh->table('Sync_Log')
            ->where('Table_Name', $tableName)
            ->value('Last_Synced_At') ?? '1900-01-01 00:00:00';
    }

    /**
     * Updates the Sync Log tracking table with the highest timestamp processed.
     *
     * @param string $tableName
     * @param string $highestTimestampSeen
     * @return void
     */
    private function updateWatermark(string $tableName, string $highestTimestampSeen): void
    {
        $this->dwh->table('Sync_Log')->updateOrInsert(
            ['Table_Name' => $tableName],
            [
                'Last_Synced_At' => $highestTimestampSeen
            ]
        );
    }

    /**
     * Resolves the internal DWH surrogate key for a given Source Grant ID.
     * Returns null if the parent grant hasn't been synced yet (Referential Integrity Check).
     *
     * @param mixed $sourceGrantId
     * @param int $sourceSystemKey
     * @return int|null
     */
    private function resolveGrantKey($sourceGrantId, int $sourceSystemKey): ?int
    {
        if (!$sourceGrantId) {
            return null;
        }

        return $this->dwh->table('Dim_Grant')
            ->where('Source_Grant_Id', $sourceGrantId)
            ->where('Source_System_Key', $sourceSystemKey)
            ->value('Grant_Key');
    }

    /**
     * Safely checks if a specific column exists on a source system table.
     * Utilizes static caching to prevent redundant schema queries during loops.
     *
     * @param string $table
     * @param string $column
     * @return bool
     */
    private function sourceHasColumn(string $table, string $column): bool
    {
        static $schemaCache = [];

        $cacheKey = "{$table}.{$column}";

        // If we have already checked this table/column during this run, return the cached result
        if (isset($schemaCache[$cacheKey])) {
            return $schemaCache[$cacheKey];
        }

        try {
            // Query the structural schema of the source database connection
            $exists = Schema::connection('mysql_src')->hasColumn($table, $column);

            $schemaCache[$cacheKey] = $exists;
            return $exists;
        } catch (\Exception $e) {
            // Fallback to false if the network blips or structural access is restricted
            return false;
        }
    }

}

<?php

namespace App\Services;

use App\Models\Rider;
use App\Services\SlackAlertService;
use Carbon\Carbon;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;
use Throwable;

class DataWarehouseSyncService
{
    protected $dwh;
    protected $source;
    protected SlackAlertService $slack;

    public function __construct(SlackAlertService $slack)
    {
        $this->dwh    = DB::connection('mysql');
        $this->source = DB::connection('mysql_src');
        $this->slack  = $slack;
    }

    /**
     * Unified orchestration runner for table syncs.
     * Manages progress bar lifecycle, isolates row failures, and dispatches Slack notifications.
     */
    protected function executeSyncRunner(
        string $tableName,
               $command,
               $queryOrCollection,
        callable $processRow,
        ?callable $onComplete = null,
        string $primaryKeyFallback = 'id'
    ): string {
        $startTime = microtime(true);
        $bar = null;
        $failedRecords = [];
        $processedCount = 0;

        try {
            // Determine if input is a builder or an already-resolved collection
            $isBuilder = $queryOrCollection instanceof \Illuminate\Database\Query\Builder
                || $queryOrCollection instanceof \Illuminate\Database\Eloquent\Builder;

            // Apply secondary tie-breaker if builder, not already sorted by it, and column exists
            if ($isBuilder) {
                $baseQuery = $queryOrCollection instanceof \Illuminate\Database\Eloquent\Builder
                    ? $queryOrCollection->getQuery()
                    : $queryOrCollection;

                $orders = $baseQuery->orders ?? [];
                $orderedCols = array_column($orders, 'column');

                // Strip alias/table prefix if present (e.g., 'instructors as i' -> 'instructors')
                $rawTable = preg_split('/\s+as\s+/i', (string) $baseQuery->from)[0] ?? $baseQuery->from;

                // Only append if not already in the ORDER BY clause AND physically exists on table
                if (
                    !in_array($primaryKeyFallback, $orderedCols, true) &&
                    !in_array("{$rawTable}.{$primaryKeyFallback}", $orderedCols, true) &&
                    $baseQuery->getConnection()->getSchemaBuilder()->hasColumn($rawTable, $primaryKeyFallback)
                ) {
                    $queryOrCollection->orderBy("{$rawTable}.{$primaryKeyFallback}", 'asc');
                }
            }

            $total = $isBuilder ? $queryOrCollection->count() : count($queryOrCollection);

            if ($total === 0) {
                if ($command) {
                    $command->comment("No new or modified records found for {$tableName}.");
                }
                return "{$tableName} is already up to date.";
            }

            $bar = $command ? $command->getOutput()->createProgressBar($total) : null;
            if ($bar) {
                $bar->start();
            }

            $state = (object) ['new' => 0, 'updated' => 0, 'highestTimestamp' => null];

            if ($isBuilder) {
                $queryOrCollection->chunk(500, function ($rows) use (&$processedCount, &$failedRecords, $bar, $processRow, $state) {
                    foreach ($rows as $row) {
                        try {
                            $processRow($row, $state);
                            $processedCount++;
                        } catch (Throwable $rowError) {
                            $rowId = $row->id ?? $row->Source_Instructor_Id ?? $row->Source_Provider_Id ?? 'N/A';
                            $failedRecords[] = [
                                'id'     => $rowId,
                                'reason' => $rowError->getMessage(),
                            ];
                        } finally {
                            if ($bar) $bar->advance();
                        }
                    }
                });
            } else {
                foreach ($queryOrCollection as $row) {
                    try {
                        $processRow($row, $state);
                        $processedCount++;
                    } catch (Throwable $rowError) {
                        $rowId = $row->id ?? $row->Source_Instructor_Id ?? $row->Source_Provider_Id ?? 'N/A';
                        $failedRecords[] = [
                            'id'     => $rowId,
                            'reason' => $rowError->getMessage(),
                        ];
                    } finally {
                        if ($bar) $bar->advance();
                    }
                }
            }

            if ($bar) {
                $bar->finish();
                $command?->newLine();
            }

            // Execute post-loop actions (such as committing the watermark)
            if ($onComplete) {
                $onComplete($state, $processedCount);
            }

            $duration = round(microtime(true) - $startTime, 2);

            // Report row-level anomalies to Slack if any occurred
            if (!empty($failedRecords)) {
                $this->slack->sendSyncError(
                    $tableName,
                    "Completed with " . count($failedRecords) . " row error(s) out of {$total} total.",
                    $failedRecords
                );
                if ($command) {
                    $command->warn("⚠️  {$tableName} finished with " . count($failedRecords) . " skipped row(s). Sent details to Slack.");
                }
            }

            return "Successfully synced {$processedCount} records into {$tableName} in {$duration}s. (Failed: " . count($failedRecords) . ")";

        } catch (Throwable $fatalException) {
            if ($bar) {
                $bar->finish();
                $command?->newLine();
            }

            $this->slack->sendSyncError(
                "{$tableName} (FATAL)",
                $fatalException->getMessage(),
                $failedRecords,
                $fatalException->getTraceAsString()
            );

            throw $fatalException;
        }
    }

    // =========================================================================
    // DIMENSIONS
    // =========================================================================

    public function syncExternalSystems($command = null)
    {
        $sourceSystemKey = $this->getSourceSystemKey();
        $sourceExtSystems = $this->source->table('external_systems')->get();

        return $this->executeSyncRunner(
            'Dim_External_Systems',
            $command,
            $sourceExtSystems,
            function ($sys) use ($sourceSystemKey) {
                $this->dwh->table('Dim_External_Systems')->updateOrInsert(
                    [
                        'Source_External_System_Id' => $sys->id,
                        'Source_System_Key'         => $sourceSystemKey,
                    ],
                    [
                        'Username'                  => $sys->username,
                        'Name'                      => $sys->name,
                        'Pref_Link_Managed_Consent' => $sys->pref_link_managed_consent,
                        'Pref_Use_Instructor_App'   => $sys->pref_use_instructor_app,
                        'Pref_Link_Managed_Comms'   => $sys->pref_link_managed_comms,
                        'created_at'                => $sys->created_at ?? now(),
                        'updated_at'                => $sys->updated_at ?? now(),
                    ]
                );
            }
        );
    }

    public function syncCourseActivities($command = null)
    {
        $sourceActivities = $this->source->table('course_activities_LOOKUP')->get();

        return $this->executeSyncRunner(
            'Dim_Course_Activity',
            $command,
            $sourceActivities,
            function ($activity) {
                $this->dwh->table('Dim_Course_Activity')->updateOrInsert(
                    ['Source_Activity_Id' => $activity->id],
                    [
                        'Course_Code'          => $activity->course_id,
                        'Activity_Code'        => $activity->activity_id,
                        'Activity_Label'       => $activity->activity_label,
                        'Activity_Description' => $activity->activity_description,
                        'Outcomes_Lookup_Id'   => $activity->outcomes_lookup_id ?? null,
                        'updated_at'           => now(),
                    ]
                );
            }
        );
    }

    public function syncInstructorFeedbackLookups($command = null)
    {
        $sourceLookups = $this->source->table('instructor_feedback_LOOKUP as ifl')
            ->leftJoin('general_LOOKUP as gl', function ($join) {
                $join->on('ifl.category_id', '=', 'gl.id')
                    ->where('gl.lookup_group', '=', 'instructor_feedback_heading');
            })
            ->select([
                'ifl.id as source_feedback_id',
                'ifl.category_id',
                'gl.lookup_code as course_code',
                'gl.lookup_text_value as category_label',
                'ifl.short_text',
                'ifl.long_text',
                'ifl.links_lookup',
            ])
            ->get();

        return $this->executeSyncRunner(
            'Dim_Instructor_Feedback_Lookup',
            $command,
            $sourceLookups,
            function ($row) {
                $this->dwh->table('Dim_Instructor_Feedback_Lookup')->updateOrInsert(
                    ['Feedback_Lookup_Key' => (int) $row->source_feedback_id],
                    [
                        'Category_Id'    => $row->category_id,
                        'Course_Code'    => $row->course_code,
                        'Category_Label' => $row->category_label ?? 'General',
                        'Short_Text'     => $row->short_text,
                        'Long_Text'      => $row->long_text,
                        'Links_Lookup'   => $row->links_lookup,
                        'updated_at'     => now(),
                    ]
                );
            }
        );
    }

    public function syncTrainingProviders($command = null)
    {
        $sourceSystemKey = $this->getSourceSystemKey();
        $sourceProviders = $this->source->table('training_providers')->get();
        $today = Carbon::now()->toDateString();

        return $this->executeSyncRunner(
            'Dim_Training_Provider',
            $command,
            $sourceProviders,
            function ($provider, $state) use ($sourceSystemKey, $today) {
                $incomingData = [
                    'Provider_Name'                           => $provider->provider_name,
                    'Provider_Number'                         => $provider->provider_number,
                    'Is_Active'                               => is_null($provider->deleted_at) ? 'Y' : 'N',
                    'Address_Line_1'                          => $provider->address_01,
                    'Address_Line_2'                          => $provider->address_02,
                    'City'                                    => $provider->city,
                    'Postcode'                                => $provider->postcode,
                    'Website'                                 => $provider->website,
                    'Telephone'                               => $provider->telephone,
                    'Public_Email'                            => $provider->public_email,
                    'Public_Telephone'                        => $provider->public_telephone,
                    'Provider_Type'                           => $provider->provider_type,
                    'Source_Created_At'                       => $provider->created_at,
                    'Source_Updated_At'                       => $provider->updated_at,
                    'Primary_Email'                           => $provider->primary_email ?? null,
                    'Secondary_Email'                         => $provider->secondary_email ?? null,
                    'Landline'                                => $provider->landline ?? null,
                    'Status'                                  => (int) ($provider->status ?? 0),
                    'Date_Inception'                          => $provider->date_inception ?? null,
                    'Date_Renewal'                            => $provider->date_renewal ?? null,
                    'Date_Deregistered'                       => $provider->date_deregistered ?? null,
                    'Deregistration_Reason'                   => $provider->deregistration_reason ?? null,
                    'Date_Eqa_Visit'                          => $provider->date_eqa_visit ?? null,
                    'Date_Insurance_Expiry'                   => $provider->date_insurance_expiry ?? null,
                    'Renewal_Blocked'                         => (bool) ($provider->renewal_blocked ?? false),
                    'Pref_Level_1'                            => (bool) ($provider->pref_level_1 ?? false),
                    'Pref_Level_2'                            => (bool) ($provider->pref_level_2 ?? false),
                    'Pref_Level_3'                            => (bool) ($provider->pref_level_3 ?? false),
                    'Pref_Plus_Balance'                       => (bool) ($provider->pref_plus_balance ?? false),
                    'Pref_Plus_Bus'                           => (bool) ($provider->pref_plus_bus ?? false),
                    'Pref_Plus_Fix'                           => (bool) ($provider->pref_plus_fix ?? false),
                    'Pref_Plus_Learn'                         => (bool) ($provider->pref_plus_learn ?? false),
                    'Pref_Plus_On_Show'                       => (bool) ($provider->pref_plus_on_show ?? false),
                    'Pref_Plus_Parents'                       => (bool) ($provider->pref_plus_parents ?? false),
                    'Pref_Plus_Promotion'                     => (bool) ($provider->pref_plus_promotion ?? false),
                    'Pref_Plus_Recycled'                      => (bool) ($provider->pref_plus_recycled ?? false),
                    'Pref_Plus_Ride'                          => (bool) ($provider->pref_plus_ride ?? false),
                    'Pref_Plus_Transition'                    => (bool) ($provider->pref_plus_transition ?? false),
                    'Pref_Plus_Family'                        => (bool) ($provider->pref_plus_family ?? false),
                    'Pref_Plus_Adult'                         => (bool) ($provider->pref_plus_adult ?? false),
                    'Legacy_Areas_Of_Operation'               => $provider->legacy_areas_of_operation ?? null,
                    'Delivery_Areas'                          => $provider->delivery_areas ?? null,
                    'Account_Notes'                           => $provider->account_notes ?? null,
                    'Terms_Url'                               => $provider->terms_url ?? null,
                    'Booking_Question_1'                      => $provider->booking_question_1 ?? null,
                    'Booking_Question_2'                      => $provider->booking_question_2 ?? null,
                    'Booking_Question_3'                      => $provider->booking_question_3 ?? null,
                    'Booking_Question_4'                      => $provider->booking_question_4 ?? null,
                    'Booking_Question_5'                      => $provider->booking_question_5 ?? null,
                    'Pref_Collect_Characteristics_In_Consent' => (bool) ($provider->pref_collect_characteristics_in_consent ?? false),
                    'Pref_Allow_Non_Riders'                   => (bool) ($provider->pref_allow_non_riders ?? false),
                    'Pref_Enable_Tp_Digi_Delivery_Access'     => (bool) ($provider->pref_enable_tp_digi_delivery_access ?? false),
                    'Pref_Enable_Tp_Digi_Consent_Upload'      => (bool) ($provider->pref_enable_tp_digi_consent_upload ?? false),
                    'Has_Fleet_Cycles'                        => (bool) ($provider->has_fleet_cycles ?? false),
                    'Provide_A_Cycle_Question_Optional'       => (bool) ($provider->provide_a_cycle_question_optional ?? false),
                    'External_System_Id'                      => !empty($provider->external_system_id) ? (int) $provider->external_system_id : null,
                    'Source_Deleted_At'                       => $provider->deleted_at ?? null,
                ];

                $current = $this->dwh->table('Dim_Training_Provider')
                    ->where('Source_Provider_Id', $provider->id)
                    ->where('Source_System_Key', $sourceSystemKey)
                    ->where('Is_Current', 1)
                    ->first();

                if (!$current) {
                    $this->createNewVersion('Dim_Training_Provider', 'Source_Provider_Id', $provider->id, $incomingData, $sourceSystemKey, $today);
                    $state->new++;
                } elseif ($this->hasChanged($current, $incomingData)) {
                    $this->dwh->table('Dim_Training_Provider')
                        ->where('Provider_Key', $current->Provider_Key)
                        ->update(['Is_Current' => 0, 'Valid_To_Date' => $today]);

                    $this->createNewVersion('Dim_Training_Provider', 'Source_Provider_Id', $provider->id, $incomingData, $sourceSystemKey, $today);
                    $state->updated++;
                }
            },
            function ($state, $count) {
                $this->updateWatermark('Dim_Training_Provider', now()->toDateTimeString(), $count);
            }
        );
    }

    public function syncSchools($command = null)
    {
        $sourceSystemKey = $this->getSourceSystemKey();
        $watermark = $this->getWatermark('Dim_School');

        $hasDeprivationIndex = $this->sourceHasColumn('vendor_edubase', 'deprivation_index');

        $selectFields = [
            'id', 'urn', 'establishment_name', 'la_code', 'la_name', 'postcode',
            'lsoa_code', 'lsoa_name', 'msoa_code', 'msoa_name',
            'parliamentary_constituency_code', 'parliamentary_constituency_name',
            'status', 'urban_rural_code', 'urban_rural_name', 'updated_at',
        ];

        if ($hasDeprivationIndex) {
            $selectFields[] = 'deprivation_index';
        }

        $query = $this->source->table('vendor_edubase')
            ->select($selectFields)
            ->where('updated_at', '>', $watermark)
            ->orderBy('id', 'asc');

        return $this->executeSyncRunner(
            'Dim_School',
            $command,
            $query,
            function ($school, $state) use ($sourceSystemKey, $hasDeprivationIndex) {
                $imdDecile = null;
                if ($hasDeprivationIndex && isset($school->deprivation_index) && is_numeric($school->deprivation_index)) {
                    $imdDecile = (int) round((float) $school->deprivation_index);
                }

                $this->dwh->table('Dim_School')->updateOrInsert(
                    [
                        'Source_School_Id'  => $school->id,
                        'Source_System_Key' => $sourceSystemKey,
                    ],
                    [
                        'School_Urn'                      => $school->urn,
                        'School_Name'                     => $school->establishment_name,
                        'La_Code'                         => $school->la_code,
                        'La_Name'                         => $school->la_name,
                        'Postcode'                        => $school->postcode,
                        'Lsoa_Code'                       => $school->lsoa_code,
                        'Lsoa_Name'                       => $school->lsoa_name,
                        'Msoa_Code'                       => $school->msoa_code,
                        'Msoa_Name'                       => $school->msoa_name,
                        'Parliamentary_Constituency_Code' => $school->parliamentary_constituency_code,
                        'Parliamentary_Constituency_Name' => $school->parliamentary_constituency_name,
                        'Status'                          => $school->status,
                        'Urban_Rural_Code'                => $school->urban_rural_code,
                        'Rural_Urban_Classification'      => $school->urban_rural_name,
                        'Imd_Decile'                      => $imdDecile,
                    ]
                );

                if ($school->updated_at && (!$state->highestTimestamp || $school->updated_at > $state->highestTimestamp)) {
                    $state->highestTimestamp = $school->updated_at;
                }
            },
            function ($state, $count) use ($watermark) {
                if ($state->highestTimestamp && $state->highestTimestamp > $watermark) {
                    $this->updateWatermark('Dim_School', $state->highestTimestamp, $count);
                }
            }
        );
    }

    public function syncOrganisations($command = null)
    {
        $sourceSystemKey = $this->getSourceSystemKey();
        $sourceOrgs = $this->source->table('organisations')
            ->select('id', 'training_provider_id', 'name')
            ->get();

        return $this->executeSyncRunner(
            'Dim_Organisation',
            $command,
            $sourceOrgs,
            function ($org) use ($sourceSystemKey) {
                $providerKey = $this->dwh->table('Dim_Training_Provider')
                    ->where('Source_Provider_Id', $org->training_provider_id)
                    ->where('Source_System_Key', $sourceSystemKey)
                    ->value('Provider_Key');

                $this->dwh->table('Dim_Organisation')->updateOrInsert(
                    [
                        'Source_Organisation_Id' => $org->id,
                        'Source_System_Key'      => $sourceSystemKey,
                    ],
                    [
                        'Provider_Key'      => $providerKey,
                        'Organisation_Name' => $org->name,
                    ]
                );
            },
            function ($state, $count) {
                $this->updateWatermark('Dim_Organisation', now()->toDateTimeString(), $count);
            }
        );
    }

    public function syncGrantRecipients($command = null)
    {
        $sourceSystemKey = $this->getSourceSystemKey();
        $sourceRecipients = $this->source->table('grant_recipients')->get();
        $today = Carbon::now()->toDateString();

        return $this->executeSyncRunner(
            'Dim_Grant_Recipient',
            $command,
            $sourceRecipients,
            function ($recipient, $state) use ($sourceSystemKey, $today) {
                $incomingData = [
                    'Recipient_Name'    => $recipient->recipient_name,
                    'Recipient_Number'  => $recipient->recipient_number,
                    'LA_Id'             => $recipient->local_authority_id,
                    'Is_Active'         => is_null($recipient->deleted_at) ? 'Y' : 'N',
                    'Address_Line_1'    => $recipient->address_01,
                    'Address_Line_2'    => $recipient->address_02,
                    'City'              => $recipient->city,
                    'Postcode'          => $recipient->postcode,
                    'Website'           => $recipient->website,
                    'Inception_Date'    => $recipient->date_inception,
                    'Renewal_Date'      => $recipient->date_renewal,
                    'Deregistered_Date' => $recipient->date_deregistered,
                    'Is_SGO'            => ($recipient->pref_sgoh == 1) ? 1 : 0,
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
                    $state->new++;
                } elseif ($this->hasChanged($current, $incomingData)) {
                    $this->dwh->table('Dim_Grant_Recipient')
                        ->where('Recipient_Key', $current->Recipient_Key)
                        ->update(['Is_Current' => 0, 'Valid_To_Date' => $today]);

                    $this->createNewVersion('Dim_Grant_Recipient', 'Source_Recipient_Id', $recipient->id, $incomingData, $sourceSystemKey, $today);
                    $state->updated++;
                }
            },
            function ($state, $count) {
                $this->updateWatermark('Dim_Grant_Recipient', now()->toDateTimeString(), $count);
            }
        );
    }

    public function syncGrants($command = null)
    {
        $sourceSystemKey = $this->getSourceSystemKey();
        $sourceGrants = $this->source->table('grants')->get();

        return $this->executeSyncRunner(
            'Dim_Grant',
            $command,
            $sourceGrants,
            function ($grant) use ($sourceSystemKey) {
                $recipientKey = $this->dwh->table('Dim_Grant_Recipient')
                    ->where('Source_Recipient_Id', $grant->grant_recipient_id)
                    ->where('Source_System_Key', $sourceSystemKey)
                    ->where('Is_Current', 1)
                    ->value('Recipient_Key');

                if (!$recipientKey) {
                    throw new \RuntimeException("Recipient ID {$grant->grant_recipient_id} not found in Dim_Grant_Recipient for Grant {$grant->id}");
                }

                $this->dwh->table('Dim_Grant')->updateOrInsert(
                    [
                        'Source_Grant_Id'   => $grant->id,
                        'Source_System_Key' => $sourceSystemKey,
                    ],
                    [
                        'Grant_Recipient_Key'     => $recipientKey,
                        'Grant_Number'            => $grant->grant_number,
                        'Grant_Label'             => $grant->grant_label,
                        'Grant_Period_Start_Year' => $grant->grant_period_start_year,
                        'Grant_Source'            => $grant->grant_source,
                    ]
                );
            },
            function ($state, $count) {
                $this->updateWatermark('Dim_Grant', now()->toDateTimeString(), $count);
            }
        );
    }

    public function syncInstructors($command = null)
    {
        $sourceSystemKey = $this->getSourceSystemKey();
        $query = $this->source->table('instructors')->orderBy('id', 'asc');

        return $this->executeSyncRunner(
            'Dim_Instructor',
            $command,
            $query,
            function ($instructor) use ($sourceSystemKey) {
                $this->dwh->table('Dim_Instructor')->updateOrInsert(
                    [
                        'Source_Instructor_Id' => $instructor->id,
                        'Source_System_Key'    => $sourceSystemKey,
                    ],
                    [
                        'Instructor_Number'                      => $instructor->instructor_number,
                        'Instructor_Type'                        => $instructor->instructor_type,
                        'First_Name'                             => $instructor->first_name,
                        'Last_Name'                              => $instructor->last_name,
                        'Email'                                  => $instructor->email,
                        'Telephone'                              => $instructor->telephone,
                        'Landline'                               => $instructor->landline,
                        'Age_Range_Id'                           => $instructor->age_range_id,
                        'Ethnicity_Id'                           => $instructor->ethnicity_id,
                        'Gender_Id'                              => $instructor->gender_id,
                        'Title_Id'                               => $instructor->title_id,
                        'Address_01'                             => $instructor->address_01,
                        'Address_02'                             => $instructor->address_02,
                        'City'                                   => $instructor->city,
                        'Postcode'                               => $instructor->postcode,
                        'Status_Raw'                             => $instructor->status,
                        'Is_Pending'                             => (bool) $instructor->is_pending,
                        'Flag_Nsi_Migrated'                      => (bool) $instructor->flag_nsi_migrated,
                        'Pref_Receive_News'                      => (bool) $instructor->pref_receive_news,
                        'Pref_Delivering_Bikeability'            => (bool) $instructor->pref_delivering_bikeability,
                        'Pref_Delivering_Other'                  => (bool) $instructor->pref_delivering_other,
                        'Pref_Bursary_Eligibility'               => (bool) $instructor->pref_bursary_eligibility,
                        'Has_Received_Bursary'                   => (bool) $instructor->has_received_bursary,
                        'Date_Registered'                        => $instructor->date_registered,
                        'Date_Renewal'                           => $instructor->date_renewal,
                        'Date_Deregistered'                      => $instructor->date_deregistered,
                        'Deregistration_Reason'                  => $instructor->deregistration_reason,
                        'First_Aid_Training_Complete_Date'       => $instructor->first_aid_training_complete_date,
                        'Safeguarding_Training_Complete_Date'    => $instructor->safeguarding_training_complete_date,
                        'Send_Training_Complete_Date'            => $instructor->send_training_complete_date,
                        'Send_Training_Overridden'               => (bool) $instructor->send_training_overridden,
                        'Send_Training_Certificate_Download_Date'=> $instructor->send_training_certificate_download_date,
                        'Account_Notes'                          => $instructor->account_notes,
                        'Source_Created_At'                      => $instructor->created_at,
                        'Source_Updated_At'                      => $instructor->updated_at,
                        'updated_at'                             => now(),
                    ]
                );
            },
            function ($state, $count) {
                $this->updateWatermark('Dim_Instructor', now()->toDateTimeString(), $count);
            }
        );
    }

    public function syncDeliveryHeaders($command = null)
    {
        $sourceSystemKey = $this->getSourceSystemKey();
        $watermark = Carbon::parse($this->getWatermark('Dim_Delivery_Header'))->subSeconds(5)->toDateTimeString();

        $query = $this->source->table('deliveries')
            ->select([
                'id', 'grant_id', 'grant_old_id', 'school_urn', 'training_provider_id',
                'status', 'date_delivery_start', 'date_delivery_end', 'short_description',
                'digitisation_booking', 'pref_alt_delivery_location', 'alt_delivery_location',
                'notes', 'instructor_general_notes', 'teacher_notes', 'school_contacts', 'venue',
                'delivery_details', 'local_funding', 'legacy_delivery_id', 'legacy_delivery_method',
                'provider_additional_questions', 'comms_start_date', 'date_completed',
                'pref_link_managed_consent', 'include_tp_terms_in_consent', 'consent_src_characteristics',
                'venue_notification_sent', 'survey_notifications_sent', 'confirm_booked_email_sent',
                'confirm_booked_reminder_email_sent', 'max_consents', 'waiting_list_enabled',
                'organisation_id', 'fleet_cycles_used', 'consent_cutoff_date', 'rider_list_uploaded',
                'url_code', 'housekeeping_selected', 'enable_school_management', 'external_system_id',
                'created_at', 'updated_at', 'deleted_at',
            ])
            ->where(function ($q) use ($watermark) {
                $q->where('deliveries.updated_at', '>', $watermark)
                    ->orWhere('deliveries.deleted_at', '>', $watermark)
                    ->orWhereNull('deliveries.updated_at');
            })
            ->orderBy('id', 'asc');

        return $this->executeSyncRunner(
            'Dim_Delivery_Header',
            $command,
            $query,
            function ($delivery, $state) use ($sourceSystemKey) {
                $grantKey = $delivery->grant_id ? $this->dwh->table('Dim_Grant')
                    ->where('Source_Grant_Id', $delivery->grant_id)
                    ->where('Source_System_Key', $sourceSystemKey)
                    ->value('Grant_Key') : null;

                $schoolKey = $delivery->school_urn ? $this->dwh->table('Dim_School')
                    ->where('School_Urn', $delivery->school_urn)
                    ->where('Source_System_Key', $sourceSystemKey)
                    ->value('School_Key') : null;

                $organisationKey = $delivery->organisation_id ? $this->dwh->table('Dim_Organisation')
                    ->where('Source_Organisation_Id', $delivery->organisation_id)
                    ->where('Source_System_Key', $sourceSystemKey)
                    ->value('Organisation_Key') : null;

                $providerKey = $this->dwh->table('Dim_Training_Provider')
                    ->where('Source_Provider_Id', $delivery->training_provider_id)
                    ->where('Source_System_Key', $sourceSystemKey)
                    ->where('Is_Current', 1)
                    ->value('Provider_Key');

                $externalSystemKey = null;
                if (!empty($delivery->external_system_id)) {
                    $externalSystemKey = $this->dwh->table('Dim_External_Systems')
                        ->where('Source_External_System_Id', $delivery->external_system_id)
                        ->value('External_System_Key');
                }

                $this->dwh->table('Dim_Delivery_Header')->updateOrInsert(
                    [
                        'Source_Delivery_Id' => $delivery->id,
                        'Source_System_Key'  => $sourceSystemKey,
                    ],
                    [
                        'Grant_Key'                          => $grantKey,
                        'School_Key'                         => $schoolKey,
                        'Organisation_Key'                   => $organisationKey,
                        'Training_Provider_Key'              => $providerKey,
                        'External_System_Key'                => $externalSystemKey,
                        'Delivery_Status'                    => ($delivery->digitisation_booking === 1) ? $this->mapDeliveryStatus($delivery->status) : 'N/A',
                        'Date_Delivery_Start'                => $delivery->date_delivery_start,
                        'Date_Delivery_End'                  => $delivery->date_delivery_end,
                        'Digitisation_Booking'               => $delivery->digitisation_booking,
                        'Fleet_Cycles_Used'                  => $delivery->fleet_cycles_used,
                        'Consent_Cutoff_Date'                => $delivery->consent_cutoff_date,
                        'Pref_Alt_Delivery_Location'         => $delivery->pref_alt_delivery_location,
                        'Alt_Delivery_Location'              => $delivery->alt_delivery_location,
                        'Notes'                              => $delivery->notes,
                        'Instructor_General_Notes'           => $delivery->instructor_general_notes,
                        'Teacher_Notes'                      => $delivery->teacher_notes,
                        'School_Contacts'                    => $delivery->school_contacts,
                        'Venue'                              => $delivery->venue,
                        'Provider_Additional_Questions'      => $delivery->provider_additional_questions,
                        'Comms_Start_Date'                   => $delivery->comms_start_date,
                        'Date_Completed'                     => $delivery->date_completed,
                        'Pref_Link_Managed_Consent'          => $delivery->pref_link_managed_consent,
                        'Include_Tp_Terms_In_Consent'        => $delivery->include_tp_terms_in_consent ?? 0,
                        'Consent_Src_Characteristics'        => $delivery->consent_src_characteristics ?? 0,
                        'Max_Consents'                       => $delivery->max_consents,
                        'Waiting_List_Enabled'               => $delivery->waiting_list_enabled ?? 0,
                        'Short_Description'                  => $delivery->short_description ?? null,
                        'Local_Funding'                      => (int) ($delivery->local_funding ?? 0),
                        'Legacy_Delivery_Id'                 => $delivery->legacy_delivery_id ?? null,
                        'Legacy_Delivery_Method'             => $delivery->legacy_delivery_method ?? null,
                        'Rider_List_Uploaded'                => (bool) ($delivery->rider_list_uploaded ?? false),
                        'Url_Code'                           => $delivery->url_code ?? null,
                        'Venue_Notification_Sent'            => (bool) ($delivery->venue_notification_sent ?? false),
                        'Survey_Notifications_Sent'          => (bool) ($delivery->survey_notifications_sent ?? false),
                        'Confirm_Booked_Email_Sent'          => $delivery->confirm_booked_email_sent ?? null,
                        'Confirm_Booked_Reminder_Email_Sent' => $delivery->confirm_booked_reminder_email_sent ?? null,
                        'Housekeeping_Selected'              => (bool) ($delivery->housekeeping_selected ?? false),
                        'Enable_School_Management'           => (bool) ($delivery->enable_school_management ?? false),
                        'Source_Created_At'                  => $delivery->created_at ?? null,
                        'Source_Updated_At'                  => $delivery->updated_at ?? null,
                        'Source_Deleted_At'                  => $delivery->deleted_at ?? null,
                    ]
                );

                if ($delivery->updated_at && (!$state->highestTimestamp || $delivery->updated_at > $state->highestTimestamp)) {
                    $state->highestTimestamp = $delivery->updated_at;
                }
            },
            function ($state, $count) {
                if ($state->highestTimestamp) {
                    $this->updateWatermark('Dim_Delivery_Header', $state->highestTimestamp, $count);
                }
            }
        );
    }

    public function syncCourses($command = null)
    {
        $sourceSystemKey = $this->getSourceSystemKey();
        $watermark = Carbon::parse($this->getWatermark('Dim_Course'))->subSeconds(5)->toDateTimeString();

        $hasCourseSoftDeletes   = $this->sourceHasColumn('courses', 'deleted_at');
        $hasDeliverySoftDeletes = $this->sourceHasColumn('deliveries', 'deleted_at');

        $query = $this->source->table('courses')
            ->leftJoin('deliveries', 'courses.delivery_id', '=', 'deliveries.id')
            ->where(function ($q) use ($watermark, $hasCourseSoftDeletes) {
                $q->where('courses.updated_at', '>', $watermark)
                    ->orWhereNull('courses.updated_at');

                if ($hasCourseSoftDeletes) {
                    $q->orWhere('courses.deleted_at', '>', $watermark);
                }
            });

//        if ($hasDeliverySoftDeletes) {
//            $query->whereNull('deliveries.deleted_at');
//        }
        if ($hasCourseSoftDeletes) {
            $query->whereNull('courses.deleted_at');
        }

        $query->select([
            'courses.id', 'courses.course_id', 'courses.parent_course_id', 'courses.delivery_id',
            'courses.status', 'courses.start_date', 'courses.date_complete', 'courses.year_group',
            'courses.is_cancelled', 'courses.cancellation_reason', 'courses.has_confirmed_booked',
            'courses.confirmed_by', 'courses.confirmed_by_email', 'courses.confirmed_at',
            'courses.provisional_booked', 'courses.booked', 'courses.adults', 'courses.children',
            'courses.attendees_overridden_metadata', 'courses.attendees_overridden_by',
            'courses.attendees_overridden_at', 'courses.updated_at',
        ])->orderBy('courses.id', 'asc');

        $result = $this->executeSyncRunner(
            'Dim_Course',
            $command,
            $query,
            function ($course, $state) use ($sourceSystemKey) {
                $delivery = $this->dwh->table('Dim_Delivery_Header')
                    ->where('Source_Delivery_Id', $course->delivery_id)
                    ->where('Source_System_Key', $sourceSystemKey)
                    ->select(['Delivery_Key', 'Source_Deleted_At'])
                    ->first();

                if (!$delivery) {
                    return; // Skip courses referencing missing/soft-deleted deliveries
                }

                $this->dwh->table('Dim_Course')->updateOrInsert(
                    [
                        'Source_Course_Id'  => $course->id,
                        'Source_System_Key' => $sourceSystemKey,
                    ],
                    [
                        'Delivery_Key'                  => $delivery->Delivery_Key,
                        'Course_Level'                  => $course->course_id,
                        'Status'                        => $course->status,
                        'Start_Date'                    => $course->start_date,
                        'Date_Complete'                 => $course->date_complete,
                        'Year_Group'                    => $course->year_group,
                        'Is_Cancelled'                  => (bool) ($course->is_cancelled ?? false),
                        'Cancellation_Reason'           => $course->cancellation_reason ?? null,
                        'Has_Confirmed_Booked'          => (bool) ($course->has_confirmed_booked ?? false),
                        'Confirmed_By'                  => $course->confirmed_by ?? null,
                        'Confirmed_By_Email'            => $course->confirmed_by_email ?? null,
                        'Confirmed_At'                  => $course->confirmed_at ?? null,
                        'Provisional_Booked'            => $course->provisional_booked ?? null,
                        'Booked'                        => $course->booked ?? null,
                        'Adults'                        => $course->adults ?? null,
                        'Children'                      => $course->children ?? null,
                        'Attendees_Overridden_Metadata' => $course->attendees_overridden_metadata ?? null,
                        'Attendees_Overridden_By'       => $course->attendees_overridden_by ?? null,
                        'Attendees_Overridden_At'       => $course->attendees_overridden_at ?? null,
                    ]
                );

                if ($course->updated_at && (!$state->highestTimestamp || $course->updated_at > $state->highestTimestamp)) {
                    $state->highestTimestamp = $course->updated_at;
                }
            },
            function ($state, $count) {
                if ($state->highestTimestamp) {
                    $this->updateWatermark('Dim_Course', $state->highestTimestamp, $count);
                }
            }
        );

        // Finalize Parent-Child Course Links
        try {
            $childCourses = $this->source->table('courses')
                ->whereNotNull('parent_course_id')
                ->select(['id', 'parent_course_id'])
                ->get();

            foreach ($childCourses as $child) {
                $parentKey = $this->dwh->table('Dim_Course')
                    ->where('Source_Course_Id', $child->parent_course_id)
                    ->where('Source_System_Key', $sourceSystemKey)
                    ->value('Course_Key');

                if ($parentKey) {
                    $this->dwh->table('Dim_Course')
                        ->where('Source_Course_Id', $child->id)
                        ->where('Source_System_Key', $sourceSystemKey)
                        ->update(['Parent_Course_Key' => $parentKey]);
                }
            }
        } catch (Throwable $e) {
            $this->slack->sendSyncError('Dim_Course (Parent Hierarchy)', $e->getMessage());
        }

        return $result;
    }

    public function syncRiders($command = null)
    {
        $sourceSystemKey = $this->getSourceSystemKey();
        $watermark = Carbon::parse($this->getWatermark('Dim_Rider'))->subSeconds(5)->toDateTimeString();

        $query = Rider::select([
            'id', 'school_urn', 'ethnicity', 'gender', 'free_school_meals', 'send_code', 'updated_at',
        ])
            ->where(function ($q) use ($watermark) {
                $q->where('updated_at', '>', $watermark)->orWhereNull('updated_at');
            })
            ->orderBy('updated_at', 'asc');

        return $this->executeSyncRunner(
            'Dim_Rider',
            $command,
            $query,
            function ($rider, $state) use ($sourceSystemKey) {
                $schoolKey = $this->dwh->table('Dim_School')
                    ->where('School_Urn', $rider->school_urn)
                    ->value('School_Key');

                $this->dwh->table('Dim_Rider')->updateOrInsert(
                    ['Source_Rider_Id' => $rider->id, 'Source_System_Key' => $sourceSystemKey],
                    [
                        'School_Key'    => $schoolKey,
                        'Ethnicity'     => $rider->ethnicity,
                        'Gender'        => $rider->gender,
                        'Pupil_Premium' => ($rider->free_school_meals === 'Yes') ? 1 : 0,
                        'Has_SEND'      => !empty($rider->send_code) ? 1 : 0,
                    ]
                );

                $riderKey = $this->dwh->table('Dim_Rider')
                    ->where('Source_Rider_Id', $rider->id)
                    ->value('Rider_Key');

                if ($riderKey) {
                    $sendCodesRaw = is_array($rider->send_code) ? $rider->send_code : [];
                    $sendCodes = array_unique(array_filter($sendCodesRaw));

                    $this->dwh->table('Map_Rider_Send')->where('Rider_Key', $riderKey)->delete();

                    foreach ($sendCodes as $code) {
                        $this->dwh->table('Dim_Send_Code')->updateOrInsert(
                            ['Send_Code' => $code],
                            ['Send_Code' => $code]
                        );

                        $sendCodeKey = $this->dwh->table('Dim_Send_Code')
                            ->where('Send_Code', $code)
                            ->value('Send_Code_Key');

                        $this->dwh->table('Map_Rider_Send')->insert([
                            'Rider_Key'     => $riderKey,
                            'Send_Code_Key' => $sendCodeKey,
                        ]);
                    }
                }

                if ($rider->updated_at && (!$state->highestTimestamp || $rider->updated_at->toDateTimeString() > $state->highestTimestamp)) {
                    $state->highestTimestamp = $rider->updated_at->toDateTimeString();
                }
            },
            function ($state, $count) {
                if ($state->highestTimestamp) {
                    $this->updateWatermark('Dim_Rider', $state->highestTimestamp, $count);
                }
            }
        );
    }

    public function syncConsents($command = null)
    {
        $sourceSystemKey = $this->getSourceSystemKey();
        $watermark = Carbon::parse($this->getWatermark('Dim_Consent'))->subSeconds(5)->toDateTimeString();

        $query = $this->source->table('consents')
            ->select([
                'id', 'rider_id', 'delivery_id', 'consent_status',
                'pref_join_bikeability_club', 'pref_further_research', 'pref_receive_news',
                'has_bike', 'cycle_ability', 'is_fsm', 'is_SEND', 'send_details',
                'has_medical_condition', 'medical_details', 'attended', 'year_group',
                'gender', 'ethnicity', 'created_at', 'updated_at', 'deleted_at', 'deleted_reason',
            ])
            ->where(function ($q) use ($watermark) {
                $q->where('updated_at', '>', $watermark)
                    ->orWhere('deleted_at', '>', $watermark)
                    ->orWhereNull('updated_at');
            })
            ->orderBy('id', 'asc'); // Deterministic ordering avoiding null/stale updated_at drift

        return $this->executeSyncRunner(
            'Dim_Consent',
            $command,
            $query,
            function ($consent, $state) use ($sourceSystemKey) {
                if (!isset($state->skipped_unmapped)) {
                    $state->skipped_unmapped = 0;
                }

                $riderKey = $this->dwh->table('Dim_Rider')
                    ->where('Source_Rider_Id', $consent->rider_id)
                    ->where('Source_System_Key', $sourceSystemKey)
                    ->value('Rider_Key');

                $deliveryKey = $this->dwh->table('Dim_Delivery_Header')
                    ->where('Source_Delivery_Id', $consent->delivery_id)
                    ->where('Source_System_Key', $sourceSystemKey)
                    ->value('Delivery_Key');

                // Referential integrity check - track orphan if parent is missing
                if (!$riderKey || !$deliveryKey) {
                    $state->skipped_unmapped++;
                    return;
                }

                $abilities = is_array($consent->cycle_ability)
                    ? $consent->cycle_ability
                    : json_decode($consent->cycle_ability, true) ?? [];

                $freqs = $this->source->table('cycle_frequency')
                    ->where('consent_id', $consent->id)
                    ->get();

                $freqData = [
                    'Pre_Freq_To_School' => $freqs->where('label_lookup_id', 1)->first()->grading_lookup_id ?? null,
                    'Pre_Freq_Leisure'   => $freqs->where('label_lookup_id', 2)->first()->grading_lookup_id ?? null,
                    'Pre_Freq_Exercise'  => $freqs->where('label_lookup_id', 3)->first()->grading_lookup_id ?? null,
                    'Pre_Freq_Other'     => $freqs->where('label_lookup_id', 4)->first()->grading_lookup_id ?? null,
                ];

                $this->dwh->table('Dim_Consent')->updateOrInsert(
                    [
                        'Source_Consent_Id' => $consent->id,
                        'Source_System_Key' => $sourceSystemKey,
                    ],
                    array_merge($freqData, [
                        'Rider_Key'                      => $riderKey,
                        'Delivery_Key'                   => $deliveryKey,
                        'Consent_Status'                 => $consent->consent_status,
                        'Pref_Join_Bikeclub'             => $consent->pref_join_bikeability_club ?: 0,
                        'Pref_Further_Research'          => $consent->pref_further_research ?: 0,
                        'Pref_Receive_News'              => $consent->pref_receive_news ?: 0,
                        'Ability_Cannot_Cycle'           => in_array("1", $abilities) ? 1 : 0,
                        'Ability_Can_Look_Over_Shoulder' => in_array("2", $abilities) ? 1 : 0,
                        'Ability_Can_One_Hand_Signal'    => in_array("3", $abilities) ? 1 : 0,
                        'Ability_Has_Level_2'            => in_array("4", $abilities) ? 1 : 0,
                        'Cycle_Ability_Raw'              => json_encode($abilities),
                        'Is_Pupil_premium'               => $consent->is_fsm ?: 0,
                        'Is_SEND'                        => $consent->is_SEND ?: 0,
                        'SEND_Details'                   => $consent->send_details,
                        'Has_Medical_Condition'          => $consent->has_medical_condition ?: 0,
                        'Medical_Details'                => $consent->medical_details,
                        'Attended'                       => $consent->attended,
                        'Year_Group'                     => $consent->year_group,
                        'Gender'                         => $consent->gender,
                        'Ethnicity'                      => $consent->ethnicity,
                        'Deleted_Reason'                 => $consent->deleted_reason ?? null,
                        'Source_Created_At'              => $consent->created_at ?? null,
                        'Source_Updated_At'              => $consent->updated_at ?? null,
                        'Source_Deleted_At'              => $consent->deleted_at ?? null,
                        'updated_at'                     => now(),
                    ])
                );

                // Watermark progression across updates and deletions
                $activeTimestamp = max(
                    $consent->updated_at ?? '1900-01-01 00:00:00',
                    $consent->deleted_at ?? '1900-01-01 00:00:00'
                );

                if ($activeTimestamp && (!$state->highestTimestamp || $activeTimestamp > $state->highestTimestamp)) {
                    $state->highestTimestamp = $activeTimestamp;
                }
            },
            function ($state, $count) {
                if ($state->highestTimestamp) {
                    $this->updateWatermark('Dim_Consent', $state->highestTimestamp, $count);
                }
            }
        );
    }

    public function syncTeacherTrainers($command = null)
    {
        $sourceSystemKey = $this->getSourceSystemKey();
        $watermark = $this->getWatermark('Dim_Teacher_Trainer');

        $query = $this->source->table('teacher_trainers')
            ->where('updated_at', '>', $watermark)
            ->orderBy('id', 'asc');

        return $this->executeSyncRunner(
            'Dim_Teacher_Trainer',
            $command,
            $query,
            function ($row, $state) use ($sourceSystemKey) {
                $fullName = trim(($row->first_name ?? '') . ' ' . ($row->last_name ?? ''));

                $this->dwh->table('Dim_Teacher_Trainer')->updateOrInsert(
                    [
                        'Source_Teacher_Trainer_Id' => $row->id,
                        'Source_System_Key'         => $sourceSystemKey,
                    ],
                    [
                        'First_Name'                                  => $row->first_name,
                        'Last_Name'                                   => $row->last_name,
                        'Full_Name'                                   => $fullName ?: null,
                        'Email'                                       => $row->email,
                        'Telephone'                                   => $row->telephone,
                        'Landline'                                    => $row->landline ?? null,
                        'Main_School_Urn'                             => $row->main_school_urn,
                        'Active'                                      => $row->active ?? 1,
                        'Status'                                      => $row->status ?? 0,
                        'Enable_Rider_Upload'                         => $row->enable_rider_upload ?? 0,
                        'Enable_App_Access'                           => $row->enable_app_access ?? 0,
                        'Enable_Get_Cycling_Admin'                    => $row->enable_get_cycling_admin ?? 0,
                        'Pref_Receive_News'                           => $row->pref_receive_news ?? 0,
                        'Pref_Delivering_Bikeability'                 => $row->pref_delivering_bikeability ?? 0,
                        'Age_Range_Id'                                => $row->age_range_id,
                        'Gender_Id'                                   => $row->gender_id,
                        'Ethnicity_Id'                                => $row->ethnicity_id,
                        'Date_Registered'                             => $row->date_registered,
                        'Date_Renewal'                                => $row->date_renewal,
                        'Date_Deregistered'                           => $row->date_deregistered,
                        'Deregistration_Reason'                       => $row->deregistration_reason,
                        'Rdc_Practical_Training_Complete_Date'        => $row->rdc_practical_training_complete_date,
                        'In_School_Training_Complete_Date'            => $row->in_school_training_complete_date,
                        'In_School_Training_Certificate_Download_Date'=> $row->in_school_training_certificate_download_date,
                        'Send_Training_Complete_Date'                 => $row->send_training_complete_date,
                        'Send_Training_Certificate_Download_Date'     => $row->send_training_certificate_download_date,
                        'Updated_At'                                  => now(),
                    ]
                );

                if ($row->updated_at && (!$state->highestTimestamp || $row->updated_at > $state->highestTimestamp)) {
                    $state->highestTimestamp = $row->updated_at;
                }
            },
            function ($state, $count) {
                if ($state->highestTimestamp) {
                    $this->updateWatermark('Dim_Teacher_Trainer', $state->highestTimestamp, $count);
                }
            }
        );
    }

    public function syncGetCyclingCourses($command = null)
    {
        $sourceSystemKey = $this->getSourceSystemKey();
        $courses = $this->source->table('get_cycling_courses')->get();

        return $this->executeSyncRunner(
            'Dim_Get_Cycling_Course',
            $command,
            $courses,
            function ($c) use ($sourceSystemKey) {
                $this->dwh->table('Dim_Get_Cycling_Course')->updateOrInsert(
                    [
                        'Source_Course_Id'  => $c->id,
                        'Source_System_Key' => $sourceSystemKey,
                    ],
                    [
                        'Course_Code' => $c->course_id,
                        'Course_Name' => $c->name,
                        'Updated_At'  => now(),
                    ]
                );
            }
        );
    }

    public function syncGetCyclingRiders($command = null)
    {
        $sourceSystemKey = $this->getSourceSystemKey();
        $watermark = $this->getWatermark('Dim_Get_Cycling_Rider');

        $query = $this->source->table('get_cycling_riders')
            ->where('updated_at', '>', $watermark)
            ->orderBy('id', 'asc');

        return $this->executeSyncRunner(
            'Dim_Get_Cycling_Rider',
            $command,
            $query,
            function ($row, $state) use ($sourceSystemKey) {
                $schoolKey = null;
                if (!empty($row->school_urn)) {
                    $schoolKey = $this->dwh->table('Dim_School')
                        ->where('School_Urn', (string) $row->school_urn)
                        ->where('Source_System_Key', $sourceSystemKey)
                        ->value('School_Key');
                }

                $trainerKey = null;
                if (!empty($row->uploaded_by_teacher_trainer_id)) {
                    $trainerKey = $this->dwh->table('Dim_Teacher_Trainer')
                        ->where('Source_Teacher_Trainer_Id', (int) $row->uploaded_by_teacher_trainer_id)
                        ->where('Source_System_Key', $sourceSystemKey)
                        ->value('Teacher_Trainer_Key');
                }

                $this->dwh->table('Dim_Get_Cycling_Rider')->updateOrInsert(
                    [
                        'Source_Rider_Id'   => $row->id,
                        'Source_System_Key' => $sourceSystemKey,
                    ],
                    [
                        'School_Key'                     => $schoolKey,
                        'Teacher_Trainer_Key'            => $trainerKey,
                        'School_Urn'                     => $row->school_urn ? (string) $row->school_urn : null,
                        'Archived'                       => (bool) ($row->archived ?? 0),
                        'Archived_At'                    => $row->archived_at,
                        'Upn'                            => $row->upn,
                        'Year_Group'                     => $row->year_group,
                        'Class_Name'                     => $row->class_name,
                        'Gender'                         => $row->gender,
                        'Ethnicity'                      => $row->ethnicity,
                        'Send_Code'                      => $row->send_code,
                        'Pupil_Premium'                  => $row->pp,
                        'Uploaded_By_Teacher_Trainer_Id' => $row->uploaded_by_teacher_trainer_id ? (int) $row->uploaded_by_teacher_trainer_id : null,
                        'Last_Activity_Date'             => $row->last_activity_date,
                        'Updated_At'                     => now(),
                    ]
                );

                if ($row->updated_at && (!$state->highestTimestamp || $row->updated_at > $state->highestTimestamp)) {
                    $state->highestTimestamp = $row->updated_at;
                }
            },
            function ($state, $count) {
                if ($state->highestTimestamp) {
                    $this->updateWatermark('Dim_Get_Cycling_Rider', $state->highestTimestamp, $count);
                }
            }
        );
    }

    public function syncGetCyclingRiderNotes($command = null)
    {
        $sourceSystemKey = $this->getSourceSystemKey();
        $watermark = $this->getWatermark('Dim_Get_Cycling_Rider_Note');

        $query = $this->source->table('get_cycling_rider_notes')
            ->where('updated_at', '>', $watermark)
            ->orderBy('id', 'asc');

        return $this->executeSyncRunner(
            'Dim_Get_Cycling_Rider_Note',
            $command,
            $query,
            function ($row, $state) use ($sourceSystemKey) {
                $riderKey = $this->dwh->table('Dim_Get_Cycling_Rider')
                    ->where('Source_Rider_Id', $row->rider_id)
                    ->where('Source_System_Key', $sourceSystemKey)
                    ->value('GC_Rider_Key');

                if (!$riderKey) {
                    return;
                }

                $this->dwh->table('Dim_Get_Cycling_Rider_Note')->updateOrInsert(
                    [
                        'Source_Note_Id'    => $row->id,
                        'Source_System_Key' => $sourceSystemKey,
                    ],
                    [
                        'GC_Rider_Key'    => $riderKey,
                        'Source_Rider_Id' => $row->rider_id,
                        'Note'            => $row->note,
                        'Updated_At'      => now(),
                    ]
                );

                if ($row->updated_at && (!$state->highestTimestamp || $row->updated_at > $state->highestTimestamp)) {
                    $state->highestTimestamp = $row->updated_at;
                }
            },
            function ($state, $count) {
                if ($state->highestTimestamp) {
                    $this->updateWatermark('Dim_Get_Cycling_Rider_Note', $state->highestTimestamp, $count);
                }
            }
        );
    }

    // =========================================================================
    // FACTS & BRIDGES
    // =========================================================================

    public function syncInstructorTrainingProviders($command = null)
    {
        $sourceSystemKey = $this->getSourceSystemKey();

        try {
            $providerMap = $this->dwh->table('Dim_Training_Provider')
                ->where('Source_System_Key', $sourceSystemKey)
                ->where('Is_Current', 1)
                ->pluck('Provider_Key', 'Source_Provider_Id')
                ->toArray();

            $instructorMap = $this->dwh->table('Dim_Instructor')
                ->where('Source_System_Key', $sourceSystemKey)
                ->pluck('Instructor_Key', 'Source_Instructor_Id')
                ->toArray();

            $sourceLinks = $this->source->table('join_instructors_training_providers')
                ->select(['instructor_id', 'training_provider_id', 'dbs_certification_date', 'deleted_at'])
                ->get();

            $total = $sourceLinks->count();
            if ($total === 0) {
                $this->dwh->statement('SET FOREIGN_KEY_CHECKS=0;');
                $this->dwh->table('Fact_Instructor_Training_Provider')->truncate();
                $this->dwh->statement('SET FOREIGN_KEY_CHECKS=1;');
                return "Fact_Instructor_Training_Provider cleared (0 source records).";
            }

            $bar = $command ? $command->getOutput()->createProgressBar($total) : null;
            if ($bar) $bar->start();

            $insertBuffer = [];
            $skippedCount = 0;

            foreach ($sourceLinks as $link) {
                $instructorKey = $instructorMap[$link->instructor_id] ?? null;
                $providerKey   = $providerMap[$link->training_provider_id] ?? null;

                if (!$instructorKey || !$providerKey) {
                    $skippedCount++;
                    if ($bar) $bar->advance();
                    continue;
                }

                $insertBuffer[] = [
                    'Instructor_Key'         => $instructorKey,
                    'Provider_Key'           => $providerKey,
                    'Source_Instructor_Id'   => $link->instructor_id,
                    'Source_Provider_Id'     => $link->training_provider_id,
                    'Source_System_Key'      => $sourceSystemKey,
                    'Dbs_Certification_Date' => $link->dbs_certification_date ?: null,
                    'Source_Deleted_At'      => $link->deleted_at ?: null,
                ];

                if ($bar) $bar->advance();
            }

            $this->dwh->statement('SET FOREIGN_KEY_CHECKS=0;');
            $this->dwh->table('Fact_Instructor_Training_Provider')->truncate();
            $this->dwh->statement('SET FOREIGN_KEY_CHECKS=1;');

            foreach (array_chunk($insertBuffer, 1000) as $batch) {
                $this->dwh->table('Fact_Instructor_Training_Provider')->insert($batch);
            }

            $this->updateWatermark('Fact_Instructor_Training_Provider', now()->toDateTimeString(), count($insertBuffer));

            if ($bar) {
                $bar->finish();
                $command?->newLine();
            }

            $skipNotice = $skippedCount > 0 ? " ({$skippedCount} orphaned records skipped)" : "";
            return "Fact_Instructor_Training_Provider refreshed with " . count($insertBuffer) . " records.{$skipNotice}";

        } catch (Throwable $e) {
            $this->slack->sendSyncError('Fact_Instructor_Training_Provider', $e->getMessage(), [], $e->getTraceAsString());
            throw $e;
        }
    }

    public function syncFactCourseDelivery($command = null)
    {
        $sourceSystemKey = $this->getSourceSystemKey();
        $useLegacy = $this->sourceHasColumn('deliveries', 'delivery_details');
        $useLegacyCourseCharacteristics = $this->sourceHasColumn('courses', 'characteristics');

        $watermark = $this->getWatermark('Fact_Course_Delivery');

        try {
            $candidateDeliveryIds = $this->source->table('deliveries')
                ->leftJoin('courses', 'deliveries.id', '=', 'courses.delivery_id')
                ->leftJoin('join_riders_courses as jrc', 'courses.id', '=', 'jrc.course_id')
                ->where(function ($q) use ($watermark) {
                    $q->where('deliveries.updated_at', '>', $watermark)
                        ->orWhere('courses.updated_at', '>', $watermark)
                        ->orWhere('jrc.updated_at', '>', $watermark)
                        ->orWhereNull('deliveries.updated_at');
                })
                ->distinct()
                ->pluck('deliveries.id')
                ->toArray();

            $total = count($candidateDeliveryIds);
            if ($total === 0) {
                return "Fact_Course_Delivery is up to date.";
            }

            $bar = $command ? $command->getOutput()->createProgressBar($total) : null;
            if ($bar) $bar->start();

            $highestTimestampSeen = $watermark;

            collect($candidateDeliveryIds)->chunk(200)->each(function ($deliveryIdChunk) use (
                $sourceSystemKey, $useLegacy, $useLegacyCourseCharacteristics, $bar, &$highestTimestampSeen
            ) {
                $recordsQuery = $this->source->table('deliveries')
                    ->leftJoin('courses', 'deliveries.id', '=', 'courses.delivery_id')
                    ->leftJoin('join_riders_courses as jrc', 'courses.id', '=', 'jrc.course_id')
                    ->whereIn('deliveries.id', $deliveryIdChunk)
                    ->select([
                        'deliveries.id as delivery_id',
                        'deliveries.date_delivery_start',
                        'deliveries.consent_src_characteristics',
                        'deliveries.updated_at as delivery_updated',
                        'courses.id as course_id',
                        'courses.start_date as course_start',
                        'courses.updated_at as course_updated',
                        DB::raw('MAX(jrc.updated_at) as rider_course_updated'),
                    ])
                    ->groupBy([
                        'deliveries.id',
                        'deliveries.date_delivery_start',
                        'deliveries.consent_src_characteristics',
                        'deliveries.updated_at',
                        'courses.id',
                        'courses.start_date',
                        'courses.updated_at',
                    ]);

                if ($useLegacy) {
                    $recordsQuery->addSelect('deliveries.delivery_details')->groupBy('deliveries.delivery_details');
                }
                if ($useLegacyCourseCharacteristics) {
                    $recordsQuery->addSelect('courses.characteristics')->groupBy('courses.characteristics');
                }

                $records = $recordsQuery->get();

                foreach ($records as $record) {
                    $activeTimestamp = max(
                        $record->delivery_updated ?? '1900-01-01 00:00:00',
                        $record->course_updated ?? '1900-01-01 00:00:00',
                        $record->rider_course_updated ?? '1900-01-01 00:00:00'
                    );

                    if ($activeTimestamp > $highestTimestampSeen) {
                        $highestTimestampSeen = $activeTimestamp;
                    }

                    $delivery = $this->dwh->table('Dim_Delivery_Header')
                        ->where('Source_Delivery_Id', $record->delivery_id)
                        ->where('Source_System_Key', $sourceSystemKey)
                        ->first();

                    if (!$delivery) {
                        continue;
                    }

                    $mockCourse = (object)[
                        'id'               => $record->course_id,
                        'delivery_id'      => $record->delivery_id,
                        'delivery_details' => $record->delivery_details ?? null,
                    ];
                    $deliveryDetailMetrics = $this->getCourseDeliveryMetrics($mockCourse, $useLegacy);

                    $courseLevelString = $deliveryDetailMetrics['Extracted_Course_Level'] ?? null;
                    $yearGroupString   = $deliveryDetailMetrics['Extracted_Year_Group'] ?? null;
                    unset($deliveryDetailMetrics['Extracted_Course_Level'], $deliveryDetailMetrics['Extracted_Year_Group']);

                    $courseKey = null;
                    if (!is_null($record->course_id)) {
                        $courseKey = $this->dwh->table('Dim_Course')
                            ->where('Source_Course_Id', $record->course_id)
                            ->where('Source_System_Key', $sourceSystemKey)
                            ->value('Course_Key');
                    } elseif (!empty($courseLevelString)) {
                        $this->dwh->table('Dim_Course')->updateOrInsert(
                            [
                                'Delivery_Key'      => $delivery->Delivery_Key,
                                'Course_Level'      => $courseLevelString,
                                'Source_Course_Id'  => null,
                                'Source_System_Key' => $sourceSystemKey,
                            ],
                            [
                                'Status'        => 1,
                                'Start_Date'    => $record->course_start ?: $record->date_delivery_start,
                                'Date_Complete' => $record->course_start ?: $record->date_delivery_start,
                                'Year_Group'    => $yearGroupString,
                            ]
                        );

                        $courseKey = $this->dwh->table('Dim_Course')
                            ->where('Delivery_Key', $delivery->Delivery_Key)
                            ->where('Course_Level', $courseLevelString)
                            ->whereNull('Source_Course_Id')
                            ->where('Source_System_Key', $sourceSystemKey)
                            ->value('Course_Key');
                    }

                    if (is_null($courseKey)) {
                        continue;
                    }

                    // Attendance fallback
                    $isDigitised = ((int) ($delivery->Digitisation_Booking ?? 0) === 1) || !is_null($record->course_id);
                    $currentAttended = (int) ($deliveryDetailMetrics['Count_Attended_Confirmed'] ?? 0);

                    if ($currentAttended === 0 && $isDigitised && !is_null($record->course_id)) {
                        $actualAttended = $this->dwh->table('Fact_Rider_Course')
                            ->where('Source_Course_Id', $record->course_id)
                            ->where('Source_System_Key', $sourceSystemKey)
                            ->where('Attended', 1)
                            ->count();

                        if ($actualAttended > 0) {
                            $deliveryDetailMetrics['Count_Attended_Confirmed'] = $actualAttended;
                        }
                    }

                    $enrolledCount = 0;
                    $completedCount = 0;
                    if (!is_null($record->course_id)) {
                        $enrolledCount = $this->source->table('join_riders_courses')->where('course_id', $record->course_id)->count();
                        $completedCount = $this->source->table('join_riders_courses')->where('course_id', $record->course_id)->where('has_completed_course', 1)->count();
                    }

                    if ($record->consent_src_characteristics == 1) {
                        $metrics = $this->aggregateFromDWHConsents($delivery->Delivery_Key);
                    } else {
                        if ($useLegacyCourseCharacteristics) {
                            $metrics = !empty($record->characteristics)
                                ? $this->aggregateFromLegacyCharacteristics($record->characteristics)
                                : $this->initializeExtendedMetricArray();
                        } elseif (!is_null($record->course_id)) {
                            $metrics = $this->aggregateFromSourceTable('course_characteristics', $record->course_id);
                        } else {
                            $metrics = $this->initializeExtendedMetricArray();
                        }
                    }

                    $fact_date = $record->course_start ?: $record->date_delivery_start;

                    $this->dwh->table('Fact_Course_Delivery')->updateOrInsert(
                        [
                            'Delivery_Key' => $delivery->Delivery_Key,
                            'Course_Key'   => $courseKey,
                        ],
                        array_merge(
                            $useLegacy ? [] : $metrics,
                            $deliveryDetailMetrics,
                            [
                                'Riders_Enrolled_Count'  => $enrolledCount,
                                'Riders_Completed_Count' => $completedCount,
                                'Date_Key'               => $fact_date ? str_replace('-', '', substr($fact_date, 0, 10)) : null,
                                'School_Key'             => $delivery->School_Key,
                                'Organisation_Key'       => $delivery->Organisation_Key,
                                'Provider_Key'           => $delivery->Training_Provider_Key,
                                'Grant_Key'              => $delivery->Grant_Key,
                            ]
                        )
                    );
                }

                if ($bar) $bar->advance(count($deliveryIdChunk));
            });

            $this->updateWatermark('Fact_Course_Delivery', $highestTimestampSeen, $total);

            if ($bar) {
                $bar->finish();
                $command?->newLine();
            }

            return "Fact_Course_Delivery synced ({$total} candidate deliveries processed).";

        } catch (Throwable $e) {
            if ($bar) {
                $bar->finish();
                $command?->newLine();
            }
            $this->slack->sendSyncError('Fact_Course_Delivery', $e->getMessage(), [], $e->getTraceAsString());
            throw $e;
        }
    }

    public function syncFactRiderCourse($command = null)
    {
        $sourceSystemKey = $this->getSourceSystemKey();
        $watermark = $this->getWatermark('Fact_Rider_Course');

        $query = $this->source->table('join_riders_courses')
            ->where('updated_at', '>', $watermark)
            ->orderBy('updated_at', 'asc');

        return $this->executeSyncRunner(
            'Fact_Rider_Course',
            $command,
            $query,
            function ($rc, $state) use ($sourceSystemKey) {
                $finalAttendance = (!empty($rc->attended_override) && $rc->attended_override == 1)
                    ? $rc->attended_override
                    : ($rc->attended ?? 1);

                $this->dwh->table('Fact_Rider_Course')->updateOrInsert(
                    [
                        'Source_Rider_Id'   => $rc->rider_id,
                        'Source_Course_Id'  => $rc->course_id,
                        'Source_System_Key' => $sourceSystemKey,
                    ],
                    [
                        'Status'               => $rc->status ?? 0,
                        'Attended'             => $finalAttendance,
                        'Withdrawn'            => $rc->withdrawn ?? 0,
                        'Withdrawal_Reason'    => $rc->withdrawal_reason,
                        'Has_Completed_Course' => $rc->has_completed_course ?? 0,
                        'Has_Survey_Completed' => $rc->has_survey_completed ?? 0,
                        'Course_Complete_Date' => $rc->course_complete_date,
                        'Source_Created_At'    => $rc->created_at,
                        'Source_Updated_At'    => $rc->updated_at,
                        'Updated_At'           => now(),
                    ]
                );

                if ($rc->updated_at && (!$state->highestTimestamp || $rc->updated_at > $state->highestTimestamp)) {
                    $state->highestTimestamp = $rc->updated_at;
                }
            },
            function ($state, $count) {
                if ($state->highestTimestamp) {
                    $this->updateWatermark('Fact_Rider_Course', $state->highestTimestamp, $count);
                }
            }
        );
    }

    public function syncFactRiderActivityOutcomes($command = null)
    {
        $sourceSystemKey = $this->getSourceSystemKey();
        $watermark = $this->getWatermark('Fact_Rider_Activity_Outcome');

        if (!$this->sourceHasColumn('join_riders_courses', 'outcomes')) {
            return "Skipped: 'outcomes' column missing.";
        }

        $query = $this->source->table('join_riders_courses as jrc')
            ->whereNotNull('jrc.outcomes')
            ->where('jrc.outcomes', '!=', '[]')
            ->where('jrc.outcomes', '!=', '')
            ->where('jrc.updated_at', '>', $watermark)
            ->select(['jrc.rider_id', 'jrc.course_id', 'jrc.outcomes', 'jrc.updated_at'])
            ->orderBy('jrc.updated_at', 'asc');

        $outcomesLookup = [
            1 => ['code' => 'O', 'grade' => 'On my own'],
            2 => ['code' => 'P', 'grade' => 'With practice'],
            3 => ['code' => 'A', 'grade' => 'With assistance'],
            4 => ['code' => 'N', 'grade' => 'Not seen'],
        ];

        $activityKeyMap = $this->dwh->table('Dim_Course_Activity')
            ->pluck('Activity_Key', 'Source_Activity_Id')
            ->toArray();

        return $this->executeSyncRunner(
            'Fact_Rider_Activity_Outcome',
            $command,
            $query,
            function ($row, $state) use ($sourceSystemKey, $outcomesLookup, &$activityKeyMap) {
                $riderKey = $this->dwh->table('Dim_Rider')
                    ->where('Source_Rider_Id', $row->rider_id)
                    ->where('Source_System_Key', $sourceSystemKey)
                    ->value('Rider_Key');

                $course = $this->dwh->table('Dim_Course')
                    ->where('Source_Course_Id', $row->course_id)
                    ->where('Source_System_Key', $sourceSystemKey)
                    ->first();

                if (!$riderKey || !$course) {
                    return;
                }

                $delivery = $this->dwh->table('Dim_Delivery_Header')
                    ->where('Delivery_Key', $course->Delivery_Key)
                    ->first();

                $outcomes = json_decode($row->outcomes, true);
                if (!is_array($outcomes)) {
                    return;
                }

                foreach ($outcomes as $item) {
                    $sourceActivityId = $item['id'] ?? null;
                    $outcomeScore     = (int) ($item['outcome'] ?? 0);

                    if (!$sourceActivityId || empty($outcomeScore)) {
                        continue;
                    }

                    $activityKey = $activityKeyMap[$sourceActivityId] ?? null;
                    if (!$activityKey) {
                        $activityKey = $this->dwh->table('Dim_Course_Activity')
                            ->where('Source_Activity_Id', $sourceActivityId)
                            ->value('Activity_Key');
                        if ($activityKey) {
                            $activityKeyMap[$sourceActivityId] = $activityKey;
                        }
                    }

                    if (!$activityKey) {
                        continue;
                    }

                    $outcomeMeta = $outcomesLookup[$outcomeScore] ?? ['code' => null, 'grade' => null];

                    $this->dwh->table('Fact_Rider_Activity_Outcome')->updateOrInsert(
                        [
                            'Course_Key'   => $course->Course_Key,
                            'Rider_Key'    => $riderKey,
                            'Activity_Key' => $activityKey,
                        ],
                        [
                            'Delivery_Key'          => $course->Delivery_Key,
                            'School_Key'            => $delivery->School_Key ?? null,
                            'Training_Provider_Key' => $delivery->Training_Provider_Key ?? null,
                            'Grant_Key'             => $delivery->Grant_Key ?? null,
                            'Outcome_Score'         => $outcomeScore,
                            'Outcome_Code'          => $outcomeMeta['code'],
                            'Outcome_Grade'         => $outcomeMeta['grade'],
                            'Created_At'            => now(),
                        ]
                    );
                }

                if ($row->updated_at && (!$state->highestTimestamp || $row->updated_at > $state->highestTimestamp)) {
                    $state->highestTimestamp = $row->updated_at;
                }
            },
            function ($state, $count) {
                if ($state->highestTimestamp) {
                    $this->updateWatermark('Fact_Rider_Activity_Outcome', $state->highestTimestamp, $count);
                }
            }
        );
    }

    public function syncFactRiderInstructorFeedback($command = null)
    {
        $sourceSystemKey = $this->getSourceSystemKey();
        $watermark = $this->getWatermark('Fact_Rider_Instructor_Feedback');

        if (!$this->sourceHasColumn('join_riders_courses', 'instructor_feedback')) {
            return "Skipped: 'instructor_feedback' column missing.";
        }

        $query = $this->source->table('join_riders_courses as jrc')
            ->whereNotNull('jrc.instructor_feedback')
            ->where('jrc.instructor_feedback', '!=', '[]')
            ->where('jrc.instructor_feedback', '!=', '')
            ->where('jrc.updated_at', '>', $watermark);

        if ($this->sourceHasColumn('join_riders_courses', 'deleted_at')) {
            $query->whereNull('jrc.deleted_at');
        }

        $query->select(['jrc.rider_id', 'jrc.course_id', 'jrc.instructor_feedback', 'jrc.updated_at'])
            ->orderBy('jrc.updated_at', 'asc');

        return $this->executeSyncRunner(
            'Fact_Rider_Instructor_Feedback',
            $command,
            $query,
            function ($row, $state) use ($sourceSystemKey) {
                $riderKey = $this->dwh->table('Dim_Rider')
                    ->where('Source_Rider_Id', $row->rider_id)
                    ->where('Source_System_Key', $sourceSystemKey)
                    ->value('Rider_Key');

                $course = $this->dwh->table('Dim_Course')
                    ->where('Source_Course_Id', $row->course_id)
                    ->where('Source_System_Key', $sourceSystemKey)
                    ->first();

                if (!$riderKey || !$course) {
                    return;
                }

                $delivery = $this->dwh->table('Dim_Delivery_Header')
                    ->where('Delivery_Key', $course->Delivery_Key)
                    ->first();

                $fbData = json_decode($row->instructor_feedback, true);
                if (!is_array($fbData)) {
                    return;
                }

                $instructorKey = null;
                if (!empty($fbData['instructor_id'])) {
                    $instructorKey = $this->dwh->table('Dim_Instructor')
                        ->where('Source_Instructor_Id', $fbData['instructor_id'])
                        ->where('Source_System_Key', $sourceSystemKey)
                        ->value('Instructor_Key');
                }

                $paragraphsRaw = $fbData['paragraphs'] ?? [];
                $paragraphs = is_string($paragraphsRaw) ? (json_decode($paragraphsRaw, true) ?? []) : (array) $paragraphsRaw;

                $feedbackDate = !empty($fbData['feedbackDate']) ? $fbData['feedbackDate'] : null;
                $notes        = $fbData['notes'] ?? null;

                foreach ($paragraphs as $paragraphId) {
                    $exactFeedbackId = (int) $paragraphId;
                    if (!$exactFeedbackId) {
                        continue;
                    }

                    $this->dwh->table('Fact_Rider_Instructor_Feedback')->updateOrInsert(
                        [
                            'Course_Key'          => $course->Course_Key,
                            'Rider_Key'           => $riderKey,
                            'Feedback_Lookup_Key' => $exactFeedbackId,
                        ],
                        [
                            'Delivery_Key'          => $course->Delivery_Key,
                            'Instructor_Key'        => $instructorKey,
                            'Grant_Key'             => $delivery->Grant_Key ?? null,
                            'Training_Provider_Key' => $delivery->Training_Provider_Key ?? null,
                            'School_Key'            => $delivery->School_Key ?? null,
                            'Feedback_Date'         => $feedbackDate,
                            'Instructor_Notes'      => $notes,
                            'Created_At'            => now(),
                        ]
                    );
                }

                if ($row->updated_at && (!$state->highestTimestamp || $row->updated_at > $state->highestTimestamp)) {
                    $state->highestTimestamp = $row->updated_at;
                }
            },
            function ($state, $count) {
                if ($state->highestTimestamp) {
                    $this->updateWatermark('Fact_Rider_Instructor_Feedback', $state->highestTimestamp, $count);
                }
            }
        );
    }

    public function syncFactActivityOutcomeSummary($command = null)
    {
        try {
            $sql = "
            REPLACE INTO Fact_Course_Activity_Outcome_Summary (
                Delivery_Key, Course_Key, Course_Code,
                l1_ac2_on_own, l1_ac2_practice, l1_ac2_assistance, l1_ac2_not_seen,
                l1_ac3_on_own, l1_ac3_practice, l1_ac3_assistance, l1_ac3_not_seen,
                l1_ac4_on_own, l1_ac4_practice, l1_ac4_assistance, l1_ac4_not_seen,
                l1_ac5_on_own, l1_ac5_practice, l1_ac5_assistance, l1_ac5_not_seen,
                l2_ac1_on_own, l2_ac1_practice, l2_ac1_assistance, l2_ac1_not_seen,
                l2_ac2_on_own, l2_ac2_practice, l2_ac2_assistance, l2_ac2_not_seen,
                l2_ac3_on_own, l2_ac3_practice, l2_ac3_assistance, l2_ac3_not_seen,
                l2_ac4_on_own, l2_ac4_practice, l2_ac4_assistance, l2_ac4_not_seen,
                l2_ac5_on_own, l2_ac5_practice, l2_ac5_assistance, l2_ac5_not_seen,
                l2_ac6_on_own, l2_ac6_practice, l2_ac6_assistance, l2_ac6_not_seen,
                l2_ac7_on_own, l2_ac7_practice, l2_ac7_assistance, l2_ac7_not_seen,
                l3_ac1_on_own, l3_ac1_practice, l3_ac1_assistance, l3_ac1_not_seen,
                l3_ac2_on_own, l3_ac2_practice, l3_ac2_assistance, l3_ac2_not_seen,
                l3_ac3_on_own, l3_ac3_practice, l3_ac3_assistance, l3_ac3_not_seen,
                l3_ac4_on_own, l3_ac4_practice, l3_ac4_assistance, l3_ac4_not_seen,
                l3_ac5_on_own, l3_ac5_practice, l3_ac5_assistance, l3_ac5_not_seen,
                l3_ac6_on_own, l3_ac6_practice, l3_ac6_assistance, l3_ac6_not_seen,
                l3_ac7_on_own, l3_ac7_practice, l3_ac7_assistance, l3_ac7_not_seen,
                l3_ac8_on_own, l3_ac8_practice, l3_ac8_assistance, l3_ac8_not_seen,
                pb_ac1_on_own, pb_ac1_practice, pb_ac1_assistance, pb_ac1_not_seen,
                pb_ac2_on_own, pb_ac2_practice, pb_ac2_assistance, pb_ac2_not_seen,
                pb_ac3_on_own, pb_ac3_practice, pb_ac3_assistance, pb_ac3_not_seen,
                pl_ac1_on_own, pl_ac1_practice, pl_ac1_assistance, pl_ac1_not_seen,
                pl_ac2_on_own, pl_ac2_practice, pl_ac2_assistance, pl_ac2_not_seen,
                pl_ac3_on_own, pl_ac3_practice, pl_ac3_assistance, pl_ac3_not_seen,
                pl_ac4_on_own, pl_ac4_practice, pl_ac4_assistance, pl_ac4_not_seen,
                pl_ac5_on_own, pl_ac5_practice, pl_ac5_assistance, pl_ac5_not_seen,
                pf_ac1_on_own, pf_ac1_practice, pf_ac1_assistance, pf_ac1_not_seen,
                pf_ac2_on_own, pf_ac2_practice, pf_ac2_assistance, pf_ac2_not_seen
            )
            SELECT
                fao.Delivery_Key, fao.Course_Key, a.Course_Code,
                SUM(CASE WHEN a.Activity_Code = 'l1-ac2' AND fao.Outcome_Score = 1 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'l1-ac2' AND fao.Outcome_Score = 2 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'l1-ac2' AND fao.Outcome_Score = 3 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'l1-ac2' AND fao.Outcome_Score = 4 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'l1-ac3' AND fao.Outcome_Score = 1 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'l1-ac3' AND fao.Outcome_Score = 2 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'l1-ac3' AND fao.Outcome_Score = 3 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'l1-ac3' AND fao.Outcome_Score = 4 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'l1-ac4' AND fao.Outcome_Score = 1 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'l1-ac4' AND fao.Outcome_Score = 2 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'l1-ac4' AND fao.Outcome_Score = 3 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'l1-ac4' AND fao.Outcome_Score = 4 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'l1-ac5' AND fao.Outcome_Score = 1 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'l1-ac5' AND fao.Outcome_Score = 2 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'l1-ac5' AND fao.Outcome_Score = 3 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'l1-ac5' AND fao.Outcome_Score = 4 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'l2-ac1' AND fao.Outcome_Score = 1 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'l2-ac1' AND fao.Outcome_Score = 2 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'l2-ac1' AND fao.Outcome_Score = 3 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'l2-ac1' AND fao.Outcome_Score = 4 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'l2-ac2' AND fao.Outcome_Score = 1 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'l2-ac2' AND fao.Outcome_Score = 2 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'l2-ac2' AND fao.Outcome_Score = 3 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'l2-ac2' AND fao.Outcome_Score = 4 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'l2-ac3' AND fao.Outcome_Score = 1 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'l2-ac3' AND fao.Outcome_Score = 2 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'l2-ac3' AND fao.Outcome_Score = 3 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'l2-ac3' AND fao.Outcome_Score = 4 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'l2-ac4' AND fao.Outcome_Score = 1 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'l2-ac4' AND fao.Outcome_Score = 2 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'l2-ac4' AND fao.Outcome_Score = 3 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'l2-ac4' AND fao.Outcome_Score = 4 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'l2-ac5' AND fao.Outcome_Score = 1 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'l2-ac5' AND fao.Outcome_Score = 2 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'l2-ac5' AND fao.Outcome_Score = 3 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'l2-ac5' AND fao.Outcome_Score = 4 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'l2-ac6' AND fao.Outcome_Score = 1 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'l2-ac6' AND fao.Outcome_Score = 2 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'l2-ac6' AND fao.Outcome_Score = 3 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'l2-ac6' AND fao.Outcome_Score = 4 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'l2-ac7' AND fao.Outcome_Score = 1 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'l2-ac7' AND fao.Outcome_Score = 2 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'l2-ac7' AND fao.Outcome_Score = 3 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'l2-ac7' AND fao.Outcome_Score = 4 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'l3-ac1' AND fao.Outcome_Score = 1 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'l3-ac1' AND fao.Outcome_Score = 2 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'l3-ac1' AND fao.Outcome_Score = 3 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'l3-ac1' AND fao.Outcome_Score = 4 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'l3-ac2' AND fao.Outcome_Score = 1 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'l3-ac2' AND fao.Outcome_Score = 2 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'l3-ac2' AND fao.Outcome_Score = 3 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'l3-ac2' AND fao.Outcome_Score = 4 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'l3-ac3' AND fao.Outcome_Score = 1 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'l3-ac3' AND fao.Outcome_Score = 2 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'l3-ac3' AND fao.Outcome_Score = 3 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'l3-ac3' AND fao.Outcome_Score = 4 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'l3-ac4' AND fao.Outcome_Score = 1 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'l3-ac4' AND fao.Outcome_Score = 2 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'l3-ac4' AND fao.Outcome_Score = 3 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'l3-ac4' AND fao.Outcome_Score = 4 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'l3-ac5' AND fao.Outcome_Score = 1 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'l3-ac5' AND fao.Outcome_Score = 2 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'l3-ac5' AND fao.Outcome_Score = 3 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'l3-ac5' AND fao.Outcome_Score = 4 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'l3-ac6' AND fao.Outcome_Score = 1 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'l3-ac6' AND fao.Outcome_Score = 2 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'l3-ac6' AND fao.Outcome_Score = 3 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'l3-ac6' AND fao.Outcome_Score = 4 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'l3-ac7' AND fao.Outcome_Score = 1 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'l3-ac7' AND fao.Outcome_Score = 2 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'l3-ac7' AND fao.Outcome_Score = 3 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'l3-ac7' AND fao.Outcome_Score = 4 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'l3-ac8' AND fao.Outcome_Score = 1 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'l3-ac8' AND fao.Outcome_Score = 2 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'l3-ac8' AND fao.Outcome_Score = 3 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'l3-ac8' AND fao.Outcome_Score = 4 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'pb-ac1' AND fao.Outcome_Score = 1 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'pb-ac1' AND fao.Outcome_Score = 2 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'pb-ac1' AND fao.Outcome_Score = 3 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'pb-ac1' AND fao.Outcome_Score = 4 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'pb-ac2' AND fao.Outcome_Score = 1 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'pb-ac2' AND fao.Outcome_Score = 2 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'pb-ac2' AND fao.Outcome_Score = 3 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'pb-ac2' AND fao.Outcome_Score = 4 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'pb-ac3' AND fao.Outcome_Score = 1 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'pb-ac3' AND fao.Outcome_Score = 2 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'pb-ac3' AND fao.Outcome_Score = 3 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'pb-ac3' AND fao.Outcome_Score = 4 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'pl-ac1' AND fao.Outcome_Score = 1 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'pl-ac1' AND fao.Outcome_Score = 2 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'pl-ac1' AND fao.Outcome_Score = 3 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'pl-ac1' AND fao.Outcome_Score = 4 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'pl-ac2' AND fao.Outcome_Score = 1 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'pl-ac2' AND fao.Outcome_Score = 2 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'pl-ac2' AND fao.Outcome_Score = 3 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'pl-ac2' AND fao.Outcome_Score = 4 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'pl-ac3' AND fao.Outcome_Score = 1 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'pl-ac3' AND fao.Outcome_Score = 2 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'pl-ac3' AND fao.Outcome_Score = 3 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'pl-ac3' AND fao.Outcome_Score = 4 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'pl-ac4' AND fao.Outcome_Score = 1 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'pl-ac4' AND fao.Outcome_Score = 2 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'pl-ac4' AND fao.Outcome_Score = 3 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'pl-ac4' AND fao.Outcome_Score = 4 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'pl-ac5' AND fao.Outcome_Score = 1 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'pl-ac5' AND fao.Outcome_Score = 2 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'pl-ac5' AND fao.Outcome_Score = 3 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'pl-ac5' AND fao.Outcome_Score = 4 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'pf-ac1' AND fao.Outcome_Score = 1 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'pf-ac1' AND fao.Outcome_Score = 2 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'pf-ac1' AND fao.Outcome_Score = 3 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'pf-ac1' AND fao.Outcome_Score = 4 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'pf-ac2' AND fao.Outcome_Score = 1 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'pf-ac2' AND fao.Outcome_Score = 2 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'pf-ac2' AND fao.Outcome_Score = 3 THEN 1 ELSE 0 END),
                SUM(CASE WHEN a.Activity_Code = 'pf-ac2' AND fao.Outcome_Score = 4 THEN 1 ELSE 0 END)
            FROM Fact_Rider_Activity_Outcome fao
            JOIN Dim_Course_Activity a ON fao.Activity_Key = a.Activity_Key
            GROUP BY fao.Delivery_Key, fao.Course_Key, a.Course_Code
            ";

            $this->dwh->statement($sql);
            return "Fact_Course_Activity_Outcome_Summary rollup rebuilt.";

        } catch (Throwable $e) {
            $this->slack->sendSyncError('Fact_Course_Activity_Outcome_Summary', $e->getMessage(), [], $e->getTraceAsString());
            throw $e;
        }
    }

    public function syncFactGrantFinancials($command = null)
    {
        $sourceSystemKey = $this->getSourceSystemKey();
        $watermark = $this->getWatermark('Fact_Grant_Financials');

        $query = $this->source->table('grant_format_dft')
            ->where('updated_at', '>', $watermark)
            ->orderBy('updated_at', 'asc');

        return $this->executeSyncRunner(
            'Fact_Grant_Financials',
            $command,
            $query,
            function ($fmt, $state) use ($sourceSystemKey) {
                $grantKey = $this->resolveGrantKey($fmt->grant_id, $sourceSystemKey);
                if (!$grantKey) {
                    return;
                }

                $this->dwh->table('Fact_Grant_Financials')->updateOrInsert(
                    ['Source_Format_Id' => $fmt->id, 'Source_System_Key' => $sourceSystemKey],
                    [
                        'Grant_Key'              => $grantKey,
                        'Max_Allocation'         => $fmt->max_allocation ?? 0.00,
                        'Total_Levels'           => $fmt->total_levels ?? 0,
                        'Total_Plus'             => $fmt->total_plus ?? 0,
                        'Places_Level_1'         => $fmt->places_level_1 ?? 0,
                        'Grant_Level_1'          => $fmt->grant_level_1 ?? 0.00,
                        'Places_Level_1_2'       => $fmt->places_level_1_2 ?? 0,
                        'Grant_Level_1_2'        => $fmt->grant_level_1_2 ?? 0.00,
                        'Places_Level_2'         => $fmt->places_level_2 ?? 0,
                        'Grant_Level_2'          => $fmt->grant_level_2 ?? 0.00,
                        'Places_Level_3'         => $fmt->places_level_3 ?? 0,
                        'Grant_Level_3'          => $fmt->grant_level_3 ?? 0.00,
                        'Places_Plus_Balance'    => $fmt->places_plus_balance ?? 0,
                        'Grant_Plus_Balance'     => $fmt->grant_plus_balance ?? 0.00,
                        'Places_Plus_Bus'        => $fmt->places_plus_bus ?? 0,
                        'Grant_Plus_Bus'         => $fmt->grant_plus_bus ?? 0.00,
                        'Places_Plus_Fix'        => $fmt->places_plus_fix ?? 0,
                        'Grant_Plus_Fix'         => $fmt->grant_plus_fix ?? 0.00,
                        'Places_Plus_Learn'      => $fmt->places_plus_learn ?? 0,
                        'Grant_Plus_Learn'       => $fmt->grant_plus_learn ?? 0.00,
                        'Places_Plus_On_Show'    => $fmt->places_plus_on_show ?? 0,
                        'Grant_Plus_On_Show'     => $fmt->grant_plus_on_show ?? 0.00,
                        'Places_Plus_Parents'    => $fmt->places_plus_parents ?? 0,
                        'Grant_Plus_Parents'     => $fmt->grant_plus_parents ?? 0.00,
                        'Places_Plus_Promotion'  => $fmt->places_plus_promotion ?? 0,
                        'Grant_Plus_Promotion'   => $fmt->grant_plus_promotion ?? 0.00,
                        'Places_Plus_Recycled'   => $fmt->places_plus_recycled ?? 0,
                        'Grant_Plus_Recycled'    => $fmt->grant_plus_recycled ?? 0.00,
                        'Places_Plus_Ride'       => $fmt->places_plus_ride ?? 0,
                        'Grant_Plus_Ride'        => $fmt->grant_plus_ride ?? 0.00,
                        'Places_Plus_Transition' => $fmt->places_plus_transition ?? 0,
                        'Grant_Plus_Transition'  => $fmt->grant_plus_transition ?? 0.00,
                        'Places_Plus_Family'     => $fmt->places_plus_family ?? 0,
                        'Grant_Plus_Family'      => $fmt->grant_plus_family ?? 0.00,
                        'Places_Plus_Adult'      => $fmt->places_plus_adult ?? 0,
                        'Grant_Plus_Adult'       => $fmt->grant_plus_adult ?? 0.00,
                        'Grant_Send'             => $fmt->grant_send ?? 0.00,
                        'Places_Send'            => $fmt->places_send ?? 0,
                        'Grant_Inclusion'        => $fmt->grant_inclusion ?? 0.00,
                        'Places_Inclusion'       => $fmt->places_inclusion ?? 0,
                        'updated_at'             => now(),
                    ]
                );

                if ($fmt->updated_at && (!$state->highestTimestamp || $fmt->updated_at > $state->highestTimestamp)) {
                    $state->highestTimestamp = $fmt->updated_at;
                }
            },
            function ($state, $count) {
                if ($state->highestTimestamp) {
                    $this->updateWatermark('Fact_Grant_Financials', $state->highestTimestamp, $count);
                }
            }
        );
    }

    public function syncFactGrantReallocations($command = null)
    {
        $sourceSystemKey = $this->getSourceSystemKey();
        $watermark = $this->getWatermark('Fact_Grant_Reallocations');

        $query = $this->source->table('grant_reallocations')
            ->where('updated_at', '>', $watermark)
            ->orderBy('updated_at', 'asc');

        return $this->executeSyncRunner(
            'Fact_Grant_Reallocations',
            $command,
            $query,
            function ($ra, $state) use ($sourceSystemKey) {
                $grantKey = $this->resolveGrantKey($ra->grant_id, $sourceSystemKey);
                if (!$grantKey) {
                    return;
                }

                $existingKeys = $this->dwh->table('Fact_Grant_Reallocations')
                    ->where('Source_Reallocation_Id', $ra->id)
                    ->where('Source_System_Key', $sourceSystemKey)
                    ->pluck('Reallocation_Key')
                    ->toArray();

                if (!empty($existingKeys)) {
                    $this->dwh->table('Fact_Grant_Reallocation_Logs')->whereIn('Reallocation_Key', $existingKeys)->delete();
                    $this->dwh->table('Fact_Grant_Reallocations')->whereIn('Reallocation_Key', $existingKeys)->delete();
                }

                $reallocationKey = $this->dwh->table('Fact_Grant_Reallocations')->insertGetId([
                    'Grant_Key'                     => $grantKey,
                    'Source_System_Key'             => $sourceSystemKey,
                    'Source_Reallocation_Id'         => $ra->id,
                    'Reallocation_Number'           => $ra->reallocation_number,
                    'Status_Raw'                    => $ra->status,
                    'Date_Approved'                 => $ra->date_approved,
                    'Reallocation_Type'             => $ra->reallocation_type,
                    'Reallocation_Notes'            => $ra->reallocation_notes,
                    'Reallocation_Increase_Reasons' => $ra->reallocation_increase_reasons,
                    'created_at'                    => now(),
                    'updated_at'                    => now(),
                ]);

                if (!$this->sourceHasColumn('grant_reallocations', 'reallocation_log')) {
                    $logs = $this->source->table('grant_reallocation_logs')->where('grant_reallocation_id', $ra->id)->get();
                    foreach ($logs as $log) {
                        $this->dwh->table('Fact_Grant_Reallocation_Logs')->insert([
                            'Reallocation_Key'  => $reallocationKey,
                            'Source_System_Key' => $sourceSystemKey,
                            'Module_Key'        => $log->module_key,
                            'Value_Count'       => $log->value,
                            'Amount'            => $log->amount,
                            'created_at'        => now(),
                            'updated_at'        => now(),
                        ]);
                    }
                } else {
                    $logs = json_decode($ra->reallocation_log, true) ?? [];
                    foreach ($logs as $key => $node) {
                        $this->dwh->table('Fact_Grant_Reallocation_Logs')->insert([
                            'Reallocation_Key'  => $reallocationKey,
                            'Source_System_Key' => $sourceSystemKey,
                            'Module_Key'        => $key,
                            'Value_Count'       => $node['value'] ?? 0,
                            'Amount'            => $node['amount'] ?? 0.00,
                            'created_at'        => now(),
                            'updated_at'        => now(),
                        ]);
                    }
                }

                if ($ra->updated_at && (!$state->highestTimestamp || $ra->updated_at > $state->highestTimestamp)) {
                    $state->highestTimestamp = $ra->updated_at;
                }
            },
            function ($state, $count) {
                if ($state->highestTimestamp) {
                    $this->updateWatermark('Fact_Grant_Reallocations', $state->highestTimestamp, $count);
                }
            }
        );
    }

    public function syncFactGrantAmendments($command = null)
    {
        $sourceSystemKey = $this->getSourceSystemKey();
        $watermark = $this->getWatermark('Fact_Grant_Amendments');

        $query = $this->source->table('grant_amendments')
            ->where('updated_at', '>', $watermark)
            ->orderBy('updated_at', 'asc');

        return $this->executeSyncRunner(
            'Fact_Grant_Amendments',
            $command,
            $query,
            function ($am, $state) use ($sourceSystemKey) {
                $grantKey = $this->resolveGrantKey($am->grant_id, $sourceSystemKey);
                if (!$grantKey) {
                    return;
                }

                $existingKeys = $this->dwh->table('Fact_Grant_Amendments')
                    ->where('Source_Amendment_Id', $am->id)
                    ->where('Source_System_Key', $sourceSystemKey)
                    ->pluck('Amendment_Key')
                    ->toArray();

                if (!empty($existingKeys)) {
                    $this->dwh->table('Fact_Grant_Amendment_Logs')->whereIn('Amendment_Key', $existingKeys)->delete();
                    $this->dwh->table('Fact_Grant_Amendments')->whereIn('Amendment_Key', $existingKeys)->delete();
                }

                $amendmentKey = $this->dwh->table('Fact_Grant_Amendments')->insertGetId([
                    'Grant_Key'           => $grantKey,
                    'Source_System_Key'   => $sourceSystemKey,
                    'Source_Amendment_Id' => $am->id,
                    'Amendment_Number'    => $am->amendment_number,
                    'Status_Raw'          => $am->status,
                    'Date_Approved'       => $am->date_approved,
                    'created_at'          => now(),
                    'updated_at'          => now(),
                ]);

                if (!$this->sourceHasColumn('grant_amendments', 'amendment_log')) {
                    $logs = $this->source->table('grant_amendment_logs')->where('grant_amendment_id', $am->id)->get();
                    foreach ($logs as $log) {
                        $this->dwh->table('Fact_Grant_Amendment_Logs')->insert([
                            'Amendment_Key'     => $amendmentKey,
                            'Source_System_Key' => $sourceSystemKey,
                            'Type_Label'        => $log->type,
                            'Module_Key'        => $log->module,
                            'Value_Count'       => $log->value,
                            'Amount'            => $log->amount,
                            'created_at'        => now(),
                            'updated_at'        => now(),
                        ]);
                    }
                } else {
                    $logs = json_decode($am->amendment_log, true) ?? [];
                    foreach ($logs as $key => $node) {
                        $this->dwh->table('Fact_Grant_Amendment_Logs')->insert([
                            'Amendment_Key'     => $amendmentKey,
                            'Source_System_Key' => $sourceSystemKey,
                            'Type_Label'        => $node['type'] ?? null,
                            'Module_Key'        => $node['module'] ?? $key,
                            'Value_Count'       => $node['value'] ?? 0,
                            'Amount'            => $node['amount'] ?? 0.00,
                            'created_at'        => now(),
                            'updated_at'        => now(),
                        ]);
                    }
                }

                if ($am->updated_at && (!$state->highestTimestamp || $am->updated_at > $state->highestTimestamp)) {
                    $state->highestTimestamp = $am->updated_at;
                }
            },
            function ($state, $count) {
                if ($state->highestTimestamp) {
                    $this->updateWatermark('Fact_Grant_Amendments', $state->highestTimestamp, $count);
                }
            }
        );
    }

    public function syncFactGrantClaims($command = null)
    {
        $sourceSystemKey = $this->getSourceSystemKey();
        $watermark = $this->getWatermark('Fact_Grant_Claims');

        $query = $this->source->table('grant_claims')
            ->where('updated_at', '>', $watermark)
            ->orderBy('updated_at', 'asc');

        return $this->executeSyncRunner(
            'Fact_Grant_Claims',
            $command,
            $query,
            function ($cl, $state) use ($sourceSystemKey) {
                $grantKey = $this->resolveGrantKey($cl->grant_id, $sourceSystemKey);
                if (!$grantKey) {
                    return;
                }

                $existingClaimKeys = $this->dwh->table('Fact_Grant_Claims')
                    ->where('Source_Claim_Id', $cl->id)
                    ->where('Source_System_Key', $sourceSystemKey)
                    ->pluck('Claim_Key')
                    ->toArray();

                if (!empty($existingClaimKeys)) {
                    $this->dwh->table('Fact_Grant_Claim_Inclusions')->whereIn('Claim_Key', $existingClaimKeys)->delete();
                    $this->dwh->table('Fact_Grant_Claim_Send_Records')->whereIn('Claim_Key', $existingClaimKeys)->delete();
                    $this->dwh->table('Fact_Grant_Claim_Logs')->whereIn('Claim_Key', $existingClaimKeys)->delete();
                    $this->dwh->table('Fact_Grant_Claims')->whereIn('Claim_Key', $existingClaimKeys)->delete();
                }

                $claimKey = $this->dwh->table('Fact_Grant_Claims')->insertGetId([
                    'Grant_Key'                   => $grantKey,
                    'Source_System_Key'           => $sourceSystemKey,
                    'Source_Claim_Id'             => $cl->id,
                    'Claim_Number'                => $cl->claim_number,
                    'Status_Raw'                  => $cl->status,
                    'Pref_Authority_Given'        => (bool) $cl->pref_authority_given,
                    'Pref_Claim_Paid'             => (bool) $cl->pref_claim_paid,
                    'Date_Approved'               => $cl->date_approved,
                    'Delivery_On_Track_Prediction'=> $cl->delivery_on_track_prediction,
                    'Send_Claimable_Amount'       => is_numeric($cl->send_claimable_amount) ? (float) $cl->send_claimable_amount : 0.00,
                    'Inclusion_Claimable_Amount'  => is_numeric($cl->inclusion_claimable_amount) ? (float) $cl->inclusion_claimable_amount : 0.00,
                    'created_at'                  => now(),
                    'updated_at'                  => now(),
                ]);

                // 1. Logs
                if (!$this->sourceHasColumn('grant_claims', 'claim_log')) {
                    $logs = $this->source->table('grant_claim_logs')->where('grant_claim_id', $cl->id)->get();
                    foreach ($logs as $log) {
                        $this->dwh->table('Fact_Grant_Claim_Logs')->insert([
                            'Claim_Key'         => $claimKey,
                            'Source_System_Key' => $sourceSystemKey,
                            'Module_Key'        => $log->module_key,
                            'Item_Count'        => $log->item_count,
                            'created_at'        => now(),
                            'updated_at'        => now(),
                        ]);
                    }
                } else {
                    $logs = json_decode($cl->claim_log, true) ?? [];
                    foreach ($logs as $key => $count) {
                        $this->dwh->table('Fact_Grant_Claim_Logs')->insert([
                            'Claim_Key'         => $claimKey,
                            'Source_System_Key' => $sourceSystemKey,
                            'Module_Key'        => $key,
                            'Item_Count'        => $count,
                            'created_at'        => now(),
                            'updated_at'        => now(),
                        ]);
                    }
                }

                // 2. SEND Records
                if (!$this->sourceHasColumn('grant_claims', 'send_records')) {
                    $sends = $this->source->table('grant_claim_send_records')->where('grant_claim_id', $cl->id)->get();
                    foreach ($sends as $s) {
                        $this->dwh->table('Fact_Grant_Claim_Send_Records')->insert([
                            'Claim_Key'         => $claimKey,
                            'Source_System_Key' => $sourceSystemKey,
                            'Send_Id_String'    => !empty($s->send_id) ? $s->send_id : (!empty($s->send_id_string) ? $s->send_id_string : 'raw'),
                            'Send_Riders_Count' => !empty($s->send_riders) ? (int) $s->send_riders : 0,
                            'Send_Amount'       => (!empty($s->send_amount) && is_numeric($s->send_amount)) ? (float) $s->send_amount : 0.00,
                            'created_at'        => now(),
                            'updated_at'        => now(),
                        ]);
                    }
                } else {
                    $sends = json_decode($cl->send_records, true) ?? [];
                    foreach ($sends as $node) {
                        $this->dwh->table('Fact_Grant_Claim_Send_Records')->insert([
                            'Claim_Key'         => $claimKey,
                            'Source_System_Key' => $sourceSystemKey,
                            'Send_Id_String'    => !empty($node['sendId']) ? $node['sendId'] : 'raw',
                            'Send_Riders_Count' => !empty($node['sendRiders']) ? (int) $node['sendRiders'] : 0,
                            'Send_Amount'       => (!empty($node['sendAmount']) && is_numeric($node['sendAmount'])) ? (float) $node['sendAmount'] : 0.00,
                            'created_at'        => now(),
                            'updated_at'        => now(),
                        ]);
                    }
                }

                // 3. Inclusion Records
                if (!$this->sourceHasColumn('grant_claims', 'inclusion_records')) {
                    $inclusions = $this->source->table('grant_claim_inclusions')->where('grant_claim_id', $cl->id)->get();
                    foreach ($inclusions as $inc) {
                        $this->dwh->table('Fact_Grant_Claim_Inclusions')->insert([
                            'Claim_Key'           => $claimKey,
                            'Source_System_Key'   => $sourceSystemKey,
                            'Inclusion_Id_String' => !empty($inc->inclusion_id) ? $inc->inclusion_id : (!empty($inc->inclusion_id_string) ? $inc->inclusion_id_string : 'raw'),
                            'Inclusion_Category'  => !empty($inc->inclusion_category) ? $inc->inclusion_category : 'N/A',
                            'Inclusion_Delivery'  => !empty($inc->inclusion_delivery) ? $inc->inclusion_delivery : null,
                            'Inclusion_Amount'    => (!empty($inc->inclusion_amount) && is_numeric($inc->inclusion_amount)) ? (float) $inc->inclusion_amount : 0.00,
                            'created_at'          => now(),
                            'updated_at'          => now(),
                        ]);
                    }
                } else {
                    $inclusions = json_decode($cl->inclusion_records, true) ?? [];
                    foreach ($inclusions as $node) {
                        $this->dwh->table('Fact_Grant_Claim_Inclusions')->insert([
                            'Claim_Key'           => $claimKey,
                            'Source_System_Key'   => $sourceSystemKey,
                            'Inclusion_Id_String' => !empty($node['inclusionId']) ? $node['inclusionId'] : 'raw',
                            'Inclusion_Category'  => !empty($node['inclusionCategory']) ? $node['inclusionCategory'] : 'N/A',
                            'Inclusion_Delivery'  => isset($node['inclusionDelivery']) && $node['inclusionDelivery'] !== '' ? $node['inclusionDelivery'] : null,
                            'Inclusion_Amount'    => (!empty($node['inclusionAmount']) && is_numeric($node['inclusionAmount'])) ? (float) $node['inclusionAmount'] : 0.00,
                            'created_at'          => now(),
                            'updated_at'          => now(),
                        ]);
                    }
                }

                if ($cl->updated_at && (!$state->highestTimestamp || $cl->updated_at > $state->highestTimestamp)) {
                    $state->highestTimestamp = $cl->updated_at;
                }
            },
            function ($state, $count) {
                if ($state->highestTimestamp) {
                    $this->updateWatermark('Fact_Grant_Claims', $state->highestTimestamp, $count);
                }
            }
        );
    }

    public function syncFactGrantFormatDft($command = null)
    {
        $sourceSystemKey = $this->getSourceSystemKey();
        $watermark = $this->getWatermark('Fact_Grant_Allocation_Dft');

        $query = $this->source->table('grant_format_dft')
            ->where('updated_at', '>', $watermark)
            ->whereNull('deleted_at')
            ->orderBy('id', 'asc');

        return $this->executeSyncRunner(
            'Fact_Grant_Allocation_Dft',
            $command,
            $query,
            function ($row, $state) use ($sourceSystemKey) {
                $grantKey = $this->resolveGrantKey($row->grant_id, $sourceSystemKey);
                if (!$grantKey) {
                    return;
                }

                $this->dwh->table('Fact_Grant_Allocation_Dft')->updateOrInsert(
                    [
                        'Source_Allocation_Id' => $row->id,
                        'Source_System_Key'    => $sourceSystemKey,
                    ],
                    [
                        'Grant_Key'                => $grantKey,
                        'Source_Grant_Id'          => $row->grant_id,
                        'Max_Allocation'           => (float) ($row->max_allocation ?? 0),
                        'Total_Levels'             => (int) ($row->total_levels ?? 0),
                        'Total_Plus'               => (int) ($row->total_plus ?? 0),
                        'Places_Level_1'           => (int) ($row->places_level_1 ?? 0),
                        'Grant_Level_1'            => (float) ($row->grant_level_1 ?? 0),
                        'Places_Level_1_2'         => (int) ($row->places_level_1_2 ?? 0),
                        'Grant_Level_1_2'          => (float) ($row->grant_level_1_2 ?? 0),
                        'Places_Level_2'           => (int) ($row->places_level_2 ?? 0),
                        'Grant_Level_2'            => (float) ($row->grant_level_2 ?? 0),
                        'Places_Level_3'           => (int) ($row->places_level_3 ?? 0),
                        'Grant_Level_3'            => (float) ($row->grant_level_3 ?? 0),
                        'Places_Plus_Balance'      => (int) ($row->places_plus_balance ?? 0),
                        'Grant_Plus_Balance'       => (float) ($row->grant_plus_balance ?? 0),
                        'Places_Plus_Bus'          => (int) ($row->places_plus_bus ?? 0),
                        'Grant_Plus_Bus'           => (float) ($row->grant_plus_bus ?? 0),
                        'Places_Plus_Fix'          => (int) ($row->places_plus_fix ?? 0),
                        'Grant_Plus_Fix'           => (float) ($row->grant_plus_fix ?? 0),
                        'Places_Plus_Learn'        => (int) ($row->places_plus_learn ?? 0),
                        'Grant_Plus_Learn'         => (float) ($row->grant_plus_learn ?? 0),
                        'Places_Plus_On_Show'      => (int) ($row->places_plus_on_show ?? 0),
                        'Grant_Plus_On_Show'       => (float) ($row->grant_plus_on_show ?? 0),
                        'Places_Plus_Parents'      => (int) ($row->places_plus_parents ?? 0),
                        'Grant_Plus_Parents'       => (float) ($row->grant_plus_parents ?? 0),
                        'Places_Plus_Promotion'    => (int) ($row->places_plus_promotion ?? 0),
                        'Grant_Plus_Promotion'     => (float) ($row->grant_plus_promotion ?? 0),
                        'Places_Plus_Recycled'     => (int) ($row->places_plus_recycled ?? 0),
                        'Grant_Plus_Recycled'      => (float) ($row->grant_plus_recycled ?? 0),
                        'Places_Plus_Ride'         => (int) ($row->places_plus_ride ?? 0),
                        'Grant_Plus_Ride'          => (float) ($row->grant_plus_ride ?? 0),
                        'Places_Plus_Transition'   => (int) ($row->places_plus_transition ?? 0),
                        'Grant_Plus_Transition'    => (float) ($row->grant_plus_transition ?? 0),
                        'Places_Plus_Family'       => (int) ($row->places_plus_family ?? 0),
                        'Grant_Plus_Family'        => (float) ($row->grant_plus_family ?? 0),
                        'Places_Plus_Adult'        => (int) ($row->places_plus_adult ?? 0),
                        'Grant_Plus_Adult'         => (float) ($row->grant_plus_adult ?? 0),
                        'Grant_Send'               => (float) ($row->grant_send ?? 0),
                        'Places_Send'              => (int) ($row->places_send ?? 0),
                        'Grant_Inclusion'          => (float) ($row->grant_inclusion ?? 0),
                        'Places_Inclusion'         => (int) ($row->places_inclusion ?? 0),
                        'Updated_At'               => now(),
                    ]
                );

                if ($row->updated_at && (!$state->highestTimestamp || $row->updated_at > $state->highestTimestamp)) {
                    $state->highestTimestamp = $row->updated_at;
                }
            },
            function ($state, $count) {
                if ($state->highestTimestamp) {
                    $this->updateWatermark('Fact_Grant_Allocation_Dft', $state->highestTimestamp, $count);
                }
            }
        );
    }

    public function syncFactInstructorDeliveries($command = null)
    {
        $sourceSystemKey = $this->getSourceSystemKey();
        $watermark = $this->getWatermark('Fact_Instructor_Delivery');

        $query = $this->source->table('join_deliveries_instructors')
            ->where('updated_at', '>', $watermark)
            ->orderBy('updated_at', 'asc');

        return $this->executeSyncRunner(
            'Fact_Instructor_Delivery',
            $command,
            $query,
            function ($alloc, $state) use ($sourceSystemKey) {
                $deliveryKey = $this->dwh->table('Dim_Delivery_Header')
                    ->where('Source_Delivery_Id', $alloc->delivery_id)
                    ->where('Source_System_Key', $sourceSystemKey)
                    ->value('Delivery_Key');

                $instructorKey = $this->dwh->table('Dim_Instructor')
                    ->where('Source_Instructor_Id', $alloc->instructor_id)
                    ->where('Source_System_Key', $sourceSystemKey)
                    ->value('Instructor_Key');

                if (!$deliveryKey || !$instructorKey) {
                    return;
                }

                $this->dwh->table('Fact_Instructor_Delivery')->updateOrInsert(
                    [
                        'Delivery_Key'      => $deliveryKey,
                        'Instructor_Key'    => $instructorKey,
                        'Source_System_Key' => $sourceSystemKey,
                    ],
                    [
                        'Instructor_Notified' => (bool) $alloc->instructor_notified,
                        'Source_Created_At'   => $alloc->created_at,
                        'Source_Updated_At'   => $alloc->updated_at,
                        'Source_Deleted_At'   => $alloc->deleted_at,
                        'updated_at'          => now(),
                    ]
                );

                if ($alloc->updated_at && (!$state->highestTimestamp || $alloc->updated_at > $state->highestTimestamp)) {
                    $state->highestTimestamp = $alloc->updated_at;
                }
            },
            function ($state, $count) {
                if ($state->highestTimestamp) {
                    $this->updateWatermark('Fact_Instructor_Delivery', $state->highestTimestamp, $count);
                }
            }
        );
    }

    public function syncFactInstructorCourse($command = null)
    {
        $sourceSystemKey = $this->getSourceSystemKey();
        $now = now()->toDateTimeString();

        try {
            $sourcePairs = $this->source->table('join_instructors_courses')
                ->select('instructor_id', 'course_id')
                ->get()
                ->map(fn($row) => "{$row->instructor_id}-{$row->course_id}")
                ->toArray();

            $dwhActiveRows = $this->dwh->table('Fact_Instructor_Course')
                ->join('Dim_Instructor', 'Fact_Instructor_Course.Instructor_Key', '=', 'Dim_Instructor.Instructor_Key')
                ->join('Dim_Course', 'Fact_Instructor_Course.Course_Key', '=', 'Dim_Course.Course_Key')
                ->where('Fact_Instructor_Course.Is_Current', 1)
                ->where('Fact_Instructor_Course.Source_System_Key', $sourceSystemKey)
                ->select([
                    'Fact_Instructor_Course.Instructor_Course_Key',
                    'Dim_Instructor.Source_Instructor_Id',
                    'Dim_Course.Source_Course_Id',
                ])
                ->get();

            $dwhActivePairs = [];
            foreach ($dwhActiveRows as $row) {
                $dwhActivePairs["{$row->Source_Instructor_Id}-{$row->Source_Course_Id}"] = $row->Instructor_Course_Key;
            }

            // Detect Removals
            $closedCount = 0;
            foreach ($dwhActivePairs as $pairStr => $dwhKey) {
                if (!in_array($pairStr, $sourcePairs)) {
                    $this->dwh->table('Fact_Instructor_Course')
                        ->where('Instructor_Course_Key', $dwhKey)
                        ->update(['Active_To' => $now, 'Is_Current' => 0]);
                    $closedCount++;
                }
            }

            // Detect Additions
            $pairsToInsert = array_diff($sourcePairs, array_keys($dwhActivePairs));
            $newCount = 0;

            $bar = ($command && count($pairsToInsert) > 0)
                ? $command->getOutput()->createProgressBar(count($pairsToInsert))
                : null;
            if ($bar) $bar->start();

            foreach ($pairsToInsert as $pairStr) {
                [$sourceInstructorId, $sourceCourseId] = explode('-', $pairStr);

                $instructorKey = $this->dwh->table('Dim_Instructor')
                    ->where('Source_Instructor_Id', $sourceInstructorId)
                    ->where('Source_System_Key', $sourceSystemKey)
                    ->value('Instructor_Key');

                $courseKey = $this->dwh->table('Dim_Course')
                    ->where('Source_Course_Id', $sourceCourseId)
                    ->where('Source_System_Key', $sourceSystemKey)
                    ->value('Course_Key');

                if ($instructorKey && $courseKey) {
                    $this->dwh->table('Fact_Instructor_Course')->insert([
                        'Instructor_Key'    => $instructorKey,
                        'Course_Key'        => $courseKey,
                        'Source_System_Key' => $sourceSystemKey,
                        'Active_From'       => $now,
                        'Active_To'         => null,
                        'Is_Current'        => 1,
                    ]);
                    $newCount++;
                }
                if ($bar) $bar->advance();
            }

            if ($bar) {
                $bar->finish();
                $command?->newLine();
            }

            return "Fact_Instructor_Course synced. (New: {$newCount}, Closed: {$closedCount})";

        } catch (Throwable $e) {
            $this->slack->sendSyncError('Fact_Instructor_Course', $e->getMessage(), [], $e->getTraceAsString());
            throw $e;
        }
    }

    public function syncFactParentSurvey($command = null)
    {
        $sourceSystemKey = $this->getSourceSystemKey();
        $useLegacyFeedback = $this->sourceHasColumn('parent_survey', 'rider_feedback');
        $watermark = $this->getWatermark('Fact_Parent_Survey');

        $query = $this->source->table('parent_survey')
            ->where('status', 1)
            ->where('updated_at', '>', $watermark)
            ->orderBy('updated_at', 'asc');

        return $this->executeSyncRunner(
            'Fact_Parent_Survey',
            $command,
            $query,
            function ($survey, $state) use ($sourceSystemKey, $useLegacyFeedback) {
                $riderKey = $this->dwh->table('Dim_Rider')->where('Source_Rider_Id', $survey->rider_id)->value('Rider_Key');
                $courseKey = $this->dwh->table('Dim_Course')->where('Source_Course_Id', $survey->course_id)->value('Course_Key');
                $delivery = $this->dwh->table('Dim_Delivery_Header')->where('Source_Delivery_Id', $survey->delivery_id)->first();

                if (!$riderKey || !$courseKey || !$delivery) {
                    return;
                }

                if ($useLegacyFeedback) {
                    $feedbackKeys = json_decode($survey->rider_feedback ?? '[]', true);
                } else {
                    $feedbackKeys = $this->source->table('parent_survey_feedback')
                        ->where('parent_survey_id', $survey->id)
                        ->pluck('feedback_key')
                        ->toArray();
                }

                $oq1_data = json_decode($survey->optional_questions, true) ?? [];
                $encouragers = [
                    'Encourage_More_Direct_Routes'        => in_array('oq1_o1', $oq1_data) ? 1 : 0,
                    'Encourage_Local_Route_Awareness'     => in_array('oq1_o2', $oq1_data) ? 1 : 0,
                    'Encourage_Storage'                   => in_array('oq1_o3', $oq1_data) ? 1 : 0,
                    'Encourage_Road_Surfaces'             => in_array('oq1_o4', $oq1_data) ? 1 : 0,
                    'Encourage_Confidence'                => in_array('oq1_o5', $oq1_data) ? 1 : 0,
                    'Encourage_Cycle_Maintenance'         => in_array('oq1_o6', $oq1_data) ? 1 : 0,
                    'Encourage_Local_Initiatives'         => in_array('oq1_o7', $oq1_data) ? 1 : 0,
                    'Encourage_Purchase_Ability'          => in_array('oq1_o8', $oq1_data) ? 1 : 0,
                    'Encourage_Doesnt_Want_To_Cycle_More' => in_array('oq1_o9', $oq1_data) ? 1 : 0,
                    'Encourage_None'                      => in_array('oq1_null', $oq1_data) ? 1 : 0,
                    'Encourage_Other_Reason'              => $survey->optional_questions_input,
                ];

                $agree_disagree_data = json_decode($survey->agree_disagree_options, true) ?? [];
                $pivoted = [];
                foreach ($agree_disagree_data as $item) {
                    if (preg_match('/(oq\d+)_o(\d+)/', $item, $matches)) {
                        $pivoted[$matches[1]] = (int) $matches[2];
                    }
                }

                $likertMetrics = [
                    'Likert_Life_Skill'                    => $pivoted['oq2'] ?? null,
                    'Likert_Self_Esteem'                   => $pivoted['oq3'] ?? null,
                    'Likert_Fitness'                       => $pivoted['oq4'] ?? null,
                    'Likert_Active'                        => $pivoted['oq5'] ?? null,
                    'Likert_Mindfulness'                   => $pivoted['oq6'] ?? null,
                    'Likert_Improve_Self_Regulate'         => $pivoted['oq7'] ?? null,
                    'Likert_Improve_Concentration'         => $pivoted['oq8'] ?? null,
                    'Likert_Improve_Academic_Performance'  => $pivoted['oq9'] ?? null,
                    'Likert_Independence'                  => $pivoted['oq10'] ?? null,
                    'Likert_Improve_Road_Awareness'        => $pivoted['oq11'] ?? null,
                    'Likert_Improve_Environment_Awareness' => $pivoted['oq12'] ?? null,
                    'Likert_Help_Socialise'                => $pivoted['oq13'] ?? null,
                    'Likert_Make_Children_Happy'           => $pivoted['oq14'] ?? null,
                    'Likert_Keep_Children_Occupied'        => $pivoted['oq15'] ?? null,
                    'Likert_Encourage_Children_Outside'    => $pivoted['oq16'] ?? null,
                    'Likert_Children_Less_Dependent'       => $pivoted['oq17'] ?? null,
                    'Likert_Reduce_Other_Transport_Expense'=> $pivoted['oq18'] ?? null,
                    'Likert_Enable_Cycle_As_Family'        => $pivoted['oq19'] ?? null,
                ];

                $this->dwh->table('Fact_Parent_Survey')->updateOrInsert(
                    ['Source_Survey_Id' => $survey->id],
                    array_merge($encouragers, $likertMetrics, [
                        'Date_Key'                          => str_replace('-', '', substr($survey->created_at, 0, 10)),
                        'Rider_Key'                         => $riderKey,
                        'Course_Key'                        => $courseKey,
                        'Delivery_Key'                      => $delivery->Delivery_Key,
                        'Grant_Key'                         => $delivery->Grant_Key,
                        'Like_To_Participate'               => $survey->like_to_participation,
                        'Like_To_Answer_Survey'             => $survey->like_to_answer_survey,
                        'Pref_Join_Bikeability'             => $survey->pref_join_bikeability,
                        'Rider_Emotion'                     => $survey->rider_emotion,
                        'Pref_More_Training'                => $survey->pref_more_training,
                        'Pref_Interest_In_Training'         => $survey->pref_interest_in_training,
                        'Feedback_Is_Fun'                   => in_array('rfq1_rf1', $feedbackKeys) ? 1 : 0,
                        'Feedback_Is_Hard'                  => in_array('rfq1_rf2', $feedbackKeys) ? 1 : 0,
                        'Feedback_Is_Healthy'               => in_array('rfq1_rf3', $feedbackKeys) ? 1 : 0,
                        'Feedback_Still_New'                => in_array('rfq1_rf4', $feedbackKeys) ? 1 : 0,
                        'Feedback_Family_Friends'           => in_array('rfq1_rf5', $feedbackKeys) ? 1 : 0,
                        'Feedback_Dont_See_Others_Like_Me'  => in_array('rfq1_rf6', $feedbackKeys) ? 1 : 0,
                        'Feedback_On_Own'                   => in_array('rfq1_rf7', $feedbackKeys) ? 1 : 0,
                        'Feedback_Not_Enjoy'                => in_array('rfq1_rf8', $feedbackKeys) ? 1 : 0,
                        'Feedback_None_Apply'               => in_array('rfq1_null', $feedbackKeys) ? 1 : 0,
                        'Feedback_None_Apply_Input'         => $survey->rider_feedback_input,
                        'Confidence_Bike_General'           => $this->extractOptionInt('co', $survey->confidence_to_use_a_bike),
                        'Confidence_Road'                   => $this->extractOptionInt('co', $survey->confidence_to_use_bike_on_road),
                        'Confidence_Independent'            => $this->extractOptionInt('co', $survey->confidence_to_use_bike_independently),
                        'Frequency_School'                  => $this->extractOptionInt('fo', $survey->frequency_usable_to_and_from_school),
                        'Frequency_Leisure'                 => $this->extractOptionInt('fo', $survey->frequency_usable_leisure_and_social),
                        'Frequency_Exercise'                => $this->extractOptionInt('fo', $survey->frequency_usable_exercise),
                        'Encouragement_Use_Bike'            => $this->extractOptionInt('eo', $survey->encouragement_to_use_a_bike),
                        'Encouragement_Use_Bike_On_Road'    => $this->extractOptionInt('eo', $survey->encouragement_to_use_bike_on_road),
                        'Likely_To_Recommend'               => $survey->like_to_recommend,
                        'Source_Created_At'                 => $survey->created_at,
                        'Source_Updated_At'                 => $survey->updated_at,
                    ])
                );

                if ($survey->updated_at && (!$state->highestTimestamp || $survey->updated_at > $state->highestTimestamp)) {
                    $state->highestTimestamp = $survey->updated_at;
                }
            },
            function ($state, $count) {
                if ($state->highestTimestamp) {
                    $this->updateWatermark('Fact_Parent_Survey', $state->highestTimestamp, $count);
                }
            }
        );
    }

    public function syncFactHandsupSurvey($command = null)
    {
        $useLegacySurvey = $this->sourceHasColumn('courses', 'survey_details');
        $watermark = $this->getWatermark('Fact_HandsUp_Survey');

        try {
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

            return $this->executeSyncRunner(
                'Fact_HandsUp_Survey',
                $command,
                $courseIds,
                function ($sourceCourseId, $state) use ($useLegacySurvey) {
                    $course = $this->dwh->table('Dim_Course')->where('Source_Course_Id', $sourceCourseId)->first();
                    if (!$course) return;

                    $m = $this->initializeHandsupMetrics();

                    if ($useLegacySurvey) {
                        $legacyData = $this->source->table('courses')->where('id', $sourceCourseId)->value('survey_details');
                        $m = $this->aggregateHandsupFromLegacyJson($legacyData, $m);
                    } else {
                        $results = $this->source->table('course_survey_results')->where('course_id', $sourceCourseId)->get();
                        $m = $this->aggregateHandsupFromSourceRows($results, $m);
                    }

                    $this->dwh->table('Fact_HandsUp_Survey')->updateOrInsert(
                        ['Course_Key' => $course->Course_Key],
                        array_merge($m, [
                            'Date_Key'     => $course->Start_Date ? str_replace('-', '', substr($course->Start_Date, 0, 10)) : null,
                            'Delivery_Key' => $course->Delivery_Key,
                        ])
                    );
                },
                function ($state, $count) use ($maxTimestamp) {
                    if ($maxTimestamp) {
                        $this->updateWatermark('Fact_HandsUp_Survey', $maxTimestamp, $count);
                    }
                }
            );

        } catch (Throwable $e) {
            $this->slack->sendSyncError('Fact_HandsUp_Survey', $e->getMessage(), [], $e->getTraceAsString());
            throw $e;
        }
    }

    public function syncFactParentFollowUpSurveys($command = null)
    {
        $sourceSystemKey = $this->getSourceSystemKey();
        $watermark = $this->getWatermark('Fact_Follow_Up_Survey');

        $query = $this->source->table('parent_follow_up_surveys')
            ->where('updated_at', '>', $watermark)
            ->orderBy('updated_at', 'asc');

        return $this->executeSyncRunner(
            'Fact_Follow_Up_Survey',
            $command,
            $query,
            function ($survey, $state) use ($sourceSystemKey) {
                $data = json_decode($survey->parent_follow_up_survey, true) ?? [];
                $label = strtolower($survey->course_label);

                $riderKey = $this->dwh->table('Dim_Rider')->where('Source_Rider_Id', $survey->rider_id)->value('Rider_Key');
                $courseKey = $this->dwh->table('Dim_Course')->where('Source_Course_Id', $survey->course_id)->value('Course_Key');
                $deliveryKey = $this->dwh->table('Dim_Delivery_Header')->where('Source_Delivery_Id', $survey->delivery_id)->value('Delivery_Key');

                $mapped = $this->mapSurveyByCourse($label, $data);

                $this->dwh->table('Fact_Follow_Up_Survey')->updateOrInsert(
                    ['Source_Survey_Id' => $survey->id, 'Source_System_Key' => $sourceSystemKey],
                    array_merge($mapped, [
                        'Delivery_Key'      => $deliveryKey,
                        'Rider_Key'         => $riderKey,
                        'Course_Key'        => $courseKey,
                        'Course_Label_Raw'  => $survey->course_label,
                        'Invitation_Month'  => $survey->invitation_month,
                        'Source_Created_At' => $survey->created_at,
                        'updated_at'        => now(),
                    ])
                );

                if ($survey->updated_at && (!$state->highestTimestamp || $survey->updated_at > $state->highestTimestamp)) {
                    $state->highestTimestamp = $survey->updated_at;
                }
            },
            function ($state, $count) {
                if ($state->highestTimestamp) {
                    $this->updateWatermark('Fact_Follow_Up_Survey', $state->highestTimestamp, $count);
                }
            }
        );
    }

    public function syncFactGrantRecipientRenewals($command = null)
    {
        $sourceSystemKey = $this->getSourceSystemKey();
        $watermark = $this->getWatermark('Fact_Grant_Recipient_Renewal');

        $query = $this->source->table('grant_recipient_renewals')
            ->where('updated_at', '>', $watermark)
            ->orderBy('updated_at', 'asc');

        return $this->executeSyncRunner(
            'Fact_Grant_Recipient_Renewal',
            $command,
            $query,
            function ($row, $state) use ($sourceSystemKey) {
                $recipientKey = $this->dwh->table('Dim_Grant_Recipient')
                    ->where('Source_Recipient_Id', $row->grant_recipient_id)
                    ->where('Source_System_Key', $sourceSystemKey)
                    ->value('Recipient_Key');

                if (!$recipientKey) {
                    return;
                }

                $this->dwh->table('Fact_Grant_Recipient_Renewal')->updateOrInsert(
                    [
                        'Source_Renewal_Id' => $row->id,
                        'Source_System_Key' => $sourceSystemKey,
                    ],
                    [
                        'Recipient_Key'                   => $recipientKey,
                        'Status'                          => $row->status,
                        'Renewal_Year'                    => $row->renewal_year,
                        'Date_Due'                        => $row->date_due,
                        'Date_Completed'                  => $row->date_completed,
                        'Date_Approved'                   => $row->date_approved,
                        'Flag_Reminder_Warning'           => $row->flag_reminder_warning,
                        'Flag_Overdue_Warning'            => $row->flag_overdue_warning,
                        'Q_Organisation_Category'         => $row->q_organisation_category,
                        'Q_Confirm_Details'               => $row->q_confirm_details,
                        'Q_Contracted_Deliveries'         => (empty($row->q_contracted_deliveries) || !json_validate($row->q_contracted_deliveries)) ? '[]' : $row->q_contracted_deliveries,
                        'Q_Confirm_Guidelines_Commitment' => $row->q_confirm_guidelines_commitment,
                        'Q_Confirm_Risk_Assessment'       => $row->q_confirm_risk_assessment,
                        'Q_Confirm_Policy_Review'         => $row->q_confirm_policy_review,
                        'Q_Confirm_Valid_Insurance'       => $row->q_confirm_valid_insurance,
                        'Q_Documents'                     => (empty($row->q_documents) || !json_validate($row->q_documents)) ? '[]' : $row->q_documents,
                        'Q_Confirm_Access'                => $row->q_confirm_access,
                        'Q_Confirm_Associated_TP'         => $row->q_confirm_assosciated_tp,
                        'Q_Confirm_Assurance_One'         => $row->q_confirm_assurance_one,
                        'Q_Confirm_Assurance_Two'         => $row->q_confirm_assurance_two,
                        'Q_Confirm_Assurance_Three'       => $row->q_confirm_assurance_three,
                        'Q_Confirm_Incidents'             => $row->q_confirm_incidents,
                        'Q_Stage_1_Complaints'            => $row->q_stage_1_complaints,
                        'Q_Serious_Complaints'            => $row->q_serious_complaints,
                        'Q_Confirm_Final'                 => $row->q_confirm_final,
                        'Q_Name'                          => $row->q_name,
                        'Q_GR_Is_Also_TP'                 => $row->q_gr_is_also_tp,
                        'Q_Safeguarding'                  => $row->q_safeguarding,
                        'Users_With_Access_To_GR'         => (empty($row->users_with_access_to_gr) || !json_validate($row->users_with_access_to_gr)) ? '[]' : $row->users_with_access_to_gr,
                        'Associated_TPs'                  => (empty($row->associated_tps) || !json_validate($row->associated_tps)) ? '[]' : $row->associated_tps,
                        'Q_Tfl_Funded'                    => $row->q_tfl_funded,
                        'Contact_Details_Confirmed'       => (empty($row->contact_details_confirmed) || !json_validate($row->contact_details_confirmed)) ? '[]' : $row->contact_details_confirmed,
                        'Source_Created_At'               => $row->created_at,
                        'Source_Updated_At'               => $row->updated_at,
                        'updated_at'                      => now(),
                    ]
                );

                if ($row->updated_at && (!$state->highestTimestamp || $row->updated_at > $state->highestTimestamp)) {
                    $state->highestTimestamp = $row->updated_at;
                }
            },
            function ($state, $count) {
                if ($state->highestTimestamp) {
                    $this->updateWatermark('Fact_Grant_Recipient_Renewal', $state->highestTimestamp, $count);
                }
            }
        );
    }

    public function syncFactInstructorRenewals($command = null)
    {
        $sourceSystemKey = $this->getSourceSystemKey();
        $watermark = $this->getWatermark('Fact_Instructor_Renewal');

        $query = $this->source->table('instructor_renewals')
            ->where('updated_at', '>', $watermark)
            ->orderBy('updated_at', 'asc');

        return $this->executeSyncRunner(
            'Fact_Instructor_Renewal',
            $command,
            $query,
            function ($row, $state) use ($sourceSystemKey) {
                $instructorKey = $this->dwh->table('Dim_Instructor')
                    ->where('Source_Instructor_Id', $row->instructor_id)
                    ->where('Source_System_Key', $sourceSystemKey)
                    ->value('Instructor_Key');

                if (!$instructorKey) {
                    return;
                }

                $this->dwh->table('Fact_Instructor_Renewal')->updateOrInsert(
                    [
                        'Source_Renewal_Id' => $row->id,
                        'Source_System_Key' => $sourceSystemKey,
                    ],
                    [
                        'Instructor_Key'                  => $instructorKey,
                        'Status'                          => $row->status,
                        'Renewal_Year'                    => $row->renewal_year,
                        'Date_Due'                        => $row->date_due,
                        'Date_Completed'                  => $row->date_completed,
                        'Date_Approved'                   => $row->date_approved,
                        'Flag_Reminder_Warning'           => $row->flag_reminder_warning,
                        'Flag_Overdue_Warning'            => $row->flag_overdue_warning,
                        'Q_Confirm_Registration_Details'  => $row->q_confirm_registration_details,
                        'Q_Confirm_Status'                => $row->q_confirm_status,
                        'Q_Confirm_Delivering_Bikeability'=> $row->q_confirm_delivering_bikeability,
                        'Q_Confirm_Observed_By_IQA_Lead'  => $row->q_confirm_observed_by_iqa_lead,
                        'Q_Confirm_Completed_CPD'         => $row->q_confirm_completed_cpd,
                        'Q_Confirm_Code_of_Practice'      => $row->q_confirm_code_of_practice,
                        'Q_Confirm_Delivering_Other'      => $row->q_confirm_delivering_other,
                        'Q_Confirm_Contact_Prefs'         => $row->q_confirm_contact_prefs,
                        'Q_Confirm_Fee'                   => $row->q_confirm_fee,
                        'Q_Delivery_Examples'             => (empty($row->q_delivery_examples) || !json_validate($row->q_delivery_examples)) ? '[]' : $row->q_delivery_examples,
                        'Q_Delivery_Amount_Level_1'       => $row->q_delivery_amount_level_1,
                        'Q_Delivery_Amount_Level_1_2'     => $row->q_delivery_amount_level_1_2,
                        'Q_Delivery_Amount_Level_2'       => $row->q_delivery_amount_level_2,
                        'Q_Delivery_Amount_Level_3'       => $row->q_delivery_amount_level_3,
                        'Q_Delivery_Amount_Plus'          => $row->q_delivery_amount_plus,
                        'Q_Professional_Development'      => (empty($row->q_professional_development) || !json_validate($row->q_professional_development)) ? '[]' : $row->q_professional_development,
                        'Q_Strengths'                     => (empty($row->q_strengths) || !json_validate($row->q_strengths)) ? '[]' : $row->q_strengths,
                        'Source_Created_At'               => $row->created_at,
                        'Source_Updated_At'               => $row->updated_at,
                        'updated_at'                      => now(),
                    ]
                );

                if ($row->updated_at && (!$state->highestTimestamp || $row->updated_at > $state->highestTimestamp)) {
                    $state->highestTimestamp = $row->updated_at;
                }
            },
            function ($state, $count) {
                if ($state->highestTimestamp) {
                    $this->updateWatermark('Fact_Instructor_Renewal', $state->highestTimestamp, $count);
                }
            }
        );
    }

    public function syncFactTrainingProviderRenewals($command = null)
    {
        $sourceSystemKey = $this->getSourceSystemKey();
        $watermark = $this->getWatermark('Fact_Training_Provider_Renewal');

        $query = $this->source->table('training_provider_renewals')
            ->where('updated_at', '>', $watermark)
            ->orderBy('updated_at', 'asc');

        return $this->executeSyncRunner(
            'Fact_Training_Provider_Renewal',
            $command,
            $query,
            function ($row, $state) use ($sourceSystemKey) {
                $providerKey = $this->dwh->table('Dim_Training_Provider')
                    ->where('Source_Provider_Id', $row->training_provider_id)
                    ->where('Source_System_Key', $sourceSystemKey)
                    ->value('Provider_Key');

                if (!$providerKey) {
                    return;
                }

                $this->dwh->table('Fact_Training_Provider_Renewal')->updateOrInsert(
                    [
                        'Source_Renewal_Id' => $row->id,
                        'Source_System_Key' => $sourceSystemKey,
                    ],
                    [
                        'Provider_Key'                        => $providerKey,
                        'Status'                              => $row->status,
                        'Approved'                            => $row->approved,
                        'Renewal_Year'                        => $row->renewal_year,
                        'Date_Due'                            => $row->date_due,
                        'Date_Completed'                      => $row->date_completed,
                        'Date_Approved'                       => $row->date_approved,
                        'Flag_Reminder_Warning'               => $row->flag_reminder_warning,
                        'Flag_Overdue_Warning'                => $row->flag_overdue_warning,
                        'Q_Organisation_Category'             => $row->q_organisation_category,
                        'Q_Confirm_Details'                   => $row->q_confirm_details,
                        'Q_Contracted_Deliveries'             => (empty($row->q_contracted_deliveries) || !json_validate($row->q_contracted_deliveries)) ? '[]' : $row->q_contracted_deliveries,
                        'Q_Delivered_Places'                  => (empty($row->q_delivered_places) || !json_validate($row->q_delivered_places)) ? '[]' : $row->q_delivered_places,
                        'Q_Instructor_Types_Breakdown'        => (empty($row->q_instructor_types_breakdown) || !json_validate($row->q_instructor_types_breakdown)) ? '[]' : $row->q_instructor_types_breakdown,
                        'Q_Confirm_Instructor_DbsCert'        => $row->q_confirm_instructor_dbcert,
                        'Q_Confirm_Instructor_SafetyPolicies' => $row->q_confirm_instructor_safetypolicies,
                        'Q_Delivery_Amount_Level_1'           => $row->q_delivery_amount_level_1,
                        'Q_Delivery_Amount_Level_1_2'         => $row->q_delivery_amount_level_1_2,
                        'Q_Delivery_Amount_Level_2'           => $row->q_delivery_amount_level_2,
                        'Q_Delivery_Amount_Level_3'           => $row->q_delivery_amount_level_3,
                        'Q_Delivery_Amount_Plus_Balance'      => $row->q_delivery_amount_plus_balance,
                        'Q_Delivery_Amount_Plus_Bus'          => $row->q_delivery_amount_plus_bus,
                        'Q_Delivery_Amount_Plus_Fix'          => $row->q_delivery_amount_plus_fix,
                        'Q_Delivery_Amount_Plus_Learn'        => $row->q_delivery_amount_plus_learn,
                        'Q_Delivery_Amount_Plus_On_Show'      => $row->q_delivery_amount_plus_on_show,
                        'Q_Delivery_Amount_Plus_Parents'      => $row->q_delivery_amount_plus_parents,
                        'Q_Delivery_Amount_Plus_Promotion'    => $row->q_delivery_amount_plus_promotion,
                        'Q_Delivery_Amount_Plus_Recycled'     => $row->q_delivery_amount_plus_recycled,
                        'Q_Delivery_Amount_Plus_Ride'         => $row->q_delivery_amount_plus_ride,
                        'Q_Delivery_Amount_Plus_Transition'   => $row->q_delivery_amount_plus_transition,
                        'Q_Delivery_Model'                    => (empty($row->q_delivery_model) || !json_validate($row->q_delivery_model)) ? '[]' : $row->q_delivery_model,
                        'Q_Confirm_Guidelines_Commitment'     => $row->q_confirm_guidelines_commitment,
                        'Q_Confirm_Risk_Assessment'           => $row->q_confirm_risk_assessment,
                        'Q_Confirm_Policy_Review'             => $row->q_confirm_policy_review,
                        'Q_Confirm_Valid_Insurance'           => $row->q_confirm_valid_insurance,
                        'Q_Documents'                         => (empty($row->q_documents) || !json_validate($row->q_documents)) ? '[]' : $row->q_documents,
                        'Q_Confirm_Iqa_Plan'                  => $row->q_confirm_iqa_plan,
                        'Q_Strengths'                         => (empty($row->q_strengths) || !json_validate($row->q_strengths)) ? '[]' : $row->q_strengths,
                        'Q_Priority_1'                        => $row->q_priority_1,
                        'Q_Action_Plan_1'                     => $row->q_action_plan_1,
                        'Q_Priority_2'                        => $row->q_priority_2,
                        'Q_Action_Plan_2'                     => $row->q_action_plan_2,
                        'Q_Priority_3'                        => $row->q_priority_3,
                        'Q_Action_Plan_3'                     => $row->q_action_plan_3,
                        'Q_Associated_Instructors_Two'        => $row->q_associated_instructors_two,
                        'Q_Children'                          => $row->q_children,
                        'Q_Adults'                            => $row->q_adults,
                        'Q_Families'                          => $row->q_families,
                        'Q_Confirm_Incidents'                 => $row->q_confirm_incidents,
                        'Q_Stage_1_Complaints'                => $row->q_stage_1_complaints,
                        'Q_Serious_Complaints'                => $row->q_serious_complaints,
                        'Q_Confirm_Assurance_One'             => $row->q_confirm_assurance_one,
                        'Q_Confirm_Assurance_Two'             => $row->q_confirm_assurance_two,
                        'Q_Confirm_Assurance_Three'           => $row->q_confirm_assurance_three,
                        'Q_Improvements'                      => (empty($row->q_improvements) || !json_validate($row->q_improvements)) ? '[]' : $row->q_improvements,
                        'Q_Confirm_Final_One'                 => $row->q_confirm_final_one,
                        'Q_Confirm_Final_Two'                 => $row->q_confirm_final_two,
                        'Q_Confirm_Final_Three'               => $row->q_confirm_final_three,
                        'Q_Associated_Instructors'            => $row->q_associated_instructors,
                        'Q_Confirm_Access'                    => $row->q_confirm_access,
                        'Q_Safeguarding_Lead'                 => $row->q_safeguarding_lead,
                        'Q_Health_And_Safety_Lead'            => $row->q_health_and_safety_lead,
                        'Q_Iqa_Lead'                          => $row->q_iqa_lead,
                        'Q_Iqa_Organisation'                  => $row->q_iqa_organisation,
                        'Q_Name'                              => $row->q_name,
                        'Q_Associated_Instructors_Three'      => $row->q_associated_instructors_three,
                        'Q_Safeguarding'                      => $row->q_safeguarding,
                        'Users_With_Access_To_TP'             => (empty($row->users_with_access_to_tp) || !json_validate($row->users_with_access_to_tp)) ? '[]' : $row->users_with_access_to_tp,
                        'Associated_Instructors'              => (empty($row->associated_instructors) || !json_validate($row->associated_instructors)) ? '[]' : $row->associated_instructors,
                        'Contact_Details_Confirmed'           => (empty($row->contact_details_confirmed) || !json_validate($row->contact_details_confirmed)) ? '[]' : $row->contact_details_confirmed,
                        'Q_Contract_Type'                     => $row->q_contract_type,
                        'Q_Contract_Type_Other_Text'          => $row->q_contract_type_other_text,
                        'Q_Adult_Training_Only'               => $row->q_adult_training_only,
                        'Q_Expected_Delivery'                 => (empty($row->q_expected_delivery) || !json_validate($row->q_expected_delivery)) ? '[]' : $row->q_expected_delivery,
                        'Q_Expected_Delivery_Empty'           => $row->q_expected_delivery_empty,
                        'Source_Created_At'                   => $row->created_at,
                        'Source_Updated_At'                   => $row->updated_at,
                        'updated_at'                          => now(),
                    ]
                );

                if ($row->updated_at && (!$state->highestTimestamp || $row->updated_at > $state->highestTimestamp)) {
                    $state->highestTimestamp = $row->updated_at;
                }
            },
            function ($state, $count) {
                if ($state->highestTimestamp) {
                    $this->updateWatermark('Fact_Training_Provider_Renewal', $state->highestTimestamp, $count);
                }
            }
        );
    }

    public function syncFactTeacherTrainerDeliveries($command = null)
    {
        $sourceSystemKey = $this->getSourceSystemKey();
        $watermark = $this->getWatermark('Fact_Teacher_Trainer_Delivery');
        $hasJsonColumn = $this->sourceHasColumn('teacher_trainer_deliveries', 'delivery_details');

        $query = $this->source->table('teacher_trainer_deliveries')
            ->where('updated_at', '>', $watermark)
            ->when($this->sourceHasColumn('teacher_trainer_deliveries', 'deleted_at'), fn($q) => $q->whereNull('deleted_at'))
            ->orderBy('id', 'asc');

        return $this->executeSyncRunner(
            'Fact_Teacher_Trainer_Delivery',
            $command,
            $query,
            function ($row, $state) use ($sourceSystemKey, $hasJsonColumn) {
                $schoolKey = null;
                if (!empty($row->school_urn)) {
                    $schoolKey = $this->dwh->table('Dim_School')
                        ->where('School_Urn', $row->school_urn)
                        ->where('Source_System_Key', $sourceSystemKey)
                        ->value('School_Key');
                }

                $trainerKey = null;
                if (!empty($row->school_urn)) {
                    $trainerKey = $this->dwh->table('Dim_Teacher_Trainer')
                        ->where('Main_School_Urn', $row->school_urn)
                        ->where('Source_System_Key', $sourceSystemKey)
                        ->value('Teacher_Trainer_Key');
                }

                $this->dwh->table('Fact_Teacher_Trainer_Delivery')->updateOrInsert(
                    [
                        'Source_TT_Delivery_Id' => $row->id,
                        'Source_System_Key'     => $sourceSystemKey,
                    ],
                    [
                        'School_Key'          => $schoolKey,
                        'Teacher_Trainer_Key' => $trainerKey,
                        'School_Urn'          => $row->school_urn,
                        'Date_Delivery_Start' => $row->date_delivery_start,
                        'Date_Delivery_End'   => $row->date_delivery_end,
                        'Completion_Date'     => $row->completion_date,
                        'Is_Mixed_Year_Group' => (bool) ($row->is_mixed_year_group ?? 0),
                        'Has_Ethnicity'       => (bool) ($row->has_ethnicity ?? 0),
                        'Has_Survey'          => (bool) ($row->has_survey ?? 0),
                        'Notes'               => $row->notes,
                        'Updated_At'          => now(),
                    ]
                );

                $ttDeliveryKey = $this->dwh->table('Fact_Teacher_Trainer_Delivery')
                    ->where('Source_TT_Delivery_Id', $row->id)
                    ->where('Source_System_Key', $sourceSystemKey)
                    ->value('TT_Delivery_Key');

                if ($ttDeliveryKey) {
                    if ($hasJsonColumn && !empty($row->delivery_details)) {
                        $this->syncModulesAndMetricsFromJson($row, $ttDeliveryKey, $sourceSystemKey);
                    } else {
                        $this->syncModulesAndMetricsFromRelational($row->id, $ttDeliveryKey, $sourceSystemKey);
                    }
                }

                if ($row->updated_at && (!$state->highestTimestamp || $row->updated_at > $state->highestTimestamp)) {
                    $state->highestTimestamp = $row->updated_at;
                }
            },
            function ($state, $count) {
                if ($state->highestTimestamp) {
                    $this->updateWatermark('Fact_Teacher_Trainer_Delivery', $state->highestTimestamp, $count);
                }
            }
        );
    }

    public function syncGetCyclingRiderCourses($command = null)
    {
        $sourceSystemKey = $this->getSourceSystemKey();
        $watermark = $this->getWatermark('Fact_Get_Cycling_Rider_Course');

        $query = $this->source->table('get_cycling_join_riders_courses')
            ->where('updated_at', '>', $watermark)
            ->orderBy('id', 'asc');

        return $this->executeSyncRunner(
            'Fact_Get_Cycling_Rider_Course',
            $command,
            $query,
            function ($row, $state) use ($sourceSystemKey) {
                $rider = $this->dwh->table('Dim_Get_Cycling_Rider')
                    ->where('Source_Rider_Id', $row->rider_id)
                    ->where('Source_System_Key', $sourceSystemKey)
                    ->first(['GC_Rider_Key', 'School_Key']);

                $courseKey = $this->dwh->table('Dim_Get_Cycling_Course')
                    ->where('Source_Course_Id', $row->course_id)
                    ->where('Source_System_Key', $sourceSystemKey)
                    ->value('GC_Course_Key');

                $trainerKey = null;
                if (!empty($row->teacher_trainer_id)) {
                    $trainerKey = $this->dwh->table('Dim_Teacher_Trainer')
                        ->where('Source_Teacher_Trainer_Id', $row->teacher_trainer_id)
                        ->where('Source_System_Key', $sourceSystemKey)
                        ->value('Teacher_Trainer_Key');
                }

                if ($rider && $courseKey) {
                    $this->dwh->table('Fact_Get_Cycling_Rider_Course')->updateOrInsert(
                        [
                            'Source_Join_Id'    => $row->id,
                            'Source_System_Key' => $sourceSystemKey,
                        ],
                        [
                            'GC_Rider_Key'              => $rider->GC_Rider_Key,
                            'GC_Course_Key'             => $courseKey,
                            'Teacher_Trainer_Key'       => $trainerKey,
                            'School_Key'                => $rider->School_Key,
                            'Source_Rider_Id'           => $row->rider_id,
                            'Source_Course_Id'          => $row->course_id,
                            'Source_Teacher_Trainer_Id' => $row->teacher_trainer_id,
                            'Does_Not_Wish_To_Continue' => (bool) ($row->does_not_wish_to_continue ?? 0),
                            'Overall_Progress'          => $row->overall_progress,
                            'Has_Survey_Completed'      => (bool) ($row->has_survey_completed ?? 0),
                            'Source_Updated_At'         => $row->updated_at,
                            'Updated_At'                => now(),
                        ]
                    );

                    $rcKey = $this->dwh->table('Fact_Get_Cycling_Rider_Course')
                        ->where('Source_Join_Id', $row->id)
                        ->where('Source_System_Key', $sourceSystemKey)
                        ->value('GC_Rider_Course_Key');

                    $activities = json_decode($row->activities, true) ?? [];
                    if (is_array($activities) && $rcKey) {
                        foreach ($activities as $act) {
                            if (isset($act['id'])) {
                                $this->dwh->table('Fact_Get_Cycling_Rider_Course_Activity')->updateOrInsert(
                                    [
                                        'GC_Rider_Course_Key' => $rcKey,
                                        'Activity_Id'         => (int) $act['id'],
                                    ],
                                    [
                                        'Source_Join_Id'    => $row->id,
                                        'Source_System_Key' => $sourceSystemKey,
                                        'Activity_Score'    => (int) ($act['value'] ?? 0),
                                        'Updated_At'        => now(),
                                    ]
                                );
                            }
                        }
                    }
                }

                if ($row->updated_at && (!$state->highestTimestamp || $row->updated_at > $state->highestTimestamp)) {
                    $state->highestTimestamp = $row->updated_at;
                }
            },
            function ($state, $count) {
                if ($state->highestTimestamp) {
                    $this->updateWatermark('Fact_Get_Cycling_Rider_Course', $state->highestTimestamp, $count);
                }
            }
        );
    }

    public function syncGetCyclingSurveys($command = null)
    {
        $sourceSystemKey = $this->getSourceSystemKey();
        $watermark = $this->getWatermark('Fact_Get_Cycling_Survey_Response');

        $query = $this->source->table('get_cycling_join_surveys_courses')
            ->where('updated_at', '>', $watermark)
            ->orderBy('id', 'asc');

        return $this->executeSyncRunner(
            'Fact_Get_Cycling_Survey_Response',
            $command,
            $query,
            function ($row, $state) use ($sourceSystemKey) {
                $schoolKey = null;
                if (!empty($row->school_urn)) {
                    $schoolKey = $this->dwh->table('Dim_School')
                        ->where('School_Urn', (string) $row->school_urn)
                        ->where('Source_System_Key', $sourceSystemKey)
                        ->value('School_Key');
                }

                $courseKey = $this->dwh->table('Dim_Get_Cycling_Course')
                    ->where('Source_Course_Id', $row->course_id)
                    ->where('Source_System_Key', $sourceSystemKey)
                    ->value('GC_Course_Key');

                $surveyData = json_decode($row->survey, true) ?? [];
                if (is_array($surveyData)) {
                    foreach ($surveyData as $q) {
                        $qId = (string) ($q['q_id'] ?? '');
                        foreach ($q['options'] ?? [] as $opt) {
                            $optId = (string) ($opt['id'] ?? '');
                            $totalCount = (int) ($opt['total'] ?? 0);

                            $this->dwh->table('Fact_Get_Cycling_Survey_Response')->updateOrInsert(
                                [
                                    'Source_Survey_Join_Id' => $row->id,
                                    'Question_Id'           => $qId,
                                    'Option_Id'             => $optId,
                                    'Source_System_Key'     => $sourceSystemKey,
                                ],
                                [
                                    'School_Key'       => $schoolKey,
                                    'GC_Course_Key'    => $courseKey,
                                    'School_Urn'       => (string) $row->school_urn,
                                    'Source_Course_Id' => $row->course_id,
                                    'Response_Count'   => $totalCount,
                                    'Updated_At'       => now(),
                                ]
                            );
                        }
                    }
                }

                if ($row->updated_at && (!$state->highestTimestamp || $row->updated_at > $state->highestTimestamp)) {
                    $state->highestTimestamp = $row->updated_at;
                }
            },
            function ($state, $count) {
                if ($state->highestTimestamp) {
                    $this->updateWatermark('Fact_Get_Cycling_Survey_Response', $state->highestTimestamp, $count);
                }
            }
        );
    }

    // =========================================================================
    // INTERNAL HELPERS & METRIC PARSERS
    // =========================================================================

    protected function createNewVersion($table, $idCol, $idVal, $data, $sysKey, $startDate)
    {
        $data[$idCol] = $idVal;
        $data['Source_System_Key'] = $sysKey;
        $data['Valid_From_Date'] = $startDate;
        $data['Valid_To_Date'] = '9999-12-31';
        $data['Is_Current'] = 1;

        $this->dwh->table($table)->insert($data);
    }

    protected function hasChanged($existing, $new)
    {
        foreach ($new as $key => $value) {
            if ($existing->$key != $value) return true;
        }
        return false;
    }

    protected function getSourceSystemKey()
    {
        return $this->dwh->table('Dim_Source_System')->where('System_Name', 'link')->value('Source_System_Key');
    }

    private function getWatermark(string $tableName): string
    {
        return $this->dwh->table('Sync_Log')
            ->where('Table_Name', $tableName)
            ->value('Last_Synced_At') ?? '1900-01-01 00:00:00';
    }

    private function updateWatermark(string $tableName, string $highestTimestampSeen, int $recordsProcessed = 0): void
    {
        $this->dwh->table('Sync_Log')->updateOrInsert(
            ['Table_Name' => $tableName],
            [
                'Last_Synced_At'    => $highestTimestampSeen,
                'Records_Processed' => $recordsProcessed,
            ]
        );
    }

    private function resolveGrantKey($sourceGrantId, int $sourceSystemKey): ?int
    {
        if (!$sourceGrantId) return null;

        return $this->dwh->table('Dim_Grant')
            ->where('Source_Grant_Id', $sourceGrantId)
            ->where('Source_System_Key', $sourceSystemKey)
            ->value('Grant_Key');
    }

    private function sourceHasColumn(string $table, string $column): bool
    {
        static $schemaCache = [];
        $cacheKey = "{$table}.{$column}";

        if (isset($schemaCache[$cacheKey])) {
            return $schemaCache[$cacheKey];
        }

        try {
            $exists = Schema::connection('mysql_src')->hasColumn($table, $column);
            $schemaCache[$cacheKey] = $exists;
            return $exists;
        } catch (\Exception $e) {
            return false;
        }
    }

    protected function syncModulesAndMetricsFromJson($row, int $ttDeliveryKey, string $sourceSystemKey): void
    {
        $modules = json_decode($row->delivery_details, true) ?? [];
        if (!is_array($modules)) return;

        foreach ($modules as $moduleIndex => $moduleEntry) {
            $moduleId    = $moduleEntry['module']['id'] ?? 'unknown';
            $moduleLabel = $moduleEntry['module']['label'] ?? null;
            $attended    = $moduleEntry['delivery']['attended'] ?? [];

            $syntheticModuleId = (int) ($row->id . sprintf('%03d', $moduleIndex));

            $this->dwh->table('Fact_Teacher_Trainer_Delivery_Module')->updateOrInsert(
                [
                    'Source_TT_Delivery_Id' => $row->id,
                    'Source_TT_Module_Id'   => $syntheticModuleId,
                    'Source_System_Key'     => $sourceSystemKey,
                ],
                [
                    'TT_Delivery_Key'       => $ttDeliveryKey,
                    'Module_Id'             => $moduleId,
                    'Module_Label'          => $moduleLabel,
                    'Updated_At'            => now(),
                ]
            );

            $ttDeliveryModuleKey = $this->dwh->table('Fact_Teacher_Trainer_Delivery_Module')
                ->where('TT_Delivery_Key', $ttDeliveryKey)
                ->where('Module_Id', $moduleId)
                ->value('TT_Delivery_Module_Key');

            if (!$ttDeliveryModuleKey) continue;

            $metricsList = [];
            if (isset($attended['total'])) {
                $metricsList[] = ['Category' => 'attended', 'Sub_Category' => 'total', 'Metric_Value' => (int) $attended['total']];
            }
            foreach ($attended['gender'] ?? [] as $sub => $val) {
                if ($val !== null) $metricsList[] = ['Category' => 'gender', 'Sub_Category' => (string) $sub, 'Metric_Value' => (int) $val];
            }
            foreach ($attended['yeargroup'] ?? [] as $sub => $val) {
                if ($val !== null) $metricsList[] = ['Category' => 'yeargroup', 'Sub_Category' => (string) $sub, 'Metric_Value' => (int) $val];
            }
            foreach ($attended['ethnicity'] ?? [] as $sub => $val) {
                if ($val !== null) $metricsList[] = ['Category' => 'ethnicity', 'Sub_Category' => (string) $sub, 'Metric_Value' => (int) $val];
            }
            if (isset($attended['send'])) {
                $metricsList[] = ['Category' => 'send', 'Sub_Category' => 'send', 'Metric_Value' => (int) $attended['send']];
            }
            if (isset($attended['send_na'])) {
                $metricsList[] = ['Category' => 'send', 'Sub_Category' => 'send_na', 'Metric_Value' => (int) $attended['send_na']];
            }
            if (isset($attended['free_school_meals'])) {
                $metricsList[] = ['Category' => 'free_school_meals', 'Sub_Category' => 'free_school_meals', 'Metric_Value' => (int) $attended['free_school_meals']];
            }
            if (isset($attended['free_school_meals_na'])) {
                $metricsList[] = ['Category' => 'free_school_meals', 'Sub_Category' => 'free_school_meals_na', 'Metric_Value' => (int) $attended['free_school_meals_na']];
            }

            foreach ($metricsList as $idx => $m) {
                $syntheticMetricId = (int) ($row->id . sprintf('%02d', crc32($moduleId) % 100) . sprintf('%02d', $idx));

                $this->dwh->table('Fact_Teacher_Trainer_Delivery_Metric')->updateOrInsert(
                    [
                        'Source_TT_Metric_Id' => $syntheticMetricId,
                        'Source_System_Key'   => $sourceSystemKey,
                    ],
                    [
                        'TT_Delivery_Key'        => $ttDeliveryKey,
                        'TT_Delivery_Module_Key' => $ttDeliveryModuleKey,
                        'Source_TT_Delivery_Id'  => $row->id,
                        'Source_TT_Module_Id'    => null,
                        'Category'               => $m['Category'],
                        'Sub_Category'           => $m['Sub_Category'],
                        'Metric_Value'           => $m['Metric_Value'],
                        'Updated_At'             => now(),
                    ]
                );
            }
        }
    }

    protected function syncModulesAndMetricsFromRelational(int $sourceDeliveryId, int $ttDeliveryKey, string $sourceSystemKey): void
    {
        if (!$this->source->getSchemaBuilder()->hasTable('teacher_trainer_delivery_modules')) {
            return;
        }

        $modules = $this->source->table('teacher_trainer_delivery_modules')
            ->where('tt_delivery_id', $sourceDeliveryId)
            ->get();

        foreach ($modules as $module) {
            $this->dwh->table('Fact_Teacher_Trainer_Delivery_Module')->updateOrInsert(
                [
                    'Source_TT_Module_Id' => $module->id,
                    'Source_System_Key'   => $sourceSystemKey,
                ],
                [
                    'TT_Delivery_Key'       => $ttDeliveryKey,
                    'Source_TT_Delivery_Id' => $sourceDeliveryId,
                    'Module_Id'             => $module->module_id,
                    'Module_Label'          => $module->module_label,
                    'Updated_At'            => now(),
                ]
            );

            $ttDeliveryModuleKey = $this->dwh->table('Fact_Teacher_Trainer_Delivery_Module')
                ->where('Source_TT_Module_Id', $module->id)
                ->where('Source_System_Key', $sourceSystemKey)
                ->value('TT_Delivery_Module_Key');

            if (!$ttDeliveryModuleKey || !$this->source->getSchemaBuilder()->hasTable('teacher_trainer_delivery_metrics')) {
                continue;
            }

            $metrics = $this->source->table('teacher_trainer_delivery_metrics')
                ->where('tt_module_id', $module->id)
                ->whereNotNull('value')
                ->get();

            foreach ($metrics as $m) {
                $this->dwh->table('Fact_Teacher_Trainer_Delivery_Metric')->updateOrInsert(
                    [
                        'Source_TT_Metric_Id' => $m->id,
                        'Source_System_Key'   => $sourceSystemKey,
                    ],
                    [
                        'TT_Delivery_Key'        => $ttDeliveryKey,
                        'TT_Delivery_Module_Key' => $ttDeliveryModuleKey,
                        'Source_TT_Delivery_Id'  => $sourceDeliveryId,
                        'Source_TT_Module_Id'    => $module->id,
                        'Category'               => (string) $m->category,
                        'Sub_Category'           => (string) $m->sub_category,
                        'Metric_Value'           => (int) ($m->value ?? 0),
                        'Updated_At'             => now(),
                    ]
                );
            }
        }
    }

    private function mapEthnicityToColumn($input)
    {
        $key = strtolower(trim($input));

        $code = match ($key) {
            'wbri', 'whitebritish', 'white_british'                    => 'WBRI',
            'wiri', 'white_irish'                                      => 'WIRI',
            'wirt', 'white_traveller'                                  => 'WIRT',
            'woth', 'white'                                            => 'WOTH',
            'wrom', 'white_gypsy'                                      => 'WROM',
            'mwbc', 'mixed_white_caribbean', 'mixed_white_black_carib' => 'MWBC',
            'mwba', 'mixed_white_african'                              => 'MWBA',
            'mwas', 'mixed_white_asian'                                => 'MWAS',
            'moth', 'mixed'                                            => 'MOTH',
            'aind', 'asian_indian', 'indian'                           => 'AIND',
            'apkn', 'asian_pakistani'                                  => 'APKN',
            'aban', 'asian_bangladeshi'                                => 'ABAN',
            'chne', 'asian_chinese'                                    => 'CHNE',
            'aoth', 'asian'                                            => 'AOTH',
            'bcrb', 'black_caribbean'                                  => 'BCRB',
            'bafr', 'black_african'                                    => 'BAFR',
            'both', 'black'                                            => 'BOTH',
            'ooth', 'other', 'other_arab'                              => 'OOTH',
            'na', 'n', 'refused', 'prefer_not_to_say'                   => 'NA',
            default                                                    => 'NA',
        };

        return match ($code) {
            'WBRI'  => 'Count_Ethnicity_White_British',
            'WIRI'  => 'Count_Ethnicity_White_Irish',
            'WIRT'  => 'Count_Ethnicity_White_Traveller',
            'WOTH'  => 'Count_Ethnicity_White_Other',
            'WROM'  => 'Count_Ethnicity_Gypsy_Romany',
            'MWBC'  => 'Count_Ethnicity_Mixed_White_Black_Carib',
            'MWBA'  => 'Count_Ethnicity_Mixed_White_Black_African',
            'MWAS'  => 'Count_Ethnicity_Mixed_White_Asian',
            'MOTH'  => 'Count_Ethnicity_Mixed_Other',
            'AIND'  => 'Count_Ethnicity_Asian_Indian',
            'APKN'  => 'Count_Ethnicity_Asian_Pakistani',
            'ABAN'  => 'Count_Ethnicity_Asian_Bangladeshi',
            'CHNE'  => 'Count_Ethnicity_Asian_Chinese',
            'AOTH'  => 'Count_Ethnicity_Asian_Other',
            'BCRB'  => 'Count_Ethnicity_Black_Caribbean',
            'BAFR'  => 'Count_Ethnicity_Black_African',
            'BOTH'  => 'Count_Ethnicity_Black_Other',
            'OOTH'  => 'Count_Ethnicity_Other_Any',
            default => 'Count_Ethnicity_Not_Stated',
        };
    }

    private function initializeExtendedMetricArray()
    {
        return [
            'Count_Female'                           => 0,
            'Count_Male'                             => 0,
            'Count_Gender_Other'                     => 0,
            'Count_Gender_Not_Stated'                => 0,
            'Count_Adults'                           => 0,
            'Count_Teens'                            => 0,
            'Count_Children'                         => 0,
            'Count_Booked_Repeat_Type_Na'            => 0,
            'Count_Booked_Repeat_Type_Unique'        => 0,
            'Count_Booked_Repeat_Type_Repeat'        => 0,
            'Count_Ethnicity_White_British'          => 0,
            'Count_Ethnicity_White_Irish'            => 0,
            'Count_Ethnicity_White_Other'            => 0,
            'Count_Ethnicity_White_Traveller'        => 0,
            'Count_Ethnicity_Gypsy_Romany'           => 0,
            'Count_Ethnicity_Mixed_White_Black_Carib'=> 0,
            'Count_Ethnicity_Mixed_White_Black_African'=> 0,
            'Count_Ethnicity_Mixed_White_Asian'      => 0,
            'Count_Ethnicity_Mixed_Other'            => 0,
            'Count_Ethnicity_Asian_Indian'           => 0,
            'Count_Ethnicity_Asian_Pakistani'        => 0,
            'Count_Ethnicity_Asian_Bangladeshi'      => 0,
            'Count_Ethnicity_Asian_Chinese'          => 0,
            'Count_Ethnicity_Asian_Other'            => 0,
            'Count_Ethnicity_Black_African'          => 0,
            'Count_Ethnicity_Black_Caribbean'        => 0,
            'Count_Ethnicity_Black_Other'            => 0,
            'Count_Ethnicity_Other_Arab'             => 0,
            'Count_Ethnicity_Other_Any'              => 0,
            'Count_Ethnicity_Not_Stated'             => 0,
            'Count_Pupil_Premium'                    => 0,
            'Count_SEND'                             => 0,
            'Count_SEND_Not_Stated'                  => 0,
            'Count_Pupil_Premium_Not_Stated'         => 0,
            'Count_Bikes_Swapped'                    => 0,
            'Count_Bikes_Recycled'                   => 0,
            'Count_Age_Range_18_24'                  => 0,
            'Count_Age_Range_25_34'                  => 0,
            'Count_Age_Range_35_44'                  => 0,
            'Count_Age_Range_45_54'                  => 0,
            'Count_Age_Range_55_64'                  => 0,
            'Count_Age_Range_Over_65'                => 0,
            'Count_Age_Range_Not_Stated'             => 0,
        ];
    }

    private function getCourseDeliveryMetrics($course, $useLegacy)
    {
        if (is_null($useLegacy)) {
            $useLegacy = $this->sourceHasColumn('deliveries', 'delivery_details');
        }

        if ($useLegacy) {
            return $this->extractFromLegacyJson($course);
        }

        return $this->extractFromNormalizedTables($course);
    }

    private function extractFromLegacyJson($course)
    {
        $details = json_decode($course->delivery_details ?? '[]', true);

        $metrics = array_merge($this->initializeExtendedMetricArray(), [
            'Count_Booked_Provisional'       => 0,
            'Count_Booked_Confirmed'         => 0,
            'Count_Attended_Confirmed'       => 0,
            'Count_Bikes_Swapped'            => 0,
            'Count_Bikes_Recycled'           => 0,
            'Extracted_Course_Level'         => null,
            'Extracted_Year_Group'           => null,
            'Count_Gender_Not_Stated'        => 0,
            'Count_SEND'                     => 0,
            'Count_Pupil_Premium'            => 0,
            'Count_SEND_Not_Stated'          => 0,
            'Count_Pupil_Premium_Not_Stated' => 0,
        ]);

        foreach ($details as $entry) {
            $isTargetModule = is_null($course->id) || (isset($entry['module']['entity_id']) && $entry['module']['entity_id'] == $course->id);

            if ($isTargetModule) {
                $metrics['Extracted_Course_Level'] = $entry['module']['id'] ?? null;
                $metrics['Extracted_Year_Group']   = $entry['delivery']['year_group'] ?? null;

                $delivery = $entry['delivery'] ?? [];
                $booked   = $delivery['booked'] ?? [];

                $metrics['Count_Booked_Provisional'] = (int) ($booked['provisional'] ?? 0);

                if (isset($delivery['confirmed']) && $delivery['confirmed'] == 1) {
                    $metrics['Count_Booked_Confirmed']   = (int) ($booked['total'] ?? 0);
                    $metrics['Count_Attended_Confirmed'] = (int) ($delivery['attended']['total'] ?? 0);
                }

                if (($entry['module']['id'] ?? '') === 'plus_family') {
                    $metrics['Count_Adults']   = (int) ($delivery['adults'] ?? 0);
                    $metrics['Count_Teens']    = (int) ($delivery['teens'] ?? 0);
                    $metrics['Count_Children'] = (int) ($delivery['children'] ?? 0);
                    break;
                }

                $gender = $booked['gender'] ?? [];
                foreach ($gender as $sub => $val) {
                    if ($sub === 'female') $metrics['Count_Female'] += (int) $val;
                    elseif ($sub === 'male') $metrics['Count_Male'] += (int) $val;
                    elseif ($sub === 'other') $metrics['Count_Gender_Other'] += (int) $val;
                    elseif ($sub === 'na') $metrics['Count_Gender_Not_Stated'] += (int) $val;
                }

                $ethnicity = $booked['ethnicity'] ?? [];
                foreach ($ethnicity as $sub => $val) {
                    $targetColumn = $this->mapEthnicityToColumn($sub);
                    if (array_key_exists($targetColumn, $metrics)) {
                        $metrics[$targetColumn] += (int) $val;
                    } else {
                        $metrics['Count_Ethnicity_Not_Stated'] += (int) $val;
                    }
                }

                $ages = $booked['age_range'] ?? [];
                foreach ($ages as $sub => $val) {
                    $col = $sub === 'na' ? 'Count_Age_Range_Not_Stated' : ($sub === 'over_65' ? 'Count_Age_Range_Over_65' : 'Count_Age_Range_' . $sub);
                    if (array_key_exists($col, $metrics)) $metrics[$col] += (int) $val;
                }

                $repeats = $booked['repeat_type'] ?? [];
                foreach ($repeats as $sub => $val) {
                    $col = 'Count_Booked_Repeat_Type_' . ucfirst(strtolower($sub));
                    if (array_key_exists($col, $metrics)) $metrics[$col] += (int) $val;
                }

                $metrics['Count_SEND']                     += (int) ($booked['send'] ?? 0);
                $metrics['Count_Pupil_Premium']            += (int) ($booked['free_school_meals'] ?? 0);
                $metrics['Count_SEND_Not_Stated']          += (int) ($booked['send_na'] ?? 0);
                $metrics['Count_Pupil_Premium_Not_Stated'] += (int) ($booked['free_school_meals_na'] ?? 0);

                $metrics['Count_Bikes_Swapped']  = (int) ($delivery['bikes_swapped'] ?? 0);
                $metrics['Count_Bikes_Recycled'] = (int) ($delivery['bikes_recycled'] ?? 0);
                break;
            }
        }
        return $metrics;
    }

    private function extractFromNormalizedTables($course)
    {
        $metrics = array_merge($this->initializeExtendedMetricArray(), [
            'Count_Booked_Provisional' => 0,
            'Count_Booked_Confirmed'   => 0,
            'Count_Attended_Confirmed' => 0,
            'Count_Bikes_Swapped'      => 0,
            'Count_Bikes_Recycled'     => 0,
        ]);

        $moduleQuery = $this->source->table('delivery_modules');
        if (!is_null($course->id)) {
            $moduleQuery->where('entity_id', $course->id);
        } else {
            $moduleQuery->where('delivery_id', $course->delivery_id);
        }

        $module = $moduleQuery->first();
        if (!$module) return $metrics;

        $metrics['Count_Booked_Provisional'] = (int) $module->booked_provisional;
        $metrics['Count_Booked_Confirmed']   = ($module->confirmed) ? (int) $module->booked_total : 0;
        $metrics['Count_Attended_Confirmed'] = ($module->confirmed) ? (int) $module->attended_total : 0;

        $metricRows = $this->source->table('delivery_metrics')
            ->where('module_id', $module->id)
            ->get();

        foreach ($metricRows as $row) {
            $cat = strtolower($row->category);
            $sub = strtolower($row->sub_category);
            $val = (int) $row->value;

            if ($cat === 'booked_gender') {
                if ($sub === 'male')   $metrics['Count_Male'] = $val;
                if ($sub === 'female') $metrics['Count_Female'] = $val;
                if ($sub === 'other')  $metrics['Count_Gender_Other'] = $val;
                if ($sub === 'na')     $metrics['Count_Gender_Not_Stated'] = $val;
            }

            if ($cat === 'booked_ethnicity') {
                $col = $this->mapEthnicityToColumn($sub);
                if (array_key_exists($col, $metrics)) {
                    $metrics[$col] = $val;
                } else {
                    $metrics['Count_Ethnicity_Not_Stated'] += $val;
                }
            }

            if ($sub === 'bikes_swapped')  $metrics['Count_Bikes_Swapped'] += $val;
            if ($sub === 'bikes_recycled') $metrics['Count_Bikes_Recycled'] += $val;

            if ($cat === 'booked_age_range') {
                $col = $sub === 'na' ? 'Count_Age_Range_Not_Stated' : ($sub === 'over_65' ? 'Count_Age_Range_Over_65' : 'Count_Age_Range_' . $sub);
                $metrics[$col] = $val;
            }

            if ($cat === 'booked_repeat_type') {
                $col = 'Count_Booked_Repeat_Type_' . ucfirst(strtolower($sub));
                $metrics[$col] = $val;
            }

            if ($cat === 'booked_demographics' || $cat === 'demographics') {
                if (in_array($sub, ['send', 'booked_send'])) {
                    $metrics['Count_SEND'] = $val;
                } elseif (in_array($sub, ['send_na', 'booked_send_na'])) {
                    $metrics['Count_SEND_Not_Stated'] = $val;
                } elseif (in_array($sub, ['free_school_meals', 'pupil_premium', 'booked_free_school_meals'])) {
                    $metrics['Count_Pupil_Premium'] = $val;
                } elseif (in_array($sub, ['free_school_meals_na', 'pupil_premium_na'])) {
                    $metrics['Count_Pupil_Premium_Not_Stated'] = $val;
                }
            }
        }

        return $metrics;
    }

    private function aggregateFromDWHConsents($deliveryKey)
    {
        $consents = $this->dwh->table('Dim_Consent')
            ->where('Delivery_Key', $deliveryKey)
            ->where('Attended', 1)
            ->get();

        $m = $this->initializeExtendedMetricArray();

        foreach ($consents as $consent) {
            if ($consent->Gender === 'Female') $m['Count_Female']++;
            elseif ($consent->Gender === 'Male') $m['Count_Male']++;

            $column = $this->mapEthnicityToColumn($consent->Ethnicity);
            if (isset($m[$column])) {
                $m[$column]++;
            } else {
                $m['Count_Ethnicity_Not_Stated']++;
            }

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

        $block = $data[0] ?? [];
        $gender = $block['gender'] ?? [];
        foreach ($gender as $sub => $val) {
            if ($sub === 'female') $m['Count_Female'] += (int) $val;
            elseif ($sub === 'male') $m['Count_Male'] += (int) $val;
        }

        $booked = $block['booked'] ?? $block;
        $ages = $booked['age_range'] ?? [];
        foreach ($ages as $sub => $val) {
            if ($sub === 'na') {
                $m['Count_Age_Range_Not_Stated'] += (int) $val;
            } elseif ($sub === 'over_65') {
                $m['Count_Age_Range_Over_65'] += (int) $val;
            } else {
                $column = 'Count_Age_Range_' . $sub;
                if (array_key_exists($column, $m)) $m[$column] += (int) $val;
            }
        }

        $repeats = $booked['repeat_type'] ?? [];
        foreach ($repeats as $sub => $val) {
            $column = 'Count_Booked_Repeat_Type_' . ucfirst(strtolower($sub));
            if (array_key_exists($column, $m)) $m[$column] += (int) $val;
        }

        $ethnicity = $booked['ethnicity'] ?? [];
        foreach ($ethnicity as $sub => $val) {
            if ($sub === 'white_traveller') {
                $m['Count_Ethnicity_White_Traveller'] += (int) $val;
                continue;
            }
            $column = 'Count_Ethnicity_' . str_replace(' ', '_', ucwords(str_replace('_', ' ', $sub)));
            if (array_key_exists($column, $m)) {
                $m[$column] += (int) $val;
            } else {
                $m['Count_Ethnicity_Other_Any'] += (int) $val;
            }
        }

        $m['Count_SEND']                     += (int) ($booked['send'] ?? 0);
        $m['Count_SEND_Not_Stated']          += (int) ($booked['send_na'] ?? 0);
        $m['Count_Pupil_Premium']            += (int) ($booked['free_school_meals'] ?? 0);
        $m['Count_Pupil_Premium_Not_Stated'] += (int) ($booked['free_school_meals_na'] ?? 0);

        return $m;
    }

    private function aggregateFromSourceTable($tableName, $courseId)
    {
        $rows = $this->source->table($tableName)->where('course_id', $courseId)->get();
        $m = $this->initializeExtendedMetricArray();

        foreach ($rows as $row) {
            $cat = strtolower($row->category);
            $sub = strtolower($row->sub_category);
            $val = (int) $row->value;

            if ($cat === 'gender') {
                if ($sub === 'female') $m['Count_Female'] += $val;
                elseif ($sub === 'male') $m['Count_Male'] += $val;
            }

            if ($cat === 'age_range') {
                if ($sub === 'na') {
                    $m['Count_Age_Range_Not_Stated'] += $val;
                } elseif ($sub === 'over_65') {
                    $m['Count_Age_Range_Over_65'] += $val;
                } else {
                    $column = 'Count_Age_Range_' . $sub;
                    if (array_key_exists($column, $m)) $m[$column] += $val;
                }
            }

            if ($cat === 'repeat_type') {
                $column = 'Count_Booked_Repeat_Type_' . ucfirst($sub);
                if (array_key_exists($column, $m)) $m[$column] += $val;
            }

            if ($cat === 'ethnicity') {
                if ($sub === 'white_traveller') {
                    $m['Count_Ethnicity_White_Traveller'] += $val;
                    continue;
                }
                $column = 'Count_Ethnicity_' . str_replace(' ', '_', ucwords(str_replace('_', ' ', $sub)));
                if (array_key_exists($column, $m)) {
                    $m[$column] += $val;
                } else {
                    $m['Count_Ethnicity_Other_Any'] += $val;
                }
            }

            if ($cat === 'pupil_premium') {
                if ($sub === 'free_school_meals_na') {
                    $m['Count_Pupil_Premium_Not_Stated'] += $val;
                } else {
                    $m['Count_Pupil_Premium'] += $val;
                }
            }

            if ($cat === 'send') {
                if ($sub === 'booked_send_na' || $sub === 'send_na') {
                    $m['Count_SEND_Not_Stated'] += $val;
                } else {
                    $m['Count_SEND'] += $val;
                }
            }
        }
        return $m;
    }

    private function aggregateHandsupFromLegacyJson($jsonString, $m)
    {
        $questions = json_decode($jsonString ?? '[]', true);
        foreach ($questions as $q) {
            $qid = $q['question_id'] ?? null;
            foreach ($q['option'] ?? [] as $opt) {
                $m = $this->mapHandsupToBucket($qid, $opt['id'], $opt['total'], $m);
            }
        }
        return $m;
    }

    private function aggregateHandsupFromSourceRows($results, $m)
    {
        foreach ($results as $res) {
            $m = $this->mapHandsupToBucket($res->question_id, $res->option_id, $res->total, $m);
        }
        return $m;
    }

    private function initializeHandsupMetrics()
    {
        return [
            'Exp_Enjoyed' => 0, 'Exp_Did_Not_Enjoy' => 0, 'Exp_Not_Sure' => 0, 'Exp_Absent' => 0,
            'Base_Yes' => 0, 'Base_No' => 0, 'Base_Not_Sure' => 0,
            'Safe_More' => 0, 'Safe_Less' => 0, 'Safe_No_Diff' => 0, 'Safe_Not_Sure' => 0,
            'Conf_More' => 0, 'Conf_Less' => 0, 'Conf_No_Diff' => 0, 'Conf_Not_Sure' => 0,
        ];
    }

    private function mapHandsupToBucket($qid, $oid, $val, $m)
    {
        $val = (int) $val;
        if (in_array($oid, [1, 15, 33, 51, 60, 69])) $m['Exp_Enjoyed'] += $val;
        if (in_array($oid, [2, 16, 34, 52, 61, 70])) $m['Exp_Did_Not_Enjoy'] += $val;
        if (in_array($oid, [3, 17, 35, 53, 62, 71])) $m['Exp_Not_Sure'] += $val;
        if (in_array($oid, [4, 18, 36, 54, 63, 72])) $m['Exp_Absent'] += $val;

        if (in_array($oid, [19, 37])) $m['Base_Yes'] += $val;
        if (in_array($oid, [20, 38])) $m['Base_No'] += $val;
        if (in_array($oid, [21, 39])) $m['Base_Not_Sure'] += $val;

        if (in_array($oid, [5, 23, 41])) $m['Safe_More'] += $val;
        if (in_array($oid, [6, 24, 42])) $m['Safe_Less'] += $val;
        if (in_array($oid, [7, 25, 43])) $m['Safe_No_Diff'] += $val;
        if (in_array($oid, [8, 26, 44])) $m['Safe_Not_Sure'] += $val;

        if (in_array($oid, [10, 28, 46, 55, 64])) $m['Conf_More'] += $val;
        if (in_array($oid, [11, 29, 47, 56, 65])) $m['Conf_Less'] += $val;
        if (in_array($oid, [12, 30, 48, 57, 66])) $m['Conf_No_Diff'] += $val;
        if (in_array($oid, [13, 31, 49, 58, 67])) $m['Conf_Not_Sure'] += $val;

        return $m;
    }

    private function mapSurveyByCourse($label, $data)
    {
        $mapped = [
            'q1a_freq_school'         => null,
            'q1b_freq_leisure'        => null,
            'q1c_freq_exercise'       => null,
            'q2a_conf_use_cycle'      => null,
            'q2b_conf_cycle_roads'    => null,
            'q3a_enc_use_cycle'       => null,
            'q3b_enc_cycle_roads'     => null,
            'q4_safety_roads'         => null,
            'q5_child_desire'         => null,
            'q6_encouragement_factors'=> null,
            'q7_conf_change'          => null,
            'q8_physical_activity'    => null,
        ];

        foreach ($data as $value) {
            if (is_array($value)) {
                $mapped['q6_encouragement_factors'] = json_encode($value);
                continue;
            }

            if (str_contains($value, '_q1a_')) $mapped['q1a_freq_school'] = $this->extractOption($value);
            if (str_contains($value, '_q1b_')) $mapped['q1b_freq_leisure'] = $this->extractOption($value);
            if (str_contains($value, '_q1c_')) $mapped['q1c_freq_exercise'] = $this->extractOption($value);

            if (str_contains($value, '_q2a_')) $mapped['q2a_conf_use_cycle'] = $this->extractOption($value);
            if (str_contains($value, '_q2b_')) $mapped['q2b_conf_cycle_roads'] = $this->extractOption($value);
            if (str_contains($value, '_q3a_')) $mapped['q3a_enc_use_cycle'] = $this->extractOption($value);
            if (str_contains($value, '_q3b_')) $mapped['q3b_enc_cycle_roads'] = $this->extractOption($value);

            if (str_contains($value, '_q4_') && !str_contains($value, '_q4a_')) {
                $mapped['q4_safety_roads'] = $this->extractOption($value);
            }

            if (str_contains($value, '_q4_o') || str_contains($value, '_q4a_o') || str_contains($value, '_q5_o')) {
                if (!str_contains($value, 'level_2_q4') && !str_contains($value, 'level_3_q4')) {
                    $mapped['q5_child_desire'] = $this->extractOption($value);
                }
            }

            if (str_contains($value, '_q7_')) $mapped['q7_conf_change'] = $this->extractOption($value);

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

    private function extractOptionInt($prefix, $code)
    {
        if (empty($code)) return null;

        $pattern = '/' . preg_quote($prefix, '/') . '(\d+)/';
        if (preg_match($pattern, $code, $matches)) {
            return (int) $matches[1];
        }

        return null;
    }

    private function mapDeliveryStatus($statusId)
    {
        return match ($statusId) {
            0       => 'Delivery Created',
            1       => 'In Progress',
            2       => 'Completed',
            3       => 'Cancelled',
            default => 'Draft',
        };
    }

    public function purgeSoftDeletedDeliveries($command = null, int $chunkSize = 500, bool $dryRun = false): string
    {
        $sourceSystemKey = $this->getSourceSystemKey();

        if ($command) {
            $command->info("[" . now()->format('Y-m-d H:i:s') . "] Scanning source system for soft-deleted deliveries...");
        }

        if (!$this->sourceHasColumn('deliveries', 'deleted_at')) {
            return "Skipped: 'deliveries.deleted_at' column does not exist.";
        }

        $totalFoundOnSource = $this->source->table('deliveries')->whereNotNull('deleted_at')->count();
        if ($totalFoundOnSource === 0) {
            return "No soft-deleted deliveries found.";
        }

        $bar = ($command && !$dryRun) ? $command->getOutput()->createProgressBar($totalFoundOnSource) : null;
        if ($bar) $bar->start();

        $purgedCount = 0;

        try {
            $this->source->table('deliveries')
                ->whereNotNull('deleted_at')
                ->select('id')
                ->orderBy('id', 'asc')
                ->chunk($chunkSize, function ($sourceRows) use ($sourceSystemKey, &$purgedCount, $command, $dryRun, $bar) {
                    $deletedIds = $sourceRows->pluck('id')->toArray();

                    $dwhDeliveries = $this->dwh->table('Dim_Delivery_Header')
                        ->where('Source_System_Key', $sourceSystemKey)
                        ->whereIn('Source_Delivery_Id', $deletedIds)
                        ->select(['Delivery_Key', 'Source_Delivery_Id'])
                        ->get();

                    if ($dwhDeliveries->isNotEmpty()) {
                        $deliveryKeys = $dwhDeliveries->pluck('Delivery_Key')->toArray();
                        $batchCount   = count($deliveryKeys);

                        if ($dryRun) {
                            $purgedCount += $batchCount;
                        } else {
                            $this->dwh->transaction(function () use ($deliveryKeys) {
                                $this->dwh->table('Fact_Course_Delivery')->whereIn('Delivery_Key', $deliveryKeys)->delete();
                                $this->dwh->table('Fact_Instructor_Delivery')->whereIn('Delivery_Key', $deliveryKeys)->delete();
                                $this->dwh->table('Fact_Rider_Instructor_Feedback')->whereIn('Delivery_Key', $deliveryKeys)->delete();
                                $this->dwh->table('Fact_Rider_Activity_Outcome')->whereIn('Delivery_Key', $deliveryKeys)->delete();
                                $this->dwh->table('Fact_Course_Activity_Outcome_Summary')->whereIn('Delivery_Key', $deliveryKeys)->delete();
                                $this->dwh->table('Fact_Parent_Survey')->whereIn('Delivery_Key', $deliveryKeys)->delete();
                                $this->dwh->table('Fact_HandsUp_Survey')->whereIn('Delivery_Key', $deliveryKeys)->delete();
                                $this->dwh->table('Fact_Follow_Up_Survey')->whereIn('Delivery_Key', $deliveryKeys)->delete();
                                $this->dwh->table('Dim_Consent')->whereIn('Delivery_Key', $deliveryKeys)->delete();
                                $this->dwh->table('Dim_Course')->whereIn('Delivery_Key', $deliveryKeys)->delete();
                                $this->dwh->table('Dim_Delivery_Header')->whereIn('Delivery_Key', $deliveryKeys)->delete();
                            });

                            $purgedCount += $batchCount;
                        }
                    }

                    if ($bar) $bar->advance(count($deletedIds));
                });

            if ($bar) {
                $bar->finish();
                $command?->newLine(2);
            }

            return "Purged {$purgedCount} ghost delivery records and dependent facts.";

        } catch (Throwable $e) {
            if ($bar) {
                $bar->finish();
                $command?->newLine();
            }
            $this->slack->sendSyncError('Purge Soft-Deleted Deliveries', $e->getMessage(), [], $e->getTraceAsString());
            throw $e;
        }
    }
}

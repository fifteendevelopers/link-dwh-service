<?php

namespace App\Console\Commands\DataWarehouse\Support;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Throwable;

class BackfillMissingDeliveries extends Command
{
    protected $signature = 'dwh:backfill-missing-deliveries {--ids= : Optional comma-separated delivery IDs}';

    protected $description = 'Backfills missing deliveries (including soft-deleted) into Dim_Delivery_Header';

    public function handle(): int
    {
        $dwh = DB::connection('mysql');
        $source = DB::connection('mysql_src');

        $this->info('[' . now()->format('Y-m-d H:i:s') . '] Scanning for Missing Deliveries...');

        $sourceSystemKey = (int)($dwh->table('Dim_Source_System')
            ->where('System_Name', 'link')
            ->value('Source_System_Key') ?? 1);

        $targetIds = [];
        if ($idsOption = $this->option('ids')) {
            $targetIds = array_filter(array_map('intval', explode(',', $idsOption)));
            $this->comment('Targeting ' . count($targetIds) . ' explicitly specified delivery ID(s).');
        } else {
            $dwhDeliveryIds = $dwh->table('Dim_Delivery_Header')
                ->where('Source_System_Key', $sourceSystemKey)
                ->whereNotNull('Source_Delivery_Id')
                ->pluck('Source_Delivery_Id')
                ->flip();

            // Fetch ALL deliveries from source (including soft-deleted)
            $sourceDeliveryIds = $source->table('deliveries')->pluck('id');

            foreach ($sourceDeliveryIds as $sId) {
                if (!isset($dwhDeliveryIds[$sId])) {
                    $targetIds[] = (int)$sId;
                }
            }
        }

        $total = count($targetIds);
        if ($total === 0) {
            $this->info('No missing deliveries detected. Dim_Delivery_Header is at 100% parity.');
            return self::SUCCESS;
        }

        $this->info("Found {$total} missing deliveries to backfill.");
        $bar = $this->output->createProgressBar($total);
        $bar->start();

        $processedCount = 0;
        $errorCount = 0;

        try {
            $source->table('deliveries')
                ->whereIn('id', $targetIds)
                ->orderBy('id', 'asc')
                ->chunk(250, function ($deliveries) use ($dwh, $sourceSystemKey, $bar, &$processedCount, &$errorCount) {
                    foreach ($deliveries as $del) {
                        try {
                            $grantKey = $del->grant_id ? $dwh->table('Dim_Grant')
                                ->where('Source_Grant_Id', $del->grant_id)
                                ->where('Source_System_Key', $sourceSystemKey)
                                ->value('Grant_Key') : null;

                            $schoolKey = $del->school_urn ? $dwh->table('Dim_School')
                                ->where('School_Urn', $del->school_urn)
                                ->where('Source_System_Key', $sourceSystemKey)
                                ->value('School_Key') : null;

                            $orgKey = $del->organisation_id ? $dwh->table('Dim_Organisation')
                                ->where('Source_Organisation_Id', $del->organisation_id)
                                ->where('Source_System_Key', $sourceSystemKey)
                                ->value('Organisation_Key') : null;

                            $providerKey = $dwh->table('Dim_Training_Provider')
                                ->where('Source_Provider_Id', $del->training_provider_id)
                                ->where('Source_System_Key', $sourceSystemKey)
                                ->where('Is_Current', 1)
                                ->value('Provider_Key');

                            $extSystemKey = !empty($del->external_system_id)
                                ? $dwh->table('Dim_External_Systems')
                                    ->where('Source_External_System_Id', $del->external_system_id)
                                    ->value('External_System_Key')
                                : null;

                            $dwh->table('Dim_Delivery_Header')->updateOrInsert(
                                [
                                    'Source_Delivery_Id' => $del->id,
                                    'Source_System_Key' => $sourceSystemKey,
                                ],
                                [
                                    'Grant_Key' => $grantKey,
                                    'School_Key' => $schoolKey,
                                    'Organisation_Key' => $orgKey,
                                    'Training_Provider_Key' => $providerKey,
                                    'External_System_Key' => $extSystemKey,
                                    'Delivery_Status' => ($del->digitisation_booking === 1) ? $del->status??0 : 0,
                                    'Date_Delivery_Start' => $del->date_delivery_start,
                                    'Date_Delivery_End' => $del->date_delivery_end,
                                    'Digitisation_Booking' => $del->digitisation_booking,
                                    'Fleet_Cycles_Used' => $del->fleet_cycles_used,
                                    'Consent_Cutoff_Date' => $del->consent_cutoff_date,
                                    'Pref_Alt_Delivery_Location' => $del->pref_alt_delivery_location,
                                    'Alt_Delivery_Location' => $del->alt_delivery_location,
                                    'Notes' => $del->notes,
                                    'Instructor_General_Notes' => $del->instructor_general_notes,
                                    'Teacher_Notes' => $del->teacher_notes,
                                    'School_Contacts' => $del->school_contacts,
                                    'Venue' => $del->venue,
                                    'Provider_Additional_Questions' => $del->provider_additional_questions,
                                    'Comms_Start_Date' => $del->comms_start_date,
                                    'Date_Completed' => $del->date_completed,
                                    'Pref_Link_Managed_Consent' => $del->pref_link_managed_consent,
                                    'Include_Tp_Terms_In_Consent' => $del->include_tp_terms_in_consent ?? 0,
                                    'Consent_Src_Characteristics' => $del->consent_src_characteristics ?? 0,
                                    'Max_Consents' => $del->max_consents,
                                    'Waiting_List_Enabled' => $del->waiting_list_enabled ?? 0,
                                    'Short_Description' => $del->short_description ?? null,
                                    'Local_Funding' => (int)($del->local_funding ?? 0),
                                    'Legacy_Delivery_Id' => $del->legacy_delivery_id ?? null,
                                    'Legacy_Delivery_Method' => $del->legacy_delivery_method ?? null,
                                    'Rider_List_Uploaded' => (bool)($del->rider_list_uploaded ?? false),
                                    'Url_Code' => $del->url_code ?? null,
                                    'Venue_Notification_Sent' => (bool)($del->venue_notification_sent ?? false),
                                    'Survey_Notifications_Sent' => (bool)($del->survey_notifications_sent ?? false),
                                    'Confirm_Booked_Email_Sent' => $del->confirm_booked_email_sent ?? null,
                                    'Confirm_Booked_Reminder_Email_Sent' => $del->confirm_booked_reminder_email_sent ?? null,
                                    'Housekeeping_Selected' => (bool)($del->housekeeping_selected ?? false),
                                    'Enable_School_Management' => (bool)($del->enable_school_management ?? false),
                                    'Source_Created_At' => $del->created_at ?? null,
                                    'Source_Updated_At' => $del->updated_at ?? null,
                                    'Source_Deleted_At' => $del->deleted_at ?? null,
                                ]
                            );

                            $processedCount++;
                        } catch (Throwable $e) {
                            $errorCount++;
                        } finally {
                            $bar->advance();
                        }
                    }
                });

            $bar->finish();
            $this->newLine(2);
            $this->info("Deliveries backfill complete: {$processedCount} inserted/updated, {$errorCount} errors.");

            return self::SUCCESS;
        } catch (Throwable $fatal) {
            $bar->finish();
            $this->newLine();
            $this->error('Fatal error during deliveries backfill: ' . $fatal->getMessage());
            return self::FAILURE;
        }
    }
}

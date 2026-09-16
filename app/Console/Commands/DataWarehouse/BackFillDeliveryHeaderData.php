<?php

namespace App\Console\Commands\DataWarehouse;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;

class BackFillDeliveryHeaderData extends Command
{
    protected $signature = 'dwh:backfill-dim-delivery-headers';
    protected $description = 'Backfills newly added delivery columns into Dim_Delivery_Header in-place';

    public function handle(): int
    {
        $this->info('Starting in-place backfill for Dim_Delivery_Header...');

        $sourceConn = DB::connection('mysql_src');
        $dwhConn    = DB::connection('mysql');

        $totalRecords = $dwhConn->table('Dim_Delivery_Header')->count();

        if ($totalRecords === 0) {
            $this->warn('No Dim_Delivery_Header records found to backfill.');
            return Command::SUCCESS;
        }

        $this->info("Found {$totalRecords} delivery headers in DWH.");
        $bar = $this->output->createProgressBar($totalRecords);
        $bar->start();

        $updatedCount = 0;

        $dwhConn->table('Dim_Delivery_Header')
            ->select('Delivery_Key', 'Source_Delivery_Id')
            ->orderBy('Delivery_Key')
            ->chunk(1000, function ($dwhDeliveries) use ($sourceConn, $dwhConn, $bar, &$updatedCount) {
                $sourceIds = $dwhDeliveries->pluck('Source_Delivery_Id')->toArray();

                $sourceRecords = $sourceConn->table('deliveries')
                    ->whereIn('id', $sourceIds)
                    ->get()
                    ->keyBy('id');

                foreach ($dwhDeliveries as $dwhDelivery) {
                    $source = $sourceRecords->get($dwhDelivery->Source_Delivery_Id);

                    if (!$source) {
                        $bar->advance();
                        continue;
                    }

                    $dwhConn->table('Dim_Delivery_Header')
                        ->where('Delivery_Key', $dwhDelivery->Delivery_Key)
                        ->update([
                            'Short_Description'                  => $source->short_description ?? null,
                            'Local_Funding'                      => (int) ($source->local_funding ?? 0),
                            'Legacy_Delivery_Id'                 => $source->legacy_delivery_id ?? null,
                            'Legacy_Delivery_Method'             => $source->legacy_delivery_method ?? null,
                            'Rider_List_Uploaded'                => (bool) ($source->rider_list_uploaded ?? false),
                            'Url_Code'                           => $source->url_code ?? null,
                            'Venue_Notification_Sent'            => (bool) ($source->venue_notification_sent ?? false),
                            'Survey_Notifications_Sent'          => (bool) ($source->survey_notifications_sent ?? false),
                            'Confirm_Booked_Email_Sent'          => $source->confirm_booked_email_sent ?? null,
                            'Confirm_Booked_Reminder_Email_Sent' => $source->confirm_booked_reminder_email_sent ?? null,
                            'Housekeeping_Selected'              => (bool) ($source->housekeeping_selected ?? false),
                            'Enable_School_Management'           => (bool) ($source->enable_school_management ?? false),
                            'Source_Created_At'                  => $source->created_at ?? null,
                            'Source_Updated_At'                  => $source->updated_at ?? null,
                            'Source_Deleted_At'                  => $source->deleted_at ?? null,
                        ]);

                    $updatedCount++;
                    $bar->advance();
                }
            });

        $bar->finish();
        $this->newLine(2);
        $this->info("Successfully backfilled {$updatedCount} Dim_Delivery_Header records.");

        return Command::SUCCESS;
    }
}

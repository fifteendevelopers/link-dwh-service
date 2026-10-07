<?php

namespace App\Console\Commands\DataWarehouse;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;

class BackfillFactAttendedConfirmed extends Command
{
    protected $signature = 'dwh:backfill-attended-confirmed {--chunk=500}';
    protected $description = 'Backfills Count_Attended_Confirmed in Fact_Course_Delivery from Fact_Rider_Course for digitised deliveries';

    public function handle(): int
    {
        $dwh = DB::connection('mysql');
        $chunkSize = (int) $this->option('chunk');

        $this->info('[' . now()->format('Y-m-d H:i:s') . '] Finding candidate digitised delivery facts with zero attendance...');

        // 1. Locate candidate Fact records needing correction
        $query = $dwh->table('Fact_Course_Delivery as f')
            ->join('Dim_Delivery_Header as dh', 'f.Delivery_Key', '=', 'dh.Delivery_Key')
            ->join('Dim_Course as c', 'f.Course_Key', '=', 'c.Course_Key')
            ->where('dh.Digitisation_Booking', 1)
            ->where(function ($q) {
                $q->whereNull('f.Count_Attended_Confirmed')
                    ->orWhere('f.Count_Attended_Confirmed', 0);
            })
            ->select([
                'f.Delivery_Fact_Key',
                'c.Source_Course_Id',
                'c.Source_System_Key',
            ]);

        $total = $query->count();

        if ($total === 0) {
            $this->info('No digitised records found with missing attended counts.');
            return self::SUCCESS;
        }

        $this->info("Found {$total} candidates. Starting backfill in chunks of {$chunkSize}...");
        $bar = $this->output->createProgressBar($total);
        $bar->start();

        $updatedCount = 0;

        $query->orderBy('f.Delivery_Fact_Key')->chunk($chunkSize, function ($rows) use ($dwh, $bar, &$updatedCount) {
            foreach ($rows as $row) {
                $actualAttended = $dwh->table('Fact_Rider_Course')
                    ->where('Source_Course_Id', $row->Source_Course_Id)
                    ->where('Source_System_Key', $row->Source_System_Key)
                    ->where('Attended', 1)
                    ->count();

                if ($actualAttended > 0) {
                    $dwh->table('Fact_Course_Delivery')
                        ->where('Delivery_Fact_Key', $row->Delivery_Fact_Key)
                        ->update([
                            'Count_Attended_Confirmed' => $actualAttended,
                        ]);
                    $updatedCount++;
                }

                $bar->advance();
            }
        });

        $bar->finish();
        $this->newLine(2);
        $this->info("✅ Backfill complete. Updated {$updatedCount} fact records with attended counts from Fact_Rider_Course.");

        return self::SUCCESS;
    }
}

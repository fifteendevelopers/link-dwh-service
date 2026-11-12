<?php

namespace App\Console\Commands\DataWarehouse\Support;

use App\Services\DataWarehouseSyncService;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

class BackfillDimCourseBookingData extends Command
{
    protected $signature = 'dwh:backfill-dim-course-booking {--chunk=1000 : Chunk size for reading and updating}';
    protected $description = 'Backfills cancellation, confirmation, booking counts, and overrides into Dim_Course from source courses';

    public function handle(): int
    {
        $source = DB::connection('mysql_src');
        $dwh    = DB::connection('mysql');
        $chunkSize = (int) $this->option('chunk');

        $this->info('[' . now()->format('Y-m-d H:i:s') . '] Initializing Dim_Course backfill...');

        $sourceSystemKey = $dwh->table('Dim_Source_System')
            ->where('System_Name', 'Link')
            ->value('Source_System_Key') ?? 1;

        $totalRecords = $source->table('courses')->count();

        if ($totalRecords === 0) {
            $this->warn('No course records located on the source connection.');
            return self::SUCCESS;
        }

        $bar = $this->output->createProgressBar($totalRecords);
        $bar->start();

        $updatedCount = 0;

        $query = $source->table('courses')
            ->leftJoin('deliveries', 'courses.delivery_id', '=', 'deliveries.id')
            ->select([
                'courses.id',
                'courses.is_cancelled',
                'courses.cancellation_reason',
                'courses.has_confirmed_booked',
                'courses.confirmed_by',
                'courses.confirmed_by_email',
                'courses.confirmed_at',
                'courses.provisional_booked',
                'courses.booked',
                'courses.adults',
                'courses.children',
                'courses.attendees_overridden_metadata',
                'courses.attendees_overridden_by',
                'courses.attendees_overridden_at',
            ]);

        if (Schema::connection('mysql_src')->hasColumn('deliveries', 'delivery_details')) {
            $query->addSelect('deliveries.delivery_details');
        }

        $query
            ->orderBy('courses.id')
            ->chunk($chunkSize, function ($courses) use ($dwh, $sourceSystemKey, $bar, &$updatedCount) {
                foreach ($courses as $course) {
                    // Update only existing Dim_Course rows
                    $affected = $dwh->table('Dim_Course')
                        ->where('Source_Course_Id', $course->id)
                        ->where('Source_System_Key', $sourceSystemKey)
                        ->update([
                            'Is_Cancelled'                  => (bool) ($course->is_cancelled ?? false),
                            'Cancellation_Reason'           => $course->cancellation_reason ?? null,
                            'Has_Confirmed_Booked'          => DataWarehouseSyncService::resolveCourseConfirmed($course->id, $course->has_confirmed_booked ?? null, $course->delivery_details ?? null),
                            'Confirmed_By'                  => $course->confirmed_by ?? null,
                            'Confirmed_By_Email'            => $course->confirmed_by_email ?? null,
                            'Confirmed_At'                  => $course->confirmed_at ?? null,
                            'Provisional'                   => $course->provisional_booked ?? null,
                            'Booked'                        => $course->booked ?? null,
                            'Adults'                        => $course->adults ?? null,
                            'Children'                      => $course->children ?? null,
                            'Attendees_Overridden_Metadata' => $course->attendees_overridden_metadata ?? null,
                            'Attendees_Overridden_By'       => $course->attendees_overridden_by ?? null,
                            'Attendees_Overridden_At'       => $course->attendees_overridden_at ?? null,
                        ]);

                    if ($affected > 0) {
                        $updatedCount++;
                    }

                    $bar->advance();
                }
            });

        $bar->finish();
        $this->newLine(2);
        $this->info("Backfill complete! {$updatedCount} Dim_Course records updated.");

        return self::SUCCESS;
    }
}

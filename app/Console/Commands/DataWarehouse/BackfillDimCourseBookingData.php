<?php

namespace App\Console\Commands\DataWarehouse;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;

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

        $source->table('courses')
            ->select([
                'id',
                'is_cancelled',
                'cancellation_reason',
                'has_confirmed_booked',
                'confirmed_by',
                'confirmed_by_email',
                'confirmed_at',
                'provisional_booked',
                'booked',
                'adults',
                'children',
                'attendees_overridden_metadata',
                'attendees_overridden_by',
                'attendees_overridden_at',
            ])
            ->orderBy('id')
            ->chunk($chunkSize, function ($courses) use ($dwh, $sourceSystemKey, $bar, &$updatedCount) {
                foreach ($courses as $course) {
                    // Update only existing Dim_Course rows
                    $affected = $dwh->table('Dim_Course')
                        ->where('Source_Course_Id', $course->id)
                        ->where('Source_System_Key', $sourceSystemKey)
                        ->update([
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

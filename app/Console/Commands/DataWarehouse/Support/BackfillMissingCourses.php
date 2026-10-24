<?php

namespace App\Console\Commands\DataWarehouse\Support;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Throwable;

class BackfillMissingCourses extends Command
{
    protected $signature = 'dwh:backfill-missing-courses {--ids= : Optional comma-separated course IDs}';

    protected $description = 'Backfills missing courses into Dim_Course (assumes parent deliveries exist)';

    public function handle(): int
    {
        $dwh = DB::connection('mysql');
        $source = DB::connection('mysql_src');

        $this->info('[' . now()->format('Y-m-d H:i:s') . '] Scanning for Missing Courses...');

        $sourceSystemKey = (int) ($dwh->table('Dim_Source_System')
            ->where('System_Name', 'link')
            ->value('Source_System_Key') ?? 1);

        $targetIds = [];
        if ($idsOption = $this->option('ids')) {
            $targetIds = array_filter(array_map('intval', explode(',', $idsOption)));
            $this->comment('Targeting ' . count($targetIds) . ' explicitly specified course ID(s).');
        } else {
            $dwhCourseIds = $dwh->table('Dim_Course')
                ->where('Source_System_Key', $sourceSystemKey)
                ->whereNotNull('Source_Course_Id')
                ->pluck('Source_Course_Id')
                ->flip();

            $sourceCourseIds = $source->table('courses')->pluck('id');

            foreach ($sourceCourseIds as $sId) {
                if (!isset($dwhCourseIds[$sId])) {
                    $targetIds[] = (int) $sId;
                }
            }
        }

        $total = count($targetIds);
        if ($total === 0) {
            $this->info('No missing courses detected. Dim_Course is at 100% parity.');
            return self::SUCCESS;
        }

        $this->info("Found {$total} missing courses to backfill.");
        $bar = $this->output->createProgressBar($total);
        $bar->start();

        $processedCount = 0;
        $skippedCount = 0;
        $errorCount = 0;

        try {
            $source->table('courses')
                ->whereIn('id', $targetIds)
                ->select([
                    'id', 'course_id', 'parent_course_id', 'delivery_id',
                    'status', 'start_date', 'date_complete', 'year_group',
                    'is_cancelled', 'cancellation_reason', 'has_confirmed_booked',
                    'confirmed_by', 'confirmed_by_email', 'confirmed_at',
                    'provisional_booked', 'booked', 'adults', 'children',
                    'attendees_overridden_metadata', 'attendees_overridden_by',
                    'attendees_overridden_at', 'updated_at',
                ])
                ->orderBy('id', 'asc')
                ->chunk(250, function ($courses) use ($dwh, $sourceSystemKey, $bar, &$processedCount, &$skippedCount, &$errorCount) {
                    foreach ($courses as $course) {
                        try {
                            $deliveryKey = $dwh->table('Dim_Delivery_Header')
                                ->where('Source_Delivery_Id', $course->delivery_id)
                                ->where('Source_System_Key', $sourceSystemKey)
                                ->value('Delivery_Key');

                            if (!$deliveryKey) {
                                $skippedCount++;
                                continue;
                            }

                            $dwh->table('Dim_Course')->updateOrInsert(
                                [
                                    'Source_Course_Id'  => $course->id,
                                    'Source_System_Key' => $sourceSystemKey,
                                ],
                                [
                                    'Delivery_Key'                  => $deliveryKey,
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

            // Re-link parent courses
            $this->comment('Resolving parent-child course hierarchies...');
            $childCourses = $source->table('courses')
                ->whereIn('id', $targetIds)
                ->whereNotNull('parent_course_id')
                ->select(['id', 'parent_course_id'])
                ->get();

            $linkedParents = 0;
            foreach ($childCourses as $child) {
                $parentKey = $dwh->table('Dim_Course')
                    ->where('Source_Course_Id', $child->parent_course_id)
                    ->where('Source_System_Key', $sourceSystemKey)
                    ->value('Course_Key');

                if ($parentKey) {
                    $dwh->table('Dim_Course')
                        ->where('Source_Course_Id', $child->id)
                        ->where('Source_System_Key', $sourceSystemKey)
                        ->update(['Parent_Course_Key' => $parentKey]);
                    $linkedParents++;
                }
            }

            $this->info("Linked {$linkedParents} parent-child relationships.");
            $this->info("Courses backfill complete: {$processedCount} inserted/updated, {$skippedCount} skipped (unmapped delivery), {$errorCount} errors.");

            return self::SUCCESS;
        } catch (Throwable $fatal) {
            $bar->finish();
            $this->newLine();
            $this->error('Fatal error during courses backfill: ' . $fatal->getMessage());
            return self::FAILURE;
        }
    }
}

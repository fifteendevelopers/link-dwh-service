<?php

namespace App\Console\Commands\DataWarehouse;

use App\Models\DwhSyncRun;
use App\Services\DataWarehouseQualityService;
use App\Services\DataWarehouseSyncService;
use App\Services\SlackAlertService;
use Carbon\Carbon;
use Illuminate\Console\Command;
use Throwable;

class SyncToDataWarehouse extends Command
{
    protected $signature = 'dwh:sync {--table=all}';

    protected $description = 'Syncs production data into the Data Warehouse';

    public function handle(
        DataWarehouseSyncService $syncService,
        DataWarehouseQualityService $qualityService,
        SlackAlertService $slack
    ): int {
        $startTime = microtime(true);
        $startedAt = Carbon::now();
        $table = $this->option('table');
        $syncErrors = [];
        $auditMismatches = [];
        $auditResults = [];

        // 1. Record starting state for live dashboard visibility
        $run = DwhSyncRun::create([
            'target_table' => $table,
            'triggered_by' => 'CRON',
            'started_at'   => $startedAt,
            'status'       => 'RUNNING',
            'sync_errors'  => [],
            'audit_results'=> [],
        ]);

        $this->info("[{$startedAt->format('Y-m-d H:i:s')}] Starting Data Warehouse Sync (Target: {$table})...");

        // Helper closure to dry up the try/catches and collect errors
        $runSyncStep = function (string $stepName, callable $callback) use (&$syncErrors) {
            $this->comment("[" . now()->format('Y-m-d H:i:s') . "] Syncing {$stepName}...");
            try {
                $result = $callback();
                $this->info($result);
            } catch (Throwable $e) {
                $msg = $e->getMessage();
                $this->error("[" . now()->format('Y-m-d H:i:s') . "] Failed to sync {$stepName}: {$msg}");
                $syncErrors[$stepName] = $msg;
            }
        };

        try {
            if ($table === 'all' || $table === 'external_systems') {
                $runSyncStep('External Systems', fn() => $syncService->syncExternalSystems($this));
            }

            if ($table === 'all' || $table === 'course_activities_lookup') {
                $runSyncStep('Course Activities lookup', fn() => $syncService->syncCourseActivities($this));
            }

            if ($table === 'all' || $table === 'instructor_feedback_lookup') {
                $runSyncStep('Instructor Feedback lookup', fn() => $syncService->syncInstructorFeedbackLookups($this));
            }

            if ($table === 'all' || $table === 'qualifications') {
                $runSyncStep('Qualifications', fn() => $syncService->syncQualifications($this));
            }

            if ($table === 'all' || $table === 'providers') {
                $runSyncStep('Training Providers', fn() => $syncService->syncTrainingProviders($this));
            }

            if ($table === 'all' || $table === 'riders') {
                $runSyncStep('Riders', fn() => $syncService->syncRiders($this));
            }

            if ($table === 'all' || $table === 'schools') {
                $runSyncStep('Schools', fn() => $syncService->syncSchools($this));
            }

            if ($table === 'all' || $table === 'training_organisations') {
                $runSyncStep('Dim_Training_Organisation', fn() => $syncService->syncTrainingOrganisations($this));
            }

            if ($table === 'all' || $table === 'organisations') {
                $runSyncStep('Organisations', fn() => $syncService->syncOrganisations($this));
            }

            if ($table === 'all' || $table === 'grant_recipients') {
                $runSyncStep('Grant Recipients', fn() => $syncService->syncGrantRecipients($this));
            }

            if ($table === 'all' || $table === 'grants') {
                $runSyncStep('Grants', fn() => $syncService->syncGrants($this));
            }

            if ($table === 'all' || $table === 'instructors') {
                $runSyncStep('Instructors', fn() => $syncService->syncInstructors($this));
            }

            if ($table === 'all' || $table === 'deliveries') {
                $runSyncStep('Deliveries', fn() => $syncService->syncDeliveryHeaders($this));
            }

            if ($table === 'all' || $table === 'courses') {
                $runSyncStep('Courses', fn() => $syncService->syncCourses($this));
            }

            if ($table === 'all' || $table === 'consents') {
                $runSyncStep('Consents', fn() => $syncService->syncConsents($this));
            }

            if ($table === 'all' || $table === 'teacher_trainers') {
                $runSyncStep('Teacher Trainers', fn() => $syncService->syncTeacherTrainers($this));
            }

            if ($table === 'all' || $table === 'get_cycling_courses') {
                $runSyncStep('Get Cycling Courses', fn() => $syncService->syncGetCyclingCourses($this));
            }

            if ($table === 'all' || $table === 'get_cycling_riders') {
                $runSyncStep('Get Cycling Riders', fn() => $syncService->syncGetCyclingRiders($this));
            }

            if ($table === 'all' || $table === 'get_cycling_rider_notes') {
                $runSyncStep('Get Cycling Rider Notes', fn() => $syncService->syncGetCyclingRiderNotes($this));
            }

            /* Fact tables */
            if ($table === 'all' || $table === 'instructors_training_providers') {
                $runSyncStep('Fact Instructor Training Providers', fn() => $syncService->syncInstructorTrainingProviders($this));
            }

            if ($table === 'all' || $table === 'fact_rider_course') {
                $runSyncStep('Fact Rider Course', fn() => $syncService->syncFactRiderCourse($this));
            }

            if ($table === 'all' || $table === 'fact_course_delivery') {
                $runSyncStep('Fact Course Delivery', fn() => $syncService->syncFactCourseDelivery($this));
            }

            if ($table === 'all' || $table === 'fact_rider_activity_outcome') {
                $runSyncStep('Fact Rider Activity Outcomes', fn() => $syncService->syncFactRiderActivityOutcomes($this));
            }

            if ($table === 'all' || $table === 'fact_rider_instructor_feedback') {
                $runSyncStep('Fact Rider Instructor Feedback', fn() => $syncService->syncFactRiderInstructorFeedback($this));
            }

            if ($table === 'all' || $table === 'fact_rider_activity_outcome_summary') {
                $runSyncStep('Fact Activity Outcome Summary', fn() => $syncService->syncFactActivityOutcomeSummary($this));
            }

            if ($table === 'all' || $table === 'fact_grant_financials') {
                $runSyncStep('Fact Grant Financials', fn() => $syncService->syncFactGrantFinancials($this));
            }

            if ($table === 'all' || $table === 'fact_grant_reallocations') {
                $runSyncStep('Fact Grant Reallocations', fn() => $syncService->syncFactGrantReallocations($this));
            }

            if ($table === 'all' || $table === 'fact_grant_amendments') {
                $runSyncStep('Fact Grant Amendments', fn() => $syncService->syncFactGrantAmendments($this));
            }

            if ($table === 'all' || $table === 'fact_grant_claims') {
                $runSyncStep('Fact Grant Claims', fn() => $syncService->syncFactGrantClaims($this));
            }

            if ($table === 'all' || $table === 'fact_grant_format_dft') {
                $runSyncStep('Fact Grant Format DFT', fn() => $syncService->syncFactGrantFormatDft($this));
            }

            if ($table === 'all' || $table === 'fact_instructor_course') {
                $runSyncStep('Fact Instructor Course', fn() => $syncService->syncFactInstructorCourse($this));
            }

            if ($table === 'all' || $table === 'fact_instructor_delivery') {
                $runSyncStep('Fact Instructor Delivery', fn() => $syncService->syncFactInstructorDeliveries($this));
            }

            if ($table === 'all' || $table === 'fact_instructor_qualifications') {
                $runSyncStep('Fact Instructor Qualification', fn() => $syncService->syncFactInstructorQualifications($this));
            }

            if ($table === 'all' || $table === 'fact_parent_survey') {
                $runSyncStep('Fact Parent Survey', fn() => $syncService->syncFactParentSurvey($this));
            }

            if ($table === 'all' || $table === 'fact_hands_up_survey') {
                $runSyncStep('Fact Hands Up Survey', fn() => $syncService->syncFactHandsupSurvey($this));
            }

            if ($table === 'all' || $table === 'fact_parent_follow_up_survey') {
                $runSyncStep('Fact Parent Follow Up Survey', fn() => $syncService->syncFactParentFollowUpSurveys($this));
            }

            if ($table === 'all' || $table === 'fact_grant_recipient_renewals') {
                $runSyncStep('Fact Grant Recipient Renewals', fn() => $syncService->syncFactGrantRecipientRenewals($this));
            }

            if ($table === 'all' || $table === 'fact_instructor_renewals') {
                $runSyncStep('Fact Instructor Renewals', fn() => $syncService->syncFactInstructorRenewals($this));
            }

            if ($table === 'all' || $table === 'fact_training_provider_renewals') {
                $runSyncStep('Fact Training Provider Renewals', fn() => $syncService->syncFactTrainingProviderRenewals($this));
            }

            if ($table === 'all' || $table === 'fact_teacher_trainer_deliveries') {
                $runSyncStep('Fact Teacher Trainer Deliveries', fn() => $syncService->syncFactTeacherTrainerDeliveries($this));
            }

            if ($table === 'all' || $table === 'fact_get_cycling_rider_courses') {
                $runSyncStep('Fact Get Cycling Rider Courses', fn() => $syncService->syncGetCyclingRiderCourses($this));
            }

            if ($table === 'all' || $table === 'fact_get_cycling_survey_courses') {
                $runSyncStep('Fact Get Cycling Survey Courses', fn() => $syncService->syncGetCyclingSurveys($this));
            }

            /** Call "Pruning" action - this requires a specific call to action the pruning */
            if ($table === 'prune_fact_instructor_delivery') {
                $runSyncStep('Prune Orphaned Pivot Records', function () use ($syncService) {
                    $stats = $syncService->pruneOrphanedFactInstructorDeliveryPivotRecords($this);
                    $pruned = $stats['Fact_Instructor_Delivery'] ?? 0;
                    return "Pruned {$pruned} ghost records from Fact_Instructor_Delivery.";
                });
            }

            // =========================================================
            // POST-SYNC QUALITY AUDIT
            // =========================================================
            $this->newLine(2);
            $this->info('===========================================================');
            $this->info('           RUNNING POST-SYNC QUALITY AUDIT                 ');
            $this->info('===========================================================');

            $auditResults = $qualityService->auditAll($this->output, 5);
            $tableRows = [];

            foreach ($auditResults as $entity => $data) {
                $status = $data['match'] ? '✅ MATCH' : '⚠️ MISMATCH';
                if (!$data['match']) {
                    $auditMismatches[$entity] = $data['missing_count'];
                }

                $missingList = $data['missing_ids'] ?? [];
                $missingSummary = $data['missing_count'] > 0
                    ? (!empty($missingList)
                        ? "{$data['missing_count']} missing (e.g. " . implode(', ', array_slice($missingList, 0, 5)) . ")"
                        : "{$data['missing_count']} missing")
                    : '0';

                $tableRows[] = [
                    $entity,
                    number_format($data['source_count']),
                    number_format($data['dwh_count']),
                    $status,
                    $missingSummary,
                ];
            }

            $this->table(
                ['Entity', 'Source Rows', 'DWH Rows', 'Status', 'Missing Sample'],
                $tableRows
            );

        } catch (Throwable $fatal) {
            $syncErrors['CRITICAL_PIPELINE_ERROR'] = $fatal->getMessage();
            $this->error("Fatal pipeline exception: {$fatal->getMessage()}");
        }

        // =========================================================
        // FINAL EXECUTION STATUS & PERSISTENCE
        // =========================================================
        $durationSeconds = (int) round(microtime(true) - $startTime);
        $minutes = floor($durationSeconds / 60);
        $seconds = $durationSeconds % 60;
        $durationText = ($minutes > 0 ? "{$minutes}m " : "") . "{$seconds}s";

        $hasSyncCrashes = !empty($syncErrors);
        $hasAuditMismatches = !empty($auditMismatches);

        // Determine final run status
        if ($hasSyncCrashes) {
            $finalStatus = 'FAILURE';
        } elseif ($hasAuditMismatches) {
            $finalStatus = 'WARNING';
        } else {
            $finalStatus = 'SUCCESS';
        }

        // 2. Persist completion to dwh_sync_runs table
        $run->update([
            'finished_at'      => Carbon::now(),
            'duration_seconds' => $durationSeconds,
            'status'           => $finalStatus,
            'sync_errors'      => $syncErrors,
            'audit_results'    => $auditResults,
        ]);

        $this->newLine();
        $this->line('===========================================================');

        if ($hasSyncCrashes) {
            $this->line(" <error> FAILURE </error> <fg=red;options=bold>DWH Sync finished WITH ERRORS</> in {$durationText}");
        } elseif ($hasAuditMismatches) {
            $this->line(" <bg=yellow;fg=black;options=bold> WARNING </bg=yellow;fg=black;options=bold> <fg=yellow;options=bold>DWH Sync completed with AUDIT MISMATCHES</> in {$durationText}");
        } else {
            $this->line(" <bg=green;fg=black;options=bold> SUCCESS </bg=green;fg=black;options=bold> <fg=green;options=bold>DWH Sync completed SUCCESSFULLY</> in {$durationText}");
        }

        $this->line('===========================================================');

        // 3. Dispatch Slack notification
        $this->sendSlackStatusNotification(
            $slack,
            $table,
            $durationText,
            $syncErrors,
            $auditMismatches
        );

        return ($hasSyncCrashes || $hasAuditMismatches) ? self::FAILURE : self::SUCCESS;
    }

    /**
     * Dispatch the final run status to Slack.
     */
    protected function sendSlackStatusNotification(
        SlackAlertService $slack,
        string $targetTable,
        string $duration,
        array $syncErrors,
        array $auditMismatches
    ): void {
        $hasErrors = !empty($syncErrors);
        $hasMismatches = !empty($auditMismatches);

        if ($hasErrors) {
            $header      = "DWH Sync Finished: ERRORS DETECTED";
            $statusColor = "#A30200"; // Crimson Red ('danger')
            $statusText  = "CRITICAL ERRORS";
        } elseif ($hasMismatches) {
            $header      = "DWH Sync Finished: AUDIT WARNINGS";
            $statusColor = "#DAA038"; // Amber / Gold ('warning')
            $statusText  = "AUDIT WARNINGS";
        } else {
            $header      = "DWH Sync Finished: SUCCESS";
            $statusColor = "#2EB886"; // Emerald Green ('good')
            $statusText  = "SUCCESS";
        }

        $message = "*Status:* `{$statusText}` | Finished in *{$duration}* (Target: `{$targetTable}`)\n";

        if ($hasErrors) {
            $message .= "\n*Failed Steps (" . count($syncErrors) . "):*\n";
            foreach ($syncErrors as $step => $err) {
                $message .= "• *{$step}:* `{$err}`\n";
            }
        }

        if ($hasMismatches) {
            $message .= "\n*Audit Mismatches (" . count($auditMismatches) . "):*\n";
            foreach ($auditMismatches as $entity => $count) {
                $message .= "• *{$entity}:* {$count} records missing\n";
            }
        }

        if (!$hasErrors && !$hasMismatches) {
            $message .= "\nAll tables synced and 100% parity verified.";
        }

        $slack->sendSyncStatus($header, $message, $statusColor);
    }
}

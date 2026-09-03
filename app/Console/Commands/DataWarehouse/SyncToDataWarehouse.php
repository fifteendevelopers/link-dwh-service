<?php

namespace App\Console\Commands\DataWarehouse;

use App\Services\DataWarehouseSyncService;
use Illuminate\Console\Command;

class SyncToDataWarehouse extends Command
{
    // The name of the command you'll type in terminal
    protected $signature = 'dwh:sync {--table=all}';

    protected $description = 'Syncs production data into the Data Warehouse';

    public function handle(DataWarehouseSyncService $syncService)
    {
        $table = $this->option('table');

        $this->info("[".now()->format('Y-m-d H:i:s')."] Starting Data Warehouse Sync...");

        if ($table === 'all' || $table === 'external_systems') {
            $this->comment("[".now()->format('Y-m-d H:i:s')."] Syncing External Systems...");

            try {
                $result = $syncService->syncExternalSystems($this);
                $this->info($result);
            } catch (\Exception $e) {
                $this->error("[".now()->format('Y-m-d H:i:s')."] Failed to sync External Systems: " . $e->getMessage());
            }
        }

        if ($table === 'all' || $table === 'course_activities_lookup') {
            $this->comment("[".now()->format('Y-m-d H:i:s')."] Syncing Course Activities lookup...");

            try {
                $result = $syncService->syncCourseActivities($this);
                $this->info($result);
            } catch (\Exception $e) {
                $this->error("[".now()->format('Y-m-d H:i:s')."] Failed to sync Course Activities lookup: " . $e->getMessage());
            }
        }

        if ($table === 'all' || $table === 'instructor_feedback_lookup') {
            $this->comment("[".now()->format('Y-m-d H:i:s')."] Syncing Instructor Feedback lookup...");

            try {
                $result = $syncService->syncInstructorFeedbackLookups($this);
                $this->info($result);
            } catch (\Exception $e) {
                $this->error("[".now()->format('Y-m-d H:i:s')."] Failed to sync Instructor Feedback lookup: " . $e->getMessage());
            }
        }

        if ($table === 'all' || $table === 'providers') {
            $this->comment("[".now()->format('Y-m-d H:i:s')."] Syncing Training Providers...");

            try {
                $result = $syncService->syncTrainingProviders($this);
                $this->info($result);
            } catch (\Exception $e) {
                $this->error("[".now()->format('Y-m-d H:i:s')."] Failed to sync Providers: " . $e->getMessage());
            }
        }

        if ($table === 'all' || $table === 'riders') {
            $this->comment("[".now()->format('Y-m-d H:i:s')."] Syncing Riders...");

            try {
                $result = $syncService->syncRiders($this);
                $this->info($result);
            } catch (\Exception $e) {
                $this->error("[".now()->format('Y-m-d H:i:s')."] Failed to sync Riders: " . $e->getMessage());
            }
        }

        if ($table === 'all' || $table === 'schools') {
            $this->comment("[".now()->format('Y-m-d H:i:s')."] Syncing Schools...");

            try {
                $result = $syncService->syncSchools($this);
                $this->info($result);
            } catch (\Exception $e) {
                $this->error("[".now()->format('Y-m-d H:i:s')."] Failed to sync Schools: " . $e->getMessage());
            }
        }

        if ($table === 'all' || $table === 'organisations') {
            $this->comment("[".now()->format('Y-m-d H:i:s')."] Syncing Organisations...");

            try {
                $result = $syncService->syncOrganisations($this);
                $this->info($result);
            } catch (\Exception $e) {
                $this->error("[".now()->format('Y-m-d H:i:s')."] Failed to sync Organisations: " . $e->getMessage());
            }
        }

        if ($table === 'all' || $table === 'grant_recipients') {
            $this->comment("[".now()->format('Y-m-d H:i:s')."] Syncing Grant Recipients...");

            try {
                $result = $syncService->syncGrantRecipients($this);
                $this->info($result);
            } catch (\Exception $e) {
                $this->error("[".now()->format('Y-m-d H:i:s')."] Failed to sync Grant Recipients: " . $e->getMessage());
            }
        }

        if ($table === 'all' || $table === 'grants') {
            $this->comment("[".now()->format('Y-m-d H:i:s')."] Syncing grants...");

            try {
                $result = $syncService->syncGrants($this);
                $this->info($result);
            } catch (\Exception $e) {
                $this->error("[".now()->format('Y-m-d H:i:s')."] Failed to sync grants: " . $e->getMessage());
            }
        }

        if ($table === 'all' || $table === 'instructors') {
            $this->comment("[".now()->format('Y-m-d H:i:s')."] Syncing Instructors...");

            try {
                $result = $syncService->syncInstructors($this);
                $this->info($result);
            } catch (\Exception $e) {
                $this->error("[".now()->format('Y-m-d H:i:s')."] Failed to sync Instructors: " . $e->getMessage());
            }
        }

        if ($table === 'all' || $table === 'deliveries') {
            $this->comment("[".now()->format('Y-m-d H:i:s')."] Syncing deliveries...");

            try {
                $result = $syncService->syncDeliveryHeaders($this);
                $this->info($result);
            } catch (\Exception $e) {
                $this->error("[".now()->format('Y-m-d H:i:s')."] Failed to sync delivery headers: " . $e->getMessage());
            }
        }

        if ($table === 'all' || $table === 'courses') {
            $this->comment("[".now()->format('Y-m-d H:i:s')."] Syncing courses...");

            try {
                $result = $syncService->syncCourses($this);
                $this->info($result);
            } catch (\Exception $e) {
                $this->error("[".now()->format('Y-m-d H:i:s')."] Failed to sync courses: " . $e->getMessage());
            }
        }

        if ($table === 'all' || $table === 'consents') {
            $this->comment("[".now()->format('Y-m-d H:i:s')."] Syncing Consents...");

            try {
                $result = $syncService->syncConsents($this);
                $this->info($result);
            } catch (\Exception $e) {
                $this->error("[".now()->format('Y-m-d H:i:s')."] Failed to sync Consents: " . $e->getMessage());
            }
        }

        if ($table === 'all' || $table === 'teacher_trainers') {
            $this->comment("[".now()->format('Y-m-d H:i:s')."] Syncing Teacher Trainers...");

            try {
                $result = $syncService->syncTeacherTrainers($this);
                $this->info($result);
            } catch (\Exception $e) {
                $this->error("[".now()->format('Y-m-d H:i:s')."] Failed to sync Teacher Trainers: " . $e->getMessage());
            }
        }

        if ($table === 'all' || $table === 'get_cycling_courses') {
            $this->comment("[".now()->format('Y-m-d H:i:s')."] Syncing Get Cycling Courses...");

            try {
                $result = $syncService->syncGetCyclingCourses($this);
                $this->info($result);
            } catch (\Exception $e) {
                $this->error("[".now()->format('Y-m-d H:i:s')."] Failed to sync Get Cycling Courses: " . $e->getMessage());
            }
        }

        if ($table === 'all' || $table === 'get_cycling_riders') {
            $this->comment("[".now()->format('Y-m-d H:i:s')."] Syncing Get Cycling Riders...");

            try {
                $result = $syncService->syncGetCyclingRiders($this);
                $this->info($result);
            } catch (\Exception $e) {
                $this->error("[".now()->format('Y-m-d H:i:s')."] Failed to sync Get Cycling Riders: " . $e->getMessage());
            }
        }

        if ($table === 'all' || $table === 'get_cycling_rider_notes') {
            $this->comment("[".now()->format('Y-m-d H:i:s')."] Syncing Get Cycling Rider Notes...");

            try {
                $result = $syncService->syncGetCyclingRiderNotes($this);
                $this->info($result);
            } catch (\Exception $e) {
                $this->error("[".now()->format('Y-m-d H:i:s')."] Failed to sync Get Cycling Rider Notes: " . $e->getMessage());
            }
        }

        /* Now Sync the FACTS */

        if ($table === 'all' || $table === 'fact_course_delivery') {
            $this->comment("[".now()->format('Y-m-d H:i:s')."] Syncing Facts from Course Deliveries...");

            try {
                $result = $syncService->syncFactCourseDelivery($this);
                $this->info($result);
            } catch (\Exception $e) {
                $this->error("[".now()->format('Y-m-d H:i:s')."] Failed to sync Course Deliveries: " . $e->getMessage());
            }
        }

        if ($table === 'all' || $table === 'fact_rider_course') {
            $this->comment("[".now()->format('Y-m-d H:i:s')."] Syncing Facts from Riders Courses...");

            try {
                $result = $syncService->syncFactRiderCourse($this);
                $this->info($result);
            } catch (\Exception $e) {
                $this->error("[".now()->format('Y-m-d H:i:s')."] Failed to sync Riders Course Facts: " . $e->getMessage());
            }
        }

        if ($table === 'all' || $table === 'fact_rider_activity_outcome') {
            $this->comment("[".now()->format('Y-m-d H:i:s')."] Syncing Facts from Riders Course Activity outcomes...");

            try {
                $result = $syncService->syncFactRiderActivityOutcomes($this);
                $this->info($result);
            } catch (\Exception $e) {
                $this->error("[".now()->format('Y-m-d H:i:s')."] Failed to sync Riders Course Activity outcomes: " . $e->getMessage());
            }
        }

        if ($table === 'all' || $table === 'fact_rider_instructor_feedback') {
            $this->comment("[".now()->format('Y-m-d H:i:s')."] Syncing Facts from Riders Instructor Feedback...");

            try {
                $result = $syncService->syncFactRiderInstructorFeedback($this);
                $this->info($result);
            } catch (\Exception $e) {
                $this->error("[".now()->format('Y-m-d H:i:s')."] Failed to sync Riders Instructor Feedback: " . $e->getMessage());
            }
        }

        if ($table === 'all' || $table === 'fact_rider_activity_outcome_summary') {
            $this->comment("[".now()->format('Y-m-d H:i:s')."] Syncing Facts from Riders Activity Outcome Summary...");

            try {
                $result = $syncService->syncFactActivityOutcomeSummary($this);
                $this->info($result);
            } catch (\Exception $e) {
                $this->error("[".now()->format('Y-m-d H:i:s')."] Failed to sync Riders Activity Outcome Summary: " . $e->getMessage());
            }
        }

        if ($table === 'all' || $table === 'fact_grant_financials') {
            $this->comment("[".now()->format('Y-m-d H:i:s')."] Syncing Facts from Grant Format DFT (Financials)...");

            try {
                $result = $syncService->syncFactGrantFinancials($this);
                $this->info($result);
            } catch (\Exception $e) {
                $this->error("[".now()->format('Y-m-d H:i:s')."] Failed to sync Grant Format DFT (Financials): " . $e->getMessage());
            }
        }

        if ($table === 'all' || $table === 'fact_grant_reallocations') {
            $this->comment("[".now()->format('Y-m-d H:i:s')."] Syncing Facts from Grant Reallocations...");

            try {
                $result = $syncService->syncFactGrantReallocations($this);
                $this->info($result);
            } catch (\Exception $e) {
                $this->error("[".now()->format('Y-m-d H:i:s')."] Failed to sync Grant Reallocations: " . $e->getMessage());
            }
        }

        if ($table === 'all' || $table === 'fact_grant_amendments') {
            $this->comment("[".now()->format('Y-m-d H:i:s')."] Syncing Facts from Grant Amendments...");

            try {
                $result = $syncService->syncFactGrantAmendments($this);
                $this->info($result);
            } catch (\Exception $e) {
                $this->error("[".now()->format('Y-m-d H:i:s')."] Failed to sync Grant Amendments: " . $e->getMessage());
            }
        }

        if ($table === 'all' || $table === 'fact_grant_claims') {
            $this->comment("[".now()->format('Y-m-d H:i:s')."] Syncing Facts from Grant Claims...");

            try {
                $result = $syncService->syncFactGrantClaims($this);
                $this->info($result);
            } catch (\Exception $e) {
                $this->error("[".now()->format('Y-m-d H:i:s')."] Failed to sync Grant Claims: " . $e->getMessage());
            }
        }

        if ($table === 'all' || $table === 'fact_grant_format_dft') {
            $this->comment("[".now()->format('Y-m-d H:i:s')."] Syncing Facts from Grant Format DFT...");

            try {
                $result = $syncService->syncFactGrantFormatDft($this);
                $this->info($result);
            } catch (\Exception $e) {
                $this->error("[".now()->format('Y-m-d H:i:s')."] Failed to sync Grant Format DFT: " . $e->getMessage());
            }
        }

        if ($table === 'all' || $table === 'fact_instructor_course') {
            $this->comment("[".now()->format('Y-m-d H:i:s')."] Syncing Facts from Instructor / Courses...");

            try {
                $result = $syncService->syncFactInstructorCourse($this);
                $this->info($result);
            } catch (\Exception $e) {
                $this->error("[".now()->format('Y-m-d H:i:s')."] Failed to sync Instructor Course: " . $e->getMessage());
            }
        }

        if ($table === 'all' || $table === 'fact_instructor_delivery') {
            $this->comment("[".now()->format('Y-m-d H:i:s')."] Syncing Facts from Instructor / Deliveries...");

            try {
                $result = $syncService->syncFactInstructorDeliveries($this);
                $this->info($result);
            } catch (\Exception $e) {
                $this->error("[".now()->format('Y-m-d H:i:s')."] Failed to sync Instructor Deliveries: " . $e->getMessage());
            }
        }

        if ($table === 'all' || $table === 'fact_parent_survey') {
            $this->comment("[".now()->format('Y-m-d H:i:s')."] Syncing Facts from Parent Survey...");

            try {
                $result = $syncService->syncFactParentSurvey($this);
                $this->info($result);
            } catch (\Exception $e) {
                $this->error("[".now()->format('Y-m-d H:i:s')."] Failed to sync Parent Survey facts: " . $e->getMessage());
            }
        }

        if ($table === 'all' || $table === 'fact_hands_up_survey') {
            $this->comment("[".now()->format('Y-m-d H:i:s')."] Syncing Facts from Hands Up Survey...");

            try {
                $result = $syncService->syncFactHandsupSurvey($this);
                $this->info($result);
            } catch (\Exception $e) {
                $this->error("[".now()->format('Y-m-d H:i:s')."] Failed to sync Hands Up Survey facts: " . $e->getMessage());
            }
        }

        if ($table === 'all' || $table === 'fact_parent_follow_up_survey') {
            $this->comment("[".now()->format('Y-m-d H:i:s')."] Syncing Facts from Parent Follow Up Survey...");

            try {
                $result = $syncService->syncFactParentFollowUpSurveys($this);
                $this->info($result);
            } catch (\Exception $e) {
                $this->error("[".now()->format('Y-m-d H:i:s')."] Failed to sync Parent Follow Up Survey facts: " . $e->getMessage());
            }
        }

        if ($table === 'all' || $table === 'fact_grant_recipient_renewals') {
            $this->comment("[".now()->format('Y-m-d H:i:s')."] Syncing Facts from Grant Recipient Renewals...");

            try {
                $result = $syncService->syncFactGrantRecipientRenewals($this);
                $this->info($result);
            } catch (\Exception $e) {
                $this->error("[".now()->format('Y-m-d H:i:s')."] Failed to sync Grant Recipient Renewals facts: " . $e->getMessage());
            }
        }

        if ($table === 'all' || $table === 'fact_instructor_renewals') {
            $this->comment("[".now()->format('Y-m-d H:i:s')."] Syncing Facts from Instructor Renewals...");

            try {
                $result = $syncService->syncFactInstructorRenewals($this);
                $this->info($result);
            } catch (\Exception $e) {
                $this->error("[".now()->format('Y-m-d H:i:s')."] Failed to sync Instructor Renewals facts: " . $e->getMessage());
            }
        }

        if ($table === 'all' || $table === 'fact_training_provider_renewals') {
            $this->comment("[".now()->format('Y-m-d H:i:s')."] Syncing Facts from Training Provider Renewals...");

            try {
                $result = $syncService->syncFactTrainingProviderRenewals($this);
                $this->info($result);
            } catch (\Exception $e) {
                $this->error("[".now()->format('Y-m-d H:i:s')."] Failed to sync Training Provider Renewals facts: " . $e->getMessage());
            }
        }

        if ($table === 'all' || $table === 'fact_teacher_trainer_deliveries') {
            $this->comment("[".now()->format('Y-m-d H:i:s')."] Syncing Teacher Trainer deliveries...");

            try {
                $result = $syncService->syncFactTeacherTrainerDeliveries($this);
                $this->info($result);
            } catch (\Exception $e) {
                $this->error("[".now()->format('Y-m-d H:i:s')."] Failed to sync Teacher Trainer deliveries: " . $e->getMessage());
            }
        }

        if ($table === 'all' || $table === 'fact_get_cycling_rider_courses') {
            $this->comment("[".now()->format('Y-m-d H:i:s')."] Syncing Get Cycling rider courses...");

            try {
                $result = $syncService->syncGetCyclingRiderCourses($this);
                $this->info($result);
            } catch (\Exception $e) {
                $this->error("[".now()->format('Y-m-d H:i:s')."] Failed to sync Get Cycling rider courses: " . $e->getMessage());
            }
        }

        if ($table === 'all' || $table === 'fact_get_cycling_survey_courses') {
            $this->comment("[".now()->format('Y-m-d H:i:s')."] Syncing Get Cycling survey courses...");

            try {
                $result = $syncService->syncGetCyclingSurveys($this);
                $this->info($result);
            } catch (\Exception $e) {
                $this->error("[".now()->format('Y-m-d H:i:s')."] Failed to sync Get Cycling survey courses: " . $e->getMessage());
            }
        }


        $this->info("[".now()->format('Y-m-d H:i:s')."] Sync Process Completed.");
    }
}

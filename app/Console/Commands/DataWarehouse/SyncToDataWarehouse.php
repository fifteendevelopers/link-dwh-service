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

        // Starting with Training Providers only
        // TODO: Add other Dimensions and Facts for sync process
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

        $this->info("[".now()->format('Y-m-d H:i:s')."] Sync Process Completed.");
    }
}

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

        $this->info("Starting Data Warehouse Sync...");

        // Starting with Training Providers only
        // TODO: Add other Dimensions and Facts for sync process
        if ($table === 'all' || $table === 'providers') {
            $this->comment("Syncing Training Providers...");

            try {
                $result = $syncService->syncTrainingProviders($this);
                $this->info($result);
            } catch (\Exception $e) {
                $this->error("Failed to sync Providers: " . $e->getMessage());
            }
        }

        if ($table === 'all' || $table === 'riders') {
            $this->comment("Syncing Riders...");

            try {
                $result = $syncService->syncRiders($this);
                $this->info($result);
            } catch (\Exception $e) {
                $this->error("Failed to sync Riders: " . $e->getMessage());
            }
        }

        if ($table === 'all' || $table === 'schools') {
            $this->comment("Syncing Schools...");

            try {
                $result = $syncService->syncSchools($this);
                $this->info($result);
            } catch (\Exception $e) {
                $this->error("Failed to sync Schools: " . $e->getMessage());
            }
        }

        if ($table === 'all' || $table === 'organisations') {
            $this->comment("Syncing Organisations...");

            try {
                $result = $syncService->syncOrganisations($this);
                $this->info($result);
            } catch (\Exception $e) {
                $this->error("Failed to sync Organisations: " . $e->getMessage());
            }
        }

        if ($table === 'all' || $table === 'grant_recipients') {
            $this->comment("Syncing Grant Recipients...");

            try {
                $result = $syncService->syncGrantRecipients($this);
                $this->info($result);
            } catch (\Exception $e) {
                $this->error("Failed to sync Grant Recipients: " . $e->getMessage());
            }
        }

        if ($table === 'all' || $table === 'grants') {
            $this->comment("Syncing grants...");

            try {
                $result = $syncService->syncGrants($this);
                $this->info($result);
            } catch (\Exception $e) {
                $this->error("Failed to sync grants: " . $e->getMessage());
            }
        }

        if ($table === 'all' || $table === 'deliveries') {
            $this->comment("Syncing deliveries...");

            try {
                $result = $syncService->syncDeliveryHeaders($this);
                $this->info($result);
            } catch (\Exception $e) {
                $this->error("Failed to sync delivery headers: " . $e->getMessage());
            }
        }

        if ($table === 'all' || $table === 'courses') {
            $this->comment("Syncing courses...");

            try {
                $result = $syncService->syncCourseDimensions($this);
                $this->info($result);
            } catch (\Exception $e) {
                $this->error("Failed to sync courses: " . $e->getMessage());
            }
        }

        $this->info("Sync Process Completed.");
    }
}

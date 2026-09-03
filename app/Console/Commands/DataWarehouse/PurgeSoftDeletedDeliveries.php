<?php

namespace App\Console\Commands\DataWarehouse;

use App\Services\DataWarehouseSyncService;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;

class PurgeSoftDeletedDeliveries extends Command
{
    protected $signature = 'dwh:purge-soft-deletes
                            {--chunk=500 : Number of source records to evaluate per chunk}
                            {--dry-run : Preview matching ghost records without deleting}';

    protected $description = 'Scans the source system for soft-deleted deliveries and cascades their removal across the DWH';

    public function handle(DataWarehouseSyncService $syncService): int
    {
        $chunkSize = (int) $this->option('chunk');
        $dryRun    = (bool) $this->option('dry-run');

        $result = $syncService->purgeSoftDeletedDeliveries($this, $chunkSize, $dryRun);

        $this->newLine();
        $this->info($result);

        return Command::SUCCESS;
    }
}

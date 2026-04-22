<?php

use Illuminate\Foundation\Inspiring;
use Illuminate\Support\Facades\Artisan;
use Illuminate\Support\Facades\Schedule;
use Illuminate\Support\Facades\File;

// Run the DWH Sync daily at 2:00 AM
Schedule::command('dwh:sync')
    ->dailyAt('02:00')
    ->onOneServer()
    ->runInBackground()
    ->appendOutputTo(storage_path('logs/dwh_sync_' . date('Y-m-d') . '.log'));

// Auto-manage logs keeping the last 7 days
Schedule::call(function () {
    $logPath = storage_path('logs');
    $files = File::glob($logPath . '/dwh_sync_*.log');

    foreach ($files as $file) {
        // If file was modified more than 7 days ago, delete it
        if (time() - filemtime($file) > (7 * 24 * 60 * 60)) {
            File::delete($file);
        }
    }
})->dailyAt('03:00');

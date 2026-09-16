<?php

namespace App\Console\Commands\DataWarehouse;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;

class BackFillConsentTimestamps extends Command
{
    protected $signature = 'dwh:backfill-dim-consent-timestamps';
    protected $description = 'Backfills Source_Created_At, Source_Updated_At, Source_Deleted_At, and Deleted_Reason into Dim_Consent in-place';

    public function handle(): int
    {
        $this->info('Starting in-place backfill for Dim_Consent timestamps...');

        $sourceConn = DB::connection('mysql_src');
        $dwhConn    = DB::connection('mysql');

        $totalRecords = $dwhConn->table('Dim_Consent')->count();

        if ($totalRecords === 0) {
            $this->warn('No Dim_Consent records found.');
            return Command::SUCCESS;
        }

        $this->info("Found {$totalRecords} consent records in DWH.");
        $bar = $this->output->createProgressBar($totalRecords);
        $bar->start();

        $updatedCount = 0;

        $dwhConn->table('Dim_Consent')
            ->select('Consent_Key', 'Source_Consent_Id')
            ->orderBy('Consent_Key')
            ->chunk(2000, function ($dwhConsents) use ($sourceConn, $dwhConn, $bar, &$updatedCount) {
                $sourceIds = $dwhConsents->pluck('Source_Consent_Id')->toArray();

                $sourceRecords = $sourceConn->table('consents')
                    ->whereIn('id', $sourceIds)
                    ->select(['id', 'created_at', 'updated_at', 'deleted_at', 'deleted_reason'])
                    ->get()
                    ->keyBy('id');

                foreach ($dwhConsents as $dwhConsent) {
                    $source = $sourceRecords->get($dwhConsent->Source_Consent_Id);

                    if (!$source) {
                        $bar->advance();
                        continue;
                    }

                    $dwhConn->table('Dim_Consent')
                        ->where('Consent_Key', $dwhConsent->Consent_Key)
                        ->update([
                            'Deleted_Reason'    => $source->deleted_reason ?? null,
                            'Source_Created_At' => $source->created_at ?? null,
                            'Source_Updated_At' => $source->updated_at ?? null,
                            'Source_Deleted_At' => $source->deleted_at ?? null,
                            'updated_at'        => now(),
                        ]);

                    $updatedCount++;
                    $bar->advance();
                }
            });

        $bar->finish();
        $this->newLine(2);
        $this->info("Successfully backfilled {$updatedCount} Dim_Consent timestamp rows.");

        return Command::SUCCESS;
    }
}

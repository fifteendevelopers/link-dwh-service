<?php

namespace App\Console\Commands\DataWarehouse\Support;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Throwable;

class BackfillMissingConsents extends Command
{
    protected $signature = 'dwh:backfill-missing-consents {--ids= : Optional comma-separated consent IDs}';

    protected $description = 'Backfills missing consents (including soft-deleted) into Dim_Consent (assumes parent deliveries exist)';

    public function handle(): int
    {
        $dwh = DB::connection('mysql');
        $source = DB::connection('mysql_src');

        $this->info('[' . now()->format('Y-m-d H:i:s') . '] Scanning for Missing Consents...');

        $sourceSystemKey = (int) ($dwh->table('Dim_Source_System')
            ->where('System_Name', 'link')
            ->value('Source_System_Key') ?? 1);

        $targetIds = [];
        if ($idsOption = $this->option('ids')) {
            $targetIds = array_filter(array_map('intval', explode(',', $idsOption)));
            $this->comment('Targeting ' . count($targetIds) . ' explicitly specified consent ID(s).');
        } else {
            $dwhConsentIds = $dwh->table('Dim_Consent')
                ->where('Source_System_Key', $sourceSystemKey)
                ->whereNotNull('Source_Consent_Id')
                ->pluck('Source_Consent_Id')
                ->flip();

            // All consents from source (including deleted_at)
            $sourceConsentIds = $source->table('consents')->pluck('id');

            foreach ($sourceConsentIds as $sId) {
                if (!isset($dwhConsentIds[$sId])) {
                    $targetIds[] = (int) $sId;
                }
            }
        }

        $totalConsents = count($targetIds);
        if ($totalConsents === 0) {
            $this->info('No missing consents detected. Dim_Consent is at 100% parity.');
            return self::SUCCESS;
        }

        $this->info("Found {$totalConsents} missing consents to backfill.");

        // Check for missing riders needed by these consents
        // 2. Pre-check for any missing riders only
        $neededRiderIds = $source->table('consents')
            ->whereIn('id', $targetIds)
            ->whereNotNull('rider_id')
            ->pluck('rider_id')
            ->unique()
            ->values()
            ->toArray();

        $existingRiderIds = $dwh->table('Dim_Rider')
            ->where('Source_System_Key', $sourceSystemKey)
            ->whereIn('Source_Rider_Id', $neededRiderIds)
            ->pluck('Source_Rider_Id')
            ->flip();

        $missingRiderIds = [];
        foreach ($neededRiderIds as $rId) {
            if (!isset($existingRiderIds[$rId])) {
                $missingRiderIds[] = $rId;
            }
        }

        if (!empty($missingRiderIds)) {
            $this->comment('Syncing ' . count($missingRiderIds) . ' riders into Dim_Rider...');

            $source->table('riders')
                ->whereIn('id', $missingRiderIds)
                ->orderBy('id', 'asc')
                ->chunk(250, function ($riders) use ($dwh, $sourceSystemKey) {
                    foreach ($riders as $rider) {
                        $schoolKey = !empty($rider->school_urn)
                            ? $dwh->table('Dim_School')
                                ->where('School_Urn', $rider->school_urn)
                                ->where('Source_System_Key', $sourceSystemKey)
                                ->value('School_Key')
                            : null;

                        $dwh->table('Dim_Rider')->updateOrInsert(
                            [
                                'Source_Rider_Id'   => $rider->id,
                                'Source_System_Key' => $sourceSystemKey,
                            ],
                            [
                                'School_Key'    => $schoolKey,
                                'Ethnicity'     => $rider->ethnicity ?? null,
                                'Gender'        => $rider->gender ?? null,
                                'Pupil_Premium' => (int) ($rider->is_fsm ?? $rider->pupil_premium ?? 0),
                                'Has_Send'      => (int) ($rider->is_send ?? $rider->has_send ?? 0),
                            ]
                        );
                    }
                });

            $this->info('Riders synced successfully.');
        }

        // Process consents
        $bar = $this->output->createProgressBar($totalConsents);
        $bar->start();

        $processedCount = 0;
        $skippedCount = 0;
        $errorCount = 0;

        try {
            $source->table('consents')
                ->whereIn('id', $targetIds)
                ->select([
                    'id', 'rider_id', 'delivery_id', 'consent_status',
                    'pref_join_bikeability_club', 'pref_further_research', 'pref_receive_news',
                    'has_bike', 'cycle_ability', 'is_fsm', 'is_SEND', 'send_details',
                    'has_medical_condition', 'medical_details', 'attended', 'year_group',
                    'gender', 'ethnicity', 'created_at', 'updated_at', 'deleted_at', 'deleted_reason',
                ])
                ->orderBy('id', 'asc')
                ->chunk(250, function ($consents) use ($dwh, $source, $sourceSystemKey, $bar, &$processedCount, &$skippedCount, &$errorCount) {
                    $consentIds = $consents->pluck('id')->toArray();
                    $frequencies = $source->table('cycle_frequency')
                        ->whereIn('consent_id', $consentIds)
                        ->get()
                        ->groupBy('consent_id');

                    foreach ($consents as $consent) {
                        try {
                            $riderKey = $dwh->table('Dim_Rider')
                                ->where('Source_Rider_Id', $consent->rider_id)
                                ->where('Source_System_Key', $sourceSystemKey)
                                ->value('Rider_Key');

                            $deliveryKey = $dwh->table('Dim_Delivery_Header')
                                ->where('Source_Delivery_Id', $consent->delivery_id)
                                ->where('Source_System_Key', $sourceSystemKey)
                                ->value('Delivery_Key');

                            if (!$riderKey || !$deliveryKey) {
                                $skippedCount++;
                                continue;
                            }

                            $abilities = is_array($consent->cycle_ability)
                                ? $consent->cycle_ability
                                : (json_decode($consent->cycle_ability, true) ?? []);

                            $consentFreqs = $frequencies->get($consent->id, collect());
                            $freqData = [
                                'Pre_Freq_To_School' => $consentFreqs->firstWhere('label_lookup_id', 1)->grading_lookup_id ?? null,
                                'Pre_Freq_Leisure'   => $consentFreqs->firstWhere('label_lookup_id', 2)->grading_lookup_id ?? null,
                                'Pre_Freq_Exercise'  => $consentFreqs->firstWhere('label_lookup_id', 3)->grading_lookup_id ?? null,
                                'Pre_Freq_Other'     => $consentFreqs->firstWhere('label_lookup_id', 4)->grading_lookup_id ?? null,
                            ];

                            $dwh->table('Dim_Consent')->updateOrInsert(
                                [
                                    'Source_Consent_Id' => $consent->id,
                                    'Source_System_Key' => $sourceSystemKey,
                                ],
                                array_merge($freqData, [
                                    'Rider_Key'                      => $riderKey,
                                    'Delivery_Key'                   => $deliveryKey,
                                    'Consent_Status'                 => $consent->consent_status,
                                    'Pref_Join_Bikeclub'             => $consent->pref_join_bikeability_club ?: 0,
                                    'Pref_Further_Research'          => $consent->pref_further_research ?: 0,
                                    'Pref_Receive_News'              => $consent->pref_receive_news ?: 0,
                                    'Ability_Cannot_Cycle'           => in_array('1', $abilities) ? 1 : 0,
                                    'Ability_Can_Look_Over_Shoulder' => in_array('2', $abilities) ? 1 : 0,
                                    'Ability_Can_One_Hand_Signal'    => in_array('3', $abilities) ? 1 : 0,
                                    'Ability_Has_Level_2'            => in_array('4', $abilities) ? 1 : 0,
                                    'Cycle_Ability_Raw'              => json_encode($abilities),
                                    'Is_Pupil_premium'               => $consent->is_fsm ?: 0,
                                    'Is_SEND'                        => $consent->is_SEND ?: 0,
                                    'SEND_Details'                   => $consent->send_details,
                                    'Has_Medical_Condition'          => $consent->has_medical_condition ?: 0,
                                    'Medical_Details'                => $consent->medical_details,
                                    'Attended'                       => $consent->attended,
                                    'Year_Group'                     => $consent->year_group,
                                    'Gender'                         => $consent->gender,
                                    'Ethnicity'                      => $consent->ethnicity,
                                    'Deleted_Reason'                 => $consent->deleted_reason ?? null,
                                    'Source_Created_At'              => $consent->created_at ?? null,
                                    'Source_Updated_At'              => $consent->updated_at ?? null,
                                    'Source_Deleted_At'              => $consent->deleted_at ?? null,
                                    'updated_at'                     => now(),
                                ])
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
            $this->info("Consents backfill complete: {$processedCount} inserted/updated, {$skippedCount} skipped (unmapped parent), {$errorCount} errors.");

            return self::SUCCESS;
        } catch (Throwable $fatal) {
            $bar->finish();
            $this->newLine();
            $this->error('Fatal error during consents backfill: ' . $fatal->getMessage());
            return self::FAILURE;
        }
    }
}

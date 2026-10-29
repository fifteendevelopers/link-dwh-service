<?php

namespace App\Reports\Handlers;

use App\Reports\Contracts\ReportHandlerInterface;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Validator;

class InstructorsHandler extends AbstractStreamingReportHandler implements ReportHandlerInterface
{
    /**
     * Master schema for the qualification slots in priority order.
     */
    protected const QUALIFICATION_SLOTS = [
        'L2AICTR'    => '1st4sport L2 Award Instructing Cycle Training',
        '23QICTAWA2' => '1st4sport L2 Award Instructing Cycle Training (23QICTAWA2)',
        '24QCTACT3'  => '1st4sport L3 Cert Tutors and Assessors (24QCTACT3)',
    ];

    public function validate(array $parameters): array
    {
        return Validator::make($parameters, [
            'instructor_type'          => 'nullable|string',
            'instructor_report_length' => 'nullable|string|in:short,long',
        ])->validate();
    }

    public function execute(array $params): array
    {
        ini_set('memory_limit', '512M');
        ini_set('max_execution_time', '600');

        $query = $this->buildQuery($params);

        if (empty($this->callbackUrl)) {
            return $this->formatRows($query->get(), $params['instructor_report_length'] ?? 'short');
        }

        $chunkSize = 250;
        $reportLength = $params['instructor_report_length'] ?? 'short';

        $query->chunk($chunkSize, function ($instructors) use ($reportLength) {
            $formattedRows = $this->formatRows($instructors, $reportLength);
            $this->transmitBatch($formattedRows, false);
        });

        // Transmit completion indicator
        $this->transmitBatch([], true);

        return ['status' => 'async_completed'];
    }

    protected function buildQuery(array $params)
    {
        $dwh = DB::connection('mysql');
        $sourceSystemKey = (int) ($dwh->table('Dim_Source_System')
            ->where('System_Name', 'link')
            ->value('Source_System_Key') ?? 1);

        $query = $dwh->table('Dim_Instructor')
            ->where('Source_System_Key', $sourceSystemKey);

        if (!empty($params['instructor_type'])) {
            $query->where('Instructor_Type', $params['instructor_type']);
        }

        return $query->orderBy('Instructor_Key', 'asc');
    }

    protected function formatRows($instructors, string $reportLength): array
    {
        if ($instructors->isEmpty()) {
            return [];
        }

        $dwh = DB::connection('mysql');
        $instructorKeys = $instructors->pluck('Instructor_Key')->toArray();

        // 1. Bulk-load Qualifications for the chunk (10 columns per slot)
        $rawQuals = $dwh->table('Fact_Instructor_Qualification as fiq')
            ->leftJoin('Dim_Qualification as dq', 'fiq.Qualification_Key', '=', 'dq.Qualification_Key')
            ->leftJoin('Dim_Training_Organisation as dto', 'fiq.Training_Organisation_Key', '=', 'dto.Training_Organisation_Key')
            ->whereIn('fiq.Instructor_Key', $instructorKeys)
            ->select([
                'fiq.Instructor_Key',
                'dq.Qualification_Code',
                'dq.Qualification_Name',
                'fiq.Source_Qualification_Code',
                'fiq.Status',
                'fiq.Registration_Date',
                'fiq.Verified_Lxp_Date',
                'fiq.Theory_Complete_Date',
                'fiq.Pca_Complete_Date',
                'fiq.Withdrawn_Date',
                'fiq.Expiry_Date',
                'fiq.Date_Provisional_Complete',
                'fiq.Date_Certified',
                'dto.Organisation_Name',
                'dto.Organisation_Type',
            ])
            ->get()
            ->groupBy('Instructor_Key');

        // 2. Bulk-load Training Providers (matches Fact_Instructor_Training_Provider)
        $rawProviders = $dwh->table('Fact_Instructor_Training_Provider as fitp')
            ->join('Dim_Training_Provider as dtp', 'fitp.Provider_Key', '=', 'dtp.Provider_Key')
            ->whereIn('fitp.Instructor_Key', $instructorKeys)
            ->whereNull('fitp.Source_Deleted_At')
            ->select(['fitp.Instructor_Key', 'dtp.Provider_Name'])
            ->get()
            ->groupBy('Instructor_Key');

        $rows = [];

        foreach ($instructors as $instructor) {
            $key = $instructor->Instructor_Key;

            // Normalize qualifications into fixed slots
            $qualsByCode = [];
            foreach ($rawQuals->get($key, collect()) as $qual) {
                $code = strtoupper(trim($qual->Qualification_Code ?? $qual->Source_Qualification_Code ?? $qual->Qualification_Name ?? ''));

                if (array_key_exists($code, self::QUALIFICATION_SLOTS)) {
                    $qualsByCode[$code] = $qual;
                } else {
                    foreach (self::QUALIFICATION_SLOTS as $slotCode => $slotName) {
                        if (strcasecmp($qual->Qualification_Name ?? '', $slotName) === 0) {
                            $qualsByCode[$slotCode] = $qual;
                            break;
                        }
                    }
                }
            }

            // Build fixed 10-column qualification slots
            $qualificationsData = [];
            foreach (array_keys(self::QUALIFICATION_SLOTS) as $code) {
                if (isset($qualsByCode[$code])) {
                    $q = $qualsByCode[$code];
                    $org = !empty($q->Organisation_Name)
                        ? "{$q->Organisation_Name}" . (!empty($q->Organisation_Type) ? " ({$q->Organisation_Type})" : '')
                        : '';

                    $qualificationsData = array_merge($qualificationsData, [
                        $q->Status ?? '',
                        !empty($q->Registration_Date) ? $q->Registration_Date : 'N/A',
                        !empty($q->Verified_Lxp_Date) ? $q->Verified_Lxp_Date : 'N/A',
                        !empty($q->Theory_Complete_Date) ? $q->Theory_Complete_Date : 'N/A',
                        !empty($q->Pca_Complete_Date) ? $q->Pca_Complete_Date : 'N/A',
                        !empty($q->Withdrawn_Date) ? $q->Withdrawn_Date : 'N/A',
                        !empty($q->Expiry_Date) ? $q->Expiry_Date : 'N/A',
                        !empty($q->Date_Provisional_Complete) ? $q->Date_Provisional_Complete : 'N/A',
                        !empty($q->Date_Certified) ? $q->Date_Certified : 'N/A',
                        $org,
                    ]);
                } else {
                    // Pad with 10 empty columns if qualification is not held
                    $qualificationsData = array_merge($qualificationsData, array_fill(0, 10, ''));
                }
            }

            // Pad Training Providers to 6
            $providerList = $rawProviders->get($key, collect())->pluck('Provider_Name')->toArray();
            $providerList = array_pad($providerList, 6, '');

            // Core attributes
            if ($reportLength === 'long') {
                $baseData = [
                    $instructor->Instructor_Number ?? '',
                    $instructor->Instructor_Type ?? '',
                    ($instructor->Status_Raw ?? ($instructor->Status ?? ''))?'Active':'Deregistered',
                    !empty($instructor->Date_Renewal) ? $instructor->Date_Renewal : 'N/A',
                    !empty($instructor->Date_Deregistered) ? $instructor->Date_Deregistered : null,
                    $instructor->Deregistration_Reason ?? '',
                    $instructor->Title ?? '',
                    $instructor->First_Name ?? '',
                    $instructor->Last_Name ?? '',
                    $instructor->Gender ?? '',
                    $instructor->Ethnicity ?? '',
                    $instructor->Age_Range ?? '',
                    $instructor->Email ?? '',
                    ($instructor->Telephone ?? '') . ' ',
                    $instructor->Address_01 ?? ($instructor->Address_Line_1 ?? ''),
                    $instructor->Address_02 ?? ($instructor->Address_Line_2 ?? ''),
                    $instructor->City ?? '',
                    $instructor->Postcode ?? '',
                    $providerList[0], $providerList[1], $providerList[2],
                    $providerList[3], $providerList[4], $providerList[5],
                    !empty($instructor->Pref_Delivering_Bikeability) ? '✓' : '',
                    !empty($instructor->Pref_Delivering_Other) ? '✓' : '',
                    !empty($instructor->Has_Received_Bursary) ? '✓' : '',
                    $instructor->Date_Registered ?? '',
                    $instructor->First_Aid_Training_Complete_Date ?? '',
                    $instructor->Safeguarding_Training_Complete_Date ?? '',
                    $instructor->Send_Training_Complete_Date ?? '',
                ];
            } else {
                $baseData = [
                    $instructor->Instructor_Number ?? '',
                    $instructor->Instructor_Type ?? '',
                    $instructor->Status_Raw ?? ($instructor->Status ?? ''),
                    !empty($instructor->Date_Renewal) ? $instructor->Date_Renewal : 'N/A',
                    $providerList[0], $providerList[1], $providerList[2],
                    $providerList[3], $providerList[4], $providerList[5],
                    $instructor->First_Aid_Training_Complete_Date ?? '',
                    $instructor->Safeguarding_Training_Complete_Date ?? '',
                    $instructor->Send_Training_Complete_Date ?? '',
                ];
            }

            $timestamps = [
                $instructor->Source_Created_At ?? ($instructor->created_at ?? ''),
                $instructor->Source_Updated_At ?? ($instructor->updated_at ?? ''),
            ];

            $rows[] = array_merge($baseData, $qualificationsData, $timestamps);
        }

        return $rows;
    }
}

<?php

namespace App\Reports\Handlers;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Validator;

class TrainingProviderRenewalsHandler extends AbstractStreamingReportHandler
{
    public function validate(array $parameters): array
    {
        return Validator::make($parameters, [
            'status'         => 'nullable|string',
            'financial_year' => 'nullable|integer',
            'year'           => 'nullable|integer',
        ])->validate();
    }

    public function execute(array $params): array
    {
        ini_set('memory_limit', '1024M');
        ini_set('max_execution_time', '300');

        $query = $this->buildQuery($params);

        if (empty($this->callbackUrl)) {
            return $query->get()->map(fn($row) => (array)$row)->toArray();
        }

        $chunkSize = 1000;

        $query->chunk($chunkSize, function ($rows) {
            $memBuffer = fopen('php://temp', 'r+');

            foreach ($rows as $row) {
                fputcsv($memBuffer, (array)$row);
            }

            rewind($memBuffer);
            $csvStringChunk = stream_get_contents($memBuffer);
            fclose($memBuffer);

            $this->transmitBatch([$csvStringChunk], false);
        });

        $this->transmitBatch([], true);

        return ['status' => 'async_completed'];
    }

    protected function buildQuery(array $params)
    {
        $query = DB::connection('mysql')->table('Fact_Training_Provider_Renewal as r')
            ->join('Dim_Training_Provider as tp', 'r.Provider_Key', '=', 'tp.Provider_Key')
            ->select([
                'r.Source_Renewal_Id',
                'r.Status',
                'tp.Status as Provider_Status',
                'r.Renewal_Year',
                'tp.Source_Provider_Id',
                'tp.Provider_Number',
                'tp.Provider_Name',
                'tp.Primary_Email',
                'tp.Secondary_Email',
                'tp.Telephone',
                DB::raw("DATE_FORMAT(tp.Date_Insurance_Expiry, '%d/%m/%Y') as Date_Insurance_Expiry"),
                DB::raw("DATE_FORMAT(r.Date_Due, '%d/%m/%Y') as Date_Due"),
                DB::raw("DATE_FORMAT(r.Date_Completed, '%d/%m/%Y') as Date_Completed"),
                DB::raw("DATE_FORMAT(r.Date_Approved, '%d/%m/%Y') as Date_Approved"),
                DB::raw("IF(r.Flag_Overdue_Warning = 1, '1', '0') as Flag_Overdue_Warning")
            ]);

        // Financial Year Filter
        $fy = $params['financial_year'] ?? $params['year'] ?? null;
        if (!empty($fy)) {
            $query->where('r.Renewal_Year', (int)$fy);
        }

        // Status Logic: 4 = Overdue Warning, 5 = Expired Insurance
        if (isset($params['status']) && $params['status'] !== '' && $params['status'] !== null) {
            $status = (string)$params['status'];

            if ($status === '4') {
                $query->where('r.Flag_Overdue_Warning', 1)
                    ->where('r.Status', 0)
                    ->whereNull('r.Date_Approved');
            } elseif ($status === '5') {
                $query->whereNotNull('tp.Date_Insurance_Expiry')
                    ->where('tp.Date_Insurance_Expiry', '<', DB::raw('CURDATE()'));
            } else {
                $query->where('r.Status', $status);
            }
        }

        return $query->orderBy('r.Status')
            ->orderBy('tp.Provider_Name');
    }
}

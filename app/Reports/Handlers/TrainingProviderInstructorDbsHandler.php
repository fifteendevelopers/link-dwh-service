<?php

namespace App\Reports\Handlers;

use Carbon\Carbon;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Validator;

class TrainingProviderInstructorDbsHandler extends AbstractStreamingReportHandler
{
    public function validate(array $parameters): array
    {
        return Validator::make($parameters, [
            'status' => 'nullable|string',
            'month'  => 'nullable|string',
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
        $db = DB::connection('mysql');

        $query = $db->table('Fact_Instructor_Training_Provider as fitp')
            ->join('Dim_Training_Provider as tp', 'fitp.Provider_Key', '=', 'tp.Provider_Key')
            ->join('Dim_Instructor as i', 'fitp.Instructor_Key', '=', 'i.Instructor_Key')
            ->whereNull('fitp.Source_Deleted_At')
            ->whereNull('i.Date_Deregistered')
            ->where('tp.Is_Current', 1)
            ->whereNull('tp.Source_Deleted_At')
            ->select([
                'tp.Source_Provider_Id as id',
                'tp.Provider_Number',
                'tp.Provider_Name',
                'tp.Primary_Email',
                'tp.Telephone',
                'tp.Status',
                'i.Source_Instructor_Id as instructor_id',
                'i.Instructor_Number',
                'i.First_Name',
                'i.Last_Name',
                'fitp.Dbs_Certification_Date',
            ]);

        // Status filter: Overdue (<= 3 years ago) or Missing (null)
        $status = $params['status'] ?? null;
        if (!empty($status) && $status !== 'All') {
            if ($status === 'Overdue') {
                $overdueDate = \Carbon\Carbon::today()->subYears(3)->toDateString();
                $query->whereDate('fitp.Dbs_Certification_Date', '<=', $overdueDate);
            } elseif ($status === 'Missing') {
                $query->whereNull('fitp.Dbs_Certification_Date');
            }
        }

        // Month filter: 1 - 12
        $month = $params['month'] ?? null;
        if (!empty($month) && $month !== 'All') {
            $query->whereMonth('fitp.Dbs_Certification_Date', '=', (int)$month);
        }

        return $query->orderBy('tp.Provider_Name')->orderBy('i.Last_Name');
    }
}

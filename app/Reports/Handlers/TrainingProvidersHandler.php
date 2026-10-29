<?php


namespace App\Reports\Handlers;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Validator;

class TrainingProvidersHandler extends AbstractStreamingReportHandler
{
    public function validate(array $parameters): array
    {
        return Validator::make($parameters, [
            'type' => 'nullable|string',
            'status' => 'nullable|string',
            'provider_id' => 'nullable|integer',
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
        $query = DB::connection('mysql')->table('Dim_Training_Provider as tp')
            ->select([
                'tp.Source_Provider_Id',
                'tp.Provider_Number',
                'tp.Provider_Name',
                'tp.Address_Line_1',
                'tp.Address_Line_2',
                'tp.City',
                'tp.Postcode',
                'tp.Website',
                'tp.Telephone',
                'tp.Primary_Email',
                'tp.Secondary_Email',
                'tp.Public_Email',
                'tp.Public_Telephone',
                'tp.Provider_Type',
                'tp.Status',
                DB::raw("DATE_FORMAT(tp.Date_Inception, '%d/%m/%Y') as Date_Inception"),
                DB::raw("DATE_FORMAT(tp.Date_Renewal, '%d/%m/%Y') as Date_Renewal"),
                DB::raw("DATE_FORMAT(tp.Date_Deregistered, '%d/%m/%Y') as Date_Deregistered"),
                DB::raw("DATE_FORMAT(tp.Date_Deregistered, '%d/%m/%Y') as Date_Deregistered"),
                DB::raw("IFNULL(tp.Deregistration_Reason, '') as Deregistration_Reason"),
                DB::raw("DATE_FORMAT(tp.Date_Eqa_Visit, '%d/%m/%Y') as Date_EQA_Visit"),
                DB::raw("IF(tp.Pref_Level_1 = 1, '1', '0') as Pref_Level_1"),
                DB::raw("IF(tp.Pref_Level_2 = 1, '1', '0') as Pref_Level_2"),
                DB::raw("IF(tp.Pref_Level_3 = 1, '1', '0') as Pref_Level_3"),
                DB::raw("IF(tp.Pref_Plus_Balance = 1, '1', '0') as Pref_Plus_Balance"),
                DB::raw("IF(tp.Pref_Plus_Bus = 1, '1', '0') as Pref_Plus_Bus"),
                DB::raw("IF(tp.Pref_Plus_Fix = 1, '1', '0') as Pref_Plus_Fix"),
                DB::raw("IF(tp.Pref_Plus_Learn = 1, '1', '0') as Pref_Plus_Learn"),
                DB::raw("IF(tp.Pref_Plus_On_Show = 1, '1', '0') as Pref_Plus_On_Show"),
                DB::raw("IF(tp.Pref_Plus_Parents = 1, '1', '0') as Pref_Plus_Parents"),
                DB::raw("IF(tp.Pref_Plus_Promotion = 1, '1', '0') as Pref_Plus_Promotion"),
                DB::raw("IF(tp.Pref_Plus_Recycled = 1, '1', '0') as Pref_Plus_Recycled"),
                DB::raw("IF(tp.Pref_Plus_Ride = 1, '1', '0') as Pref_Plus_Ride"),
                DB::raw("IF(tp.Pref_Plus_Transition = 1, '1', '0') as Pref_Plus_Transition"),
                DB::raw("DATE_FORMAT(tp.Source_Created_At, '%d/%m/%Y @ %H:%i') as Created_At"),
                DB::raw("DATE_FORMAT(tp.Source_Updated_At, '%d/%m/%Y @ %H:%i') as Updated_At"),
            ])
            ->where('tp.Is_Current','=',1)
            ->whereNull('tp.Source_Deleted_At');

        if (!empty($params['type'])) {
            $query->where('tp.Provider_Type', $params['type']);
        }

        if (!empty($params['status'])) {
            $query->where('tp.Provider_Status', $params['status']);
        }

        if (!empty($params['provider_id'])) {
            $query->where('tp.Source_Provider_Id', $params['provider_id']);
        }

        return $query->orderBy('tp.Provider_Name');
    }
}

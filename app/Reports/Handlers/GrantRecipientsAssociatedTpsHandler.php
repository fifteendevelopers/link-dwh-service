<?php

namespace App\Reports\Handlers;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Validator;

class GrantRecipientsAssociatedTpsHandler extends AbstractStreamingReportHandler
{
    public function validate(array $parameters): array
    {
        return Validator::make($parameters, [])->validate();
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

        return $db->table('Dim_Grant_Recipient as gr')
            ->leftJoin('Fact_Grant_Recipient_Training_Provider as j', 'gr.Recipient_Key', '=', 'j.Grant_Recipient_Key')
            ->leftJoin('Dim_Training_Provider as tp', function ($join) {
                $join->on('j.Provider_Key', '=', 'tp.Provider_Key')
                    ->where('tp.Is_Current', 1)
                    ->whereNull('tp.Source_Deleted_At');
            })
            ->select([
                'gr.Recipient_Number',
                'gr.Recipient_Name',
                'tp.Provider_Number',
                'tp.Provider_Name',
                'tp.Status as provider_status',
            ])
            ->orderBy('gr.Recipient_Name')
            ->orderBy('tp.Provider_Name');
    }
}

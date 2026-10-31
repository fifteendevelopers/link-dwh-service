<?php

namespace App\Reports\Handlers;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Validator;

class GrantRecipientsReportHandler extends AbstractStreamingReportHandler
{
    public function validate(array $parameters): array
    {
        return Validator::make($parameters, [
            'status' => 'nullable|string',
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

        $query = $db->table('Dim_Grant_Recipient as gr')
            ->select([
                'gr.Source_Recipient_Id as id',
                'gr.Recipient_Number as recipient_number',
                'gr.Recipient_Name as recipient_name',
                'gr.Address_Line_1 as address_01',
                'gr.Address_Line_2 as address_02',
                'gr.City as city',
                'gr.Postcode as postcode',
                'gr.Website as website',
                'gr.Primary_Email as primary_email',
                'gr.Secondary_Email as secondary_email',
                'gr.Status as status',
                'gr.Inception_Date as date_inception',
                'gr.Renewal_Date as date_renewal',
                'gr.Deregistered_Date as date_deregistered',
                'gr.Deregistration_Reason as deregistration_reason',
                'gr.Is_SGO as pref_sgoh',
                'gr.Source_Created_At as created_at',
                'gr.Source_Updated_At as updated_at',
            ])
            ->where('Is_Current','=', 1)
            ->whereNull('gr.Source_Deleted_At')
            ->orderBy('gr.Recipient_Name')
            ->orderBy('gr.Source_Recipient_Id');

        if (!empty($params['status'])) {
            $query->where('gr.Status', $params['status']);
        }

        return $query;
    }
}

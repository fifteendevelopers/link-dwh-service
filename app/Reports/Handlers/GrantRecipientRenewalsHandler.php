<?php

namespace App\Reports\Handlers;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Validator;
use Carbon\Carbon;

class GrantRecipientRenewalsHandler extends AbstractStreamingReportHandler
{
    public function validate(array $parameters): array
    {
        return Validator::make($parameters, [
            'financial_year' => 'nullable|integer',
            'status' => 'nullable',
        ])->validate();
    }

    public function execute(array $params): array
    {
        ini_set('memory_limit', '512M');
        ini_set('max_execution_time', '600');

        $query = $this->buildQuery($params);

        if (empty($this->callbackUrl)) {
            return $query->get()->map(fn($row) => (array)$row)->toArray();
        }

        $query->chunk(1000, function ($rows) {
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

        $query = $db->table("Fact_Grant_Recipient_Renewal as r")
            ->leftJoin('Dim_Grant_Recipient as gr', 'r.Recipient_Key', '=', 'gr.Recipient_Key')
            ->select([
                'r.Source_Renewal_Id as id',
                'r.Status as status',
                'r.Renewal_Year as renewal_year',
                'gr.Source_Recipient_Id as recipient_id',
                DB::raw("IFNULL(gr.Recipient_Number, '') as recipient_number"),
                DB::raw("IFNULL(gr.Recipient_Name, '') as recipient_name"),
                DB::raw("IFNULL(gr.Primary_Email, '') as primary_email"),
                DB::raw("IFNULL(gr.Secondary_Email, '') as secondary_email"),
                DB::raw("IFNULL(gr.Telephone, '') as telephone"),
                'gr.Inception_Date as date_inception',
                'gr.Renewal_Date as date_renewal',
                'r.Date_Due as date_due',
                'r.Date_Completed as date_completed',
                'r.Date_Approved as date_approved',
                DB::raw("IF(r.Flag_Overdue_Warning = 1, 1, 0) as flag_overdue_warning"),
                'r.Source_Created_At as created_at',
                'r.Source_Updated_At as updated_at',
            ])->whereNull('Source_Deleted_At');

        if (isset($params['status']) && $params['status'] !== '' && $params['status'] !== null) {
            if ((int)$params['status'] === 4) {
                $query->where('r.Flag_Overdue_Warning', 1)
                    ->where('r.Status', 0)
                    ->whereNull('r.Date_Approved');
            } else {
                $query->where('r.Status', $params['status']);
            }
        }

        if (!empty($params['financial_year'])) {
            $query->where('r.Renewal_Year', $params['financial_year']);
        }

        return $query->orderBy('r.Status', 'asc')
            ->orderBy('r.Source_Renewal_Id', 'asc');
    }
}

<?php

namespace App\Reports\Handlers;

use App\Reports\Contracts\ReportHandlerInterface;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Validator;

class InstructorRenewalsHandler extends AbstractStreamingReportHandler implements ReportHandlerInterface
{
    /**
     * Standard column headers for the renewals export.
     */
    public const HEADINGS = [
        'ID',
        'Status',
        'Active/Deregistered',
        'Renewal Year',
        'Instructor ID',
        'Instructor Number',
        'Instructor Name',
        'Email',
        'Telephone',
        'Date Due',
        'Date Completed',
        'Date Approved',
        'Overdue',
        'Link',
    ];

    private const STATUSES = [
        '0' => 'In progress',
        '1' => 'Requires approval',
        '2' => 'Approved',
        '3' => 'Failed'
    ];

    public function validate(array $parameters): array
    {
        return Validator::make($parameters, [
            'status' => 'nullable|string',
            'financial_year' => 'nullable|integer',
        ])->validate();
    }

    public function execute(array $params): array
    {
        ini_set('memory_limit', '512M');
        ini_set('max_execution_time', '600');

        $query = $this->buildQuery($params);

        // Synchronous immediate return if no streaming callback URL is provided
        if (empty($this->callbackUrl)) {
            return $this->formatRows($query->get());
        }

        // Asynchronous chunked streaming
        $chunkSize = 500;

        $query->chunk($chunkSize, function ($renewals) {
            $formattedRows = $this->formatRows($renewals);
            $this->transmitBatch($formattedRows, false);
        });

        // Transmit completion indicator
        $this->transmitBatch([], true);

        return ['status' => 'async_completed'];
    }

    protected function buildQuery(array $params)
    {
        $dwh = DB::connection('mysql');

        $sourceSystemKey = (int)($dwh->table('Dim_Source_System')
            ->where('System_Name', 'link')
            ->value('Source_System_Key') ?? 1);

        $query = $dwh->table('Fact_Instructor_Renewal as fir')
            ->leftJoin('Dim_Instructor as di', 'fir.Instructor_Key', '=', 'di.Instructor_Key')
            ->where('fir.Source_System_Key', $sourceSystemKey)
            ->select([
                // Renewal Fact Attributes
                'fir.Source_Renewal_ID as id',
                'fir.Status as renewal_status',
                'fir.Renewal_Year as renewal_year',
                'fir.Date_Due as date_due',
                'fir.Date_Completed as date_completed',
                'fir.Date_Approved as date_approved',
                'fir.Flag_Overdue_Warning as flag_overdue_warning',

                // Linked Instructor Attributes
                'di.Source_Instructor_ID as instructor_id',
                'di.Instructor_Number as instructor_number',
                'di.Status_Raw as instructor_status_raw',
                'di.First_Name',
                'di.Last_Name',
                'di.Email as instructor_email',
                'di.Telephone as instructor_telephone',
            ]);

        // Status Filter (handles the original warning flag vs status condition)
        if (!empty($params['status'])) {
            if ($params['status'] === '4' || $params['status'] === 4) {
                $query->where('fir.Flag_Overdue_Warning', 1);
            } else {
                $query->where('fir.Status', $params['status']);
            }
        }

        // Financial Year Filter
        if (!empty($params['financial_year'])) {
            $query->where('fir.Renewal_Year', $params['financial_year']);
        }

        return $query->orderBy('fir.Status', 'asc')
            ->orderBy('fir.Renewal_Key', 'asc');
    }

    protected function formatRows($renewals): array
    {
        if ($renewals->isEmpty()) {
            return [];
        }

        $appBaseUrl = rtrim(config('services.source_app.url', config('app.url')), '/');
        $rows = [];

        foreach ($renewals as $renewal) {
            // Resolve Overdue business logic
            $rawStatus = (string) ($renewal->renewal_status ?? '');
            $status = self::STATUSES[$rawStatus] ?? $renewal->renewal_status ?? '';

            $isOverdue = !empty($renewal->flag_overdue_warning);

            if ($isOverdue && in_array($status, ['In progress', 'Requires approval'], true)) {
                $status = 'Overdue';
            }

            // Instructor name
            $fullName = trim(($renewal->First_Name ?? '') . ' ' . ($renewal->Last_Name ?? ''));

            // Web link to the source operational app
            $renewalLink = (!empty($renewal->instructor_id) && !empty($renewal->id))
                ? "{$appBaseUrl}/instructors/{$renewal->instructor_id}/renewals/{$renewal->id}"
                : '';

            $rows[] = [
                $renewal->id ?? '',
                $status,
                $renewal->instructor_status_raw?'Active':'Deregistered',
                $renewal->renewal_year ?? '',
                $renewal->instructor_id ?? '',
                $renewal->instructor_number ?? '',
                $fullName,
                $renewal->instructor_email ?? '',
                ($renewal->instructor_telephone ?? '') . ' ',
                !empty($renewal->date_due) ? date('d/m/Y', strtotime($renewal->date_due)) : '',
                !empty($renewal->date_completed) ? date('d/m/Y', strtotime($renewal->date_completed)) : '',
                !empty($renewal->date_approved) ? date('d/m/Y', strtotime($renewal->date_approved)) : '',
                !empty($renewal->flag_overdue_warning) ? '✓' : '',
                $renewalLink,
            ];
        }

        return $rows;
    }
}

<?php

namespace App\Reports\Handlers;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;
use Illuminate\Support\Facades\Validator;

class SchoolDeliveriesAuditHandler extends AbstractStreamingReportHandler
{
    public function validate(array $parameters): array
    {
        return Validator::make($parameters, [
            'recipient_id'    => 'nullable|integer',
            'start_date'      => 'nullable|string',
            'end_date'        => 'nullable|string',
            'deliveries_type' => 'nullable|string', // Fixed: now preserved in validated array
        ])->validate();
    }

    public function execute(array $params): array
    {
        ini_set('memory_limit', '1024M');
        ini_set('max_execution_time', '1200');

        $query = $this->buildQuery($params);

        // If no async callback URL is set, return raw array (synchronous fallback)
        if (empty($this->callbackUrl)) {
            return $query->get()->map(fn($row) => (array)$row)->toArray();
        }

        // Stream chunks back to Link webhook
        $chunkSize = 500;

        $query->chunk($chunkSize, function ($rows) {
            $chunkArray = $rows->map(fn($row) => (array)$row)->toArray();
            $this->transmitBatch($chunkArray, false);
        });

        // Send final batch flag to tell Link the export is complete
        $this->transmitBatch([], true);

        return ['status' => 'async_completed'];
    }

    protected function buildQuery(array $params)
    {
        $startDate = !empty($params['start_date']) ? $params['start_date'] : null;
        $endDate   = !empty($params['end_date']) ? $params['end_date'] : null;

        $query = DB::connection('mysql')->table('Dim_School as s')
            // 1. Left join Fact_Course_Delivery
            ->leftJoin('Fact_Course_Delivery as f', 'f.School_Key', '=', 's.School_Key')

            // 2. Left join Delivery Header with DATE filters moved INTO the JOIN condition
            ->leftJoin('Dim_Delivery_Header as dh', function ($join) use ($startDate, $endDate) {
                $join->on('f.Delivery_Key', '=', 'dh.Delivery_Key');

                if ($startDate && $endDate) {
                    $join->whereBetween('dh.Date_Delivery_Start', [$startDate, $endDate]);
                } elseif ($startDate) {
                    $join->where('dh.Date_Delivery_Start', '>=', $startDate);
                } elseif ($endDate) {
                    $join->where('dh.Date_Delivery_Start', '<=', $endDate);
                }
            })

            // 3. Left join Course ensuring parent course exclusion stays scoped to deliveries
            ->leftJoin('Dim_Course as c', function ($join) {
                $join->on('f.Course_Key', '=', 'c.Course_Key')
                    ->whereNull('c.Parent_Course_Key');
            })

            // 4. Left join Grant & Recipient
            ->leftJoin('Dim_Grant as g', 'f.Grant_Key', '=', 'g.Grant_Key')
            ->leftJoin('Dim_Grant_Recipient as gr', 'g.Grant_Recipient_Key', '=', 'gr.Recipient_Key')

            ->select([
                DB::raw("IFNULL(g.Grant_Number, 'N/A') as Grant_Number"),
                DB::raw("IFNULL(g.Grant_Source, 'N/A') as Grant_Source"),
                DB::raw("IFNULL(gr.Recipient_Name, 'Unlinked') as Recipient_Name"),
                's.School_Urn',
                's.School_Name',
                's.LA_Name',
                's.LA_Code',
                DB::raw("IFNULL(dh.Source_Delivery_Id, '') as Source_Delivery_Id"),
                DB::raw("IFNULL(dh.Delivery_Status, 'No Deliveries Logged') as Delivery_Status"),
                DB::raw("IFNULL(DATE_FORMAT(dh.Date_Delivery_Start, '%d/%m/%Y'), '') as Date_Delivery_Start"),
                DB::raw("IFNULL(f.Riders_Enrolled_Count, 0) as Count_Booked"),
                DB::raw("IFNULL(f.Riders_Completed_Count, 0) as Count_Attended"),
            ]);

        // Recipient filtering: If filtered by recipient, include matching deliveries OR schools with no deliveries
        if (!empty($params['recipient_id'])) {
            $query->where(function ($sub) use ($params) {
                $sub->where('gr.Source_Recipient_Id', $params['recipient_id'])
                    ->orWhereNull('dh.Delivery_Key');
            });
        }

        // Deliveries Type filter: safely operates after the conditional join
        if (!empty($params['deliveries_type'])) {
            if ($params['deliveries_type'] === 'with_deliveries') {
                $query->whereNotNull('dh.Delivery_Key');
            } elseif ($params['deliveries_type'] === 'no_deliveries') {
                $query->whereNull('dh.Delivery_Key');
            }
        }

        return $query->orderBy('s.School_Urn', 'asc')
            ->orderBy('dh.Source_Delivery_Id', 'asc');
    }
}

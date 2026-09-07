<?php

namespace App\Reports\Handlers;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Validator;

class SchoolDeliveriesAuditHandler extends AbstractStreamingReportHandler
{
    public function validate(array $parameters): array
    {
        return Validator::make($parameters, [
            'recipient_id'    => 'nullable|integer',
            'start_date'      => 'nullable|string',
            'end_date'        => 'nullable|string',
            'deliveries_type' => 'nullable|string',
        ])->validate();
    }

    public function execute(array $params): array
    {
        ini_set('memory_limit', '1024M');
        ini_set('max_execution_time', '1200');

        $query = $this->buildQuery($params);

        if (empty($this->callbackUrl)) {
            return $query->get()->map(fn($row) => (array)$row)->toArray();
        }

        $chunkSize = 500;

        $query->chunk($chunkSize, function ($rows) {
            $chunkArray = $rows->map(fn($row) => (array)$row)->toArray();
            $this->transmitBatch($chunkArray, false);
        });

        $this->transmitBatch([], true);

        return ['status' => 'async_completed'];
    }

    protected function buildQuery(array $params)
    {
        $startDate = !empty($params['start_date']) ? $params['start_date'] : null;
        $endDate   = !empty($params['end_date']) ? $params['end_date'] : null;
        $deliveriesType = $params['deliveries_type'] ?? null;
        $recipientId = !empty($params['recipient_id']) ? (int) $params['recipient_id'] : null;

        // -------------------------------------------------------------------------
        // Case A: Strictly Schools with NO Deliveries
        // -------------------------------------------------------------------------
        if ($deliveriesType === 'no_deliveries') {
            $query = DB::connection('mysql')->table('Dim_School as s')
                ->select([
                    DB::raw("'N/A' as Grant_Number"),
                    DB::raw("'N/A' as Grant_Source"),
                    DB::raw("'Unlinked' as Recipient_Name"),
                    's.School_Urn',
                    's.School_Name',
                    's.LA_Name',
                    's.LA_Code',
                    DB::raw("'' as Source_Delivery_Id"),
                    DB::raw("'' as Provider_Name"),
                    DB::raw("'No Deliveries Logged' as Delivery_Status"),
                    DB::raw("'' as Date_Delivery_Start"),
                    DB::raw("0 as Count_Booked"),
                    DB::raw("0 as Count_Attended"),
                ])
                ->whereNotExists(function ($sub) use ($startDate, $endDate, $recipientId) {
                    $sub->select(DB::raw(1))
                        ->from('Dim_Delivery_Header as dh')
                        ->join('Dim_Grant as g', 'dh.Grant_Key', '=', 'g.Grant_Key')
                        ->join('Dim_Grant_Recipient as gr', 'g.Grant_Recipient_Key', '=', 'gr.Recipient_Key')
                        ->whereColumn('dh.School_Key', 's.School_Key');

                    if ($startDate && $endDate) {
                        $sub->whereBetween('dh.Date_Delivery_Start', [$startDate, $endDate]);
                    } elseif ($startDate) {
                        $sub->where('dh.Date_Delivery_Start', '>=', $startDate);
                    } elseif ($endDate) {
                        $sub->where('dh.Date_Delivery_Start', '<=', $endDate);
                    }

                    if ($recipientId) {
                        $sub->where('gr.Source_Recipient_Id', $recipientId);
                    }
                });

            return $query->orderBy('s.School_Urn', 'asc');
        }

        // -------------------------------------------------------------------------
        // Case B: Schools WITH Deliveries (or Combined All)
        // -------------------------------------------------------------------------
        // Pre-filter delivery headers strictly to the target window and recipient
        $activeDeliveries = DB::connection('mysql')->table('Dim_Delivery_Header as dh')
            ->join('Fact_Course_Delivery as f', 'dh.Delivery_Key', '=', 'f.Delivery_Key')
            ->leftJoin('Dim_Course as c', function ($join) {
                $join->on('f.Course_Key', '=', 'c.Course_Key')
                    ->whereNull('c.Parent_Course_Key');
            })
            ->leftJoin('Dim_Grant as g', 'dh.Grant_Key', '=', 'g.Grant_Key')
            ->leftJoin('Dim_Grant_Recipient as gr', 'g.Grant_Recipient_Key', '=', 'gr.Recipient_Key')
            ->leftJoin('Dim_Training_Provider as tp', 'dh.Training_Provider_Key', '=', 'tp.Provider_Key')
            ->select([
                'dh.School_Key',
                'dh.Source_Delivery_Id',
                'dh.Delivery_Status',
                'dh.Date_Delivery_Start',
                'g.Grant_Number',
                'g.Grant_Source',
                'gr.Recipient_Name',
                'gr.Source_Recipient_Id',
                'tp.Provider_Name',
                'f.Riders_Enrolled_Count',
                'f.Riders_Completed_Count',
            ]);

        if ($startDate && $endDate) {
            $activeDeliveries->whereBetween('dh.Date_Delivery_Start', [$startDate, $endDate]);
        } elseif ($startDate) {
            $activeDeliveries->where('dh.Date_Delivery_Start', '>=', $startDate);
        } elseif ($endDate) {
            $activeDeliveries->where('dh.Date_Delivery_Start', '<=', $endDate);
        }

        if ($recipientId) {
            $activeDeliveries->where('gr.Source_Recipient_Id', $recipientId);
        }

        if ($deliveriesType === 'with_deliveries') {
            // Inner join active deliveries to schools
            $query = DB::connection('mysql')->table('Dim_School as s')
                ->joinSub($activeDeliveries, 'ad', 's.School_Key', '=', 'ad.School_Key')
                ->select([
                    DB::raw("IFNULL(ad.Grant_Number, 'N/A') as Grant_Number"),
                    DB::raw("IFNULL(ad.Grant_Source, 'N/A') as Grant_Source"),
                    DB::raw("IFNULL(ad.Recipient_Name, 'Unlinked') as Recipient_Name"),
                    's.School_Urn',
                    's.School_Name',
                    's.LA_Name',
                    's.LA_Code',
                    'ad.Source_Delivery_Id',
                    'ad.Provider_Name',
                    'ad.Delivery_Status',
                    DB::raw("DATE_FORMAT(ad.Date_Delivery_Start, '%d/%m/%Y') as Date_Delivery_Start"),
                    DB::raw("IFNULL(ad.Riders_Enrolled_Count, 0) as Count_Booked"),
                    DB::raw("IFNULL(ad.Riders_Completed_Count, 0) as Count_Attended"),
                ]);

            return $query->orderBy('s.School_Urn', 'asc')
                ->orderBy('ad.Source_Delivery_Id', 'asc');
        }

        // Default / Combined: Left join active deliveries to schools
        $query = DB::connection('mysql')->table('Dim_School as s')
            ->leftJoinSub($activeDeliveries, 'ad', 's.School_Key', '=', 'ad.School_Key')
            ->select([
                DB::raw("IFNULL(ad.Grant_Number, 'N/A') as Grant_Number"),
                DB::raw("IFNULL(ad.Grant_Source, 'N/A') as Grant_Source"),
                DB::raw("IFNULL(ad.Recipient_Name, 'Unlinked') as Recipient_Name"),
                's.School_Urn',
                's.School_Name',
                's.LA_Name',
                's.LA_Code',
                DB::raw("IFNULL(ad.Source_Delivery_Id, '') as Source_Delivery_Id"),
                DB::raw("IFNULL(ad.Provider_Name, '') as Provider_Name"),
                DB::raw("IFNULL(ad.Delivery_Status, 'No Deliveries Logged') as Delivery_Status"),
                DB::raw("IFNULL(DATE_FORMAT(ad.Date_Delivery_Start, '%d/%m/%Y'), '') as Date_Delivery_Start"),
                DB::raw("IFNULL(ad.Riders_Enrolled_Count, 0) as Count_Booked"),
                DB::raw("IFNULL(ad.Riders_Completed_Count, 0) as Count_Attended"),
            ]);

        return $query->orderBy('s.School_Urn', 'asc')
            ->orderBy('ad.Source_Delivery_Id', 'asc');
    }
}

<?php

namespace App\Reports\Handlers;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Validator;
use Carbon\Carbon;

class SchoolDeliveriesAuditHandler extends AbstractStreamingReportHandler
{
    public function validate(array $parameters): array
    {
        return Validator::make($parameters, [
            'recipient_id'    => 'nullable|integer',
            'start_date'      => 'nullable|string',
            'end_date'        => 'nullable|string',
            'deliveries_type' => 'nullable|string',
            'year'            => 'nullable|integer',
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

        $chunkSize = 2000;

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

        if (!empty($params['year']) && (!$startDate || !$endDate)) {
            $yr        = (int) $params['year'];
            $startDate = "{$yr}-04-01 00:00:00";
            $endDate   = ($yr + 1) . "-03-31 23:59:59";
        } elseif ($startDate && $endDate) {
            $startDate = Carbon::parse($startDate)->startOfDay()->toDateTimeString();
            $endDate   = Carbon::parse($endDate)->endOfDay()->toDateTimeString();
        }

        $deliveriesType = $params['deliveries_type'] ?? null;
        $recipientId    = !empty($params['recipient_id']) ? (int) $params['recipient_id'] : null;

        // -------------------------------------------------------------------------
        // Case A: Strictly Schools with NO Deliveries
        // -------------------------------------------------------------------------
        if ($deliveriesType === 'no_deliveries') {
            return $this->buildNoDeliveriesQuery($startDate, $endDate, $recipientId)
                ->orderBy('School_Urn', 'asc');
        }

        // -------------------------------------------------------------------------
        // Case B: Schools WITH Deliveries
        // -------------------------------------------------------------------------
        if ($deliveriesType === 'with_deliveries') {
            return $this->buildWithDeliveriesQuery($startDate, $endDate, $recipientId)
                ->orderBy('School_Urn', 'asc')
                ->orderBy('Source_Delivery_Id', 'asc');
        }

        // -------------------------------------------------------------------------
        // Case C: Combined All Schools (With Deliveries UNION ALL Without Deliveries)
        // -------------------------------------------------------------------------
        $withDeliveries = $this->buildWithDeliveriesQuery($startDate, $endDate, $recipientId);
        $noDeliveries   = $this->buildNoDeliveriesQuery($startDate, $endDate, $recipientId);

        return $withDeliveries->unionAll($noDeliveries)
            ->orderBy('School_Urn', 'asc')
            ->orderBy('Source_Delivery_Id', 'asc');
    }

    /**
     * Active deliveries query: Dim_Delivery_Header + Fact_Course_Delivery + Dim_School
     */
    protected function buildWithDeliveriesQuery(?string $startDate, ?string $endDate, ?int $recipientId)
    {
        $query = DB::connection('mysql')->table('Dim_Delivery_Header as dh')
            ->join('Fact_Course_Delivery as f', 'dh.Delivery_Key', '=', 'f.Delivery_Key')
            ->join('Dim_School as s', 'dh.School_Key', '=', 's.School_Key')
            ->join('Dim_Course as c', 'f.Course_Key', '=', 'c.Course_Key')
            ->leftJoin('Dim_Course as parent_c', 'c.Parent_Course_Key', '=', 'parent_c.Course_Key')
            ->leftJoin('Dim_Grant as g', 'dh.Grant_Key', '=', 'g.Grant_Key')
            ->leftJoin('Dim_Grant_Recipient as gr', 'g.Grant_Recipient_Key', '=', 'gr.Recipient_Key')
            ->leftJoin('Dim_Training_Provider as tp', 'dh.Training_Provider_Key', '=', 'tp.Provider_Key')
            ->select([
                DB::raw("IFNULL(g.Grant_Number, 'N/A') as Grant_Number"),
                DB::raw("IFNULL(g.Grant_Source, 'N/A') as Grant_Source"),
                DB::raw("IFNULL(gr.Recipient_Name, 'Unlinked') as Recipient_Name"),
                's.School_Urn',
                's.School_Name',
                's.LA_Name',
                's.LA_Code',
                'dh.Source_Delivery_Id',
                DB::raw("IFNULL(c.Course_Level, '') as Course_Level"),
                DB::raw("IFNULL(tp.Provider_Name, '') as Provider_Name"),
                DB::raw("IFNULL(dh.Delivery_Status, 'Delivery Created') as Delivery_Status"),
                DB::raw("IFNULL(DATE_FORMAT(dh.Date_Delivery_Start, '%d/%m/%Y'), '') as Date_Delivery_Start"),
                DB::raw("IFNULL(f.Riders_Enrolled_Count, 0) as Count_Booked"),
                DB::raw("IFNULL(f.Riders_Completed_Count, 0) as Count_Attended"),
            ])
            // 1. Exclude the level_1_2 parent header row
            ->where('c.Course_Level', '!=', 'level_1_2')
            // 2. Only allow root courses OR children whose parent is a level_1_2 composite
            ->where(function ($q) {
                $q->whereNull('c.Parent_Course_Key')
                    ->orWhere('parent_c.Course_Level', '=', 'level_1_2');
            });

        if ($startDate && $endDate) {
            $query->whereBetween('dh.Date_Delivery_Start', [$startDate, $endDate]);
        } elseif ($startDate) {
            $query->where('dh.Date_Delivery_Start', '>=', $startDate);
        } elseif ($endDate) {
            $query->where('dh.Date_Delivery_Start', '<=', $endDate);
        }

        if ($recipientId) {
            $query->where('gr.Source_Recipient_Id', $recipientId);
        }

        return $query;
    }

    /**
     * Schools with zero reportable deliveries in the active scope
     */
    protected function buildNoDeliveriesQuery(?string $startDate, ?string $endDate, ?int $recipientId)
    {
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
                DB::raw("'' as Course_Level"),
                DB::raw("'' as Provider_Name"),
                DB::raw("'No Deliveries Logged' as Delivery_Status"),
                DB::raw("'' as Date_Delivery_Start"),
                DB::raw("0 as Count_Booked"),
                DB::raw("0 as Count_Attended"),
            ])
            ->whereNotExists(function ($sub) use ($startDate, $endDate, $recipientId) {
                $sub->select(DB::raw(1))
                    ->from('Dim_Delivery_Header as dh')
                    ->join('Fact_Course_Delivery as f', 'dh.Delivery_Key', '=', 'f.Delivery_Key')
                    ->join('Dim_Course as c', 'f.Course_Key', '=', 'c.Course_Key')
                    ->leftJoin('Dim_Course as parent_c', 'c.Parent_Course_Key', '=', 'parent_c.Course_Key')
                    ->whereColumn('dh.School_Key', 's.School_Key')
                    ->where('c.Course_Level', '!=', 'level_1_2')
                    ->where(function ($q) {
                        $q->whereNull('c.Parent_Course_Key')
                            ->orWhere('parent_c.Course_Level', '=', 'level_1_2');
                    });

                if ($startDate && $endDate) {
                    $sub->whereBetween('dh.Date_Delivery_Start', [$startDate, $endDate]);
                } elseif ($startDate) {
                    $sub->where('dh.Date_Delivery_Start', '>=', $startDate);
                } elseif ($endDate) {
                    $sub->where('dh.Date_Delivery_Start', '<=', $endDate);
                }

                if ($recipientId) {
                    $sub->join('Dim_Grant as g', 'dh.Grant_Key', '=', 'g.Grant_Key')
                        ->join('Dim_Grant_Recipient as gr', 'g.Grant_Recipient_Key', '=', 'gr.Recipient_Key')
                        ->where('gr.Source_Recipient_Id', $recipientId);
                }
            });

        return $query;
    }
}

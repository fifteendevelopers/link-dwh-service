<?php

namespace App\Reports\Handlers;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Validator;

class SchoolDeliveriesAuditHandler extends AbstractStreamingReportHandler
{
    public function validate(array $parameters): array
    {
        return Validator::make($parameters, [
            'recipient_id' => 'required|integer', // Required to lock down the local territory scope
            'start_date'   => 'nullable|string',
            'end_date'     => 'nullable|string',
        ])->validate();
    }

    public function execute(array $params): array
    {
        ini_set('memory_limit', '1024M');
        ini_set('max_execution_time', '600');

        $query = $this->buildQuery($params);

        // Always run synchronously since this is highly targeted per recipient
        return $query->get()->map(fn($row) => (array)$row)->toArray();
    }

    protected function buildQuery(array $params)
    {
        $query = DB::connection('mysql')->table('Dim_School as s')
            // Drive from schools and pull down optional matching transaction slices
            ->leftJoin('Fact_Course_Delivery as f', 'f.School_Key', '=', 's.School_Key')
            ->leftJoin('Dim_Delivery_Header as dh', 'f.Delivery_Key', '=', 'dh.Delivery_Key')
            ->leftJoin('Dim_Course as c', 'f.Course_Key', '=', 'c.Course_Key')
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
            ])
            ->where(function($q) {
                $q->whereNull('c.Parent_Course_Key');
            });

        // Scope strictly down to the schools belonging to or currently assigned to the target GR
        if (!empty($params['recipient_id'])) {
            $query->where(function($sub) use ($params) {
                $sub->where('gr.Source_Recipient_Id', $params['recipient_id'])
                    ->orWhereNull('f.School_Key'); // Catch local schools that have zero transaction associations
            });
        }

        if (!empty($params['start_date']) && !empty($params['end_date'])) {
            $query->whereBetween('dh.Date_Delivery_Start', [$params['start_date'], $params['end_date']]);
        }

        // Return alphabetically by URN to make gaps visually striking
        return $query->orderBy('s.School_Urn', 'asc')
            ->orderBy('dh.Source_Delivery_Id', 'asc');
    }
}

<?php

namespace App\Reports\Handlers;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Validator;

class GrFleetCyclesUsedHandler extends AbstractStreamingReportHandler
{
    public function validate(array $parameters): array
    {
        return Validator::make($parameters, [
            'recipient_id' => 'nullable|integer',
            'provider_id'  => 'nullable|integer',
            'year'         => 'nullable|integer|digits:4',
            'start_date'   => 'nullable|date_format:Y-m-d',
            'end_date'     => 'nullable|date_format:Y-m-d',
        ])->validate();
    }

    public function execute(array $params): array
    {
        ini_set('memory_limit', '1024M');
        ini_set('max_execution_time', '600');

        $query = $this->buildQuery($params);

        $mapRecord = function ($row) {
            $row = (array) $row;

            $rawCycles = $row['total_fleet_cycles_used']
                ?? $row['Total Fleet Cycles Used']
                ?? ($row[6] ?? null);

            $cycles = ($rawCycles !== null && $rawCycles !== '') ? (int) $rawCycles : 0;

            return [
                'GR Name'                 => (string) ($row['gr_name'] ?? ($row['GR Name'] ?? ($row[0] ?? 'Unlinked'))),
                'TP Name'                 => (string) ($row['tp_name'] ?? ($row['TP Name'] ?? ($row[1] ?? ''))),
                'TP Active?'              => (string) ($row['tp_active'] ?? ($row['TP Active?'] ?? ($row[2] ?? 'Inactive'))),
                'Delivery Month'          => (string) ($row['delivery_month'] ?? ($row['Delivery Month'] ?? ($row[3] ?? ''))),
                'Delivery Year'           => (string) ($row['delivery_year'] ?? ($row['Delivery Year'] ?? ($row[4] ?? ''))),
                'Delivery Count'          => (int) ($row['delivery_count'] ?? ($row['Delivery Count'] ?? ($row[5] ?? 0))),
                'Total Fleet Cycles Used' => $cycles,
            ];
        };

        if (empty($this->callbackUrl)) {
            return $query->get()->map($mapRecord)->toArray();
        }

        $chunkSize = 1000;
        $query->chunk($chunkSize, function ($rows) use ($mapRecord) {

            foreach ($rows as $rawRow) {
                $rawArr = (array) $rawRow;
                if (str_contains($rawArr['gr_name'] ?? $rawArr['GR Name'] ?? '', 'Anthony Gell')) {
                    Log::info("DWH DEBUG: Raw SQL row for Anthony Gell", [
                        'raw_keys'   => array_keys($rawArr),
                        'raw_cycles' => $rawArr['total_fleet_cycles_used'] ?? ($rawArr['Total Fleet Cycles Used'] ?? 'KEY_NOT_FOUND'),
                        'var_dump'   => var_export($rawArr['total_fleet_cycles_used'] ?? null, true),
                    ]);
                    break;
                }
            }

            $chunkArray = $rows->map($mapRecord)->toArray();

            foreach ($chunkArray as $mapped) {
                if (str_contains($mapped['GR Name'] ?? '', 'Anthony Gell')) {
                    Log::info("DWH DEBUG: Transmitting Mapped row for Anthony Gell", [
                        'mapped_val'  => $mapped['Total Fleet Cycles Used'],
                        'mapped_type' => gettype($mapped['Total Fleet Cycles Used']),
                    ]);
                    break;
                }
            }
            
            $this->transmitBatch($chunkArray, false);
        });

        $this->transmitBatch([], true);

        return ['status' => 'async_completed'];
    }

    protected function buildQuery(array $params)
    {
        $query = DB::connection('mysql')->table('Dim_Delivery_Header as dh')
            ->join('Dim_Grant as g', 'dh.Grant_Key', '=', 'g.Grant_Key')
            ->join('Dim_Grant_Recipient as gr', 'g.Grant_Recipient_Key', '=', 'gr.Recipient_Key')
            ->join('Dim_Training_Provider as tp', 'dh.Training_Provider_Key', '=', 'tp.Provider_Key')
            ->select([
                'gr.Recipient_Name as gr_name',
                'tp.Provider_Name as tp_name',
                DB::raw("CASE WHEN tp.Is_Active = 'Y' THEN 'Active' ELSE 'Inactive' END as tp_active"),
                DB::raw("MONTHNAME(dh.Date_Delivery_Start) as delivery_month"),
                DB::raw("YEAR(dh.Date_Delivery_Start) as delivery_year"),
                DB::raw("COUNT(dh.Delivery_Key) as delivery_count"),
                DB::raw("COALESCE(SUM(CASE
                    WHEN dh.fleet_cycles_used IS NOT NULL AND dh.fleet_cycles_used != ''
                    THEN CAST(dh.fleet_cycles_used AS SIGNED)
                    ELSE 0
                END), 0) as total_fleet_cycles_used")
            ])
            ->where('gr.Is_Current', 1)
            ->where('tp.Is_Current', 1);

        if (!empty($params['recipient_id'])) {
            $query->where('gr.Source_Recipient_Id', $params['recipient_id']);
        }

        if (!empty($params['provider_id'])) {
            $query->where('tp.Source_Provider_Id', $params['provider_id']);
        }

        if (!empty($params['start_date']) && !empty($params['end_date'])) {
            $query->whereBetween('dh.Date_Delivery_Start', [$params['start_date'], $params['end_date']]);
        } elseif (!empty($params['year'])) {
            $query->whereRaw('YEAR(dh.Date_Delivery_Start) = ?', [$params['year']]);
        }

        $query->groupBy([
            'gr.Recipient_Name',
            'tp.Provider_Name',
            'tp.Is_Active',
            DB::raw('YEAR(dh.Date_Delivery_Start)'),
            DB::raw('MONTH(dh.Date_Delivery_Start)'),
            DB::raw('MONTHNAME(dh.Date_Delivery_Start)')
        ]);

        return $query->orderBy('gr.Recipient_Name', 'asc')
            ->orderBy('tp.Provider_Name')
            ->orderBy(DB::raw('YEAR(dh.Date_Delivery_Start)'))
            ->orderBy(DB::raw('MONTH(dh.Date_Delivery_Start)'));
    }
}

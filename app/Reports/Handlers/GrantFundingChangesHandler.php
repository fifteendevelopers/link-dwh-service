<?php

namespace App\Reports\Handlers;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Validator;

class GrantFundingChangesHandler extends AbstractStreamingReportHandler
{
    public function validate(array $parameters): array
    {
        return Validator::make($parameters, [
            'date_from'    => 'nullable|date',
            'date_to'      => 'nullable|date',
            'grant_source' => 'nullable|string',
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
        $db = DB::connection('mysql');

        $query->chunk($chunkSize, function ($rows) use ($db) {
            $reallocationKeys = $rows->pluck('Reallocation_Key')->filter()->all();

            // Fetch and group all module logs for this chunk in one query
            $logsByReallocation = [];
            if (!empty($reallocationKeys)) {
                $logs = $db->table('Fact_Grant_Reallocation_Logs')
                    ->whereIn('Reallocation_Key', $reallocationKeys)
                    ->whereNull('Source_Deleted_At')
                    ->select([
                        'Reallocation_Key',
                        'Module_Key',
                        'Value_Count',
                        'Amount',
                    ])
                    ->get();

                foreach ($logs as $log) {
                    $normModule = strtolower(trim($log->Module_Key));
                    $logsByReallocation[$log->Reallocation_Key][$normModule] = [
                        'places' => (int) $log->Value_Count,
                        'amount' => (float) $log->Amount,
                    ];
                }
            }

            $memBuffer = fopen('php://temp', 'r+');

            foreach ($rows as $row) {
                $moduleLogs = $logsByReallocation[$row->Reallocation_Key] ?? [];

                $getModule = fn(string $key) => [
                    'places' => $moduleLogs[$key]['places'] ?? 0,
                    'amount' => $moduleLogs[$key]['amount'] ?? 0.00,
                ];

                $l1   = $getModule('level_1');
                $l1_2 = $getModule('level_1_2');
                $l2   = $getModule('level_2');
                $l3   = $getModule('level_3');
                $pb   = $getModule('plus_balance');
                $pbus = $getModule('plus_bus');
                $pf   = $getModule('plus_fix');
                $pl   = $getModule('plus_learn');
                $pos  = $getModule('plus_on_show');
                $pp   = $getModule('plus_parents');
                $ppr  = $getModule('plus_promotion');
                $prc  = $getModule('plus_recycled');
                $prd  = $getModule('plus_ride');
                $pt   = $getModule('plus_transition');
                $pfam = $getModule('plus_family');
                $pad  = $getModule('plus_adult');

                $mapped = [
                    $row->grant_number ?? '',
                    $row->grant_source ?? '',
                    $row->recipient_name ?? 'Unlinked',
                    (!empty($row->is_sgo) && (int)$row->is_sgo === 1) || (!empty($row->pref_sgoh) && (int)$row->pref_sgoh === 1) ? 'SGO' : '',
                    $row->reallocation_number ?? '',
                    ucfirst((string)($row->reallocation_type ?? '')),
                    $row->status ?? '',
                    $row->reallocation_notes ?? '',
                    $row->reallocation_increase_reasons ?? '',
                    !empty($row->created_at) ? date('d/m/Y', strtotime($row->created_at)) : '',
                    !empty($row->date_approved) ? date('d/m/Y', strtotime($row->date_approved)) : '',
                    '', // Spacer Col L
                    $l1['places'],
                    $l1['amount'],
                    $l1_2['places'],
                    $l1_2['amount'],
                    $l2['places'],
                    $l2['amount'],
                    $l3['places'],
                    $l3['amount'],
                    '', // Spacer Col U
                    $pb['places'],
                    $pb['amount'],
                    $pbus['places'],
                    $pbus['amount'],
                    $pf['places'],
                    $pf['amount'],
                    $pl['places'],
                    $pl['amount'],
                    $pos['places'],
                    $pos['amount'],
                    $pp['places'],
                    $pp['amount'],
                    $ppr['places'],
                    $ppr['amount'],
                    $prc['places'],
                    $prc['amount'],
                    $prd['places'],
                    $prd['amount'],
                    $pt['places'],
                    $pt['amount'],
                    $pfam['places'],
                    $pfam['amount'],
                    $pad['places'],
                    $pad['amount'],
                ];

                fputcsv($memBuffer, $mapped);
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

        $query = $db->table('Fact_Grant_Reallocations as r')
            ->leftJoin('Dim_Grant as g', 'r.Grant_Key', '=', 'g.Grant_Key')
            ->leftJoin('Dim_Grant_Recipient as gr', 'g.Grant_Recipient_Key', '=', 'gr.Recipient_Key')
            ->select([
                'r.Reallocation_Key',
                'g.Grant_Number as grant_number',
                'g.Grant_Source as grant_source',
                'gr.Recipient_Name as recipient_name',
                'gr.Is_SGO as pref_sgoh',
                'r.Reallocation_Number as reallocation_number',
                'r.Reallocation_Type as reallocation_type',
                'r.Status_Raw as status',
                'r.Reallocation_Notes as reallocation_notes',
                'r.Reallocation_Increase_Reasons as reallocation_increase_reasons',
                'r.Source_Created_at as created_at',
                'r.Date_Approved as date_approved',
            ])
            ->where('r.Status_Raw', '=', 1)
            ->whereNull('r.Source_Deleted_At')
            ->orderBy('r.Source_Created_At', 'desc');

        if (!empty($params['date_from']) && !empty($params['date_to'])) {
            $query->whereBetween('r.Source_Created_At', [$params['date_from'], $params['date_to']]);
        }

        if (!empty($params['grant_source'])) {
            $query->where('g.Grant_Source', $params['grant_source']);
        }

        return $query;
    }
}

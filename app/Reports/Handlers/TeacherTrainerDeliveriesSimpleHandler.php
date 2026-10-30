<?php

namespace App\Reports\Handlers;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Validator;

class TeacherTrainerDeliveriesSimpleHandler extends AbstractStreamingReportHandler
{
    public function validate(array $parameters): array
    {
        return Validator::make($parameters, [
            'start_date'  => 'nullable|string',
            'end_date'    => 'nullable|string',
            'school_urn'  => 'nullable|string',
            'course_type' => 'nullable|string',
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

        // 1. Subquery for total attended metrics
        $attendedSub = $db->table('Fact_Teacher_Trainer_Delivery_Metric')
            ->select('TT_Delivery_Module_Key', DB::raw('SUM(Metric_Value) as total_attended'))
            ->where('Category', 'attended')
            ->where('Sub_Category', 'total')
            ->groupBy('TT_Delivery_Module_Key');

        // 2. Primary query joining delivery dimension, bridge fact, trainer, school, and attended subquery
        $query = $db->table('Fact_Teacher_Trainer_Delivery_Module as m')
            ->join('Dim_Teacher_Trainer_Delivery as d', 'm.TT_Delivery_Key', '=', 'd.TT_Delivery_Key')
            ->leftJoin('Dim_School as s', 'd.School_Key', '=', 's.School_Key')
            ->leftJoin('Fact_Deliveries_Teacher_Trainers as fdtt', function ($join) {
                $join->on('d.TT_Delivery_Key', '=', 'fdtt.TT_Delivery_Key')
                    ->whereNull('fdtt.Source_Deleted_At');
            })
            ->leftJoin('Dim_Teacher_Trainer as tt', 'fdtt.Teacher_Trainer_Key', '=', 'tt.Teacher_Trainer_Key')
            ->leftJoinSub($attendedSub, 'att', 'm.TT_Delivery_Module_Key', '=', 'att.TT_Delivery_Module_Key')
            ->whereNull('d.Source_Deleted_At');

        if (!empty($params['start_date']) && !empty($params['end_date'])) {
            $query->whereBetween(DB::raw('COALESCE(d.Completion_Date, d.Date_Delivery_End)'), [
                $params['start_date'],
                $params['end_date'],
            ]);
        }

        if (!empty($params['school_urn'])) {
            $query->where('d.School_Urn', $params['school_urn']);
        }

        if (!empty($params['course_type']) && $params['course_type'] !== 'All') {
            $query->where('m.Module_Id', $params['course_type']);
        }

        return $query->select([
            DB::raw("IFNULL(tt.Source_Teacher_Trainer_Id, '') as teacher_id"),
            DB::raw("IFNULL(tt.First_Name, '') as first_name"),
            DB::raw("IFNULL(tt.Last_Name, '') as last_name"),
            DB::raw("IFNULL(s.School_Name, 'N/A') as school_name"),
            DB::raw("IFNULL(s.La_Name, '') as la_name"),
            DB::raw("IFNULL(DATE_FORMAT(COALESCE(d.Completion_Date, d.Date_Delivery_End), '%d/%m/%Y'), '') as completion_date"),
            DB::raw("IFNULL(m.Module_Label, m.Module_Id) as course_label"),
            DB::raw("IFNULL(att.total_attended, 0) as no_attended"),
        ])
            ->orderBy('d.Completion_Date', 'desc')
            ->orderBy('d.TT_Delivery_Key', 'asc')
            ->orderBy('m.TT_Delivery_Module_Key', 'asc');
    }
}

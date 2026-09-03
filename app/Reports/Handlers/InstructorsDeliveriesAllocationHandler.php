<?php

namespace App\Reports\Handlers;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Validator;

class InstructorsDeliveriesAllocationHandler extends AbstractStreamingReportHandler
{
    public function validate(array $parameters): array
    {
        return Validator::make($parameters, [
            'training_provider_id' => 'required|integer',
            'financial_year'       => 'nullable',
            'start_date'           => 'nullable|string',
            'end_date'             => 'nullable|string',
            'year'                 => 'nullable|integer',
        ])->validate();
    }

    public function execute(array $params): array
    {
        ini_set('memory_limit', '1024M');
        ini_set('max_execution_time', '300');

        $query = DB::connection('mysql')->table('Fact_Instructor_Delivery as fi')
            ->join('Dim_Delivery_Header as dh', 'fi.Delivery_Key', '=', 'dh.Delivery_Key')
            ->join('Dim_Instructor as i', 'fi.Instructor_Key', '=', 'i.Instructor_Key')
            ->join('Dim_Training_Provider as tp', 'dh.Training_Provider_Key', '=', 'tp.Provider_Key')
            ->leftJoin('Dim_School as s', 'dh.School_Key', '=', 's.School_Key')
            ->leftJoin('Dim_Organisation as o', 'dh.Organisation_Key', '=', 'o.Organisation_Key')
            ->select([
                'dh.Source_Delivery_Id as delivery_id',
                DB::raw("DATE_FORMAT(dh.Date_Delivery_Start, '%d/%m/%Y') as date_delivery_start"),
                DB::raw("COALESCE(NULLIF(s.School_Name, ''), NULLIF(o.Organisation_Name, ''), 'N/A') as establishment_name"),
                'i.Source_Instructor_Id as instructor_number',
                'i.First_Name as first_name',
                'i.Last_Name as last_name',
                'dh.Delivery_Status as delivery_status'
            ])
            ->where('tp.Source_Provider_Id', (int) $params['training_provider_id']);

        if (!empty($params['start_date']) && !empty($params['end_date'])) {
            $query->whereBetween('dh.Date_Delivery_Start', [$params['start_date'], $params['end_date']]);
        } elseif (!empty($params['year'])) {
            $query->whereRaw('YEAR(dh.Date_Delivery_Start) = ?', [$params['year']]);
        }

        $query->orderBy('dh.Source_Delivery_Id')
            ->orderBy('i.Last_Name')
            ->orderBy('i.First_Name');

        if (empty($this->callbackUrl)) {
            return $query->get()->map(fn($row) => (array) $row)->toArray();
        }

        $query->chunk(500, function ($rows) {
            $chunk = $rows->map(fn($row) => array_values((array) $row))->toArray();
            $this->transmitBatch($chunk, false);
        });

        $this->transmitBatch([], true);

        return ['status' => 'async_completed'];
    }
}

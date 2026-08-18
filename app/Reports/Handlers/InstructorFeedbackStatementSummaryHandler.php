<?php

namespace App\Reports\Handlers;

use App\Reports\Contracts\ReportHandlerInterface;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Validator;

class InstructorFeedbackStatementSummaryHandler extends AbstractStreamingReportHandler implements ReportHandlerInterface
{
    public function validate(array $parameters): array
    {
        return Validator::make($parameters, [
            'year'         => 'nullable|integer',
            'start_date'   => 'nullable|string',
            'end_date'     => 'nullable|string',
            'recipient_id' => 'nullable|integer',
            'provider_id'  => 'nullable|integer',
        ])->validate();
    }

    public function execute(array $params): array
    {
        ini_set('memory_limit', '512M');
        ini_set('max_execution_time', '600');

        $query = $this->buildQuery($params);

        if (empty($this->callbackUrl)) {
            return $query->get()->map(fn($row) => (array) $row)->toArray();
        }

        $chunkSize = 500;

        $query->chunk($chunkSize, function ($rows) {
            $chunkArray = $rows->map(fn($row) => (array) $row)->toArray();
            $this->transmitBatch($chunkArray, false);
        });

        $this->transmitBatch([], true);

        return ['status' => 'async_completed'];
    }

    protected function buildQuery(array $params)
    {
        $query = DB::connection('mysql')->table('Fact_Rider_Instructor_Feedback as fif')
            ->join('Dim_Instructor_Feedback_Lookup as l', 'fif.Feedback_Lookup_Key', '=', 'l.Feedback_Lookup_Key')
            ->join('Dim_Course as c', 'fif.Course_Key', '=', 'c.Course_Key')
            ->join('Dim_Delivery_Header as dh', 'fif.Delivery_Key', '=', 'dh.Delivery_Key')
            ->leftJoin('Dim_Grant as g', 'dh.Grant_Key', '=', 'g.Grant_Key')
            ->leftJoin('Dim_Training_Provider as tp', 'dh.Training_Provider_Key', '=', 'tp.Provider_Key')
            ->leftJoin('Dim_Grant_Recipient as gr', 'g.Grant_Recipient_Key', '=', 'gr.Recipient_Key')
            ->select([
                DB::raw("IFNULL(c.Course_Level, l.Course_Code) as Course_Level"),
                'l.Category_Label as Feedback_Category',
                'l.Short_Text as Feedback_Statement',
                DB::raw("COUNT(fif.Feedback_Fact_Key) as Answer_Count"),
            ]);

        if (!empty($params['year'])) {
            $query->where('g.Grant_Period_Start_Year', (int) $params['year']);
        }

        if (!empty($params['recipient_id'])) {
            $query->where('gr.Source_Recipient_Id', (int) $params['recipient_id']);
        }

        if (!empty($params['provider_id'])) {
            $query->where('tp.Source_Provider_Id', (int) $params['provider_id']);
        }

        if (!empty($params['start_date']) && !empty($params['end_date'])) {
            $query->whereBetween('dh.Date_Delivery_Start', [$params['start_date'], $params['end_date']]);
        }

        return $query->groupBy([
            DB::raw("IFNULL(c.Course_Level, l.Course_Code)"),
            'l.Category_Label',
            'l.Short_Text',
        ])
            ->orderBy('Course_Level', 'asc')
            ->orderBy('Feedback_Category', 'asc')
            ->orderBy('Answer_Count', 'desc');
    }
}

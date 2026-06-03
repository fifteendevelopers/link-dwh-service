<?php

namespace App\Reports\Handlers;

use App\Reports\Contracts\ReportHandlerInterface;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Validator;

class DeliveriesPerGrantRecipientHandler implements ReportHandlerInterface
{
    /**
     * Define and validate optional filters for the Deliveries per Grant Recipient report.
     */
    public function validate(array $parameters): array
    {
        return Validator::make($parameters, [
            'recipient_id' => 'nullable|integer',
            'grant_id'     => 'nullable|integer',
            'provider_id'  => 'nullable|integer',
            'status'       => 'nullable|string',
        ])->validate();
    }

    /**
     * Execute the DWH Query using your verified working SQL structure.
     */
    public function execute(array $params): array
    {
        $query = DB::connection('mysql')->table('Fact_Course_Delivery as f')
            ->join('Dim_Delivery_Header as dh', 'f.Delivery_Key', '=', 'dh.Delivery_Key')
            ->join('Dim_Course as c', 'f.Course_Key', '=', 'c.Course_Key')
            ->join('Dim_Grant as g', 'f.Grant_Key', '=', 'g.Grant_Key')
            ->join('Dim_Grant_Recipient as gr', 'g.Grant_Recipient_Key', '=', 'gr.Recipient_Key')
            ->join('Dim_Training_Provider as tp', 'f.Provider_Key', '=', 'tp.Provider_Key')
            ->leftJoin('Dim_School as s', 'f.School_Key', '=', 's.School_Key')
            ->leftJoin('Dim_Organisation as o', 'f.Organisation_Key', '=', 'o.Organisation_Key')
            ->select([
                'gr.Recipient_Name as Grant Recipient',
                'g.Grant_Number as Grant Number',
                'dh.Source_Delivery_Id as Delivery ID',
                'tp.Provider_Name as Training Provider',
                DB::raw("COALESCE(s.School_Name, o.Organisation_Name) as 'School / Organisation'"),
                'c.Course_Level as Module',
                'c.Year_Group as Year Group',
                'dh.Delivery_Status as Status',
                'f.Count_Booked_Confirmed as Booked',
                'f.Count_Attended_Confirmed as Attended',
                'f.Count_Male as Male',
                'f.Count_Female as Female',
                'f.Count_SEND as SEND',
                'f.Count_Pupil_Premium as Pupil Premium',

                // Granular Ethnicity Mappings matching your working query aliases exactly
                'f.Count_Ethnicity_White_British as Ethnicity: White British',
                'f.Count_Ethnicity_White_Irish as Ethnicity: White Irish',
                'f.Count_Ethnicity_Gypsy_Romany as Ethnicity: Gypsy / Romany',
                'f.Count_Ethnicity_White_Other as Ethnicity: White Other',
                'f.Count_Ethnicity_Mixed_White_Black_Carib as Ethnicity: Mixed White & Black Caribbean',
                'f.Count_Ethnicity_Mixed_White_Black_African as Ethnicity: Mixed White & Black African',
                'f.Count_Ethnicity_Mixed_White_Asian as Ethnicity: Mixed White & Asian',
                'f.Count_Ethnicity_Mixed_Other as Ethnicity: Mixed Other',
                'f.Count_Ethnicity_Asian_Indian as Ethnicity: Asian Indian',
                'f.Count_Ethnicity_Asian_Pakistani as Ethnicity: Asian Pakistani',
                'f.Count_Ethnicity_Asian_Bangladeshi as Ethnicity: Asian Bangladeshi',
                'f.Count_Ethnicity_Asian_Chinese as Ethnicity: Asian Chinese',
                'f.Count_Ethnicity_Asian_Other as Ethnicity: Asian Other',
                'f.Count_Ethnicity_Black_African as Ethnicity: Black African',
                'f.Count_Ethnicity_Black_Caribbean as Ethnicity: Black Caribbean',
                'f.Count_Ethnicity_Black_Other as Ethnicity: Black Other',
                'f.Count_Ethnicity_Other_Arab as Ethnicity: Other Arab',
                'f.Count_Ethnicity_Other_Any as Ethnicity: Other Any',
                'f.Count_Ethnicity_Not_Stated as Ethnicity: Not Stated'
            ])
            // Matches: WHERE c.Parent_Course_Key IS NULL
            ->whereNull('c.Parent_Course_Key');

        // Optional Runtime Parameter Filters (Only applied if sent in the API request body)
        if (!empty($params['recipient_id'])) {
            $query->where('gr.Source_Recipient_Id', $params['recipient_id']);
        }

        if (!empty($params['grant_id'])) {
            $query->where('g.Source_Grant_Id', $params['grant_id']);
        }

        if (!empty($params['provider_id'])) {
            $query->where('tp.Source_Provider_Id', $params['provider_id']);
        }

        if (!empty($params['status'])) {
            $query->where('dh.Delivery_Status', $params['status']);
        }

        // Matches: ORDER BY gr.Recipient_Name, g.Grant_Number, dh.Source_Delivery_Id
        return $query->orderBy('gr.Recipient_Name')
            ->orderBy('g.Grant_Number')
            ->orderBy('dh.Source_Delivery_Id')
            ->get()
            ->toArray();
    }
}

<?php

namespace App\Reports\Handlers;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Validator;

class DeliveriesHandler extends AbstractStreamingReportHandler
{
    public function validate(array $parameters): array
    {
        return Validator::make($parameters, [
            'recipient_id' => 'nullable|integer',
            'grant_id'     => 'nullable|integer',
            'provider_id'  => 'nullable|integer',
            'status'       => 'nullable|string',
        ])->validate();
    }

    public function execute(array $params): array
    {
        ini_set('memory_limit', '1024M');
        ini_set('max_execution_time', '600');

        $query = $this->buildQuery($params);

        // Synchronous track fallback
        if (empty($this->callbackUrl)) {
            return $query->get()->map(fn($row) => (array)$row)->toArray();
        }

        // Asynchronous Chunk Streaming Size
        $chunkSize = 1000;

        // We use a custom flag structure to know when we are hitting the final database records block
        $query->chunk($chunkSize, function ($rows) {
            $chunkArray = $rows->map(fn($row) => (array)$row)->toArray();

            // Send the chunk back
            $this->transmitBatch($chunkArray, false);
        });

        // Trigger the empty handshake packet with the EOF True header flag to close out the storage stream
        $this->transmitBatch([], true);

        return ['status' => 'async_completed'];
    }

    protected function buildQuery(array $params)
    {

        $query = DB::connection('mysql')->table('Fact_Course_Delivery as f')
            ->join('Dim_Delivery_Header as dh', 'f.Delivery_Key', '=', 'dh.Delivery_Key')
            ->join('Dim_Course as c', 'f.Course_Key', '=', 'c.Course_Key')
            ->join('Dim_Grant as g', 'f.Grant_Key', '=', 'g.Grant_Key')
            ->join('Dim_Grant_Recipient as gr', 'g.Grant_Recipient_Key', '=', 'gr.Recipient_Key')
            ->join('Dim_Training_Provider as tp', 'f.Provider_Key', '=', 'tp.Provider_Key')
            ->leftJoin('Dim_School as s', 'f.School_Key', '=', 's.School_Key')
            ->select([
                'g.Grant_Number as Grant Number',
                'g.Grant_Source as Grant Source',
                'gr.Recipient_Name as Grant Recipient',
                'dh.Source_Delivery_Id as Delivery ID',
                DB::raw("CASE WHEN dh.Digitisation_Booking = 1 THEN 'Yes' ELSE 'No' END as Digitisation"),
                's.School_Urn',
                's.School_Name',
                DB::raw("CASE
                    WHEN JSON_VALID(dh.Alt_Delivery_Location) THEN
                        CONCAT_WS(', ',
                            dh.Alt_Delivery_Location->>'$.venueName',
                            dh.Alt_Delivery_Location->>'$.venueAddress',
                            dh.Alt_Delivery_Location->>'$.venuePostcode'
                        )
                    ELSE
                        dh.Alt_Delivery_Location
                END as 'Alternative Location'"),
                'tp.Provider_Name as Training Provider',
                'dh.Date_Delivery_Start as Date From',
                'dh.Date_Completed as Date To',
                'c.Year_Group as Year Group',
                'c.Course_Level as Module',
                'dh.Delivery_Status as Status',
                'f.Count_Booked_Confirmed as Booked - Confirmed',
                'f.Count_Attended_Confirmed as Attended - Confirmed',
                'f.Count_Male as Gender - Male',
                'f.Count_Female as Gender - Female',
                'f.Count_Gender_Other as Gender - Other',
                'f.Count_SEND as SEND',
                'f.Count_Pupil_Premium as Pupil Premium',

                // Granular Ethnicity Mappings
                'f.Count_Ethnicity_Asian_Bangladeshi as Ethnicity - Asian - Bangladeshi',
                'f.Count_Ethnicity_Asian_Chinese as Ethnicity - Asian - Chinese',
                'f.Count_Ethnicity_Asian_Indian as Ethnicity - Asian - Indian',
                'f.Count_Ethnicity_Asian_Pakistani as Ethnicity-  Asian - Pakistani',
                'f.Count_Ethnicity_Asian_Other as Ethnicity - Asian - Any other Asian background',
                'f.Count_Ethnicity_Black_African as Ethnicity- Black - Black African',
                'f.Count_Ethnicity_Black_Caribbean as Ethnicity - Black - Black Caribbean',
                'f.Count_Ethnicity_Black_Other as Ethnicity - Black - Any other Black background',
                'f.Count_Ethnicity_Mixed_White_Asian as Ethnicity - Mixed - White & Asian',
                'f.Count_Ethnicity_Mixed_White_Black_African as Ethnicity- Mixed - White and Black African',
                'f.Count_Ethnicity_Mixed_White_Black_Carib as Ethnicity - Mixed - White and Black Caribbean',
                'f.Count_Ethnicity_Mixed_Other as Ethnicity - Mixed - Any other Mixed background',
                'f.Count_Ethnicity_Gypsy_Romany as Ethnicity - White - Gypsy/Romany',
                'f.Count_Ethnicity_White_Irish as Ethnicity - White - Irish',
                'f.Count_Ethnicity_White_Traveller as Ethnicity - White - Irish Traveller',
                'f.Count_Ethnicity_White_British as Ethnicity - White - White British',
                'f.Count_Ethnicity_White_Other as Ethnicity - White - Any other White background',
                'f.Count_Ethnicity_Other_Any as Ethnicity - Any other ethnic group',
                'f.Count_Ethnicity_Other_Arab as Ethnicity - Any other ethnic group - Arab',
                'f.Count_Ethnicity_Not_Stated as Ethnicity - Prefer not to say',
                // Age Ranges
                'f.Count_Age_Range_18_24 as Age Range - 18 to 24',
                'f.Count_Age_Range_25_34 as Age Range - 25 to 34',
                'f.Count_Age_Range_35_44 as Age Range - 35 to 44',
                'f.Count_Age_Range_45_54 as Age Range - 45 to 54',
                'f.Count_Age_Range_55_64 as Age Range - 55 to 64',
                'f.Count_Age_Range_Over_65 as Age Range - Over 65',
                'f.Count_Age_Range_Not_Stated as Age Range - Prefer not to say',
                // Repeat Types
                'f.Count_Booked_Repeat_Type_Repeat as Repeat Type - Repeat',
                'f.Count_Booked_Repeat_Type_Unique as Repeat Type - Unique',
                'f.Count_Booked_Repeat_Type_Na as Repeat Type - Prefer not to say',
                // SEND
                'f.Count_SEND as SEND',
                'f.Count_SEND_Not_Stated as SEND - Prefer not to say',
                // Pupil Premium
                'f.Count_Pupil_Premium as Pupil Premium',
                'f.Count_Pupil_Premium_Not_Stated as Pupil Premium - Prefer not to say',
                // Bikes
                'f.Count_Bikes_Recycled as Bikes Recycled',
                'f.Count_Bikes_Swapped as Bikes Swapped',
                // Family
                'f.Count_Adults as Adults',
                'f.Count_Children as Children Under 18',
            ])
            ->whereNull('c.Parent_Course_Key');

        // Optional Runtime Parameter Filters
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

        return $query->orderBy('gr.Recipient_Name')
            ->orderBy('g.Grant_Number')
            ->orderBy('dh.Source_Delivery_Id');

    }


}

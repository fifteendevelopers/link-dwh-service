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
            'start_date'       => 'nullable|string',
            'end_date'       => 'nullable|string',
        ])->validate();
    }

    public function execute(array $params): array
    {
        ini_set('memory_limit', '1024M');
        ini_set('max_execution_time', '600');

        $query = $this->buildQuery($params);

        // Synchronous track fallback
        $isScopedFilter = !empty($params['recipient_id']) || !empty($params['provider_id']) || !empty($params['grant_id']);
        if ($isScopedFilter || empty($this->callbackUrl)) {
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
                'g.Grant_Number',
                'g.Grant_Source',
                'gr.Recipient_Name',
                'dh.Source_Delivery_Id',
                DB::raw("CASE WHEN dh.Digitisation_Booking = 1 THEN 'Yes' ELSE 'No' END as Digitisation"),
                's.School_Urn',
                's.School_Name',
                's.LA_Name',
                's.LA_Code',
                DB::raw("CASE
                    WHEN JSON_VALID(dh.Alt_Delivery_Location) THEN
                        CONCAT_WS(', ',
                            NULLIF(dh.Alt_Delivery_Location->>'$.venueName', ''),
                            NULLIF(dh.Alt_Delivery_Location->>'$.venueAddress', ''),
                            NULLIF(dh.Alt_Delivery_Location->>'$.venuePostcode', '')
                        )
                    ELSE
                        dh.Alt_Delivery_Location
                END as Alt_Delivery_Location"),
                'tp.Provider_Name',
                'tp.Source_Provider_Id',
                DB::raw("DATE_FORMAT(dh.Date_Delivery_Start, '%d/%m/%Y') as Date_Delivery_Start"),
                DB::raw("DATE_FORMAT(dh.Date_Completed, '%d/%m/%Y') as Date_Completed"),
                'c.Year_Group',
                'c.Course_Level',
                'dh.Delivery_Status',
                'f.Count_Booked_Provisional',
                'f.Riders_Enrolled_Count', //Booked
                'f.Riders_Completed_Count', //Attended
                'f.Count_Booked_Confirmed',
                'f.Count_Attended_Confirmed',
                'f.Count_Male',
                'f.Count_Female',
                'f.Count_Gender_Other',
                'f.Count_Gender_Not_Stated',
                // Granular Ethnicity Mappings
                'f.Count_Ethnicity_Asian_Bangladeshi',
                'f.Count_Ethnicity_Asian_Chinese',
                'f.Count_Ethnicity_Asian_Indian',
                'f.Count_Ethnicity_Asian_Pakistani',
                'f.Count_Ethnicity_Asian_Other',
                'f.Count_Ethnicity_Black_African',
                'f.Count_Ethnicity_Black_Caribbean',
                'f.Count_Ethnicity_Black_Other',
                'f.Count_Ethnicity_Mixed_White_Asian',
                'f.Count_Ethnicity_Mixed_White_Black_African',
                'f.Count_Ethnicity_Mixed_White_Black_Carib',
                'f.Count_Ethnicity_Mixed_Other',
                'f.Count_Ethnicity_Gypsy_Romany',
                'f.Count_Ethnicity_White_Irish',
                'f.Count_Ethnicity_White_Traveller',
                'f.Count_Ethnicity_White_British',
                'f.Count_Ethnicity_White_Other',
                'f.Count_Ethnicity_Other_Any',
                'f.Count_Ethnicity_Other_Arab',
                'f.Count_Ethnicity_Not_Stated',
                // Age Ranges
                'f.Count_Age_Range_18_24',
                'f.Count_Age_Range_25_34',
                'f.Count_Age_Range_35_44',
                'f.Count_Age_Range_45_54',
                'f.Count_Age_Range_55_64',
                'f.Count_Age_Range_Over_65',
                'f.Count_Age_Range_Not_Stated',
                // Repeat Types
                'f.Count_Booked_Repeat_Type_Repeat',
                'f.Count_Booked_Repeat_Type_Unique',
                'f.Count_Booked_Repeat_Type_Na',
                // SEND
                'f.Count_SEND',
                'f.Count_SEND_Not_Stated',
                // Pupil Premium
                'f.Count_Pupil_Premium',
                'f.Count_Pupil_Premium_Not_Stated',
                // Bikes
                'f.Count_Bikes_Recycled',
                'f.Count_Bikes_Swapped',
                // Family
                'f.Count_Adults',
                'f.Count_Children',
                //Fleet Cycles
                'dh.Fleet_Cycles_Used',
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
        if (!empty($params['start_date']) && !empty($params['end_date'])) {
            $query->whereBetween('dh.Date_Delivery_Start', [$params['start_date'],$params['end_date']]);
        }

        return $query->orderBy('gr.Recipient_Name')
            ->orderBy('g.Grant_Number')
            ->orderBy('dh.Source_Delivery_Id');

    }


}

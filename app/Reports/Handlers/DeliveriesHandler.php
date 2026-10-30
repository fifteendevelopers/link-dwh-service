<?php

namespace App\Reports\Handlers;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;
use Illuminate\Support\Facades\Validator;

class DeliveriesHandler extends AbstractStreamingReportHandler
{
    public function validate(array $parameters): array
    {
        return Validator::make($parameters, [
            'recipient_id' => 'nullable|integer',
            'grant_id'     => 'nullable|integer',
            'provider_id'  => 'nullable|integer',
            'year'         => 'nullable|integer',
            'start_date'   => 'nullable|string',
            'end_date'     => 'nullable|string',
        ])->validate();
    }

    public function execute(array $params): array
    {
        ini_set('memory_limit', '2048M');
        ini_set('max_execution_time', '600');

        $query = $this->buildQuery($params);

        if (empty($this->callbackUrl)) {
            return $query->get()->map(fn($row) => (array)$row)->toArray();
        }

        // 1. Run the heavy multi-table query ONCE (~99s)
        $allRows = $query->get();

        if ($allRows->isEmpty()) {
            $this->transmitBatch([], true);
            return ['status' => 'async_completed', 'total_rows' => 0];
        }

        // 2. Chunk the resolved in-memory collection without hitting SQL OFFSET
        $chunkSize = 1000;
        $batchCount = 0;

        $allRows->chunk($chunkSize)->each(function ($rows) use (&$batchCount) {
            $batchCount++;

            // Collect only the courses present in this 1,000-row chunk
            $courseIds = $rows->pluck('Source_Course_Id')->filter()->unique()->toArray();

            // Fast indexed lookup for attendance counts for ONLY this slice (~5-15ms)
            $attendanceMap = [];
            if (!empty($courseIds)) {
                $attendanceMap = DB::connection('mysql')->table('Fact_Rider_Course')
                    ->whereIn('Source_Course_Id', $courseIds)
                    ->where('Attended', 1)
                    ->groupBy('Source_Course_Id')
                    ->select('Source_Course_Id', DB::raw('COUNT(*) as total_attended'))
                    ->pluck('total_attended', 'Source_Course_Id')
                    ->toArray();
            }

            $memBuffer = fopen('php://temp', 'r+');

            foreach ($rows as $row) {
                $rowData = (array) $row;
                $srcCourseId = $rowData['Source_Course_Id'] ?? null;
                $isConfirmed = (int) ($rowData['Has_Confirmed_Booked'] ?? 0) === 1;

                // Resolve attended: rider attendance count -> fallback to f.Count_Attended -> fallback to completed count
                $riderAttended = ($srcCourseId && isset($attendanceMap[$srcCourseId]))
                    ? (int) $attendanceMap[$srcCourseId]
                    : null;

                $finalAttended = $riderAttended
                    ?? (int) ($rowData['Count_Attended_Raw'] ?? 0)
                    ?: (int) ($rowData['Riders_Completed_Count'] ?? 0);

                $finalBooked = (int) ($rowData['Booked_Raw'] ?? 0)
                    ?: (int) ($rowData['Count_Booked_Raw'] ?? 0)
                        ?: (int) ($rowData['Riders_Enrolled_Count'] ?? 0);

                // Assign mutually exclusive columns based on confirmed status
                $rowData['Count_Booked']             = $isConfirmed ? 0 : $finalBooked;
                $rowData['Count_Attended']           = $isConfirmed ? 0 : $finalAttended;
                $rowData['Count_Booked_Confirmed']   = $isConfirmed ? $finalBooked : 0;
                $rowData['Count_Attended_Confirmed'] = $isConfirmed ? $finalAttended : 0;

                // Strip internal helper columns so output matches DeliveriesExport headings (65 columns)
                unset(
                    $rowData['Source_Course_Id'],
                    $rowData['Has_Confirmed_Booked'],
                    $rowData['Booked_Raw'],
                    $rowData['Count_Booked_Raw'],
                    $rowData['Count_Attended_Raw']
                );

                fputcsv($memBuffer, $rowData);
            }

            rewind($memBuffer);
            $csvStringChunk = stream_get_contents($memBuffer);
            fclose($memBuffer);

            // Transmit batch to Link webhook
            $this->transmitBatch([$csvStringChunk], false);
        });

        // 3. Send final closing EOF handshake packet
        $this->transmitBatch([], true);

        return ['status' => 'async_completed', 'total_rows' => $allRows->count()];
    }

    protected function buildQuery(array $params)
    {
        $query = DB::connection('mysql')->table('Fact_Course_Delivery as f')
            ->join('Dim_Delivery_Header as dh', 'f.Delivery_Key', '=', 'dh.Delivery_Key')
            ->join('Dim_Course as c', 'f.Course_Key', '=', 'c.Course_Key')
            ->join('Dim_Grant as g', 'f.Grant_Key', '=', 'g.Grant_Key')
            ->leftJoin('Dim_Grant_Recipient as gr', 'g.Grant_Recipient_Key', '=', 'gr.Recipient_Key')
            ->leftJoin('Dim_Training_Provider as tp', 'f.Provider_Key', '=', 'tp.Provider_Key')
            ->leftJoin('Dim_School as s', 'f.School_Key', '=', 's.School_Key')
            ->select([
                'g.Grant_Number',
                'g.Grant_Source',
                DB::raw("IFNULL(gr.Recipient_Name, 'not set') as Recipient_Name"),
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
                DB::raw("IFNULL(tp.Provider_Name, '') as Provider_Name"),
                'tp.Source_Provider_Id',
                DB::raw("DATE_FORMAT(dh.Date_Delivery_Start, '%d/%m/%Y') as Date_Delivery_Start"),
                DB::raw("DATE_FORMAT(dh.Date_Completed, '%d/%m/%Y') as Date_Completed"),
                'c.Year_Group',
                'c.Course_Level',
                'dh.Delivery_Status',

                // Column 18: Provisional
                DB::raw("IFNULL(NULLIF(c.Provisional, 0), IFNULL(f.Count_Provisional, 0)) as Count_Booked_Provisional"),

                // Columns 19-22: Placeholders (computed dynamically in chunk loop)
                DB::raw("0 as Count_Booked"),
                DB::raw("0 as Count_Attended"),
                DB::raw("0 as Count_Booked_Confirmed"),
                DB::raw("0 as Count_Attended_Confirmed"),

                // Demographics & metrics (Columns 23-65)
                'f.Count_Male',
                'f.Count_Female',
                'f.Count_Gender_Other',
                'f.Count_Gender_Not_Stated',
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
                'f.Count_Age_Range_18_24',
                'f.Count_Age_Range_25_34',
                'f.Count_Age_Range_35_44',
                'f.Count_Age_Range_45_54',
                'f.Count_Age_Range_55_64',
                'f.Count_Age_Range_Over_65',
                'f.Count_Age_Range_Not_Stated',
                'f.Count_Booked_Repeat_Type_Repeat',
                'f.Count_Booked_Repeat_Type_Unique',
                'f.Count_Booked_Repeat_Type_Na',
                'f.Count_SEND',
                'f.Count_SEND_Not_Stated',
                'f.Count_Pupil_Premium',
                'f.Count_Pupil_Premium_Not_Stated',
                'f.Count_Bikes_Recycled',
                'f.Count_Bikes_Swapped',
                'f.Count_Adults',
                'f.Count_Children',
                'dh.Fleet_Cycles_Used',

                // Internal fields used by the chunk loop (unset before fputcsv)
                'c.Source_Course_Id',
                'c.Has_Confirmed_Booked',
                'c.Booked as Booked_Raw',
                'f.Count_Booked as Count_Booked_Raw',
                'f.Count_Attended as Count_Attended_Raw',
                'f.Riders_Enrolled_Count',
                'f.Riders_Completed_Count',
            ])
            ->whereNull('dh.Source_Deleted_At')
            ->whereNull('c.parent_course_key');

        $fy = $params['financial_year'] ?? $params['year'] ?? null;
        if (!empty($fy)) {
            $startYear = is_numeric($fy) ? (int)$fy : (int)substr((string)$fy, 0, 4);
            $query->where('g.Grant_Period_Start_Year', $startYear);
        }

        if (!empty($params['recipient_id'])) {
            $query->where('gr.Source_Recipient_Id', $params['recipient_id']);
        }
        if (!empty($params['grant_id'])) {
            $query->where('g.Source_Grant_Id', $params['grant_id']);
        }
        if (!empty($params['provider_id'])) {
            $query->where('tp.Source_Provider_Id', $params['provider_id']);
        }

        return $query->orderBy('f.Delivery_Key')->orderBy('f.Course_Key');
    }
}

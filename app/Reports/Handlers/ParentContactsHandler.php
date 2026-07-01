<?php

namespace App\Reports\Handlers;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Validator;

class ParentContactsHandler extends AbstractStreamingReportHandler
{
    /**
     * Validate incoming routing parameters.
     */
    public function validate(array $parameters): array
    {
        return Validator::make($parameters, [
            'start_date' => 'required|date_format:Y-m-d',
            'end_date'   => 'required|date_format:Y-m-d',
        ])->validate();
    }

    /**
     * Compile and chunk data back to your application source endpoint.
     */
    public function execute(array $params): array
    {
        ini_set('memory_limit', '1024M');
        ini_set('max_execution_time', '600');

        $query = $this->buildQuery($params);

        // Synchronous track fallback if no callback URL is provided
        if (empty($this->callbackUrl)) {
            return $query->get()->map(fn($row) => (array)$row)->toArray();
        }

        // Asynchronous batch streaming loop
        $chunkSize = 1000;

        $query->chunk($chunkSize, function ($rows) {
            $chunkArray = $rows->map(fn($row) => [
                'Primary Parent First Name'   => $row->Primary_Parent_First_Name,
                'Primary Parent Last Name'    => $row->Primary_Parent_Last_Name,
                'Primary Parent Email'        => $row->Primary_Parent_Email,
                'Secondary Parent First Name' => $row->Secondary_Parent_First_Name,
                'Secondary Parent Last Name'  => $row->Secondary_Parent_Last_Name,
                'Secondary Parent Email'      => $row->Secondary_Parent_Email,
                'Date Added'                  => !empty($row->Created_At) ? date('d/m/Y @ H:i', strtotime($row->Created_At)) : '',
                'Course'                      => 'tbc',
                'Child Forename'              => $row->Child_First_Name,
            ])->toArray();

            // Push the payload chunk back across the network layout
            $this->transmitBatch($chunkArray, false);
        });

        // Close out the remote file handle completely with the trailing EOF envelope
        $this->transmitBatch([], true);

        return ['status' => 'async_completed'];
    }

    /**
     * Map the query strategy directly into your star schema dimensions tables.
     */
    protected function buildQuery(array $params)
    {
        return DB::connection('mysql')->table('Dim_Consent as c')
            ->join('Dim_Rider as r', 'c.Rider_Key', '=', 'r.Rider_Key')
            ->select([
                'c.Primary_Parent_First_Name',
                'c.Primary_Parent_Last_Name',
                'c.Primary_Parent_Email',
                'c.Secondary_Parent_First_Name',
                'c.Secondary_Parent_Last_Name',
                'c.Secondary_Parent_Email',
                'c.Created_At',
                'g.Grant_Label',
                'r.First_Name as Child_First_Name'
            ])
            ->where('c.Pref_Receive_News', 1)
//            ->where('c.Is_Current', 1) // Ensures historical SCD tracking versions are skipped
            ->whereBetween('c.Created_At', [$params['start_date'] . ' 00:00:00', $params['end_date'] . ' 23:59:59'])
            ->orderBy('c.Consent_Key');
    }
}

<?php

namespace App\Reports\Handlers;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Validator;

class GrantClaimSendAndInclusionHandler extends AbstractStreamingReportHandler
{
    public function validate(array $parameters): array
    {
        return Validator::make($parameters, [
            'grant_source' => 'nullable|string',
            'financial_year' => 'nullable|integer',
        ])->validate();
    }

    /**
     * Runs query selections and streams rows chunk-by-chunk to the source server.
     */
    public function execute(array $params): array
    {
        ini_set('memory_limit', '512M');
        ini_set('max_execution_time', '600');

        $query = $this->buildQuery($params);

        if (empty($this->callbackUrl)) {
            return $query->get()->map(fn($row) => $this->mapRowToPayload($row))->toArray();
        }

        $chunkSize = 250;

        $query->chunk($chunkSize, function ($rows) {
            $chunkArray = $rows->map(function ($row) {
                return $this->mapRowToPayload($row);
            })->toArray();

            $this->transmitBatch($chunkArray, false);
        });

        // Trigger closing handshake EOF packet
        $this->transmitBatch([], true);

        return ['status' => 'async_completed'];
    }

    /**
     * Formats database attributes and safely compiles pre-aggregated text elements.
     */
    protected function mapRowToPayload($row): array
    {
        $sendAmount      = (float)$row->Send_Claimable_Amount;
        $inclusionAmount = (float)$row->Inclusion_Claimable_Amount;

        // Clean up SQL trailing separation characters
        $sendDeliveries   = !empty($row->send_deliveries_compiled) ? rtrim($row->send_deliveries_compiled, ' |') : '';
        $inclusionReasons = !empty($row->inclusion_reasons_compiled) ? rtrim($row->inclusion_reasons_compiled, ' |') : '';
        $categoriesList   = !empty($row->categories_compiled) ? $row->categories_compiled : '';

        return [
            $row->Grant_Number,
            $row->Claim_Number,
            $row->Recipient_Name ?? 'No recipient provided',
            $sendAmount,
            $sendDeliveries,
            $inclusionAmount,
            $categoriesList,
            $inclusionReasons,
            ($sendAmount + $inclusionAmount)
        ];
    }

    /**
     * Query directly from child tables using GROUP_CONCAT to normalize rows safely.
     */
    protected function buildQuery(array $params)
    {
        // 1. Group and concatenate the child SEND table rows per Claim_Key
        $sendSubquery = DB::connection('mysql')->table('Fact_Grant_Claim_Send_Records')
            ->select('Claim_Key')
            ->selectRaw("GROUP_CONCAT(CONCAT('Delivery raw - £', Send_Amount, ' - ', Send_Riders_Count, ' Riders - No details given') SEPARATOR ' | ') as send_deliveries_compiled")
            ->groupBy('Claim_Key');

        // 2. Group and concatenate the child Inclusion categories and details per Claim_Key
        $inclusionSubquery = DB::connection('mysql')->table('Fact_Grant_Claim_Inclusions')
            ->select('Claim_Key')
            ->selectRaw("GROUP_CONCAT(Inclusion_Category SEPARATOR ',') as categories_compiled")
            ->selectRaw("GROUP_CONCAT(CONCAT(Inclusion_Category, ' - £', Inclusion_Amount, ' - ', COALESCE(Inclusion_Delivery, 'No delivery provided'), ' - No details given') SEPARATOR ' | ') as inclusion_reasons_compiled")
            ->groupBy('Claim_Key');

        // 3. Construct main structural query tracking criteria rules
        $query = DB::connection('mysql')->table('Fact_Grant_Claims as c')
            ->join('Dim_Grant as g', 'c.Grant_Key', '=', 'g.Grant_Key')
            ->join('Dim_Grant_Recipient as gr', 'g.Grant_Recipient_Key', '=', 'gr.Recipient_Key')
            ->leftJoinSub($sendSubquery, 's_sub', 'c.Claim_Key', '=', 's_sub.Claim_Key')
            ->leftJoinSub($inclusionSubquery, 'i_sub', 'c.Claim_Key', '=', 'i_sub.Claim_Key')
            ->select([
                'g.Grant_Number',
                'c.Claim_Number',
                'gr.Recipient_Name',
                'c.Send_Claimable_Amount',
                'c.Inclusion_Claimable_Amount',
                's_sub.send_deliveries_compiled',
                'i_sub.categories_compiled',
                'i_sub.inclusion_reasons_compiled'
            ])
            ->where('c.Status_Raw', 1);

        // Filter on selected financial year
        if (!empty($params['financial_year'])) {
            $query->where('g.Grant_Period_Start_Year', (int)$params['financial_year']);
        }

        // Filter the grant source
        if (!empty($params['grant_source'])) {
            $query->where('g.Grant_Source', $params['grant_source']);
        }

        return $query->orderBy('c.Claim_Number');
    }
}

<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    protected $connection = 'mysql';

    /**
     * Run the migrations.
     */
    public function up(): void
    {
        ini_set('memory_limit', '1024M');
        ini_set('max_execution_time', '600');

        $dwh = DB::connection($this->connection);
        $src = DB::connection('mysql_src');

        // -------------------------------------------------------------
        // Step 1: Schema Updates (Rename created_at/updated_at and add Source_Deleted_At)
        // -------------------------------------------------------------

        // 1. Fact_Grant_Claims
        Schema::connection($this->connection)->table('Fact_Grant_Claims', function (Blueprint $table) {
            $table->renameColumn('created_at', 'Source_Created_At');
            $table->renameColumn('updated_at', 'Source_Updated_At');
            $table->timestamp('Source_Deleted_At')->nullable()->after('Inclusion_Claimable_Amount');
        });

        // 2. Fact_Grant_Claim_Send_Records
        Schema::connection($this->connection)->table('Fact_Grant_Claim_Send_Records', function (Blueprint $table) {
            $table->renameColumn('created_at', 'Source_Created_At');
            $table->renameColumn('updated_at', 'Source_Updated_At');
            $table->timestamp('Source_Deleted_At')->nullable()->after('Send_Amount');
        });

        // 3. Fact_Grant_Claim_Inclusions
        Schema::connection($this->connection)->table('Fact_Grant_Claim_Inclusions', function (Blueprint $table) {
            $table->renameColumn('created_at', 'Source_Created_At');
            $table->renameColumn('updated_at', 'Source_Updated_At');
            $table->timestamp('Source_Deleted_At')->nullable()->after('Inclusion_Amount');
        });

        // -------------------------------------------------------------
        // Step 2: Backfill Fact_Grant_Claims from operational source (grant_claims)
        // -------------------------------------------------------------
        $src->table('grant_claims')
            ->select(['id', 'created_at', 'updated_at', 'deleted_at'])
            ->orderBy('id')
            ->chunk(1000, function ($claims) use ($dwh) {
                foreach ($claims as $claim) {
                    $createdAt = $claim->created_at ?? $claim->updated_at ?? now()->toDateTimeString();
                    $updatedAt = $claim->updated_at ?? $createdAt;

                    $dwh->table('Fact_Grant_Claims')
                        ->where('Source_Claim_Id', $claim->id)
                        ->update([
                            'Source_Created_At' => $createdAt,
                            'Source_Updated_At' => $updatedAt,
                            'Source_Deleted_At' => $claim->deleted_at,
                        ]);
                }
            });

        // -------------------------------------------------------------
        // Step 3: Propagate parent timestamps to Send Records & Inclusions
        // -------------------------------------------------------------
        $dwh->statement("
            UPDATE Fact_Grant_Claim_Send_Records s
            JOIN Fact_Grant_Claims c ON s.Claim_Key = c.Claim_Key
            SET
                s.Source_Created_At = c.Source_Created_At,
                s.Source_Updated_At = c.Source_Updated_At,
                s.Source_Deleted_At = c.Source_Deleted_At
            WHERE c.Source_Created_At IS NOT NULL
        ");

        $dwh->statement("
            UPDATE Fact_Grant_Claim_Inclusions i
            JOIN Fact_Grant_Claims c ON i.Claim_Key = c.Claim_Key
            SET
                i.Source_Created_At = c.Source_Created_At,
                i.Source_Updated_At = c.Source_Updated_At,
                i.Source_Deleted_At = c.Source_Deleted_At
            WHERE c.Source_Created_At IS NOT NULL
        ");
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::connection($this->connection)->table('Fact_Grant_Claim_Inclusions', function (Blueprint $table) {
            $table->dropColumn('Source_Deleted_At');
            $table->renameColumn('Source_Created_At', 'created_at');
            $table->renameColumn('Source_Updated_At', 'updated_at');
        });

        Schema::connection($this->connection)->table('Fact_Grant_Claim_Send_Records', function (Blueprint $table) {
            $table->dropColumn('Source_Deleted_At');
            $table->renameColumn('Source_Created_At', 'created_at');
            $table->renameColumn('Source_Updated_At', 'updated_at');
        });

        Schema::connection($this->connection)->table('Fact_Grant_Claims', function (Blueprint $table) {
            $table->dropColumn('Source_Deleted_At');
            $table->renameColumn('Source_Created_At', 'created_at');
            $table->renameColumn('Source_Updated_At', 'updated_at');
        });
    }
};

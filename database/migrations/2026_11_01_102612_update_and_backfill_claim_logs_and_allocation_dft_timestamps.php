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
        // Step 1: Schema Updates
        // -------------------------------------------------------------

        // 1. Fact_Grant_Claim_Logs
        Schema::connection($this->connection)->table('Fact_Grant_Claim_Logs', function (Blueprint $table) {
            $table->renameColumn('created_at', 'Source_Created_At');
            $table->renameColumn('updated_at', 'Source_Updated_At');
            $table->timestamp('Source_Deleted_At')->nullable()->after('Item_Count');
        });

        // 2. Fact_Grant_Allocation_Dft
        Schema::connection($this->connection)->table('Fact_Grant_Allocation_Dft', function (Blueprint $table) {
            $table->renameColumn('created_at', 'Source_Created_At');
            $table->renameColumn('updated_at', 'Source_Updated_At');
            $table->timestamp('Source_Deleted_At')->nullable()->after('Places_Inclusion');
        });

        // -------------------------------------------------------------
        // Step 2: Backfill Fact_Grant_Claim_Logs from parent Fact_Grant_Claims
        // -------------------------------------------------------------
        $dwh->statement("
            UPDATE Fact_Grant_Claim_Logs l
            JOIN Fact_Grant_Claims c ON l.Claim_Key = c.Claim_Key
            SET
                l.Source_Created_At = c.Source_Created_At,
                l.Source_Updated_At = c.Source_Updated_At,
                l.Source_Deleted_At = c.Source_Deleted_At
            WHERE c.Source_Created_At IS NOT NULL
        ");

        // -------------------------------------------------------------
        // Step 3: Backfill Fact_Grant_Allocation_Dft from operational source (grant_format_dft)
        // -------------------------------------------------------------
        $src->table('grant_format_dft')
            ->select(['id', 'created_at', 'updated_at', 'deleted_at'])
            ->orderBy('id')
            ->chunk(1000, function ($allocations) use ($dwh) {
                foreach ($allocations as $row) {
                    $createdAt = $row->created_at ?? $row->updated_at ?? now()->toDateTimeString();
                    $updatedAt = $row->updated_at ?? $createdAt;

                    $dwh->table('Fact_Grant_Allocation_Dft')
                        ->where('Source_Allocation_Id', $row->id)
                        ->update([
                            'Source_Created_At' => $createdAt,
                            'Source_Updated_At' => $updatedAt,
                            'Source_Deleted_At' => $row->deleted_at,
                        ]);
                }
            });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::connection($this->connection)->table('Fact_Grant_Allocation_Dft', function (Blueprint $table) {
            $table->dropColumn('Source_Deleted_At');
            $table->renameColumn('Source_Created_At', 'created_at');
            $table->renameColumn('Source_Updated_At', 'updated_at');
        });

        Schema::connection($this->connection)->table('Fact_Grant_Claim_Logs', function (Blueprint $table) {
            $table->dropColumn('Source_Deleted_At');
            $table->renameColumn('Source_Created_At', 'created_at');
            $table->renameColumn('Source_Updated_At', 'updated_at');
        });
    }
};

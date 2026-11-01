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

        // 1. Fact_Grant_Amendments
        Schema::connection($this->connection)->table('Fact_Grant_Amendments', function (Blueprint $table) {
            $table->renameColumn('created_at', 'Source_Created_At');
            $table->renameColumn('updated_at', 'Source_Updated_At');
            $table->timestamp('Source_Deleted_At')->nullable()->after('Date_Approved');
        });

        // 2. Fact_Grant_Amendment_Logs
        Schema::connection($this->connection)->table('Fact_Grant_Amendment_Logs', function (Blueprint $table) {
            $table->renameColumn('created_at', 'Source_Created_At');
            $table->renameColumn('updated_at', 'Source_Updated_At');
            $table->timestamp('Source_Deleted_At')->nullable()->after('Amount');
        });

        // 3. Fact_Grant_Reallocation_Logs
        Schema::connection($this->connection)->table('Fact_Grant_Reallocation_Logs', function (Blueprint $table) {
            $table->timestamp('Source_Deleted_At')->nullable()->after('Source_Updated_At');
        });

        // -------------------------------------------------------------
        // Step 2: Backfill Fact_Grant_Amendments from operational source (grant_amendments)
        // -------------------------------------------------------------
        $src->table('grant_amendments')
            ->select(['id', 'created_at', 'updated_at', 'deleted_at'])
            ->orderBy('id')
            ->chunk(1000, function ($amendments) use ($dwh) {
                foreach ($amendments as $row) {
                    $createdAt = $row->created_at ?? $row->updated_at ?? now()->toDateTimeString();
                    $updatedAt = $row->updated_at ?? $createdAt;

                    $dwh->table('Fact_Grant_Amendments')
                        ->where('Source_Amendment_Id', $row->id)
                        ->update([
                            'Source_Created_At' => $createdAt,
                            'Source_Updated_At' => $updatedAt,
                            'Source_Deleted_At' => $row->deleted_at,
                        ]);
                }
            });

        // -------------------------------------------------------------
        // Step 3: Propagate parent timestamps to Fact_Grant_Amendment_Logs
        // -------------------------------------------------------------
        $dwh->statement("
            UPDATE Fact_Grant_Amendment_Logs l
            JOIN Fact_Grant_Amendments a ON l.Amendment_Key = a.Amendment_Key
            SET
                l.Source_Created_At = a.Source_Created_At,
                l.Source_Updated_At = a.Source_Updated_At,
                l.Source_Deleted_At = a.Source_Deleted_At
            WHERE a.Source_Created_At IS NOT NULL
        ");

        // -------------------------------------------------------------
        // Step 4: Propagate Source_Deleted_At to Fact_Grant_Reallocation_Logs
        // -------------------------------------------------------------
        $dwh->statement("
            UPDATE Fact_Grant_Reallocation_Logs rl
            JOIN Fact_Grant_Reallocations r ON rl.Reallocation_Key = r.Reallocation_Key
            SET
                rl.Source_Deleted_At = r.Source_Deleted_At
            WHERE r.Source_Deleted_At IS NOT NULL
        ");
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::connection($this->connection)->table('Fact_Grant_Reallocation_Logs', function (Blueprint $table) {
            $table->dropColumn('Source_Deleted_At');
        });

        Schema::connection($this->connection)->table('Fact_Grant_Amendment_Logs', function (Blueprint $table) {
            $table->dropColumn('Source_Deleted_At');
            $table->renameColumn('Source_Created_At', 'created_at');
            $table->renameColumn('Source_Updated_At', 'updated_at');
        });

        Schema::connection($this->connection)->table('Fact_Grant_Amendments', function (Blueprint $table) {
            $table->dropColumn('Source_Deleted_At');
            $table->renameColumn('Source_Created_At', 'created_at');
            $table->renameColumn('Source_Updated_At', 'updated_at');
        });
    }
};

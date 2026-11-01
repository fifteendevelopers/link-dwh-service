<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    /**
     * Run the migrations.
     */
    public function up(): void
    {
        Schema::table('Fact_Grant_Reallocations', function (Blueprint $table) {
            // Rename timestamp columns to match DWH source metadata conventions
            $table->renameColumn('created_at', 'Source_Created_At');
            $table->renameColumn('updated_at', 'Source_Updated_At');

            // Add soft delete tracking column
            $table->timestamp('Source_Deleted_At')->nullable()->after('Date_Approved');
        });

        Schema::table('Fact_Grant_Reallocation_Logs', function (Blueprint $table) {
            // Rename timestamp columns to match DWH source metadata conventions
            $table->renameColumn('created_at', 'Source_Created_At');
            $table->renameColumn('updated_at', 'Source_Updated_At');

        });

        ini_set('memory_limit', '1024M');
        ini_set('max_execution_time', '600');

        $dwh = DB::connection($this->connection);
        $src = DB::connection('mysql_src');

        // -------------------------------------------------------------
        // Phase 1: Backfill Fact_Grant_Reallocation from operational source
        // -------------------------------------------------------------
        $src->table('grant_reallocations')
            ->select(['id', 'created_at', 'updated_at', 'deleted_at'])
            ->orderBy('id')
            ->chunk(1000, function ($sourceRows) use ($dwh) {
                foreach ($sourceRows as $row) {
                    $createdAt = $row->created_at ?? $row->updated_at ?? now()->toDateTimeString();
                    $updatedAt = $row->updated_at ?? $createdAt;

                    $dwh->table('Fact_Grant_Reallocations')
                        ->where('Source_Reallocation_Id', $row->id)
                        ->update([
                            'Source_Created_At' => $createdAt,
                            'Source_Updated_At' => $updatedAt,
                            'Source_Deleted_At' => $row->deleted_at,
                        ]);
                }
            });

        // -------------------------------------------------------------
        // Phase 2: Propagate parent timestamps to Fact_Grant_Reallocation_Logs
        // -------------------------------------------------------------
        // Executes via an indexed SQL JOIN across tables within the DWH connection
        $dwh->statement("
            UPDATE Fact_Grant_Reallocation_Logs l
            JOIN Fact_Grant_Reallocations r ON l.Reallocation_Key = r.Reallocation_Key
            SET
                l.Source_Created_At = r.Source_Created_At,
                l.Source_Updated_At = r.Source_Updated_At
            WHERE r.Source_Created_At IS NOT NULL
        ");

    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::table('Fact_Grant_Reallocations', function (Blueprint $table) {
            // Drop the added soft-delete tracking column
            $table->dropColumn('Source_Deleted_At');

            // Revert column names
            $table->renameColumn('Source_Created_At', 'created_at');
            $table->renameColumn('Source_Updated_At', 'updated_at');
        });

        Schema::table('Fact_Grant_Reallocation_Logs', function (Blueprint $table) {
            // Revert column names
            $table->renameColumn('Source_Created_At', 'created_at');
            $table->renameColumn('Source_Updated_At', 'updated_at');
        });
    }
};

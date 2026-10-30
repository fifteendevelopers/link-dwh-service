<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    /**
     * Run the migrations.
     */
    public function up(): void
    {
        // 1. Rename table if needed
        if (Schema::hasTable('Fact_Teacher_Trainer_Delivery') && !Schema::hasTable('Dim_Teacher_Trainer_Delivery')) {
            Schema::rename('Fact_Teacher_Trainer_Delivery', 'Dim_Teacher_Trainer_Delivery');
        }

        // 2. Safely remove any existing foreign key on Teacher_Trainer_Key
        $existingFk = DB::table('information_schema.KEY_COLUMN_USAGE')
            ->where('TABLE_SCHEMA', DB::getDatabaseName())
            ->where('TABLE_NAME', 'Dim_Teacher_Trainer_Delivery')
            ->where('COLUMN_NAME', 'Teacher_Trainer_Key')
            ->whereNotNull('REFERENCED_TABLE_NAME')
            ->value('CONSTRAINT_NAME');

        if ($existingFk) {
            Schema::table('Dim_Teacher_Trainer_Delivery', function (Blueprint $table) use ($existingFk) {
                $table->dropForeign($existingFk);
            });
        }

        // 3. Add Source_Deleted_At and drop Teacher_Trainer_Key column
        Schema::table('Dim_Teacher_Trainer_Delivery', function (Blueprint $table) {
            if (!Schema::hasColumn('Dim_Teacher_Trainer_Delivery', 'Source_Deleted_At')) {
                $table->timestamp('Source_Deleted_At')->nullable()->after('Notes');
            }

            if (Schema::hasColumn('Dim_Teacher_Trainer_Delivery', 'Teacher_Trainer_Key')) {
                $table->dropColumn('Teacher_Trainer_Key');
            }
        });

        // 4. Create Bridge Fact Table: Fact_Delivery_Teacher_Trainer
        if (!Schema::hasTable('Fact_Deliveries_Teacher_Trainers')) {
            Schema::create('Fact_Deliveries_Teacher_Trainers', function (Blueprint $table) {
                $table->id('Delivery_Teacher_Trainer_Key');
                $table->unsignedBigInteger('TT_Delivery_Key');
                $table->unsignedBigInteger('Teacher_Trainer_Key');
                $table->unsignedBigInteger('Source_TT_Delivery_Id');
                $table->unsignedBigInteger('Source_Teacher_Trainer_Id');
                $table->integer('Source_System_Key')->default(1);
                $table->date('Source_Deleted_At')->nullable();
                $table->timestamps();

                // Explicit short index names (< 64 chars)
                $table->index('TT_Delivery_Key', 'idx_fdtt_tt_del_key');
                $table->index('Teacher_Trainer_Key', 'idx_fdtt_tt_key');
                $table->index('Source_TT_Delivery_Id', 'idx_fdtt_src_del_id');
                $table->index('Source_Teacher_Trainer_Id', 'idx_fdtt_src_tt_id');

                // Unique constraint
                $table->unique(
                    ['TT_Delivery_Key', 'Teacher_Trainer_Key', 'Source_System_Key'],
                    'uniq_fdtt_del_tt_sys'
                );

                // Foreign keys with short explicit names
                $table->foreign('TT_Delivery_Key', 'fk_fdtt_tt_del_key')
                    ->references('TT_Delivery_Key')
                    ->on('Dim_Teacher_Trainer_Delivery')
                    ->onDelete('cascade');

                $table->foreign('Teacher_Trainer_Key', 'fk_fdtt_tt_trn_key')
                    ->references('Teacher_Trainer_Key')
                    ->on('Dim_Teacher_Trainer')
                    ->onDelete('cascade');
            });
        }
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::dropIfExists('Fact_Deliveries_Teacher_Trainers');

        Schema::table('Dim_Teacher_Trainer_Delivery', function (Blueprint $table) {
            if (Schema::hasColumn('Dim_Teacher_Trainer_Delivery', 'Source_Deleted_At')) {
                $table->dropColumn('Source_Deleted_At');
            }
        });

        if (Schema::hasTable('Dim_Teacher_Trainer_Delivery') && !Schema::hasTable('Fact_Teacher_Trainer_Delivery')) {
            Schema::rename('Dim_Teacher_Trainer_Delivery', 'Fact_Teacher_Trainer_Delivery');
        }
    }
};

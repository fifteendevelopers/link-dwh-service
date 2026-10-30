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
        // Dim_Delivery_Header: Filter out soft-deleted headers cleanly
        Schema::table('Dim_Delivery_Header', function (Blueprint $table) {
            $table->index(['Delivery_Key', 'Source_Deleted_At'], 'idx_ddh_key_deleted');
        });

        // Dim_Course: Parent-child filter + Source_Course_Id projection
        Schema::table('Dim_Course', function (Blueprint $table) {
            $table->index(['Course_Key', 'Parent_Course_Key', 'Source_Course_Id'], 'idx_dc_key_parent_src');
        });

        // Dim_Grant: Year filtering + Recipient join
        Schema::table('Dim_Grant', function (Blueprint $table) {
            $table->index(['Grant_Period_Start_Year', 'Grant_Key', 'Grant_Recipient_Key'], 'idx_dg_year_keys');
        });

        // Fact_Course_Delivery: Composite key for joined filtering
        Schema::table('Fact_Course_Delivery', function (Blueprint $table) {
            $table->index(['Grant_Key', 'Delivery_Key', 'Course_Key'], 'idx_fcd_grant_deliv_course');
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::table('Dim_Delivery_Header', function (Blueprint $table) {
            $table->dropIndex('idx_ddh_key_deleted');
        });

        Schema::table('Dim_Course', function (Blueprint $table) {
            $table->dropIndex('idx_dc_key_parent_src');
        });

        Schema::table('Dim_Grant', function (Blueprint $table) {
            $table->dropIndex('idx_dg_year_keys');
        });

        Schema::table('Fact_Course_Delivery', function (Blueprint $table) {
            $table->dropIndex('idx_fcd_grant_deliv_course');
        });
    }
};

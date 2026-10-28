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
        Schema::table('Dim_Course', function (Blueprint $table) {
            // Accelerates joins with Dim_Delivery_Header and Fact tables
            if (!Schema::hasIndex('Dim_Course', 'idx_dim_course_delivery_key')) {
                $table->index('Delivery_Key', 'idx_dim_course_delivery_key');
            }

            // Accelerates Source_Course_Id lookups and IS NOT NULL checks
            if (!Schema::hasIndex('Dim_Course', 'idx_dim_course_source_course_id')) {
                $table->index('Source_Course_Id', 'idx_dim_course_source_course_id');
            }

            // Accelerates parent-child course hierarchy resolution and whereNull('Parent_Course_Key')
            if (!Schema::hasIndex('Dim_Course', 'idx_dim_course_parent_key')) {
                $table->index('Parent_Course_Key', 'idx_dim_course_parent_key');
            }

            // Accelerates non-digitised module lookups and upserts
            if (!Schema::hasIndex('Dim_Course', 'idx_dim_course_lookup_composite')) {
                $table->index(['Delivery_Key', 'Course_Level', 'Year_Group'], 'idx_dim_course_lookup_composite');
            }
        });

        // 2. Indexes for Dim_Delivery_Header
        Schema::connection('mysql')->table('Dim_Delivery_Header', function (Blueprint $table) {
            // Accelerates digitisation filtering across audit and report handlers
            if (!Schema::hasIndex('Dim_Delivery_Header', 'idx_dim_deliv_digitisation')) {
                $table->index('Digitisation_Booking', 'idx_dim_deliv_digitisation');
            }

            // Accelerates source delivery lookups
            if (!Schema::hasIndex('Dim_Delivery_Header', 'idx_dim_deliv_source_id')) {
                $table->index('Source_Delivery_Id', 'idx_dim_deliv_source_id');
            }

            // Accelerates date range filtering for financial year reporting
            if (!Schema::hasIndex('Dim_Delivery_Header', 'idx_dim_deliv_start_date')) {
                $table->index('Date_Delivery_Start', 'idx_dim_deliv_start_date');
            }
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::table('Dim_Course', function (Blueprint $table) {
            $table->dropIndex('idx_dim_course_delivery_key');
            $table->dropIndex('idx_dim_course_source_course_id');
            $table->dropIndex('idx_dim_course_parent_key');
            $table->dropIndex('idx_dim_course_lookup_composite');
        });

        Schema::table('Dim_Delivery_Header', function (Blueprint $table) {
            $table->dropIndex('idx_dim_deliv_digitisation');
            $table->dropIndex('idx_dim_deliv_source_id');
            $table->dropIndex('idx_dim_deliv_start_date');
        });
    }
};

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

        Schema::table('Fact_Instructor_Delivery', function (Blueprint $table) {
            $table->index(['Source_System_Key', 'Delivery_Key', 'Instructor_Key'], 'idx_fid_sys_deliv_inst');
        });

        Schema::table('Dim_Delivery_Header', function (Blueprint $table) {
            $table->index(['Source_Delivery_Id', 'Source_System_Key'], 'idx_ddh_source_lookup');
            $table->index(['Delivery_Key', 'Source_Deleted_At', 'Source_Delivery_Id'], 'idx_ddh_key_del_src');
        });

        Schema::table('Fact_Rider_Course', function (Blueprint $table) {
            $table->index(['Source_Course_Id', 'Source_System_Key', 'Attended'], 'idx_frc_course_attended');
        });

        Schema::table('Fact_Course_Delivery', function (Blueprint $table) {
            $table->unique(['Delivery_Key', 'Course_Key'], 'idx_fcd_delivery_course');
        });

        Schema::table('Dim_Instructor', function (Blueprint $table) {
            $table->index(['Instructor_Key', 'Source_Instructor_Id'], 'idx_di_key_src');
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {

        Schema::table('Dim_Instructor', function (Blueprint $table) {
            $table->dropIndex('idx_di_key_src');
        });

        Schema::table('Fact_Course_Delivery', function (Blueprint $table) {
            $table->dropUnique('idx_fcd_delivery_course');
            $table->dropIndex('idx_ddh_key_del_src');
        });

        Schema::table('Fact_Rider_Course', function (Blueprint $table) {
            $table->dropIndex('idx_frc_course_attended');
        });

        Schema::table('Dim_Delivery_Header', function (Blueprint $table) {
            $table->dropIndex('idx_ddh_source_lookup');
        });

        Schema::table('Fact_Instructor_Delivery', function (Blueprint $table) {
            $table->dropIndex('idx_fid_sys_deliv_inst');
        });

    }
};

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
            $table->renameColumn('Provisional_Booked','Provisional');
        });

        Schema::table('Fact_Course_Delivery', function (Blueprint $table) {
            $table->renameColumn('Count_Booked_Provisional','Count_Provisional');
            $table->renameColumn('Count_Booked_Confirmed','Count_Booked');
            $table->renameColumn('Count_Attended_Confirmed','Count_Attended');
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::table('Dim_Course', function (Blueprint $table) {

            $table->renameColumn('Provisional','Provisional_Booked');
        });

        Schema::table('Fact_Course_Delivery', function (Blueprint $table) {
            $table->renameColumn('Count_Provisional','Count_Booked_Provisional');
            $table->renameColumn('Count_Booked','Count_Booked_Confirmed');
            $table->renameColumn('Count_Attended','Count_Attended_Confirmed');
        });

    }
};

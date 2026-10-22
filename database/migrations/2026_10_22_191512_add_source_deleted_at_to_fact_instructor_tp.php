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
        Schema::table('Fact_Instructor_Training_Provider', function (Blueprint $table) {
            $table->timestamp('Source_Deleted_At')->nullable()->after('Dbs_Certification_Date');
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::table('Fact_Instructor_Training_Provider', function (Blueprint $table) {
            //
        });
    }
};

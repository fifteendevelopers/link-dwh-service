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
        Schema::create('Dim_Course_Activity', function (Blueprint $table) {
            $table->id('Activity_Key');

            // Link to source system ID (course_activities_lookup.id)
            $table->unsignedBigInteger('Source_Activity_Id')->unique('uk_source_activity');

            $table->string('Course_Code', 45)->nullable()->index('idx_course_code');
            $table->string('Activity_Code', 45)->nullable()->index('idx_activity_code');
            $table->string('Activity_Label', 250)->nullable();
            $table->text('Activity_Description')->nullable();
            $table->unsignedBigInteger('Outcomes_Lookup_Id')->nullable();

            $table->timestamps();
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::dropIfExists('Dim_Course_Activity');
    }
};

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
        Schema::create('Fact_Rider_Activity_Outcome', function (Blueprint $table) {
            $table->id('Outcome_Fact_Key');

            // Foreign Dimensional Surrogate Keys
            $table->unsignedBigInteger('Delivery_Key')->nullable()->index('idx_delivery_key');
            $table->unsignedBigInteger('Course_Key')->nullable()->index('idx_course_key');
            $table->unsignedBigInteger('Rider_Key')->nullable()->index('idx_rider_key');
            $table->unsignedBigInteger('Activity_Key')->index('idx_activity_key');
            $table->unsignedBigInteger('School_Key')->nullable()->index('idx_school_key');
            $table->unsignedBigInteger('Training_Provider_Key')->nullable()->index('idx_provider_key');
            $table->unsignedBigInteger('Grant_Key')->nullable()->index('idx_grant_key');

            // Outcome Evaluation Values
            $table->unsignedTinyInteger('Outcome_Score')->nullable();
            $table->char('Outcome_Code', 2)->nullable()->comment('Short code: O, P, A, N');
            $table->string('Outcome_Grade', 50)->nullable()->comment('Text grade: On my own, With practice, etc.');

            $table->timestamp('Created_At')->useCurrent();

            // Composite Indexes
            $table->index(['Rider_Key', 'Course_Key'], 'idx_rider_course');
            $table->index(['Delivery_Key', 'Activity_Key'], 'idx_delivery_activity');
            $table->unique(['Course_Key', 'Rider_Key', 'Activity_Key'], 'uk_rider_course_activity_outcome');
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::dropIfExists('Fact_Rider_Activity_Outcome');
    }
};

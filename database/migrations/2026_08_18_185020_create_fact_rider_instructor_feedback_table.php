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
        Schema::create('Fact_Rider_Instructor_Feedback', function (Blueprint $table) {
            $table->id('Feedback_Fact_Key');

            // Dimensional Foreign Keys
            $table->unsignedBigInteger('Delivery_Key')->nullable()->index('idx_delivery_key');
            $table->unsignedBigInteger('Course_Key')->nullable()->index('idx_course_key');
            $table->unsignedBigInteger('Rider_Key')->nullable()->index('idx_rider_key');
            $table->unsignedBigInteger('Instructor_Key')->nullable()->index('idx_instructor_key');
            $table->unsignedBigInteger('Feedback_Lookup_Key')->index('idx_feedback_lookup_key');
            $table->unsignedBigInteger('Grant_Key')->nullable()->index('idx_grant_key');
            $table->unsignedBigInteger('Training_Provider_Key')->nullable()->index('idx_tp_key');
            $table->unsignedBigInteger('School_Key')->nullable()->index('idx_school_key');

            // Attributes
            $table->date('Feedback_Date')->nullable();
            $table->text('Instructor_Notes')->nullable();
            $table->timestamp('Created_At')->useCurrent();

            // Composite Unique Constraint
            $table->unique(['Course_Key', 'Rider_Key', 'Feedback_Lookup_Key'], 'uk_rider_course_feedback');
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::dropIfExists('Fact_Rider_Instructor_Feedback');
    }
};

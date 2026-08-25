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
        // 1. Dim_Get_Cycling_Course (Full sync catalog)
        Schema::create('Dim_Get_Cycling_Course', function (Blueprint $table) {
            $table->id('GC_Course_Key');
            $table->unsignedBigInteger('Source_Course_Id')->index('idx_gcc_source_id');
            $table->string('Source_System_Key', 50)->index('idx_gcc_system_key');

            $table->string('Course_Code', 100)->index('idx_gcc_code');
            $table->string('Course_Name', 255);

            $table->timestamps();
            $table->unique(['Source_Course_Id', 'Source_System_Key'], 'uk_gcc_source_system');
        });

        // 2. Dim_Get_Cycling_Rider (Anonymized demographics)
        Schema::create('Dim_Get_Cycling_Rider', function (Blueprint $table) {
            $table->id('GC_Rider_Key');
            $table->unsignedBigInteger('Source_Rider_Id')->index('idx_gcr_source_id');
            $table->string('Source_System_Key', 50)->index('idx_gcr_system_key');

            $table->unsignedBigInteger('School_Key')->nullable()->index('idx_gcr_school');
            $table->unsignedBigInteger('Teacher_Trainer_Key')->nullable()->index('idx_gcr_trainer');

            $table->string('School_Urn', 50)->nullable()->index('idx_gcr_urn');
            $table->boolean('Archived')->default(0);
            $table->date('Archived_At')->nullable();
            $table->string('Upn', 100)->nullable();
            $table->string('Year_Group', 50)->nullable();
            $table->string('Class_Name', 100)->nullable();
            $table->string('Gender', 50)->nullable();
            $table->string('Ethnicity', 100)->nullable();
            $table->text('Send_Code')->nullable();
            $table->string('Pupil_Premium', 50)->nullable();
            $table->unsignedBigInteger('Uploaded_By_Teacher_Trainer_Id')->nullable();
            $table->date('Last_Activity_Date')->nullable();

            $table->timestamps();
            $table->unique(['Source_Rider_Id', 'Source_System_Key'], 'uk_gcr_source_system');
        });

        // 3. Dim_Get_Cycling_Rider_Note
        Schema::create('Dim_Get_Cycling_Rider_Note', function (Blueprint $table) {
            $table->id('GC_Rider_Note_Key');
            $table->unsignedBigInteger('Source_Note_Id')->index('idx_gcrn_source_id');
            $table->string('Source_System_Key', 50)->index('idx_gcrn_system_key');

            $table->unsignedBigInteger('GC_Rider_Key')->index('idx_gcrn_rider_key');
            $table->unsignedBigInteger('Source_Rider_Id')->index('idx_gcrn_source_rider');
            $table->string('Note', 500);

            $table->timestamps();
            $table->unique(['Source_Note_Id', 'Source_System_Key'], 'uk_gcrn_source_system');
        });

        // 4. Fact_Get_Cycling_Rider_Course (Enrollment & Progress)
        Schema::create('Fact_Get_Cycling_Rider_Course', function (Blueprint $table) {
            $table->id('GC_Rider_Course_Key');
            $table->unsignedBigInteger('Source_Join_Id')->index('idx_gcrc_source_id');
            $table->string('Source_System_Key', 50)->index('idx_gcrc_system_key');

            // Dimension Keys
            $table->unsignedBigInteger('GC_Rider_Key')->index('idx_gcrc_rider_key');
            $table->unsignedBigInteger('GC_Course_Key')->index('idx_gcrc_course_key');
            $table->unsignedBigInteger('Teacher_Trainer_Key')->nullable()->index('idx_gcrc_trainer_key');
            $table->unsignedBigInteger('School_Key')->nullable()->index('idx_gcrc_school_key');

            $table->unsignedBigInteger('Source_Rider_Id')->index('idx_gcrc_source_rider');
            $table->unsignedBigInteger('Source_Course_Id')->index('idx_gcrc_source_course');
            $table->unsignedBigInteger('Source_Teacher_Trainer_Id')->nullable();

            $table->boolean('Does_Not_Wish_To_Continue')->default(0);
            $table->integer('Overall_Progress')->nullable();
            $table->boolean('Has_Survey_Completed')->default(0);

            $table->timestamps();
            $table->unique(['Source_Join_Id', 'Source_System_Key'], 'uk_gcrc_source_system');
        });

        // 5. Fact_Get_Cycling_Rider_Course_Activity (Normalized Activity Scores)
        Schema::create('Fact_Get_Cycling_Rider_Course_Activity', function (Blueprint $table) {
            $table->id('GC_Activity_Key');
            $table->unsignedBigInteger('GC_Rider_Course_Key')->index('idx_gcra_rc_key');
            $table->unsignedBigInteger('Source_Join_Id')->index('idx_gcra_join_id');
            $table->string('Source_System_Key', 50)->index('idx_gcra_system_key');

            $table->unsignedBigInteger('Activity_Id')->index('idx_gcra_activity_id');
            $table->integer('Activity_Score')->default(0);

            $table->timestamps();
            $table->unique(['GC_Rider_Course_Key', 'Activity_Id'], 'uk_gcra_row');
        });

        // 6. Fact_Get_Cycling_Survey_Response (Normalized Survey Results)
        Schema::create('Fact_Get_Cycling_Survey_Response', function (Blueprint $table) {
            $table->id('GC_Survey_Response_Key');
            $table->unsignedBigInteger('Source_Survey_Join_Id')->index('idx_gcsr_source_id');
            $table->string('Source_System_Key', 50)->index('idx_gcsr_system_key');

            // Dimension Keys
            $table->unsignedBigInteger('School_Key')->nullable()->index('idx_gcsr_school');
            $table->unsignedBigInteger('GC_Course_Key')->nullable()->index('idx_gcsr_course');

            $table->string('School_Urn', 50)->index('idx_gcsr_urn');
            $table->unsignedBigInteger('Source_Course_Id')->index('idx_gcsr_src_course');
            $table->string('Question_Id', 50)->index('idx_gcsr_qid');
            $table->string('Option_Id', 50)->index('idx_gcsr_oid');
            $table->integer('Response_Count')->default(0);

            $table->timestamps();
            $table->unique(
                ['Source_Survey_Join_Id', 'Question_Id', 'Option_Id', 'Source_System_Key'],
                'uk_gcsr_response_row'
            );
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::dropIfExists('Fact_Get_Cycling_Survey_Response');
        Schema::dropIfExists('Fact_Get_Cycling_Rider_Course_Activity');
        Schema::dropIfExists('Fact_Get_Cycling_Rider_Course');
        Schema::dropIfExists('Dim_Get_Cycling_Rider_Note');
        Schema::dropIfExists('Dim_Get_Cycling_Rider');
        Schema::dropIfExists('Dim_Get_Cycling_Course');
    }
};

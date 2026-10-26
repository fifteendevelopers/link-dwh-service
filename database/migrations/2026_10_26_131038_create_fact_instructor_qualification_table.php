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
        if (!Schema::hasTable('Dim_Qualification')) {
            Schema::create('Dim_Qualification', function (Blueprint $table) {
                $table->bigIncrements('Qualification_Key');
                $table->string('Qualification_Code', 100)->unique();
                $table->string('Qualification_Name', 255)->nullable();
                $table->timestamps();
            });
        }

        // 2. Create Fact_Instructor_Qualification
        Schema::create('Fact_Instructor_Qualification', function (Blueprint $table) {
            $table->bigIncrements('Instructor_Qual_Fact_Key');
            $table->unsignedBigInteger('Source_Qual_Id')->index();
            $table->unsignedInteger('Source_System_Key')->default(1);

            // Surrogate Foreign Keys
            $table->unsignedBigInteger('Instructor_Key')->index();
            $table->unsignedBigInteger('Qualification_Key')->nullable()->index();
            $table->unsignedBigInteger('Training_Organisation_Key')->nullable()->index();

            // Natural / Degenerate Source Identifiers
            $table->unsignedBigInteger('Source_Instructor_Id')->nullable()->index();
            $table->unsignedBigInteger('Source_Training_Org_Id')->nullable()->index();
            $table->string('Source_Qualification_Code', 100)->nullable();
            $table->string('Event_Number', 255)->nullable();

            // Status and Lifecycle Dates
            $table->string('Status', 100)->nullable();
            $table->date('Latest_Status_Date')->nullable();
            $table->date('Registration_Date')->nullable();
            $table->date('Verified_Lxp_Date')->nullable();
            $table->date('Theory_Complete_Date')->nullable();
            $table->date('Date_Provisional_Complete')->nullable();
            $table->date('Provisional_Email_Sent')->nullable();
            $table->date('Pca_Complete_Date')->nullable();
            $table->date('Qual_Cert_Due_Date')->nullable();
            $table->date('Qualification_Complete_Email_Sent')->nullable();
            $table->date('Withdrawn_Date')->nullable();
            $table->date('Migrated_Email_Sent')->nullable();
            $table->date('Date_Certified')->nullable();
            $table->date('Expiry_Date')->nullable();
            $table->date('Expiry_Warning_Sent')->nullable();

            // Audit Timestamps
            $table->timestamp('Source_Created_At')->nullable();
            $table->timestamp('Source_Updated_At')->nullable();
            $table->timestamps();

            // Composite Constraints for Idempotency and High-Performance Point Upserts
            $table->unique(['Source_Qual_Id', 'Source_System_Key'], 'idx_fiq_source_unique');
            $table->index(['Instructor_Key', 'Qualification_Key'], 'idx_fiq_inst_qual');
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::dropIfExists('Fact_Instructor_Qualification');
    }
};

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
        Schema::create('Dim_Instructor', function (Blueprint $table) {
            $table->id('Instructor_Key'); // DWH Surrogate Key
            $table->unsignedBigInteger('Source_Instructor_Id');
            $table->integer('Source_System_Key');

            // Core Identity Details
            $table->unsignedInteger('Instructor_Number');
            $table->string('Instructor_Type')->nullable();
            $table->string('First_Name');
            $table->string('Last_Name');
            $table->string('Email');
            $table->string('Telephone')->nullable();
            $table->string('Landline')->nullable();

            // Demographics & System Lookup IDs
            $table->unsignedBigInteger('Age_Range_Id')->default(1);
            $table->unsignedBigInteger('Ethnicity_Id')->default(1);
            $table->unsignedBigInteger('Gender_Id')->default(1);
            $table->unsignedBigInteger('Title_Id')->default(1);

            // Address Fields
            $table->string('Address_01')->nullable();
            $table->string('Address_02')->nullable();
            $table->string('City')->nullable();
            $table->string('Postcode')->nullable();

            // Statuses and Flags (Tinyint mapped cleanly to Booleans)
            $table->tinyInteger('Status_Raw')->default(0);
            $table->boolean('Is_Pending')->default(0);
            $table->boolean('Flag_Nsi_Migrated')->default(0);

            // Communication & Program Preferences
            $table->boolean('Pref_Receive_News')->default(0);
            $table->boolean('Pref_Delivering_Bikeability')->default(0);
            $table->boolean('Pref_Delivering_Other')->default(0);
            $table->boolean('Pref_Bursary_Eligibility')->default(0);
            $table->boolean('Has_Received_Bursary')->default(0);

            // Lifecycle Operational Dates
            $table->date('Date_Registered')->nullable();
            $table->date('Date_Renewal')->nullable();
            $table->date('Date_Deregistered')->nullable();
            $table->text('Deregistration_Reason')->nullable();

            // Compliance & Training Milestones (Crucial for CPD tracking reports)
            $table->date('First_Aid_Training_Complete_Date')->nullable();
            $table->date('Safeguarding_Training_Complete_Date')->nullable();
            $table->date('Send_Training_Complete_Date')->nullable();
            $table->boolean('Send_Training_Overridden')->default(0);
            $table->date('Send_Training_Certificate_Download_Date')->nullable();

            $table->text('Account_Notes')->nullable();
            $table->timestamp('Source_Created_At')->nullable();
            $table->timestamp('Source_Updated_At')->nullable();
            $table->timestamps(); // DWH creation track

            // High-performance index structures
            $table->index(['Source_Instructor_Id', 'Source_System_Key']);
            $table->index('Instructor_Number');
            $table->index('Last_Name');

        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::dropIfExists('Dim_Instructor');
    }
};

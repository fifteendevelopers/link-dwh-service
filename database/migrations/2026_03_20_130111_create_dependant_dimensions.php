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

        Schema::connection('mysql')->create('Dim_Organisation', function (Blueprint $table) {
            $table->id('Organisation_Key', true);
            $table->unsignedBigInteger('Source_Organisation_Id');
            $table->unsignedBigInteger('Source_System_Key');
            $table->unsignedBigInteger('Provider_Key');
            $table->string('Organisation_Name', 255);

            // Foreign Key Constraints
            $table->foreign('Source_System_Key')
                ->references('Source_System_Key')
                ->on('Dim_Source_System');

            $table->foreign('Provider_Key')
                ->references('Provider_Key')
                ->on('Dim_Training_Provider');

            $table->index(['Source_Organisation_Id', 'Source_System_Key'], 'idx_org_source');
        });

        Schema::connection('mysql')->create('Dim_Grant', function (Blueprint $table) {
            $table->id('Grant_Key');
            $table->unsignedBigInteger('Source_Grant_Id');
            $table->unsignedBigInteger('Source_System_Key');
            $table->unsignedBigInteger('Grant_Recipient_Key');
            $table->string('Grant_Number', 255);
            $table->string('Grant_Label', 255)->nullable();
            $table->string('Grant_Source', 255)->nullable();
            $table->smallInteger('Grant_Period_Start_Year');

            $table->foreign('Source_System_Key')
                ->references('Source_System_Key')
                ->on('Dim_Source_System');

            $table->foreign('Grant_Recipient_Key')
                ->references('Recipient_Key')
                ->on('Dim_Grant_Recipient');

            $table->index(['Source_Grant_Id', 'Source_System_Key'], 'idx_grant_source');
        });

        Schema::connection('mysql')->create('Dim_Delivery_Header', function (Blueprint $table) {
            $table->id('Delivery_Key');
            $table->unsignedBigInteger('Source_Delivery_Id');
            $table->unsignedBigInteger('Source_System_Key');
            $table->unsignedBigInteger('Grant_Key')->nullable();
            $table->unsignedBigInteger('School_Key')->nullable();
            $table->unsignedBigInteger('Organisation_Key')->nullable();
            $table->unsignedBigInteger('Training_Provider_Key')->nullable();
            $table->string('Delivery_Status', 50);
            $table->date('Date_Delivery_Start')->nullable();
            $table->date('Date_Delivery_End')->nullable();
            $table->tinyInteger('Digitisation_Booking')->default(0);

            $table->foreign('Source_System_Key')->references('Source_System_Key')->on('Dim_Source_System');
            $table->foreign('School_Key')->references('School_Key')->on('Dim_School');
            $table->foreign('Organisation_Key')->references('Organisation_Key')->on('Dim_Organisation');
            $table->foreign('Training_Provider_Key')->references('Provider_Key')->on('Dim_Training_Provider');
        });

        Schema::connection('mysql')
            ->create('Dim_Course', function (Blueprint $table) {
            // Course_Key (Primary Key) - Creates unsignedBigInteger
            $table->id('Course_Key');

            // Source Identifiers
            $table->unsignedBigInteger('Source_Course_Id');
            $table->unsignedBigInteger('Source_System_Key');

            // Link to Delivery Header (Must be unsignedBigInteger)
            $table->unsignedBigInteger('Delivery_Key');

            // Course Details
            $table->string('Course_Level', 45);
            $table->integer('Status')->nullable();
            $table->date('Start_Date')->nullable();
            $table->date('Date_Complete')->nullable();
            $table->string('Year_Group', 45)->nullable();

            // Self-referencing Foreign Key for Hierarchy
            $table->unsignedBigInteger('Parent_Course_Key')->nullable();

            // --- Foreign Key Constraints ---

            $table->foreign('Source_System_Key')
                ->references('Source_System_Key')
                ->on('Dim_Source_System');

            $table->foreign('Delivery_Key')
                ->references('Delivery_Key')
                ->on('Dim_Delivery_Header');

            $table->foreign('Parent_Course_Key')
                ->references('Course_Key')
                ->on('Dim_Course')
                ->onDelete('set null');

            // --- Indices for Performance ---

            $table->index(['Source_Course_Id', 'Source_System_Key'], 'idx_course_source');
        });

        Schema::connection('mysql')->create('Dim_Consent', function (Blueprint $table) {
            $table->id('Consent_Key');
            $table->unsignedBigInteger('Source_Consent_Id');
            $table->unsignedBigInteger('Source_System_Key');
            $table->unsignedBigInteger('Rider_Key');
            $table->unsignedBigInteger('Delivery_Key');
            $table->tinyInteger('Consent_Status')->nullable();
            $table->tinyInteger('Has_Bike')->nullable();

            $table->tinyInteger('Ability_Cannot_Cycle')->default(0)->comment('1: My child cannot yet cycle');
            $table->tinyInteger('Ability_Can_Look_Over_Shoulder')->default(0)->comment('2: Can look over shoulder');
            $table->tinyInteger('Ability_Can_One_Hand_Signal')->default(0)->comment('3: Can signal with one hand');
            $table->tinyInteger('Ability_Has_Level_2')->default(0)->comment('4: Already completed Level 2');

            // We still keep the raw JSON just in case
            $table->text('Cycle_Ability_Raw')->nullable();

            $table->tinyInteger('Is_FSM')->nullable();
            $table->tinyInteger('Is_SEND')->nullable();
            $table->tinyInteger('Has_Medical_Condition')->nullable();
            $table->tinyInteger('Attended')->default(1);
            $table->string('Gender', 50)->nullable();
            $table->string('Ethnicity', 100)->nullable();

            $table->foreign('Source_System_Key')->references('Source_System_Key')->on('Dim_Source_System');
            $table->foreign('Rider_Key')->references('Rider_Key')->on('Dim_Rider');
            $table->foreign('Delivery_Key')->references('Delivery_Key')->on('Dim_Delivery_Header');
            $table->index('Source_Consent_Id', 'idx_consent_source');
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::connection('mysql')->dropIfExists('Dim_Organisation');
        Schema::connection('mysql')->dropIfExists('Dim_Grant');
        Schema::connection('mysql')->dropIfExists('Dim_Delivery_Header');
        Schema::connection('mysql')->dropIfExists('Dim_Course');
        Schema::connection('mysql')->dropIfExists('Dim_Consent');
    }
};

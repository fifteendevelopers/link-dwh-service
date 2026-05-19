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
        // Core Grant Format - e.g from grant_format_dft
        Schema::create('Fact_Grant_Financials', function (Blueprint $table) {
            $table->id('Grant_Financial_Key');
            $table->unsignedBigInteger('Source_Format_Id');
            $table->unsignedBigInteger('Grant_Key'); // Joins to existing Dim_Grant
            $table->integer('Source_System_Key');

            $table->decimal('Max_Allocation', 15, 2)->default(0.00);
            $table->integer('Total_Levels')->default(0);
            $table->integer('Total_Plus')->default(0);

            // Level 1, 1_2, 2, 3
            $table->integer('Places_Level_1')->default(0);
            $table->decimal('Grant_Level_1', 15, 2)->default(0.00);
            $table->integer('Places_Level_1_2')->default(0);
            $table->decimal('Grant_Level_1_2', 15, 2)->default(0.00);
            $table->integer('Places_Level_2')->default(0);
            $table->decimal('Grant_Level_2', 15, 2)->default(0.00);
            $table->integer('Places_Level_3')->default(0);
            $table->decimal('Grant_Level_3', 15, 2)->default(0.00);

            // Core Plus Modules
            $table->integer('Places_Plus_Balance')->default(0);
            $table->decimal('Grant_Plus_Balance', 15, 2)->default(0.00);
            $table->integer('Places_Plus_Bus')->default(0);
            $table->decimal('Grant_Plus_Bus', 15, 2)->default(0.00);
            $table->integer('Places_Plus_Fix')->default(0);
            $table->decimal('Grant_Plus_Fix', 15, 2)->default(0.00);
            $table->integer('Places_Plus_Learn')->default(0);
            $table->decimal('Grant_Plus_Learn', 15, 2)->default(0.00);
            $table->integer('Places_Plus_On_Show')->default(0);
            $table->decimal('Grant_Plus_On_Show', 15, 2)->default(0.00);
            $table->integer('Places_Plus_Parents')->default(0);
            $table->decimal('Grant_Plus_Parents', 15, 2)->default(0.00);
            $table->integer('Places_Plus_Promotion')->default(0);
            $table->decimal('Grant_Plus_Promotion', 15, 2)->default(0.00);
            $table->integer('Places_Plus_Recycled')->default(0);
            $table->decimal('Grant_Plus_Recycled', 15, 2)->default(0.00);
            $table->integer('Places_Plus_Ride')->default(0);
            $table->decimal('Grant_Plus_Ride', 15, 2)->default(0.00);
            $table->integer('Places_Plus_Transition')->default(0);
            $table->decimal('Grant_Plus_Transition', 15, 2)->default(0.00);
            $table->integer('Places_Plus_Family')->default(0);
            $table->decimal('Grant_Plus_Family', 15, 2)->default(0.00);
            $table->integer('Places_Plus_Adult')->default(0);
            $table->decimal('Grant_Plus_Adult', 15, 2)->default(0.00);

            // SEND & Inclusion Adjustments
            $table->decimal('Grant_Send', 15, 2)->default(0.00);
            $table->integer('Places_Send')->default(0);
            $table->decimal('Grant_Inclusion', 15, 2)->default(0.00);
            $table->integer('Places_Inclusion')->default(0);

            $table->timestamps();
            $table->index(['Grant_Key', 'Source_System_Key']);
        });

        // Grant Reallocation Headers
        Schema::create('Fact_Grant_Reallocations', function (Blueprint $table) {
            $table->id('Reallocation_Key');
            $table->unsignedBigInteger('Grant_Key'); // Joins to Dim_Grant
            $table->integer('Source_System_Key');
            $table->unsignedBigInteger('Source_Reallocation_Id');
            $table->string('Reallocation_Number')->nullable();
            $table->tinyInteger('Status_Raw')->default(0);
            $table->date('Date_Approved')->nullable();
            $table->timestamps();

            $table->index(['Grant_Key', 'Source_Reallocation_Id'], 'idx_realloc_base');
        });

        // Grant Reallocation Log items
        Schema::create('Fact_Grant_Reallocation_Logs', function (Blueprint $table) {
            $table->id('Reallocation_Log_Key');
            $table->unsignedBigInteger('Reallocation_Key'); // Joins to Fact_Grant_Reallocations
            $table->integer('Source_System_Key');
            $table->string('Module_Key');
            $table->integer('Value_Count')->default(0);
            $table->decimal('Amount', 15, 2)->default(0.00);
            $table->timestamps();

            $table->index('Reallocation_Key');
        });

        // Grant Amendment Headers
        Schema::create('Fact_Grant_Amendments', function (Blueprint $table) {
            $table->id('Amendment_Key');
            $table->unsignedBigInteger('Grant_Key'); // Joins to Dim_Grant
            $table->integer('Source_System_Key');
            $table->unsignedBigInteger('Source_Amendment_Id');
            $table->string('Amendment_Number')->nullable();
            $table->tinyInteger('Status_Raw')->default(0);
            $table->date('Date_Approved')->nullable();
            $table->timestamps();

            $table->index(['Grant_Key', 'Source_Amendment_Id'], 'idx_amend_base');
        });

        // Grant Amendment Log items
        Schema::create('Fact_Grant_Amendment_Logs', function (Blueprint $table) {
            $table->id('Amendment_Log_Key');
            $table->unsignedBigInteger('Amendment_Key'); // Joins to Fact_Grant_Amendments
            $table->integer('Source_System_Key');
            $table->string('Type_Label')->nullable(); // From the "type" field
            $table->string('Module_Key')->nullable(); // From the "module" field
            $table->integer('Value_Count')->default(0);
            $table->decimal('Amount', 15, 2)->default(0.00);
            $table->timestamps();

            $table->index('Amendment_Key');
        });

        // Grant Claim Header (Core status, dates, and metadata)
        Schema::create('Fact_Grant_Claims', function (Blueprint $table) {
            $table->id('Claim_Key');
            $table->unsignedBigInteger('Grant_Key'); // Joins to Dim_Grant
            $table->integer('Source_System_Key');
            $table->unsignedBigInteger('Source_Claim_Id');
            $table->string('Claim_Number')->nullable();
            $table->tinyInteger('Status_Raw')->default(0);
            $table->boolean('Pref_Authority_Given')->default(0);
            $table->boolean('Pref_Claim_Paid')->default(0);
            $table->date('Date_Approved')->nullable();
            $table->string('Delivery_On_Track_Prediction', 10)->nullable();
            $table->decimal('Send_Claimable_Amount', 15, 2)->nullable();
            $table->decimal('Inclusion_Claimable_Amount', 15, 2)->nullable();

            $table->timestamps();
            $table->index(['Grant_Key', 'Source_Claim_Id'], 'idx_claims_base');
        });

        // Grant Claim Logs (Standard baseline item/delivery counts)
        Schema::create('Fact_Grant_Claim_Logs', function (Blueprint $table) {
            $table->id('Claim_Log_Key');
            $table->unsignedBigInteger('Claim_Key'); // Joins to Fact_Grant_Claims
            $table->integer('Source_System_Key');
            $table->string('Module_Key');
            $table->integer('Item_Count')->default(0);

            $table->timestamps();
            $table->index('Claim_Key');
        });

        // Grant Claim SEND Records (Granular tracking of SEND riders and funding)
        Schema::create('Fact_Grant_Claim_Send_Records', function (Blueprint $table) {
            $table->id('Claim_Send_Key');
            $table->unsignedBigInteger('Claim_Key'); // Joins to Fact_Grant_Claims
            $table->integer('Source_System_Key');
            $table->string('Send_Id_String');
            $table->integer('Send_Riders_Count')->default(0);
            $table->decimal('Send_Amount', 15, 2)->default(0.00);

            $table->timestamps();
            $table->index('Claim_Key');
        });

        // Grant Claim Inclusions (Granular tracking of unique barrier-removal delivery models)
        Schema::create('Fact_Grant_Claim_Inclusions', function (Blueprint $table) {
            $table->id('Claim_Inclusion_Key');
            $table->unsignedBigInteger('Claim_Key'); // Joins to Fact_Grant_Claims
            $table->integer('Source_System_Key');
            $table->string('Inclusion_Id_String');
            $table->string('Inclusion_Category');
            $table->string('Inclusion_Delivery')->nullable();
            $table->decimal('Inclusion_Amount', 15, 2)->default(0.00);

            $table->timestamps();
            $table->index('Claim_Key');
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::dropIfExists('Fact_Grant_Financials');
        Schema::dropIfExists('Fact_Grant_Claim_Inclusions');
        Schema::dropIfExists('Fact_Grant_Claim_Send_Records');
        Schema::dropIfExists('Fact_Grant_Claim_Logs');
        Schema::dropIfExists('Fact_Grant_Claims');
        Schema::dropIfExists('Fact_Grant_Amendment_Logs');
        Schema::dropIfExists('Fact_Grant_Amendments');
        Schema::dropIfExists('Fact_Grant_Reallocation_Logs');
        Schema::dropIfExists('Fact_Grant_Reallocations');

    }
};

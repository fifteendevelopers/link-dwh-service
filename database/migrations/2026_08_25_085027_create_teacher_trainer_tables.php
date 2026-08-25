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
        // 1. Dimension: Dim_Teacher_Trainer
        Schema::create('Dim_Teacher_Trainer', function (Blueprint $table) {
            $table->id('Teacher_Trainer_Key');
            $table->unsignedBigInteger('Source_Teacher_Trainer_Id')->index('idx_tt_source_id');
            $table->string('Source_System_Key', 50)->index('idx_tt_system_key');

            $table->string('First_Name', 100)->nullable();
            $table->string('Last_Name', 100)->nullable();
            $table->string('Full_Name', 200)->nullable();
            $table->string('Email', 255)->nullable();
            $table->string('Telephone', 50)->nullable();
            $table->string('Landline', 50)->nullable();
            $table->string('Main_School_Urn', 50)->nullable()->index('idx_tt_school_urn');

            // Attributes & Permissions
            $table->tinyInteger('Active')->default(1);
            $table->tinyInteger('Status')->default(0);
            $table->boolean('Enable_Rider_Upload')->default(0);
            $table->boolean('Enable_App_Access')->default(0);
            $table->boolean('Enable_Get_Cycling_Admin')->default(0);
            $table->boolean('Pref_Receive_News')->default(0);
            $table->boolean('Pref_Delivering_Bikeability')->default(0);

            // Demographics & Milestone Dates
            $table->unsignedBigInteger('Age_Range_Id')->nullable();
            $table->unsignedBigInteger('Gender_Id')->nullable();
            $table->unsignedBigInteger('Ethnicity_Id')->nullable();
            $table->date('Date_Registered')->nullable();
            $table->date('Date_Renewal')->nullable();
            $table->date('Date_Deregistered')->nullable();
            $table->text('Deregistration_Reason')->nullable();
            $table->date('Rdc_Practical_Training_Complete_Date')->nullable();
            $table->date('In_School_Training_Complete_Date')->nullable();
            $table->date('In_School_Training_Certificate_Download_Date')->nullable();
            $table->date('Send_Training_Complete_Date')->nullable();
            $table->date('Send_Training_Certificate_Download_Date')->nullable();

            $table->timestamps();
            $table->unique(['Source_Teacher_Trainer_Id', 'Source_System_Key'], 'uk_tt_source_system');
        });

        // 2. Fact Header: Fact_Teacher_Trainer_Delivery
        Schema::create('Fact_Teacher_Trainer_Delivery', function (Blueprint $table) {
            $table->id('TT_Delivery_Key');
            $table->unsignedBigInteger('Source_TT_Delivery_Id')->index('idx_ttd_source_id');
            $table->string('Source_System_Key', 50)->index('idx_ttd_system_key');

            // Surrogate Foreign Keys
            $table->unsignedBigInteger('School_Key')->nullable()->index('idx_ttd_school');
            $table->unsignedBigInteger('Teacher_Trainer_Key')->nullable()->index('idx_ttd_trainer');

            // Header Attributes
            $table->string('School_Urn', 50)->index('idx_ttd_urn');
            $table->date('Date_Delivery_Start')->nullable();
            $table->date('Date_Delivery_End')->nullable();
            $table->date('Completion_Date')->nullable();
            $table->boolean('Is_Mixed_Year_Group')->default(0);
            $table->boolean('Has_Ethnicity')->default(0);
            $table->boolean('Has_Survey')->default(0);
            $table->text('Notes')->nullable();

            $table->timestamps();
            $table->unique(['Source_TT_Delivery_Id', 'Source_System_Key'], 'uk_ttd_source_system');
        });

        Schema::create('Fact_Teacher_Trainer_Delivery_Module', function (Blueprint $table) {
            $table->id('TT_Delivery_Module_Key');
            $table->unsignedBigInteger('TT_Delivery_Key')->index('idx_ttdm_delivery_key');
            $table->unsignedBigInteger('Source_TT_Module_Id')->nullable()->index('idx_ttdm_source_mod_id');
            $table->unsignedBigInteger('Source_TT_Delivery_Id')->index('idx_ttdm_source_id');
            $table->string('Source_System_Key', 50)->index('idx_ttdm_system_key');

            // Module Classification Only
            $table->string('Module_Id', 100)->index('idx_ttdm_module_id');
            $table->string('Module_Label', 150)->nullable();

            $table->timestamps();
            $table->unique(['Source_TT_Module_Id', 'Source_System_Key'], 'uk_ttdm_source_system');
        });

        Schema::create('Fact_Teacher_Trainer_Delivery_Metric', function (Blueprint $table) {
            $table->id('TT_Delivery_Metric_Key');
            $table->unsignedBigInteger('Source_TT_Metric_Id')->index('idx_ttm_source_id');
            $table->string('Source_System_Key', 50)->index('idx_ttm_system_key');

            // Foreign Keys to Dimension & Fact Tables
            $table->unsignedBigInteger('TT_Delivery_Key')->index('idx_ttm_delivery_key');
            $table->unsignedBigInteger('TT_Delivery_Module_Key')->index('idx_ttm_module_key');
            $table->unsignedBigInteger('Source_TT_Delivery_Id')->index('idx_ttm_source_del_id');
            $table->unsignedBigInteger('Source_TT_Module_Id')->nullable()->index('idx_ttm_source_mod_id');

            // Metric Classification
            $table->string('Category', 100)->index('idx_ttm_category');
            $table->string('Sub_Category', 100)->index('idx_ttm_sub_category');
            $table->integer('Metric_Value')->default(0);

            $table->timestamps();

            $table->unique(['Source_TT_Metric_Id', 'Source_System_Key'], 'uk_ttm_source_system');
        });

    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::dropIfExists('Fact_Teacher_Trainer_Delivery_Metric');
        Schema::dropIfExists('Fact_Teacher_Trainer_Delivery_Module');
        Schema::dropIfExists('Fact_Teacher_Trainer_Delivery');
        Schema::dropIfExists('Dim_Teacher_Trainer');
    }
};

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
        Schema::create('Dim_Training_Organisation', function (Blueprint $table) {
            $table->bigIncrements('Training_Organisation_Key');
            $table->unsignedBigInteger('Source_Organisation_Id')->index();
            $table->unsignedInteger('Source_System_Key')->default(1);

            $table->string('Organisation_Type', 255)->nullable();
            $table->string('Organisation_Name', 255)->nullable();
            $table->string('Organisation_Email', 255)->nullable();
            $table->boolean('Status_Level_2')->default(false);
            $table->boolean('Status_Level_3')->default(false);
            $table->string('Legacy_Ito_Id', 255)->nullable();
            $table->text('Rdc_Code')->nullable();

            $table->timestamp('Source_Created_At')->nullable();
            $table->timestamp('Source_Updated_At')->nullable();
            $table->timestamps();

            // Composite uniqueness and lookup optimization
            $table->unique(['Source_Organisation_Id', 'Source_System_Key'], 'idx_dto_source_lookup');
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::dropIfExists('Dim_Training_Organisation');
    }
};

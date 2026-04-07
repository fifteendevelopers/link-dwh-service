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
        Schema::connection('mysql')->create('Dim_School', function (Blueprint $table) {
            $table->id('School_Key');
            $table->integer('Source_School_Id')->nullable();
            $table->foreignId('Source_System_Key')->constrained('Dim_Source_System', 'Source_System_Key');
            $table->string('School_Urn')->nullable();
            $table->string('School_Name');
            $table->string('LA_Code')->nullable();
            $table->string('LA_Name')->nullable();
        });

        Schema::connection('mysql')->create('Dim_Training_Provider', function (Blueprint $table) {
            $table->id('Provider_Key');
            $table->unsignedBigInteger('Source_Provider_Id');
            $table->foreignId('Source_System_Key')->constrained('Dim_Source_System', 'Source_System_Key');
            $table->string('Provider_Name');
            $table->string('Provider_Number')->nullable();
            $table->char('Is_Active', 1);
            $table->date('Valid_From_Date');
            $table->date('Valid_To_Date')->default('9999-12-31');
            $table->boolean('Is_Current')->default(1);
            $table->index(['Source_Provider_Id', 'Is_Current'], 'idx_tp_source');
        });

        Schema::connection('mysql')->create('Dim_Grant_Recipient', function (Blueprint $table) {
            $table->id('Recipient_Key');
            $table->unsignedBigInteger('Source_Recipient_Id');
            $table->foreignId('Source_System_Key')->constrained('Dim_Source_System', 'Source_System_Key');
            $table->string('Recipient_Name');
            $table->string('Recipient_Number')->nullable();
            $table->string('LA_Id')->nullable();
            $table->char('Is_Active', 1);
            $table->date('Valid_From_Date');
            $table->date('Valid_To_Date')->default('9999-12-31');
            $table->boolean('Is_Current')->default(1);
            $table->index(['Source_Recipient_Id', 'Is_Current'], 'idx_gr_source');
        });

        Schema::connection('mysql')->create('Dim_Rider', function (Blueprint $table) {
            $table->id('Rider_Key');
            $table->unsignedBigInteger('Source_Rider_Id');
            $table->unsignedBigInteger('Source_System_Key');
            $table->unsignedBigInteger('School_Key')->nullable();
            $table->string('Ethnicity', 50)->nullable();
            $table->string('Gender', 20)->nullable();
            $table->tinyInteger('Pupil_Premium')->default(0);
            $table->tinyInteger('Has_Send')->default(0);

            $table->foreign('Source_System_Key')->references('Source_System_Key')->on('Dim_Source_System');
            $table->foreign('School_Key')->references('School_Key')->on('Dim_School');
            $table->index('Source_Rider_Id');
        });

        Schema::connection('mysql')->create('Map_Rider_Send', function (Blueprint $table) {
            $table->unsignedBigInteger('Rider_Key');
            $table->unsignedBigInteger('Send_Code_Key');
            $table->primary(['Rider_Key', 'Send_Code_Key']);

            $table->foreign('Rider_Key')->references('Rider_Key')->on('Dim_Rider')->onDelete('cascade');
            $table->foreign('Send_Code_Key')->references('Send_Code_Key')->on('Dim_Send_Code');
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::connection('mysql')->dropIfExists('Dim_School');
        Schema::connection('mysql')->dropIfExists('Dim_Training_Provider');
        Schema::connection('mysql')->dropIfExists('Dim_Grant_Recipient');
        Schema::connection('mysql')->dropIfExists('Dim_Rider');
        Schema::connection('mysql')->dropIfExists('Map_Rider_Send');
    }
};

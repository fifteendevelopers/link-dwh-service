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
        Schema::connection('mysql')->create('Sync_Log', function (Blueprint $table) {
            $table->string('Table_Name', 50)->primary();
            $table->dateTime('Last_Synced_At')->nullable();
            $table->integer('Records_Processed')->nullable();
        });

        Schema::connection('mysql')->create('Dim_Source_System', function (Blueprint $table) {
            $table->id('Source_System_Key');
            $table->string('System_Name', 50);
            $table->string('System_Type', 50);
            $table->dateTime('Last_Sync_Date')->nullable();
        });

        Schema::connection('mysql')->create('Dim_Date', function (Blueprint $table) {
            $table->integer('Date_Key')->primary();
            $table->date('Full_Date')->unique();
            $table->string('Month_Name', 10);
            $table->integer('Calendar_Year');
            $table->string('Financial_Year', 10);
            $table->char('Financial_Quarter', 2);
            $table->integer('Financial_Month_Number');
            $table->boolean('Is_Weekend');
        });

        Schema::connection('mysql')->create('Dim_Send_Code', function (Blueprint $table) {
            $table->id('Send_Code_Key');
            $table->string('Send_Code', 20)->unique();
            $table->string('Description')->nullable();
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::connection('mysql')->dropIfExists('Sync_Log');
        Schema::connection('mysql')->dropIfExists('Dim_Source_System');
        Schema::connection('mysql')->dropIfExists('Dim_Date');
        Schema::connection('mysql')->dropIfExists('Dim_Send_Code');

    }
};

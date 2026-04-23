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
        Schema::table('Dim_Consent', function (Blueprint $table) {
            $table->integer('Pre_Freq_To_School')->nullable();
            $table->integer('Pre_Freq_Leisure')->nullable();
            $table->integer('Pre_Freq_Exercise')->nullable();
            $table->integer('Pre_Freq_Other')->nullable();
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::table('Dim_Consent', function (Blueprint $table) {
            $table->dropColumn(['Pre_Freq_To_School','Pre_Freq_Leisure','Pre_Freq_Exercise','Pre_Freq_Other']);
        });
    }
};

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
        Schema::table('Fact_Course_Delivery', function (Blueprint $table) {
            $table->unsignedBigInteger('Provider_Key')->nullable()->change();
        });
    }


    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::table('Fact_Course_Delivery', function (Blueprint $table) {
            $table->unsignedBigInteger('Provider_Key')->change();
        });
    }
};

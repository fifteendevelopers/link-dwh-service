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
        Schema::table('Dim_Training_Provider', function (Blueprint $table) {
            $table->string('Address_Line_1')->nullable();
            $table->string('Address_Line_2')->nullable();
            $table->string('City')->nullable();
            $table->string('Postcode')->nullable();
            $table->string('Website')->nullable();
            $table->string('Telephone')->nullable();
            $table->string('Public_Email')->nullable();
            $table->string('Public_Telephone')->nullable();
            $table->string('Provider_Type')->nullable();
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::table('Dim_Training_Provider', function (Blueprint $table) {
            $table->dropColumn(['Address_Line_1','Address_Line_2','City','Postcode','Website','Telephone','Public_Email','Public_Telephone','Provider_Type']);
        });
    }
};

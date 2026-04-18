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
        Schema::table('Dim_Grant_Recipient', function (Blueprint $table) {
            $table->string('Address_Line_1')->nullable();
            $table->string('Address_Line_2')->nullable();
            $table->string('City')->nullable();
            $table->string('Postcode')->nullable();
            $table->string('Website')->nullable();
            $table->date('Inception_Date')->nullable();
            $table->date('Renewal_Date')->nullable();
            $table->date('Deregistered_Date')->nullable();
            $table->boolean('Is_SGO')->default(0); // Derived from pref_sgoh
            $table->timestamp('Source_Created_At')->nullable();
            $table->timestamp('Source_Updated_At')->nullable();
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::table('Dim_Grant_Recipient', function (Blueprint $table) {
            $table->dropColumn(['Address_Line_1','Address_Line_2','City','Postcode','Website','Inception_Date','Renewal_Date','Deregistration_Date','Is_SGO','Source_Created_At','Source_Updated_At']);
        });
    }
};

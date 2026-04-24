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
        Schema::table('Dim_Delivery_header', function (Blueprint $table) {
            $table->date('Consent_Cutoff_Date')->nullable();
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::table('dim_delivery_header', function (Blueprint $table) {
            $table->dropColumn('Consent_Cutoff_Date');
        });
    }
};

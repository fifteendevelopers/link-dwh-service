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
        Schema::table('Dim_Delivery_Header', function (Blueprint $table) {
            $table->integer('Fleet_Cycles_Used')->default(0)->nullable();
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::table('Dim_Delivery_Header', function (Blueprint $table) {
            $table->dropColumn('Fleet_Cycles_Used');
        });
    }
};

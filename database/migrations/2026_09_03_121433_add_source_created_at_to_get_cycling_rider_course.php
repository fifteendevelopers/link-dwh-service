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
        Schema::table('Fact_Get_Cycling_Rider_Course', function (Blueprint $table) {
            $table->timestamp('Source_Updated_At')->after('Has_Survey_Completed');
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::table('Fact_Get_Cycling_Rider_Course', function (Blueprint $table) {
            $table->dropColumn('Source_Updated_At');
        });
    }
};

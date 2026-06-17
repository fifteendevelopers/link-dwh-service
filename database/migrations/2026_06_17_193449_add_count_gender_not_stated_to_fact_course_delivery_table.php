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
            if (!Schema::hasColumn('Fact_Course_Delivery', 'Count_Gender_Not_Stated')) {
                $table->integer('Count_Gender_Not_Stated')
                    ->default(0)
                    ->after('Count_Gender_Other');
            }
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::table('Fact_Course_Delivery', function (Blueprint $table) {
            $table->dropColumn('Count_Gender_Not_Stated');
        });
    }
};

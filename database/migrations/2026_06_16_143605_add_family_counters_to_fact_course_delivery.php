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
            $table->integer('Count_Adults')->default(0)->after('Count_Attended_Confirmed');
            $table->integer('Count_Teens')->default(0)->after('Count_Adults');
            $table->integer('Count_Children')->default(0)->after('Count_Teens');
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::table('Fact_Course_Delivery', function (Blueprint $table) {
            $table->dropColumn(['Count_Adults', 'Count_Teens', 'Count_Children']);
        });
    }
};

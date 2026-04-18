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
        Schema::table('Fact_Parent_Survey', function (Blueprint $table) {
            $table->boolean('Like_To_Participate')->default(false)->after('Grant_Key');
            $table->boolean('Like_To_Answer_Survey')->default(false)->after('Like_To_Participate');
            $table->boolean('Pref_Join_Bikeability')->default(false)->after('Like_To_Answer_Survey');
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::table('Fact_Parent_Survey', function (Blueprint $table) {
            $table->dropColumn(['Like_To_Participate', 'Like_To_Answer_Survey', 'Pref_Join_Bikeability']);
        });
    }
};

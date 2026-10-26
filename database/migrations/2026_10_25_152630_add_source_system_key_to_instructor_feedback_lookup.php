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
        Schema::table('Dim_Instructor_Feedback_Lookup', function (Blueprint $table) {
            $table->unsignedBigInteger('Source_System_Key')->after('Feedback_Lookup_Key')->default(1);
        });

        Schema::table('Fact_Parent_Survey', function (Blueprint $table) {
            $table->unsignedBigInteger('Source_System_Key')->after('Parent_Survey_Key')->default(1);
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::table('Dim_Instructor_Feedback_Lookup', function (Blueprint $table) {
            $table->dropColumn('Source_System_Key');
        });
        Schema::table('Fact_Parent_Survey', function (Blueprint $table) {
            $table->dropColumn('Source_System_Key');
        });
    }
};

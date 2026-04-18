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
            $table->timestamp('Source_Created_At')->nullable();
            $table->timestamp('Source_Updated_At')->nullable();
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::table('Dim_Training_Provider', function (Blueprint $table) {
            $table->dropColumn(['Source_Created_At', 'Source_Updated_At']);
        });
    }
};

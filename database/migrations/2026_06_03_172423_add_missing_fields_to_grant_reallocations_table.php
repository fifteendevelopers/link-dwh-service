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
        Schema::connection('mysql')->table('Fact_Grant_Reallocations', function (Blueprint $table) {
            $table->string('Reallocation_Type', 255)->nullable()->after('Reallocation_Number');
            $table->text('Reallocation_Notes')->nullable()->after('Status_Raw');
            $table->text('Reallocation_Increase_Reasons')->nullable()->after('Reallocation_Notes');
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::connection('mysql')->table('Fact_Grant_Reallocations', function (Blueprint $table) {
            $table->dropColumn([
                'Reallocation_Type',
                'Reallocation_Notes',
                'Reallocation_Increase_Reasons'
            ]);
        });
    }
};

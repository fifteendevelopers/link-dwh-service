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
        Schema::table('Dim_School', function (Blueprint $table) {
            $table->string('Urban_Rural_Code', 20)->nullable()->after('La_Name');
            $table->string('Rural_Urban_Classification', 150)->nullable()->after('Urban_Rural_Code');
            $table->integer('Imd_Decile')->nullable()->after('Rural_Urban_Classification');
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::table('Dim_School', function (Blueprint $table) {
            $table->dropColumn(['Urban_Rural_Code', 'Rural_Urban_Classification', 'Imd_Decile']);
        });
    }
};

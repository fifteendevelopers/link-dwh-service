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
            $table->string('Postcode')->nullable()->after('School_Name');
            $table->string('Lsoa_Code')->nullable()->after('LA_Name');
            $table->string('Lsoa_Name')->nullable()->after('Lsoa_Code');
            $table->string('Msoa_Code')->nullable()->after('Lsoa_Name');
            $table->string('Msoa_Name')->nullable()->after('Msoa_Code');
            $table->string('Parliamentary_Constituency_Code')->nullable()->after('Msoa_Name');
            $table->string('Parliamentary_Constituency_Name')->nullable()->after('Parliamentary_Constituency_Code');
            $table->string('Status')->nullable()->after('Parliamentary_Constituency_Name');
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::table('Dim_School', function (Blueprint $table) {
            $table->dropColumn([
                'Postcode',
                'Lsoa_Code',
                'Lsoa_Name',
                'Msoa_Code',
                'Msoa_Name',
                'Parliamentary_Constituency_Code',
                'Parliamentary_Constituency_Name',
                'Status',
            ]);
        });
    }
};

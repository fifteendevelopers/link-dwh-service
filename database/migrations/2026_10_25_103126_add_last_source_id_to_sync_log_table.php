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
        Schema::table('sync_log', function (Blueprint $table) {
            $table->unsignedBigInteger('Last_Source_Id')->nullable()->after('Last_Synced_At');
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::table('sync_log', function (Blueprint $table) {
            $table->dropColumn('Last_Source_Id');
        });
    }
};

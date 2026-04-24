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
        Schema::table('Dim_Consent', function (Blueprint $table) {
            $table->boolean('Pref_Join_Bikeclub')->default(false)->after('Consent_Status');
            $table->boolean('Pref_Further_Research')->default(false)->after('Pref_Join_Bikeclub');
            $table->boolean('Pref_Receive_News')->default(false)->after('Pref_Further_Research');
            $table->string('Year_Group',10)->nullable()->after('Attended');
            $table->text('SEND_Details')->nullable()->after('Is_SEND');
            $table->text('Medical_Details')->nullable()->after('Has_Medical_Condition');
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::table('Dim_Consent', function (Blueprint $table) {
            $table->dropColumn(['Pref_Join_Bikeclub','Pref_Further_Research','Pref_Receive_News','Year_Group','SEND_Details','Medical_Details']);
        });
    }
};

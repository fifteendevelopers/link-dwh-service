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
            $table->string('Deleted_Reason')->nullable()->after('Pre_Freq_Other');
            $table->timestamp('Source_Created_At')->nullable()->after('Deleted_Reason');
            $table->timestamp('Source_Updated_At')->nullable()->after('Source_Created_At');
            $table->timestamp('Source_Deleted_At')->nullable()->after('Source_Updated_At');
            $table->timestamp('updated_at')->nullable()->after('Source_Deleted_At');

            // Index Source_Deleted_At for efficient report filtering
            $table->index('Source_Deleted_At', 'idx_dim_consent_deleted_at');
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::table('Dim_Consent', function (Blueprint $table) {
            $table->dropIndex('idx_dim_consent_deleted_at');
            $table->dropColumn([
                'Deleted_Reason',
                'Source_Created_At',
                'Source_Updated_At',
                'Source_Deleted_At',
                'updated_at',
            ]);
        });
    }
};

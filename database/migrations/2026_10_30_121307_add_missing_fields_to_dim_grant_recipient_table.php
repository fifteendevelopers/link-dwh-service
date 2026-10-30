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
        Schema::table('Dim_Grant_Recipient', function (Blueprint $table) {
            $table->string('Telephone')->nullable()->after('Website');
            $table->string('Landline')->nullable()->after('Telephone');
            $table->string('Primary_Email')->nullable()->after('Landline');
            $table->string('Secondary_Email')->nullable()->after('Primary_Email');
            $table->string('Public_Email')->nullable()->after('Secondary_Email');
            $table->string('Public_Telephone')->nullable()->after('Public_Email');
            $table->tinyInteger('Status')->default(1)->after('LA_Id');
            $table->date('Date_Insurance_Expiry')->nullable()->after('Deregistered_Date');
            $table->text('Deregistration_Reason')->nullable()->after('Deregistered_Date');
            $table->unsignedBigInteger('Sister_Training_Provider_Id')->nullable()->after('Is_SGO');
            $table->string('Legacy_Id')->nullable()->after('Sister_Training_Provider_Id');
            $table->text('Account_Notes')->nullable()->after('Legacy_Id');
            $table->timestamp('Source_Deleted_At')->nullable()->after('Source_Updated_At');

            $table->index('Primary_Email', 'idx_dgr_primary_email');
            $table->index('Status', 'idx_dgr_status');
            $table->index('Source_Deleted_At', 'idx_dgr_source_deleted_at');
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::table('Dim_Grant_Recipient', function (Blueprint $table) {
            $table->dropIndex('idx_dgr_primary_email');
            $table->dropIndex('idx_dgr_status');
            $table->dropIndex('idx_dgr_source_deleted_at');

            $table->dropColumn([
                'Telephone',
                'Landline',
                'Primary_Email',
                'Secondary_Email',
                'Public_Email',
                'Public_Telephone',
                'Status',
                'Date_Insurance_Expiry',
                'Deregistration_Reason',
                'Sister_Training_Provider_Id',
                'Legacy_Id',
                'Account_Notes',
                'Source_Deleted_At',
            ]);
        });
    }
};

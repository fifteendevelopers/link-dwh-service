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
        Schema::create('Fact_Grant_Recipient_Training_Provider', function (Blueprint $table) {
            $table->id('Grant_Recipient_Provider_Key');
            $table->unsignedBigInteger('Grant_Recipient_Key');
            $table->unsignedBigInteger('Provider_Key');
            $table->unsignedBigInteger('Source_Grant_Recipient_Id');
            $table->unsignedBigInteger('Source_Training_Provider_Id');
            $table->unsignedBigInteger('Source_System_Key')->default(1);
            $table->tinyInteger('Pref_Enable_Tp_Access')->default(0);
            $table->tinyInteger('Pref_Enable_Tp_Digi_Delivery_Access')->default(0);
            $table->timestamp('Source_Deleted_At')->nullable();
            $table->timestamps();

            // Explicit short index names (all < 64 chars)
            $table->index('Grant_Recipient_Key', 'idx_fgrtp_gr_key');
            $table->index('Provider_Key', 'idx_fgrtp_tp_key');
            $table->index('Source_Grant_Recipient_Id', 'idx_fgrtp_src_gr_id');
            $table->index('Source_Training_Provider_Id', 'idx_fgrtp_src_tp_id');

            // Unique constraint
            $table->unique(
                ['Grant_Recipient_Key', 'Provider_Key', 'Source_System_Key'],
                'uniq_fgrtp_gr_tp_sys'
            );

            // Foreign keys
            $table->foreign('Grant_Recipient_Key', 'fk_fgrtp_recipient_key')
                ->references('Recipient_Key')
                ->on('Dim_Grant_Recipient')
                ->onDelete('cascade');

            $table->foreign('Provider_Key', 'fk_fgrtp_provider_key')
                ->references('Provider_Key')
                ->on('Dim_Training_Provider')
                ->onDelete('cascade');
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::dropIfExists('Fact_Grant_Recipient_Training_Provider');
    }
};

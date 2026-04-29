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
        Schema::create('Fact_Follow_Up_Survey', function (Blueprint $table) {
            $table->id('Follow_Up_Key');
            $table->unsignedBigInteger('Source_Survey_Id');
            $table->integer('Source_System_Key');

            // Relationships
            $table->unsignedBigInteger('Delivery_Key');
            $table->unsignedBigInteger('Rider_Key');
            $table->unsignedBigInteger('Course_Key');

            // Flattened Survey Data
            $table->string('Course_Label_Raw');
            $table->integer('Invitation_Month')->nullable();

            // Common Answer Buckets
            $table->string('q1a_freq_school')->nullable();
            $table->string('q1b_freq_leisure')->nullable();
            $table->string('q1c_freq_exercise')->nullable();
            $table->string('q2a_conf_use_cycle')->nullable();
            $table->string('q2b_conf_cycle_roads')->nullable();
            $table->string('q3a_enc_use_cycle')->nullable();
            $table->string('q3b_enc_cycle_roads')->nullable();
            $table->string('q4_safety_roads')->nullable();
            $table->string('q5_child_desire')->nullable();
            $table->json('q6_encouragement_factors')->nullable(); // JSON for multi-select
            $table->string('q7_conf_change')->nullable();
            $table->string('q8_physical_activity')->nullable();

            $table->timestamp('Source_Created_At')->nullable();
            $table->timestamps(); // DWH timestamps

            $table->index(['Source_Survey_Id', 'Source_System_Key']);
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::dropIfExists('Fact_Parent_Follow_Up_Survey');
    }
};

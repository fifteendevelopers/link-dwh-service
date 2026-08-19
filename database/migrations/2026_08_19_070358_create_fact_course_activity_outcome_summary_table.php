<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        Schema::create('Fact_Course_Activity_Outcome_Summary', function (Blueprint $table) {
            $table->id('Summary_Key');
            $table->unsignedBigInteger('Delivery_Key')->index('idx_sum_delivery');
            $table->unsignedBigInteger('Course_Key')->index('idx_sum_course');
            $table->string('Course_Code', 50)->nullable()->index('idx_sum_code');

            // Level 1
            $table->unsignedInteger('l1_ac2_on_own')->default(0);
            $table->unsignedInteger('l1_ac2_practice')->default(0);
            $table->unsignedInteger('l1_ac2_assistance')->default(0);
            $table->unsignedInteger('l1_ac2_not_seen')->default(0);
            $table->unsignedInteger('l1_ac3_on_own')->default(0);
            $table->unsignedInteger('l1_ac3_practice')->default(0);
            $table->unsignedInteger('l1_ac3_assistance')->default(0);
            $table->unsignedInteger('l1_ac3_not_seen')->default(0);
            $table->unsignedInteger('l1_ac4_on_own')->default(0);
            $table->unsignedInteger('l1_ac4_practice')->default(0);
            $table->unsignedInteger('l1_ac4_assistance')->default(0);
            $table->unsignedInteger('l1_ac4_not_seen')->default(0);
            $table->unsignedInteger('l1_ac5_on_own')->default(0);
            $table->unsignedInteger('l1_ac5_practice')->default(0);
            $table->unsignedInteger('l1_ac5_assistance')->default(0);
            $table->unsignedInteger('l1_ac5_not_seen')->default(0);

            // Level 2
            $table->unsignedInteger('l2_ac1_on_own')->default(0);
            $table->unsignedInteger('l2_ac1_practice')->default(0);
            $table->unsignedInteger('l2_ac1_assistance')->default(0);
            $table->unsignedInteger('l2_ac1_not_seen')->default(0);
            $table->unsignedInteger('l2_ac2_on_own')->default(0);
            $table->unsignedInteger('l2_ac2_practice')->default(0);
            $table->unsignedInteger('l2_ac2_assistance')->default(0);
            $table->unsignedInteger('l2_ac2_not_seen')->default(0);
            $table->unsignedInteger('l2_ac3_on_own')->default(0);
            $table->unsignedInteger('l2_ac3_practice')->default(0);
            $table->unsignedInteger('l2_ac3_assistance')->default(0);
            $table->unsignedInteger('l2_ac3_not_seen')->default(0);
            $table->unsignedInteger('l2_ac4_on_own')->default(0);
            $table->unsignedInteger('l2_ac4_practice')->default(0);
            $table->unsignedInteger('l2_ac4_assistance')->default(0);
            $table->unsignedInteger('l2_ac4_not_seen')->default(0);
            $table->unsignedInteger('l2_ac5_on_own')->default(0);
            $table->unsignedInteger('l2_ac5_practice')->default(0);
            $table->unsignedInteger('l2_ac5_assistance')->default(0);
            $table->unsignedInteger('l2_ac5_not_seen')->default(0);
            $table->unsignedInteger('l2_ac6_on_own')->default(0);
            $table->unsignedInteger('l2_ac6_practice')->default(0);
            $table->unsignedInteger('l2_ac6_assistance')->default(0);
            $table->unsignedInteger('l2_ac6_not_seen')->default(0);
            $table->unsignedInteger('l2_ac7_on_own')->default(0);
            $table->unsignedInteger('l2_ac7_practice')->default(0);
            $table->unsignedInteger('l2_ac7_assistance')->default(0);
            $table->unsignedInteger('l2_ac7_not_seen')->default(0);

            // Level 3
            $table->unsignedInteger('l3_ac1_on_own')->default(0);
            $table->unsignedInteger('l3_ac1_practice')->default(0);
            $table->unsignedInteger('l3_ac1_assistance')->default(0);
            $table->unsignedInteger('l3_ac1_not_seen')->default(0);
            $table->unsignedInteger('l3_ac2_on_own')->default(0);
            $table->unsignedInteger('l3_ac2_practice')->default(0);
            $table->unsignedInteger('l3_ac2_assistance')->default(0);
            $table->unsignedInteger('l3_ac2_not_seen')->default(0);
            $table->unsignedInteger('l3_ac3_on_own')->default(0);
            $table->unsignedInteger('l3_ac3_practice')->default(0);
            $table->unsignedInteger('l3_ac3_assistance')->default(0);
            $table->unsignedInteger('l3_ac3_not_seen')->default(0);
            $table->unsignedInteger('l3_ac4_on_own')->default(0);
            $table->unsignedInteger('l3_ac4_practice')->default(0);
            $table->unsignedInteger('l3_ac4_assistance')->default(0);
            $table->unsignedInteger('l3_ac4_not_seen')->default(0);
            $table->unsignedInteger('l3_ac5_on_own')->default(0);
            $table->unsignedInteger('l3_ac5_practice')->default(0);
            $table->unsignedInteger('l3_ac5_assistance')->default(0);
            $table->unsignedInteger('l3_ac5_not_seen')->default(0);
            $table->unsignedInteger('l3_ac6_on_own')->default(0);
            $table->unsignedInteger('l3_ac6_practice')->default(0);
            $table->unsignedInteger('l3_ac6_assistance')->default(0);
            $table->unsignedInteger('l3_ac6_not_seen')->default(0);
            $table->unsignedInteger('l3_ac7_on_own')->default(0);
            $table->unsignedInteger('l3_ac7_practice')->default(0);
            $table->unsignedInteger('l3_ac7_assistance')->default(0);
            $table->unsignedInteger('l3_ac7_not_seen')->default(0);
            $table->unsignedInteger('l3_ac8_on_own')->default(0);
            $table->unsignedInteger('l3_ac8_practice')->default(0);
            $table->unsignedInteger('l3_ac8_assistance')->default(0);
            $table->unsignedInteger('l3_ac8_not_seen')->default(0);

            // Plus Balance
            $table->unsignedInteger('pb_ac1_on_own')->default(0);
            $table->unsignedInteger('pb_ac1_practice')->default(0);
            $table->unsignedInteger('pb_ac1_assistance')->default(0);
            $table->unsignedInteger('pb_ac1_not_seen')->default(0);
            $table->unsignedInteger('pb_ac2_on_own')->default(0);
            $table->unsignedInteger('pb_ac2_practice')->default(0);
            $table->unsignedInteger('pb_ac2_assistance')->default(0);
            $table->unsignedInteger('pb_ac2_not_seen')->default(0);
            $table->unsignedInteger('pb_ac3_on_own')->default(0);
            $table->unsignedInteger('pb_ac3_practice')->default(0);
            $table->unsignedInteger('pb_ac3_assistance')->default(0);
            $table->unsignedInteger('pb_ac3_not_seen')->default(0);

            // Plus Learn
            $table->unsignedInteger('pl_ac1_on_own')->default(0);
            $table->unsignedInteger('pl_ac1_practice')->default(0);
            $table->unsignedInteger('pl_ac1_assistance')->default(0);
            $table->unsignedInteger('pl_ac1_not_seen')->default(0);
            $table->unsignedInteger('pl_ac2_on_own')->default(0);
            $table->unsignedInteger('pl_ac2_practice')->default(0);
            $table->unsignedInteger('pl_ac2_assistance')->default(0);
            $table->unsignedInteger('pl_ac2_not_seen')->default(0);
            $table->unsignedInteger('pl_ac3_on_own')->default(0);
            $table->unsignedInteger('pl_ac3_practice')->default(0);
            $table->unsignedInteger('pl_ac3_assistance')->default(0);
            $table->unsignedInteger('pl_ac3_not_seen')->default(0);
            $table->unsignedInteger('pl_ac4_on_own')->default(0);
            $table->unsignedInteger('pl_ac4_practice')->default(0);
            $table->unsignedInteger('pl_ac4_assistance')->default(0);
            $table->unsignedInteger('pl_ac4_not_seen')->default(0);
            $table->unsignedInteger('pl_ac5_on_own')->default(0);
            $table->unsignedInteger('pl_ac5_practice')->default(0);
            $table->unsignedInteger('pl_ac5_assistance')->default(0);
            $table->unsignedInteger('pl_ac5_not_seen')->default(0);

            // Plus Fix
            $table->unsignedInteger('pf_ac1_on_own')->default(0);
            $table->unsignedInteger('pf_ac1_practice')->default(0);
            $table->unsignedInteger('pf_ac1_assistance')->default(0);
            $table->unsignedInteger('pf_ac1_not_seen')->default(0);
            $table->unsignedInteger('pf_ac2_on_own')->default(0);
            $table->unsignedInteger('pf_ac2_practice')->default(0);
            $table->unsignedInteger('pf_ac2_assistance')->default(0);
            $table->unsignedInteger('pf_ac2_not_seen')->default(0);

            $table->unique(['Delivery_Key', 'Course_Key'], 'uk_del_course_summary');
            $table->timestamp('Updated_At')->useCurrent()->useCurrentOnUpdate();
        });
    }

    public function down(): void
    {
        Schema::dropIfExists('Fact_Course_Activity_Outcome_Summary');
    }
};

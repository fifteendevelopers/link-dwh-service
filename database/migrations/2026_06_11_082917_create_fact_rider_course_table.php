<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    /**
     * Run the migrations.
     */
    public function up(): void
    {
        Schema::create('Fact_Rider_Course', function (Blueprint $blueprint) {
            // Primary Unique Identifiers
            $blueprint->bigInteger('Source_Rider_Id')->unsigned();
            $blueprint->bigInteger('Source_Course_Id')->unsigned();
            $blueprint->string('Source_System_Key', 50);

            // Row Properties & Operational Status Flags
            $blueprint->integer('Status')->default(0);
            $blueprint->tinyInteger('Attended')->nullable()->default(1);
            $blueprint->boolean('Withdrawn')->nullable()->default(false);
            $blueprint->text('Withdrawal_Reason')->nullable();
            $blueprint->boolean('Has_Completed_Course')->nullable()->default(false);
            $blueprint->boolean('Has_Survey_Completed')->default(false);
            $blueprint->date('Course_Complete_Date')->nullable();

            // Timestamps
            $blueprint->timestamp('Source_Created_At')->nullable();
            $blueprint->timestamp('Source_Updated_At')->nullable();

            // System Sync Watermark Tracker
            // Automatically records the current time whenever an upsert modifies this row
            $blueprint->timestamp('Updated_At')
                ->default(DB::raw('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'));

            // 🛠️ Constraints & High-Performance Search Indexes
            $blueprint->primary(['Source_Rider_Id', 'Source_Course_Id']);
            $blueprint->index('Source_Updated_At', 'idx_dwh_source_updated');
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::dropIfExists('fact_rider_course');
    }
};

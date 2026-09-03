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
        Schema::create('Fact_Grant_Allocation_Dft', function (Blueprint $table) {
            $table->id('Grant_Allocation_Dft_Key');
            $table->unsignedBigInteger('Source_Allocation_Id')->index('idx_fgad_source_id');
            $table->string('Source_System_Key', 50)->index('idx_fgad_system_key');

            // Foreign Keys to Dimensions
            $table->unsignedBigInteger('Grant_Key')->index('idx_fgad_grant_key');
            $table->unsignedBigInteger('Source_Grant_Id')->index('idx_fgad_source_grant_id');

            // High-Level Totals
            $table->decimal('Max_Allocation', 15, 2)->default(0.00);
            $table->bigInteger('Total_Levels')->default(0);
            $table->bigInteger('Total_Plus')->default(0);

            // Level Allocations & Places
            $table->unsignedSmallInteger('Places_Level_1')->default(0);
            $table->decimal('Grant_Level_1', 15, 2)->default(0.00);
            $table->unsignedSmallInteger('Places_Level_1_2')->default(0);
            $table->decimal('Grant_Level_1_2', 15, 2)->default(0.00);
            $table->unsignedSmallInteger('Places_Level_2')->default(0);
            $table->decimal('Grant_Level_2', 15, 2)->default(0.00);
            $table->unsignedSmallInteger('Places_Level_3')->default(0);
            $table->decimal('Grant_Level_3', 15, 2)->default(0.00);

            // Plus Modules & Places
            $table->unsignedSmallInteger('Places_Plus_Balance')->default(0);
            $table->decimal('Grant_Plus_Balance', 15, 2)->default(0.00);
            $table->unsignedSmallInteger('Places_Plus_Bus')->default(0);
            $table->decimal('Grant_Plus_Bus', 15, 2)->default(0.00);
            $table->unsignedSmallInteger('Places_Plus_Fix')->default(0);
            $table->decimal('Grant_Plus_Fix', 15, 2)->default(0.00);
            $table->unsignedSmallInteger('Places_Plus_Learn')->default(0);
            $table->decimal('Grant_Plus_Learn', 15, 2)->default(0.00);
            $table->unsignedSmallInteger('Places_Plus_On_Show')->default(0);
            $table->decimal('Grant_Plus_On_Show', 15, 2)->default(0.00);
            $table->unsignedSmallInteger('Places_Plus_Parents')->default(0);
            $table->decimal('Grant_Plus_Parents', 15, 2)->default(0.00);
            $table->unsignedSmallInteger('Places_Plus_Promotion')->default(0);
            $table->decimal('Grant_Plus_Promotion', 15, 2)->default(0.00);
            $table->unsignedSmallInteger('Places_Plus_Recycled')->default(0);
            $table->decimal('Grant_Plus_Recycled', 15, 2)->default(0.00);
            $table->unsignedSmallInteger('Places_Plus_Ride')->default(0);
            $table->decimal('Grant_Plus_Ride', 15, 2)->default(0.00);
            $table->unsignedSmallInteger('Places_Plus_Transition')->default(0);
            $table->decimal('Grant_Plus_Transition', 15, 2)->default(0.00);
            $table->unsignedSmallInteger('Places_Plus_Family')->default(0);
            $table->decimal('Grant_Plus_Family', 15, 2)->default(0.00);
            $table->unsignedSmallInteger('Places_Plus_Adult')->default(0);
            $table->decimal('Grant_Plus_Adult', 15, 2)->default(0.00);

            // SEND & Inclusion Allocations
            $table->decimal('Grant_Send', 15, 2)->default(0.00);
            $table->integer('Places_Send')->default(0);
            $table->decimal('Grant_Inclusion', 15, 2)->default(0.00);
            $table->integer('Places_Inclusion')->default(0);

            $table->timestamps();

            $table->unique(['Source_Allocation_Id', 'Source_System_Key'], 'uk_fgad_source_system');
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::dropIfExists('Fact_Grant_Allocation_Dft');
    }
};

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
        Schema::create('Fact_Instructor_Delivery', function (Blueprint $table) {
            $table->id('Instructor_Delivery_Key'); // DWH Primary Key
            $table->integer('Source_System_Key');

            // DWH Foreign Keys
            $table->unsignedBigInteger('Delivery_Key');
            $table->unsignedBigInteger('Instructor_Key');

            // Operational Flags & Source Audit Dates
            $table->boolean('Instructor_Notified')->default(0);
            $table->timestamp('Source_Created_At')->nullable();
            $table->timestamp('Source_Updated_At')->nullable();
            $table->timestamp('Source_Deleted_At')->nullable(); // For soft-deletes track

            $table->timestamps(); // DWH Processing dates

            // Composite performance index for rapid reporting lookups
            $table->index(['Delivery_Key', 'Instructor_Key'], 'idx_delivery_instructor');
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::dropIfExists('Fact_Instructor_Delivery');
    }
};

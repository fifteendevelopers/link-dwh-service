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
        Schema::create('Fact_Instructor_Course', function (Blueprint $table) {
            $table->id('Instructor_Course_Key'); // DWH primary key

            // DWH Foreign Keys to your existing dimensions
            $table->unsignedBigInteger('Instructor_Key');
            $table->unsignedBigInteger('Course_Key');
            $table->integer('Source_System_Key');

            // Temporal Tracking Columns
            $table->dateTime('Active_From');
            $table->dateTime('Active_To')->nullable()->comment('Null means currently active');
            $table->boolean('Is_Current')->default(1);

            // Indexes for rapid report joining
            $table->index(['Instructor_Key', 'Is_Current']);
            $table->index(['Course_Key', 'Is_Current']);
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::dropIfExists('Fact_Instructor_Course');
    }
};

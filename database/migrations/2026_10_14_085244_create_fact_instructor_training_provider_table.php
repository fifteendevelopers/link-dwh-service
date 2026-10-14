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
        Schema::create('Fact_Instructor_Training_Provider', function (Blueprint $table) {
            $table->id('Instructor_Provider_Key');
            $table->unsignedBigInteger('Instructor_Key')->index();
            $table->unsignedBigInteger('Provider_Key')->index();
            $table->unsignedBigInteger('Source_Instructor_Id')->index();
            $table->unsignedBigInteger('Source_Provider_Id')->index();
            $table->unsignedBigInteger('Source_System_Key');
            $table->date('Dbs_Certification_Date')->nullable();
            $table->date('Source_Deleted_At')->nullable();

            // Foreign Key Constraints to Dimensions
            $table->foreign('Instructor_Key')
                ->references('Instructor_Key')
                ->on('Dim_Instructor')
                ->onDelete('cascade');

            $table->foreign('Provider_Key')
                ->references('Provider_Key')
                ->on('Dim_Training_Provider')
                ->onDelete('cascade');

            $table->foreign('Source_System_Key')
                ->references('Source_System_Key')
                ->on('Dim_Source_System');

            // Prevent duplicate active linkages per source system
            $table->unique(
                ['Instructor_Key', 'Provider_Key', 'Source_System_Key'],
                'uq_inst_tp_source'
            );
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::dropIfExists('Fact_Instructor_Training_Provider');
    }
};

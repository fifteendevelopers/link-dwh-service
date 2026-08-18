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
        Schema::create('Dim_Instructor_Feedback_Lookup', function (Blueprint $table) {
            $table->unsignedBigInteger('Feedback_Lookup_Key')->primary();

            $table->integer('Category_Id')->index('idx_category_id');
            $table->string('Course_Code', 50)->nullable()->index('idx_course_code');
            $table->string('Category_Label', 100)->nullable();
            $table->text('Short_Text');
            $table->text('Long_Text');
            $table->longText('Links_Lookup')->nullable();
            $table->timestamps();
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::dropIfExists('Dim_Instructor_Feedback_Lookup');
    }
};

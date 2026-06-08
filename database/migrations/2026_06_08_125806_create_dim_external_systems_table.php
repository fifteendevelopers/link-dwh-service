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
        Schema::create('Dim_External_Systems', function (Blueprint $table) {
            $table->bigIncrements('External_System_Key');
            $table->unsignedBigInteger('Source_External_System_Id')->unique();
            $table->foreignId('Source_System_Key')->constrained('Dim_Source_System', 'Source_System_Key');
            $table->string('Username', 255);
            $table->string('Name', 255)->nullable();
            $table->tinyInteger('Pref_Link_Managed_Consent')->default(0);
            $table->tinyInteger('Pref_Use_Instructor_App')->default(0);
            $table->tinyInteger('Pref_Link_Managed_Comms')->default(0);
            $table->timestamps();
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::dropIfExists('Dim_External_Systems');
    }
};

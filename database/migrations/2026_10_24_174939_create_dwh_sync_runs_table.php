<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        Schema::create('dwh_sync_runs', function (Blueprint $table) {
            $table->id();
            $table->string('target_table', 100)->default('ALL');
            $table->string('triggered_by', 50)->default('CLI/CRON');
            $table->timestamp('started_at');
            $table->timestamp('finished_at')->nullable();
            $table->unsignedInteger('duration_seconds')->default(0);
            $table->enum('status', ['RUNNING', 'SUCCESS', 'WARNING', 'FAILURE'])->default('RUNNING');
            $table->json('sync_errors')->nullable();
            $table->json('audit_results')->nullable();
            $table->timestamps();

            $table->index(['started_at', 'status']);
        });
    }

    public function down(): void
    {
        Schema::connection('mysql')->dropIfExists('dwh_sync_runs');
    }
};

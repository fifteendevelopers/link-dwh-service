<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration {
    public function up(): void
    {
        Schema::create('Dwh_Api_Consumers', function (Blueprint $table) {
            $table->id();
            $table->string('client_name')->unique()->comment('e.g., Primary Source System, PowerBI Gateway');
            $table->string('client_id')->unique()->index();
            $table->string('api_secret_hash')->comment('Bcrypt/SHA256 hashed secret token string');

            // Network Security Configuration
            $table->json('allowed_ips')->nullable()->comment('Array of explicit server IPs allowed to call with this key');

            $table->boolean('is_active')->default(1);
            $table->timestamp('last_used_at')->nullable();
            $table->timestamps();
        });
    }

    public function down(): void
    {
        Schema::dropIfExists('Dwh_Api_Consumers');
    }
};

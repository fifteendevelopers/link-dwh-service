<?php

namespace App\Console\Commands;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Str;

class CreateDwhConsumer extends Command
{
    protected $signature = 'dwh:create-consumer
                            {name : Descriptive name of the consumer system (e.g., Primary LMS)}
                            {--ip=* : Explicit IP addresses allowed to call the API (Optional)}';

    protected $description = 'Generates a secure cryptographically unique Client ID and Secret for a DWH API consumer';

    public function handle()
    {
        $name = $this->argument('name');
        $ips = $this->option('ip');

        // 1. Generate clean, secure keys
        $clientId = 'dwh_cli_' . Str::lower(Str::random(16));
        $plaintextSecret = Str::random(64); // Long, timing-attack secure string

        // 2. Format IP Whitelisting
        // If no IPs are passed during local Postman testing, we default to localhost
        if (empty($ips)) {
            $ips = ['127.0.0.1', '::1'];
            $this->comment('No explicit IPs provided. Defaulting whitelist to localhost (127.0.0.1, ::1).');
        }

        // 3. Persist the record with a SHA-256 hash
        DB::connection('mysql')->table('Dwh_Api_Consumers')->insert([
            'client_name'      => $name,
            'client_id'        => $clientId,
            'api_secret_hash'  => hash('sha256', $plaintextSecret),
            'allowed_ips'      => json_encode($ips),
            'is_active'        => 1,
            'created_at'       => now(),
            'updated_at'       => now(),
        ]);

        // 4. Output plaintext values to terminal screen
        $this->newLine();
        $this->info("🚀 API Consumer Identity Created Successfully!");
        $this->line("--------------------------------------------------------------------------------");
        $this->line("Client Name:    <comment>{$name}</comment>");
        $this->line("Client ID:      <info>{$clientId}</info>");
        $this->line("Client Secret:  <info>{$plaintextSecret}</info>");
        $this->line("Whitelisted IPs:<comment>" . implode(', ', $ips) . "</comment>");
        $this->line("--------------------------------------------------------------------------------");
        $this->warn("⚠️  SAVE THIS SECRET NOW! It is securely hashed and cannot be retrieved later.");
        $this->newLine();

        return Command::SUCCESS;
    }
}

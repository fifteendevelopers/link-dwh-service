<?php

namespace App\Http\Middleware;

use Closure;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\DB;
use Symfony\Component\HttpFoundation\Response;

class ValidateDwhConsumer
{
    public function handle(Request $request, Closure $next): Response
    {
        // Get credentials from secure Request Headers
        $clientId = $request->header('X-DWH-Client-ID');
        $clientSecret = $request->header('X-DWH-Client-Secret');

        if (!$clientId || !$clientSecret) {
            return response()->json([
                'success' => false,
                'message' => 'Access Denied: Missing authentication identity wrappers.'
            ], 401);
        }

        // Get the consumer profile from the DWH database
        $consumer = DB::connection('mysql')->table('Dwh_Api_Consumers')
            ->where('client_id', $clientId)
            ->where('is_active', 1)
            ->first();

        if (!$consumer) {
            return response()->json([
                'success' => false,
                'message' => 'Access Denied: Invalid or revoked Client Identifier.'
            ], 401);
        }

        // Crypto Validation Layer
        // Uses standard SHA-256 validation against the stored secure secret hash
        $incomingHash = hash('sha256', $clientSecret);
        if (!hash_equals($consumer->api_secret_hash, $incomingHash)) {
            return response()->json([
                'success' => false,
                'message' => 'Access Denied: Signature authentication failed.'
            ], 401);
        }

        // Network Security Layer (Application-Level IP Whitelisting)
        if (!empty($consumer->allowed_ips)) {
            $allowedIps = json_decode($consumer->allowed_ips, true) ?? [];
            $incomingIp = $request->ip();

            if (!in_array($incomingIp, $allowedIps)) {
                return response()->json([
                    'success' => false,
                    'message' => "Access Denied: Origin network address '{$incomingIp}' is not authorized."
                ], 403); // 403 Forbidden indicates auth passed but network layer blocked it
            }
        }

        // Audit Logging Trail: Log the successful hit timestamp asynchronously
        DB::connection('mysql')->table('Dwh_Api_Consumers')
            ->where('id', $consumer->id)
            ->update(['last_used_at' => now()]);

        return $next($request);
    }
}

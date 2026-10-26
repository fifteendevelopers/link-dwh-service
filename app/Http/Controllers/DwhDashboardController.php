<?php

namespace App\Http\Controllers;

use App\Services\DataWarehouseQualityService;
use Carbon\Carbon;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\DB;
use Illuminate\View\View;

class DwhDashboardController extends Controller
{
    public function index(Request $request, DataWarehouseQualityService $qualityService): View
    {
        // 1. Fetch recent pipeline executions
        $runs = DB::table('dwh_sync_runs')
            ->orderByDesc('id')
            ->limit(25)
            ->get()
            ->map(function ($run) {
                $run->sync_errors = json_decode($run->sync_errors ?? '[]', true) ?: [];
                $run->audit_results = json_decode($run->audit_results ?? '[]', true) ?: [];
                return $run;
            });

        $latestRun = $runs->first();

        // 2. High-level KPI aggregations
        $past24hFailures = DB::table('dwh_sync_runs')
            ->where('started_at', '>=', now()->subDay())
            ->where('status', 'FAILURE')
            ->count();

        // 3. Optional live parity check (triggered via ?live=1 or on load)
        $liveParity = null;
        if ($request->boolean('live', false)) {
            $liveParity = $qualityService->auditAll(null, 5);
        }

        $syncLogs = DB::table('Sync_Log')
            ->orderBy('table_name', 'asc')
            ->get();

        return view('dwh.dashboard', [
            'runs'             => $runs,
            'latestRun'        => $latestRun,
            'past24hFailures'  => $past24hFailures,
            'liveParity'       => $liveParity,
            'syncLogs'         => $syncLogs,
        ]);
    }
}

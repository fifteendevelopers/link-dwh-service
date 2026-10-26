<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>DWH Telemetry & Audit Dashboard</title>
    <script src="https://cdn.tailwindcss.com"></script>
</head>
<body class="bg-slate-100 text-slate-800 antialiased font-sans">
<div class="min-h-screen flex flex-col">

    <!-- Navigation Bar -->
    <header class="bg-white border-b border-slate-200 px-6 py-4 flex justify-between items-center shadow-sm">
        <div class="flex items-center space-x-3">
            <div class="w-8 h-8 rounded bg-slate-900 text-white flex items-center justify-center font-bold text-sm">DW</div>
            <div>
                <h1 class="font-bold text-slate-900 text-base leading-tight">Link DWH Operations</h1>
                <span class="text-xs text-slate-500">Live Parity & Sync Monitor</span>
            </div>
        </div>
        <div class="flex items-center space-x-3 text-xs">
            <span class="text-slate-500">Logged in as: <strong class="text-slate-800">{{ auth()->user()->email ?? 'Admin' }}</strong></span>
            <form action="{{ route('dwh.logout') }}" method="POST">
                @csrf
                <button type="submit" class="px-3 py-1.5 rounded bg-slate-200 hover:bg-slate-300 text-slate-700 font-semibold transition">
                    Sign Out
                </button>
            </form>
        </div>
    </header>

    <main class="flex-1 max-w-7xl w-full mx-auto p-6 space-y-6">

        <!-- KPI Cards Row -->
        <div class="grid grid-cols-1 md:grid-cols-4 gap-4">

            <div class="bg-white p-5 rounded-lg border border-slate-200 shadow-sm space-y-1">
                <span class="text-xs font-semibold uppercase tracking-wider text-slate-400">Last Sync Status</span>
                <div class="flex items-center space-x-2 pt-1">
                    @if(!$latestRun)
                        <span class="text-slate-400 font-medium">No Runs</span>
                    @elseif($latestRun->status === 'SUCCESS')
                        <span class="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-bold bg-emerald-100 text-emerald-800">
                                COMPLETE MATCH
                            </span>
                    @elseif($latestRun->status === 'WARNING')
                        <span class="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-bold bg-amber-100 text-amber-800">
                                AUDIT DRIFT
                            </span>
                    @else
                        <span class="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-bold bg-rose-100 text-rose-800">
                                CRASH / ERROR
                            </span>
                    @endif
                </div>
                <p class="text-xs text-slate-500 pt-1">
                    {{ $latestRun ? \Carbon\Carbon::parse($latestRun->started_at)->diffForHumans() : '-' }}
                </p>
            </div>

            <div class="bg-white p-5 rounded-lg border border-slate-200 shadow-sm space-y-1">
                <span class="text-xs font-semibold uppercase tracking-wider text-slate-400">Latest Pipeline Duration</span>
                <div class="text-2xl font-bold text-slate-900 pt-1">
                    {{ $latestRun ? gmdate('H:i:s', $latestRun->duration_seconds) : '--:--:--' }}
                </div>
                <p class="text-xs text-slate-500 pt-1">Target: <span class="font-mono">{{ $latestRun->target_table ?? 'ALL' }}</span></p>
            </div>

            <div class="bg-white p-5 rounded-lg border border-slate-200 shadow-sm space-y-1">
                <span class="text-xs font-semibold uppercase tracking-wider text-slate-400">Audited Dimensions</span>
                <div class="text-2xl font-bold text-slate-900 pt-1">
                    {{ $latestRun && !empty($latestRun->audit_results) ? count($latestRun->audit_results) : 12 }}
                </div>
                <p class="text-xs text-slate-500 pt-1">Core & pivot tables evaluated</p>
            </div>

            <div class="bg-white p-5 rounded-lg border border-slate-200 shadow-sm space-y-1">
                <span class="text-xs font-semibold uppercase tracking-wider text-slate-400">24h Pipeline Failures</span>
                <div class="text-2xl font-bold {{ $past24hFailures > 0 ? 'text-rose-600' : 'text-emerald-600' }} pt-1">
                    {{ $past24hFailures }}
                </div>
                <p class="text-xs text-slate-500 pt-1">Uncaught exceptions in 24 hours</p>
            </div>

        </div>

        <!-- Live Parity Overview / Snapshot Section -->
        <div class="bg-white rounded-lg border border-slate-200 shadow-sm overflow-hidden">
            <div class="px-6 py-4 border-b border-slate-200 flex justify-between items-center bg-slate-50">
                <div>
                    <h2 class="font-bold text-slate-900 text-sm">Entity Parity Health Matrix</h2>
                    <p class="text-xs text-slate-500">
                        {{ $liveParity ? 'Live real-time database query results' : 'Results recorded from last nightly audit' }}
                    </p>
                </div>
                <div class="flex items-center space-x-2">
                    @if(!$liveParity)
                        <a href="{{ route('dwh.dashboard', ['live' => 1]) }}"
                           class="px-3 py-1.5 bg-indigo-50 border border-indigo-200 text-indigo-700 hover:bg-indigo-100 rounded text-xs font-semibold transition">
                            Run Live Audit Now
                        </a>
                    @else
                        <a href="{{ route('dwh.dashboard') }}"
                           class="px-3 py-1.5 bg-slate-200 text-slate-700 hover:bg-slate-300 rounded text-xs font-semibold transition">
                            View Snapshot Mode
                        </a>
                    @endif
                </div>
            </div>

            @php
                $rawMatrix = $liveParity ?? ($latestRun->audit_results ?? []);
                // Sort alphabetically by entity name (key)
                $matrixData = collect($rawMatrix)->sortKeys()->all();
            @endphp

            <table class="w-full text-left text-xs border-collapse">
                <thead class="bg-slate-100/75 text-slate-600 uppercase font-semibold border-b border-slate-200">
                <tr>
                    <th class="px-6 py-3">Entity</th>
                    <th class="px-6 py-3 text-right">Source Count</th>
                    <th class="px-6 py-3 text-right">DWH Count</th>
                    <th class="px-6 py-3 text-center">Variance</th>
                    <th class="px-6 py-3 text-center">Status</th>
                    <th class="px-6 py-3">Missing Samples</th>
                </tr>
                </thead>
                <tbody class="divide-y divide-slate-200">
                @forelse($matrixData as $entity => $data)
                    @php
                        $isMatch = !empty($data['match']);
                        $diff = ($data['dwh_count'] ?? 0) - ($data['source_count'] ?? 0);
                    @endphp
                    <tr class="hover:bg-slate-50 transition-colors">
                        <td class="px-6 py-3 font-semibold text-slate-900">{{ $entity }}</td>
                        <td class="px-6 py-3 text-right font-mono text-slate-700">{{ number_format($data['source_count'] ?? 0) }}</td>
                        <td class="px-6 py-3 text-right font-mono text-slate-700">{{ number_format($data['dwh_count'] ?? 0) }}</td>
                        <td class="px-6 py-3 text-center font-mono {{ $diff === 0 ? 'text-slate-400' : ($diff > 0 ? 'text-blue-600 font-bold' : 'text-amber-600 font-bold') }}">
                            {{ $diff > 0 ? "+{$diff}" : $diff }}
                        </td>
                        <td class="px-6 py-3 text-center">
                            @if($isMatch)
                                <span class="inline-block px-2 py-0.5 rounded text-[11px] font-bold bg-emerald-100 text-emerald-800">
                                        MATCH
                                    </span>
                            @else
                                <span class="inline-block px-2 py-0.5 rounded text-[11px] font-bold bg-amber-100 text-amber-800">
                                        MISMATCH
                                    </span>
                            @endif
                        </td>
                        <td class="px-6 py-3 font-mono text-slate-500">
                            @if(!empty($data['missing_ids']))
                                <span class="text-rose-600 font-medium">[{{ implode(', ', array_slice($data['missing_ids'], 0, 5)) }}]</span>
                            @else
                                <span class="text-slate-400">None</span>
                            @endif
                        </td>
                    </tr>
                @empty
                    <tr>
                        <td colspan="6" class="px-6 py-6 text-center text-slate-400 italic">No audit metrics recorded yet.</td>
                    </tr>
                @endforelse
                </tbody>
            </table>
        </div>

        <!-- Historical Sync Executions Table with Collapsible Logs -->
        <div class="bg-white rounded-lg border border-slate-200 shadow-sm overflow-hidden">
            <div class="px-6 py-4 border-b border-slate-200 bg-slate-50">
                <h2 class="font-bold text-slate-900 text-sm">Execution History & Detailed Run Logs</h2>
                <p class="text-xs text-slate-500">Last 25 pipeline cycles with step failure diagnostics</p>
            </div>

            <table class="w-full text-left text-xs border-collapse">
                <thead class="bg-slate-100/75 text-slate-600 uppercase font-semibold border-b border-slate-200">
                <tr>
                    <th class="px-6 py-3">Timestamp</th>
                    <th class="px-6 py-3">Target</th>
                    <th class="px-6 py-3">Duration</th>
                    <th class="px-6 py-3 text-center">Status</th>
                    <th class="px-6 py-3">Action & Diagnostics</th>
                </tr>
                </thead>
                <tbody class="divide-y divide-slate-200">
                @forelse($runs as $run)
                    <tr class="hover:bg-slate-50 transition-colors">
                        <td class="px-6 py-3.5 font-medium text-slate-900 whitespace-nowrap">
                            {{ \Carbon\Carbon::parse($run->started_at)->format('Y-m-d H:i:s') }}
                        </td>
                        <td class="px-6 py-3.5 font-mono text-slate-600">{{ $run->target_table }}</td>
                        <td class="px-6 py-3.5 font-mono text-slate-600">{{ gmdate('H:i:s', $run->duration_seconds) }}</td>
                        <td class="px-6 py-3.5 text-center">
                            @if($run->status === 'SUCCESS')
                                <span class="px-2 py-0.5 rounded text-[11px] font-bold bg-emerald-100 text-emerald-800">SUCCESS</span>
                            @elseif($run->status === 'WARNING')
                                <span class="px-2 py-0.5 rounded text-[11px] font-bold bg-amber-100 text-amber-800">WARNINGS</span>
                            @else
                                <span class="px-2 py-0.5 rounded text-[11px] font-bold bg-rose-100 text-rose-800">FAILED</span>
                            @endif
                        </td>
                        <td class="px-6 py-3.5">
                            <details class="group">
                                <summary class="cursor-pointer text-indigo-600 hover:text-indigo-800 font-semibold select-none">
                                    Inspect Logs & Errors ({{ count($run->sync_errors) }} errors)
                                </summary>
                                <div class="mt-2.5 p-3 bg-slate-900 text-slate-200 rounded text-xs font-mono space-y-2">
                                    @if(!empty($run->sync_errors))
                                        <div class="text-rose-400 font-bold border-b border-slate-800 pb-1">Failed Steps:</div>
                                        @foreach($run->sync_errors as $step => $err)
                                            <div><span class="text-amber-400">{{ $step }}:</span> {{ $err }}</div>
                                        @endforeach
                                    @else
                                        <div class="text-emerald-400">All pipeline ETL stages executed with 0 errors.</div>
                                    @endif
                                </div>
                            </details>
                        </td>
                    </tr>
                @empty
                    <tr>
                        <td colspan="5" class="px-6 py-6 text-center text-slate-400 italic">No execution logs found.</td>
                    </tr>
                @endforelse
                </tbody>
            </table>
        </div>
        <div class="mt-8 bg-white dark:bg-gray-900 shadow rounded-lg border border-gray-200 dark:border-gray-800 overflow-hidden">
            <div class="px-6 py-4 border-b border-gray-200 dark:border-gray-800 flex justify-between items-center bg-gray-50 dark:bg-gray-800/50">
                <div>
                    <h3 class="text-base font-semibold text-gray-900 dark:text-white">DWH Table Sync Log & Watermarks</h3>
                    <p class="text-xs text-gray-500 dark:text-gray-400">Current high-water marks and record counters per synchronized entity.</p>
                </div>
                <span class="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-blue-100 text-blue-800 dark:bg-blue-900/30 dark:text-blue-300">
            {{ $syncLogs->count() }} Entities Monitored
        </span>
            </div>

            <div class="overflow-x-auto">
                <table class="min-w-full divide-y divide-gray-200 dark:divide-gray-800 text-sm text-left">
                    <thead class="bg-gray-50 dark:bg-gray-800/50 text-xs font-semibold text-gray-500 uppercase tracking-wider">
                    <tr>
                        <th scope="col" class="px-6 py-3">Table Name</th>
                        <th scope="col" class="px-6 py-3">Last Sync Watermark</th>
                        <th scope="col" class="px-6 py-3 text-right">Last Source ID</th>
                        <th scope="col" class="px-6 py-3 text-right">Records Processed</th>
                    </tr>
                    </thead>
                    <tbody class="divide-y divide-gray-200 dark:divide-gray-800 font-mono text-xs">
                    @forelse($syncLogs as $log)
                        <tr class="hover:bg-gray-50 dark:hover:bg-gray-800/40 transition">
                            <td class="px-6 py-3 font-semibold text-gray-900 dark:text-gray-100">
                                {{ $log->Table_Name }}
                            </td>
                            <td class="px-6 py-3 text-gray-600 dark:text-gray-400">
                                @if(!empty($log->Last_Synced_At))
                                    <span class="bg-gray-100 dark:bg-gray-800 px-2 py-0.5 rounded text-gray-800 dark:text-gray-200">
                                    {{ $log->Last_Synced_At }}
                                </span>
                                @else
                                    <span class="text-gray-400 italic">None (Full Scan)</span>
                                @endif
                            </td>
                            <td class="px-6 py-3 text-right text-gray-600 dark:text-gray-300">
                                {{ $log->Last_Source_Id }}
                            </td>
                            <td class="px-6 py-3 text-right font-medium text-emerald-600 dark:text-emerald-400">
                                {{ number_format($log->Records_Processed ?? 0) }}
                            </td>
                        </tr>
                    @empty
                        <tr>
                            <td colspan="5" class="px-6 py-8 text-center text-gray-500 italic">
                                No sync log entries recorded yet.
                            </td>
                        </tr>
                    @endforelse
                    </tbody>
                </table>
            </div>
        </div>


    </main>
</div>
</body>
</html>

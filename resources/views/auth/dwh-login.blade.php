<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>DWH Portal &mdash; Login</title>
    <script src="https://cdn.tailwindcss.com"></script>
</head>
<body class="bg-slate-900 min-h-screen flex items-center justify-center p-4">
<div class="max-w-md w-full bg-slate-800 border border-slate-700 rounded-xl shadow-xl p-8 space-y-6">
    <div class="text-center space-y-2">
        <div class="inline-flex items-center justify-center w-12 h-12 rounded-lg bg-indigo-600/20 text-indigo-400 mb-2">
            <svg class="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 7v10c0 2 1.5 3 3.5 3h9c2 0 3.5-1 3.5-3V7c0-2-1.5-3-3.5-3h-9C5.5 4 4 5 4 7zm0 5h16" />
            </svg>
        </div>
        <h1 class="text-xl font-bold text-white tracking-wide">Data Warehouse Portal</h1>
        <p class="text-xs text-slate-400">Authenticate using your Link source administrator account</p>
    </div>

    @if($errors->any())
        <div class="p-3.5 bg-rose-500/10 border border-rose-500/30 rounded-lg text-xs text-rose-400 space-y-1">
            @foreach($errors->all() as $error)
                <div>{{ $error }}</div>
            @endforeach
        </div>
    @endif

    <form action="{{ route('dwh.login.submit') }}" method="POST" class="space-y-4">
        @csrf
        <div>
            <label for="email" class="block text-xs font-semibold uppercase tracking-wider text-slate-400 mb-1">Source Account Email</label>
            <input type="email" name="email" id="email" required autofocus
                   class="w-full px-3.5 py-2.5 bg-slate-900 border border-slate-700 rounded-lg text-white text-sm focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:border-transparent placeholder-slate-600"
                   placeholder="admin@link.internal">
        </div>

        <div>
            <label for="password" class="block text-xs font-semibold uppercase tracking-wider text-slate-400 mb-1">Password</label>
            <input type="password" name="password" id="password" required
                   class="w-full px-3.5 py-2.5 bg-slate-900 border border-slate-700 rounded-lg text-white text-sm focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:border-transparent placeholder-slate-600"
                   placeholder="••••••••••••">
        </div>

        <div class="flex items-center justify-between text-xs text-slate-400">
            <label class="flex items-center space-x-2 cursor-pointer">
                <input type="checkbox" name="remember" class="rounded bg-slate-900 border-slate-700 text-indigo-600 focus:ring-0">
                <span>Remember session</span>
            </label>
        </div>

        <button type="submit"
                class="w-full py-2.5 px-4 bg-indigo-600 hover:bg-indigo-500 text-white text-sm font-semibold rounded-lg shadow transition-colors">
            Sign In to DWH
        </button>
    </form>
</div>
</body>
</html>

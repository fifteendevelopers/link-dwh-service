<?php

use App\Http\Controllers\Auth\DwhLoginController;
use App\Http\Controllers\DwhDashboardController;
use Illuminate\Support\Facades\Route;

/**
 * instruct legitimate web crawlers (Google, Bing, etc.) not to index the domain.
 */
Route::get('/robots.txt', function () {
    return response("User-agent: *\nDisallow: /", 200)
        ->header('Content-Type', 'text/plain');
});

Route::prefix('admin/dwh')->group(function () {
    // Guest authentication
    Route::middleware('guest')->group(function () {
        Route::get('login', [DwhLoginController::class, 'showLoginForm'])->name('dwh.login'); // Standard Laravel name
        Route::post('login', [DwhLoginController::class, 'login'])->name('dwh.login.submit');
    });

    // Protected DWH dashboard
    Route::middleware('auth')->group(function () {
        Route::get('/', [DwhDashboardController::class, 'index'])->name('dwh.dashboard');
        Route::post('logout', [DwhLoginController::class, 'logout'])->name('dwh.logout');
    });
});

/**
 * Redirect all other requests but API requests to the main bikeability website
 */
Route::any('{any}', function () {
    return redirect('https://bikeability.org.uk', 301); // 301 = Permanent Redirect
})->where('any', '^(?!api).*$');

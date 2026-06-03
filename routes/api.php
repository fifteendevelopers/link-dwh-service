<?php

use App\Http\Controllers\Api\ReportRouterController;
use Illuminate\Support\Facades\Route;

Route::middleware('dwh.auth')->group(function () {
    Route::post('/reports/execute', [ReportRouterController::class, 'execute']);
});


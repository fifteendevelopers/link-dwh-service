<?php

use Illuminate\Support\Facades\Route;

/**
 * instruct legitimate web crawlers (Google, Bing, etc.) not to index the domain.
 */
Route::get('/robots.txt', function () {
    return response("User-agent: *\nDisallow: /", 200)
        ->header('Content-Type', 'text/plain');
});

/**
 * Redirect all but API requests to the main bikeability website
 */
Route::any('{any}', function () {
    return redirect('https://bikeability.org.uk', 301); // 301 = Permanent Redirect
})->where('any', '^(?!api).*$');

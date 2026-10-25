<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Model;

class DwhSyncRun extends Model
{
    protected $connection = 'mysql';
    protected $table = 'dwh_sync_runs';

    protected $fillable = [
        'target_table',
        'triggered_by',
        'started_at',
        'finished_at',
        'duration_seconds',
        'status',
        'sync_errors',
        'audit_results',
    ];

    protected $casts = [
        'started_at'    => 'datetime',
        'finished_at'   => 'datetime',
        'sync_errors'   => 'array',
        'audit_results' => 'array',
    ];
}

<?php

namespace App\Models;

use Illuminate\Foundation\Auth\User as Authenticatable;

class SourceUser extends Authenticatable
{
    protected $connection = 'mysql_src';
    protected $table = 'users';

    // Disable timestamps or write operations to prevent DWH modifying source credentials
    public $timestamps = false;
    protected $guarded = ['*'];

    /**
     * Map password column if your source system uses a non-standard name.
     */
    public function getAuthPassword(): string
    {
        return $this->password;
    }
}

<?php

namespace Database\Seeders;

use Illuminate\Database\Seeder;
use Illuminate\Support\Facades\DB;

class SourceSystemSeeder extends Seeder
{
    public function run(): void
    {
        DB::connection('mysql')->table('Dim_Source_System')->updateOrInsert(
            ['System_Name' => 'link'],
            [
                'System_Type' => 'laravel',
                'Last_Sync_Date' => now()
            ]
        );
    }
}

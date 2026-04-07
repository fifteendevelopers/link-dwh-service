<?php

namespace Database\Seeders;

use Illuminate\Database\Seeder;
use Illuminate\Support\Facades\DB;

class DateSeeder extends Seeder
{
    public function run(): void
    {
        $conn = DB::connection('mysql');

        // Increase recursion depth for long date ranges
        $conn->statement("SET SESSION cte_max_recursion_depth = 50000;");

        $start = '2019-01-01';
        $end   = '2050-12-31';

        $sql = "
            INSERT INTO Dim_Date (
                Date_Key, Full_Date, Month_Name, Calendar_Year,
                Financial_Year, Financial_Quarter, Financial_Month_Number, Is_Weekend
            )
            WITH RECURSIVE DateRange AS (
                SELECT CAST(? AS DATE) AS d
                UNION ALL
                SELECT d + INTERVAL 1 DAY FROM DateRange WHERE d < ?
            )
            SELECT
                REPLACE(d, '-', '') AS Date_Key,
                d AS Full_Date,
                MONTHNAME(d) AS Month_Name,
                YEAR(d) AS Calendar_Year,
                CASE
                    WHEN MONTH(d) >= 4 THEN CONCAT('FY ', YEAR(d), '-', SUBSTRING(YEAR(d) + 1, 3, 2))
                    ELSE CONCAT('FY ', YEAR(d) - 1, '-', SUBSTRING(YEAR(d), 3, 2))
                END AS Financial_Year,
                CASE
                    WHEN MONTH(d) IN (4, 5, 6) THEN 'Q1'
                    WHEN MONTH(d) IN (7, 8, 9) THEN 'Q2'
                    WHEN MONTH(d) IN (10, 11, 12) THEN 'Q3'
                    ELSE 'Q4'
                END AS Financial_Quarter,
                CASE
                    WHEN MONTH(d) >= 4 THEN MONTH(d) - 3
                    ELSE MONTH(d) + 9
                END AS Financial_Month_Number,
                CASE WHEN DAYOFWEEK(d) IN (1, 7) THEN TRUE ELSE FALSE END AS Is_Weekend
            FROM DateRange
            ON DUPLICATE KEY UPDATE Full_Date = VALUES(Full_Date);
        ";

        $conn->statement($sql, [$start, $end]);
    }
}

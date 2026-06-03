<?php
// app/Reports/ReportFactory.php

namespace App\Reports;

use App\Reports\Contracts\ReportHandlerInterface;
use App\Reports\Handlers\GrFleetCyclesUsedHandler;
use App\Reports\Handlers\PreCourseCycleFrequencyHandler;
// Import other report classes here...

class ReportFactory
{
    /**
     * Internal report routing registry map.
     */
    protected static array $registry = [
        'pre-course-cycle-frequency' => PreCourseCycleFrequencyHandler::class,
        'report-29.0'                => PreCourseCycleFrequencyHandler::class, // Supports aliases
        'gr-fleet-cycles-used'       => GrFleetCyclesUsedHandler::class,
    ];

    /**
     * Resolves and builds the requested report handler instance.
     */
    public static function make(string $key): ReportHandlerInterface
    {
        if (!isset(self::$registry[$key])) {
            throw new \InvalidArgumentException("Report profile key '{$key}' is not registered.");
        }

        $class = self::$registry[$key];
        return new $class();
    }
}

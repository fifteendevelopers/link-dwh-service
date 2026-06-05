<?php
// app/Reports/ReportFactory.php

namespace App\Reports;

use App\Reports\Contracts\ReportHandlerInterface;
use App\Reports\Handlers\DeliveriesPerGrantRecipientHandler;
use App\Reports\Handlers\GrantMovementsFinancialsHandler;
use App\Reports\Handlers\GrFleetCyclesUsedHandler;
use App\Reports\Handlers\InstructorsDeliveriesAllocationHandler;
use App\Reports\Handlers\PostCourseSurveyHandler;
use App\Reports\Handlers\PreCourseCycleFrequencyHandler;
use App\Reports\Handlers\PreCourseFrequencyHandler;
use App\Reports\Handlers\TpHandsUpSurveyHandler;

// Import other report classes here...

class ReportFactory
{
    /**
     * Internal report routing registry map.
     */
    protected static array $registry = [
        'pre-course-cycle-frequency'        => PreCourseCycleFrequencyHandler::class,
        'report-29.0'                       => PreCourseCycleFrequencyHandler::class, // Supports aliases
        'gr-fleet-cycles-used'              => GrFleetCyclesUsedHandler::class,
        'deliveries-per-grant-recipient'    => DeliveriesPerGrantRecipientHandler::class,
        'pre-course-frequency'              => PreCourseFrequencyHandler::class,
        'tp-hands-up-survey'                => TpHandsUpSurveyHandler::class,
        'instructors-deliveries-allocation' => InstructorsDeliveriesAllocationHandler::class,
        'grant-movements-financials'        => GrantMovementsFinancialsHandler::class,
        'post-course-survey'                => PostCourseSurveyHandler::class,
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

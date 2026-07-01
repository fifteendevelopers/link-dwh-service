<?php

namespace App\Reports;

use App\Reports\Contracts\ReportHandlerInterface;
use App\Reports\Handlers\DeliveriesHandler;
use App\Reports\Handlers\DeliveriesPerGrantRecipientHandler;
use App\Reports\Handlers\GrantMovementsFinancialsHandler;
use App\Reports\Handlers\GrFleetCyclesUsedHandler;
use App\Reports\Handlers\InstructorsDeliveriesAllocationHandler;
use App\Reports\Handlers\ParentContactsHandler;
use App\Reports\Handlers\PostCourseSurveyHandler;
use App\Reports\Handlers\PreCourseCycleFrequencyHandler;
use App\Reports\Handlers\PreCourseFrequencyHandler;
use App\Reports\Handlers\SchoolDeliveriesAuditHandler;
use App\Reports\Handlers\SchoolDeliveriesHandler;
use App\Reports\Handlers\TpHandsUpSurveyHandler;

class ReportFactory
{
    /**
     * Internal report routing registry map.
     */
    protected static array $registry = [
        'deliveries'                        => DeliveriesHandler::class,
        'deliveries-per-grant-recipient'    => DeliveriesPerGrantRecipientHandler::class,
        'gr-fleet-cycles-used'              => GrFleetCyclesUsedHandler::class,
        'grant-funding-overview'            => Handlers\GrantFundingOverviewHandler::class,
        'grant-movements-financials'        => GrantMovementsFinancialsHandler::class,
        'instructors-deliveries-allocation' => InstructorsDeliveriesAllocationHandler::class,
        'parent_contacts'                   => ParentContactsHandler::class,
        'post-course-survey'                => PostCourseSurveyHandler::class,
        'pre-course-cycle-frequency'        => PreCourseCycleFrequencyHandler::class,
        'pre-course-frequency'              => PreCourseFrequencyHandler::class,
        'report-29.0'                       => PreCourseCycleFrequencyHandler::class, // e.g. Supports aliases
        'school-deliveries'                 => SchoolDeliveriesHandler::class,
        'school-deliveries-audit'           => SchoolDeliveriesAuditHandler::class,
        'tp-hands-up-survey'                => TpHandsUpSurveyHandler::class,
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

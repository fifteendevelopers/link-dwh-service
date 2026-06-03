<?php
// app/Reports/Contracts/ReportHandlerInterface.php

namespace App\Reports\Contracts;

interface ReportHandlerInterface
{
    /**
     * Define and validate specific rules required for this report.
     */
    public function validate(array $parameters): array;

    /**
     * Run the DWH query and return the formatted raw array payload.
     */
    public function execute(array $parameters): array;
}

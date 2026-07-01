<?php

namespace App\Http\Controllers\Api;

use App\Http\Controllers\Controller;
use App\Jobs\StreamReportToCallbackJob;
use App\Reports\ReportFactory;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\Log;
use Illuminate\Support\Facades\Validator;
use Illuminate\Http\JsonResponse;

class ReportRouterController extends Controller
{
    public function execute(Request $request): JsonResponse
    {
        // Validate the primary wrapper payload structure
        $wrapperValidator = Validator::make($request->all(), [
            'report_key'    => 'required|string',
            'parameters'    => 'present|array',
            'callback_url'  => 'nullable|url',
            'job_id'        => 'nullable|integer'
        ]);

        if ($wrapperValidator->fails()) {
            return response()->json([
                'success' => false,
                'message' => 'Invalid request structure.',
                'errors'  => $wrapperValidator->errors()
            ], 422);
        }

        $reportKey   = $request->input('report_key');
        $parameters  = $request->input('parameters');
        $callbackUrl = $request->input('callback_url');
        $jobId       = $request->input('job_id');

        try {
            // Resolve the concrete reporting handler class
            $handler = ReportFactory::make($reportKey);

            // Enforce report-specific parameter validation rules (dates, IDs, etc.)
            $validatedParams = $handler->validate($parameters);

            if (!empty($callbackUrl)) {
                // Offload the entire handler processing execution task to the queue workers
                StreamReportToCallbackJob::dispatch($reportKey, $validatedParams, $callbackUrl, $jobId);

                return response()->json([
                    'success'    => true,
                    'status'     => 'accepted',
                    'job_uuid'   => $jobId,
                    'message'    => 'The report generation sequence has been safely deferred to background data stream processing workers.'
                ], 202);
            }

            // Run the query on the DWH connection
            $reportData = $handler->execute($validatedParams);

            return response()->json([
                'success'     => true,
                'report_key'  => $reportKey,
                'row_count'   => count($reportData),
                'data'        => $reportData
            ], 200);

        } catch (\InvalidArgumentException $e) {
            return response()->json([
                'success' => false,
                'message' => $e->getMessage()
            ], 404);
        } catch (\Illuminate\Validation\ValidationException $e) {
            return response()->json([
                'success' => false,
                'message' => 'Report parameter validation failed.',
                'errors'  => $e->errors()
            ], 422);
        } catch (\Exception $e) {
            // Catch database timeouts, locks or query execution breakages safely
            return response()->json([
                'success' => false,
                'message' => 'An anomaly occurred during DWH query processing.'.$e->getMessage(),
                'error_debug' => config('app.debug') ? $e->getMessage() : 'Internal Server Error'
            ], 500);
        }
    }
}

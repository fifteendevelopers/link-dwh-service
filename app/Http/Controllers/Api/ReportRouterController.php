<?php
// app/Http/Controllers/Api/ReportRouterController.php

namespace App\Http\Controllers\Api;

use App\Http\Controllers\Controller;
use App\Reports\ReportFactory;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\Validator;
use Illuminate\Http\JsonResponse;

class ReportRouterController extends Controller
{
    public function execute(Request $request): JsonResponse
    {
        // 1. Validate the primary wrapper payload structure
        $wrapperValidator = Validator::make($request->all(), [
            'report_key' => 'required|string',
            'parameters' => 'present|array'
        ]);

        if ($wrapperValidator->fails()) {
            return response()->json([
                'success' => false,
                'message' => 'Invalid request structure.',
                'errors'  => $wrapperValidator->errors()
            ], 422);
        }

        try {
            // 2. Resolve the concrete reporting handler class
            $handler = ReportFactory::make($request->input('report_key'));

            // 3. Enforce report-specific parameter validation rules (dates, IDs, etc.)
            $validatedParams = $handler->validate($request->input('parameters'));

            // 4. Run the query on the DWH connection
            $reportData = $handler->execute($validatedParams);

            return response()->json([
                'success'     => true,
                'report_key'  => $request->input('report_key'),
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
                'message' => 'An anomaly occurred during DWH query processing.',
                'error_debug' => config('app.debug') ? $e->getMessage() : 'Internal Server Error'
            ], 500);
        }
    }
}

// utils/api-client.ts - REFACTORED with Paginated Bulk Connections

import type {
  EntityConnectionDataResponse,
  EdgeReviewApiPayload,
  EdgeReviewApiResponse,
  DisconnectDependentServicesRequest,
  DisconnectDependentServicesResponse,
  NodeDetailResponse,
  BulkNodeDetailsRequest,
  BulkConnectionsRequest,
  PaginatedBulkConnectionsResponse, // ✨ UPDATED: Using the new paginated response type
  BulkVisualizationsRequest,
  BulkVisualizationsResponse,
  PaginatedClustersResponse,
  EntityCluster,
  ClusterFilterStatus,
  WorkflowFilter,
  ClusterProgressResponse,
  OpinionPreferences,
  GetOpinionPreferencesResponse,
  UpdateOpinionPreferencesRequest,
  UpdateOpinionPreferencesResponse,
  ResolutionMode,
} from "@/types/entity-resolution";
import {
  BulkMarkReviewedRequest,
  BulkMarkReviewedResponse,
  ClustersWithPostProcessingResponse,
  PostProcessingAuditParams,
  PostProcessingAuditResponse,
} from "@/types/post-processing";

const API_BASE_URL = "/api";

/**
 * Creates standard request headers, including the custom opinion header if provided.
 * @param opinionName - The name of the opinion to use.
 * @returns A HeadersInit object.
 */
function getApiHeaders(opinionName?: string): HeadersInit {
  const headers: HeadersInit = {
    "Content-Type": "application/json",
  };

  if (opinionName) {
    headers["X-Opinion-Name"] = opinionName;
  }

  return headers;
}

/**
 * Handles API errors by logging them and throwing a standardized error.
 * @param error - The error caught from a fetch call.
 * @param context - A string describing the context of the API call.
 * @returns Never, as it always throws an error.
 */
function handleApiError(error: unknown, context?: string): never {
  const contextMsg = context ? ` (${context})` : "";

  // Handle AbortError silently - these are expected during cancellation
  if (error instanceof Error && error.name === "AbortError") {
    console.log(`🛑 [API_CLIENT] Request cancelled${contextMsg}`);
    throw error; // Re-throw to be handled by caller
  }

  let errorMessage = "An unknown error occurred";
  if (error instanceof Error) {
    errorMessage = error.message;
  } else if (typeof error === "string") {
    errorMessage = error;
  }

  console.error(`[API_CLIENT] API Error${contextMsg}:`, errorMessage, error);
  throw new Error(errorMessage);
}

/**
 * Validates the HTTP response, checks for errors, and parses the JSON body.
 * @param response - The raw Response object from a fetch call.
 * @param context - A string describing the context of the API call.
 * @returns A promise that resolves to the parsed JSON data of type T.
 */
async function validateResponse<T>(
  response: Response,
  context?: string
): Promise<T> {
  if (!response.ok) {
    let errorData;
    try {
      errorData = await response.json();
    } catch (e) {
      // Check if the parse error is due to an aborted request
      if (e instanceof Error && e.name === "AbortError") {
        throw e; // Re-throw AbortError
      }

      // If the body isn't valid JSON, use the status text.
      errorData = {
        message:
          response.statusText ||
          `Request failed with status ${response.status}`,
      };
    }

    const errorMessage =
      errorData.message ||
      errorData.error ||
      `Request failed with status ${response.status}`;
    const fullContext = context ? `${context}: ${errorMessage}` : errorMessage;

    console.error(
      `[API_CLIENT] API Response Error: ${response.status} ${response.statusText}`,
      { context: context, error: errorData }
    );
    throw new Error(fullContext);
  }

  // Handle successful responses with no content
  if (response.status === 204) {
    return {} as T;
  }

  try {
    const responseData = await response.json();
    if (context) {
      console.log(`[API_CLIENT] ✅ ${context}: Success`);
    }
    return responseData as T;
  } catch (e) {
    // Check if the parse error is due to an aborted request
    if (e instanceof Error && e.name === "AbortError") {
      throw e; // Re-throw AbortError
    }
    throw new Error(`Failed to parse response JSON: ${(e as Error).message}`);
  }
}

// --- Audit Cluster Validation Function ---

/**
 * Validate that audit clusters have actual visualization data (nodes or links).
 * @param clusters - An array of objects, each with a cluster ID and its specific itemType.
 * @param opinionName - The name of the opinion to use for the API call.
 * @returns An object containing arrays of valid and invalid cluster IDs.
 */
export async function validateAuditClustersHaveData(
  clusters: { id: string; itemType: ResolutionMode }[],
  opinionName?: string,
  signal?: AbortSignal
): Promise<{ valid: string[]; invalid: string[] }> {
  if (clusters.length === 0) {
    return { valid: [], invalid: [] };
  }

  const context = `validateAuditClustersHaveData`;
  const url = `${API_BASE_URL}/validate-audit-clusters`;

  const payload = { clusters };

  try {
    const response = await fetch(url, {
      method: "POST",
      headers: getApiHeaders(opinionName),
      body: JSON.stringify(payload),
      signal,
    });
    const data = await validateResponse<{ valid: string[]; invalid: string[] }>(
      response,
      context
    );
    return data;
  } catch (error) {
    if (error instanceof Error && error.name === "AbortError") {
      console.log(`[API_CLIENT] Request aborted: ${context}`);
      throw error;
    }
    return handleApiError(error, context);
  }
}

// --- Post-Processing Audit API Functions ---

export async function getPostProcessingAuditData(
  params: PostProcessingAuditParams,
  opinionName?: string,
  signal?: AbortSignal
): Promise<PostProcessingAuditResponse> {
  const searchParams = new URLSearchParams();

  // ✅ FIX: Ensure all parameters are properly encoded
  if (params.entityType) {
    searchParams.set("entity_type", params.entityType);
    console.log(`[API_CLIENT] Setting entity_type: ${params.entityType}`);
  }
  if (params.postProcFilter) {
    searchParams.set("post_proc_filter", params.postProcFilter);
    console.log(
      `[API_CLIENT] Setting post_proc_filter: ${params.postProcFilter}`
    );
  }
  if (params.reviewedByHuman !== undefined) {
    searchParams.set("reviewed_by_human", params.reviewedByHuman.toString());
    console.log(
      `[API_CLIENT] Setting reviewed_by_human: ${params.reviewedByHuman}`
    );
  }
  if (params.clusterId) {
    searchParams.set("cluster_id", params.clusterId);
    console.log(`[API_CLIENT] Setting cluster_id: ${params.clusterId}`);
  }
  if (params.page) {
    searchParams.set("page", params.page.toString());
    console.log(`[API_CLIENT] Setting page: ${params.page}`);
  }
  if (params.limit) {
    searchParams.set("limit", params.limit.toString());
    console.log(`[API_CLIENT] Setting limit: ${params.limit}`);
  }

  const url = `${API_BASE_URL}/post-processing-audit?${searchParams.toString()}`;
  const context = `getPostProcessingAuditData (${searchParams.toString()})`;

  // ✅ FIX: Add detailed logging
  console.log(`[API_CLIENT] Making audit data request:`, {
    url,
    params,
    opinion: opinionName || "default",
    searchParams: searchParams.toString(),
  });

  try {
    const response = await fetch(url, {
      headers: getApiHeaders(opinionName),
      signal,
    });

    const result = await validateResponse<PostProcessingAuditResponse>(
      response,
      context
    );

    // ✅ FIX: Add response validation logging
    console.log(`[API_CLIENT] Audit data response received:`, {
      decisions: result.decisions,
      decisionsCount: result.decisions?.length || 0,
      total: result.total,
      page: result.page,
      clusterId: params.clusterId,
      uniqueClusterIds: params.clusterId
        ? [...new Set(result.decisions?.map((d) => d.clusterId) || [])]
        : "not filtering by cluster",
    });

    // ✅ FIX: Validate cluster-specific responses
    if (params.clusterId && result.decisions) {
      const relevantDecisions = result.decisions.filter(
        (d) => d.clusterId === params.clusterId
      );
      if (relevantDecisions.length !== result.decisions.length) {
        console.warn(
          `[API_CLIENT] Response contains decisions for other clusters:`,
          {
            requested: params.clusterId,
            relevant: relevantDecisions.length,
            total: result.decisions.length,
            otherClusters: [
              ...new Set(
                result.decisions
                  .filter((d) => d.clusterId !== params.clusterId)
                  .map((d) => d.clusterId)
              ),
            ],
          }
        );
      }
    }

    return result;
  } catch (error) {
    if (error instanceof Error && error.name === "AbortError") {
      console.log(`[API_CLIENT] Request aborted: ${context}`);
      throw error;
    }
    console.error(`[API_CLIENT] Audit data request failed:`, {
      url,
      params,
      error: error,
    });
    return handleApiError(error, context);
  }
}

export async function detectAuditDataEntityTypes(
  opinionName?: string,
  signal?: AbortSignal
): Promise<{
  entityTypes: ("entity" | "service")[];
  counts: Record<string, number>;
}> {
  console.log("[API_CLIENT] Detecting available audit data entity types...");

  const results: {
    entityTypes: ("entity" | "service")[];
    counts: Record<string, number>;
  } = {
    entityTypes: [],
    counts: {},
  };

  // Check both entity types
  for (const entityType of ["entity", "service"] as const) {
    try {
      const response = await getClustersWithPostProcessingDecisions(
        {
          entityType,
          reviewedByHuman: false,
          page: 1,
          limit: 1, // Just need to check if any exist
        },
        opinionName,
        signal
      );

      if (
        response.clusters.length > 0 ||
        response.auditCounts.totalUnreviewed > 0
      ) {
        results.entityTypes.push(entityType);
        results.counts[entityType] = response.auditCounts.totalUnreviewed;
        console.log(
          `[API_CLIENT] Found ${response.auditCounts.totalUnreviewed} unreviewed audit decisions for ${entityType} type`
        );
      }
    } catch (error) {
      console.warn(
        `[API_CLIENT] Error checking ${entityType} audit data:`,
        error
      );
    }
  }

  console.log("[API_CLIENT] Audit data detection complete:", results);
  return results;
}

export async function getClustersWithPostProcessingDecisions(
  params: PostProcessingAuditParams,
  opinionName?: string,
  signal?: AbortSignal
): Promise<ClustersWithPostProcessingResponse> {
  const searchParams = new URLSearchParams();

  if (params.entityType) searchParams.set("entity_type", params.entityType);
  if (params.postProcFilter)
    searchParams.set("post_proc_filter", params.postProcFilter);
  if (params.reviewedByHuman !== undefined)
    searchParams.set("reviewed_by_human", params.reviewedByHuman.toString());
  if (params.page) searchParams.set("page", params.page.toString());
  if (params.limit) searchParams.set("limit", params.limit.toString());
  if (params.workflowFilter)
    searchParams.set("workflow_filter", params.workflowFilter);

  const url = `${API_BASE_URL}/post-processing-audit/clusters?${searchParams.toString()}`;
  const context = `getClustersWithPostProcessingDecisions (${searchParams.toString()})`;

  // ✅ ENHANCED: Add more detailed logging
  console.log(`[API_CLIENT] Requesting audit clusters:`, {
    url,
    params,
    opinion: opinionName || "default",
    searchParams: searchParams.toString(),
  });

  try {
    const response = await fetch(url, {
      headers: getApiHeaders(opinionName),
      signal,
    });

    const result = await validateResponse<ClustersWithPostProcessingResponse>(
      response,
      context
    );

    // ✅ ENHANCED: Add response validation logging
    console.log(`[API_CLIENT] Audit clusters response:`, {
      clustersCount: result.clusters?.length || 0,
      totalUnreviewed: result.auditCounts?.totalUnreviewed || 0,
      entityTypeRequested: params.entityType,
      clustersEntityTypes: result.clusters?.map((c) => c.entityType) || [],
    });

    return result;
  } catch (error) {
    if (error instanceof Error && error.name === "AbortError") {
      console.log(`[API_CLIENT] Request aborted: ${context}`);
      throw error;
    }
    console.error(`[API_CLIENT] Audit clusters request failed:`, {
      url,
      params,
      error: error,
    });
    return handleApiError(error, context);
  }
}

export async function bulkMarkPostProcessingReviewed(
  request: BulkMarkReviewedRequest,
  opinionName?: string,
  signal?: AbortSignal
): Promise<BulkMarkReviewedResponse> {
  const url = `${API_BASE_URL}/post-processing-audit/bulk-mark-reviewed`;
  const context = `bulkMarkPostProcessingReviewed (${request.decisionIds.length} decisions)`;

  try {
    const response = await fetch(url, {
      method: "POST",
      headers: getApiHeaders(opinionName),
      body: JSON.stringify(request),
      signal,
    });

    return await validateResponse<BulkMarkReviewedResponse>(response, context);
  } catch (error) {
    if (error instanceof Error && error.name === "AbortError") {
      console.log(`[API_CLIENT] Request aborted: ${context}`);
      throw error;
    }
    return handleApiError(error, context);
  }
}

// --- Cluster and Edge Review API Functions ---

export async function getClusterProgress(
  page: number = 1,
  limit: number = 10,
  reviewStatus: ClusterFilterStatus = "unreviewed",
  workflowFilter: WorkflowFilter,
  type: "entity" | "service" = "entity",
  opinionName?: string,
  signal?: AbortSignal
): Promise<ClusterProgressResponse> {
  const context = `getClusterProgress (page: ${page}, type: ${type}, filter: ${workflowFilter})`;

  const searchParams = new URLSearchParams({
    page: page.toString(),
    limit: limit.toString(),
    review_status: reviewStatus,
    workflow_filter: workflowFilter,
    type: type,
  });

  const url = `${API_BASE_URL}/cluster-progress?${searchParams}`;

  try {
    const response = await fetch(url, {
      headers: getApiHeaders(opinionName),
      signal,
    });

    return await validateResponse<ClusterProgressResponse>(response, context);
  } catch (error) {
    // Handle AbortError silently
    if (error instanceof Error && error.name === "AbortError") {
      console.log(`🛑 [API_CLIENT] ${context} - Request cancelled`);
      throw error;
    }
    return handleApiError(error, context);
  }
}

export async function postEdgeReview(
  edgeId: string,
  payload: EdgeReviewApiPayload,
  opinionName?: string,
  signal?: AbortSignal
): Promise<EdgeReviewApiResponse> {
  const context = `postEdgeReview for edge ${edgeId}, opinion: ${
    opinionName || "none"
  }`;
  const url = `${API_BASE_URL}/edge-visualizations/${edgeId}/review`;
  try {
    const response = await fetch(url, {
      method: "POST",
      headers: getApiHeaders(opinionName),
      body: JSON.stringify(payload),
      signal,
    });
    console.log("[API_CLIENT] Response for postEdgeReview:", response);
    return await validateResponse<EdgeReviewApiResponse>(response, context);
  } catch (error) {
    if (error instanceof Error && error.name === "AbortError") {
      console.log(`[API_CLIENT] Request aborted: ${context}`);
      throw error;
    }
    return handleApiError(error, context);
  }
}

export async function postDisconnectDependentServices(
  payload: DisconnectDependentServicesRequest,
  opinionName?: string,
  signal?: AbortSignal
): Promise<DisconnectDependentServicesResponse> {
  const context = `postDisconnectDependentServices, opinion: ${
    opinionName || "none"
  }`;
  const url = `${API_BASE_URL}/disconnect-dependent-service-matches`;
  try {
    console.log(
      "[API_CLIENT] Requesting bulk disconnect of dependent services:",
      payload
    );
    const response = await fetch(url, {
      method: "POST",
      headers: getApiHeaders(opinionName),
      body: JSON.stringify(payload),
      signal,
    });
    return await validateResponse<DisconnectDependentServicesResponse>(
      response,
      context
    );
  } catch (error) {
    if (error instanceof Error && error.name === "AbortError") {
      console.log(`[API_CLIENT] Request aborted: ${context}`);
      throw error;
    }
    return handleApiError(error, context);
  }
}

// --- Generic Node Detail Functions ---

export async function getBulkNodeDetails(
  payload: BulkNodeDetailsRequest,
  opinionName?: string,
  signal?: AbortSignal
): Promise<NodeDetailResponse[]> {
  const context = `getBulkNodeDetails, opinion: ${opinionName || "none"}`;
  const url = `${API_BASE_URL}/bulk-node-details`;
  try {
    console.log(`[API_CLIENT] Requesting bulk node details:`, payload);
    const response = await fetch(url, {
      method: "POST",
      headers: getApiHeaders(opinionName),
      body: JSON.stringify(payload),
      signal,
    });
    return await validateResponse<NodeDetailResponse[]>(response, context);
  } catch (error) {
    if (error instanceof Error && error.name === "AbortError") {
      console.log(`[API_CLIENT] Request aborted: ${context}`);
      throw error;
    }
    return handleApiError(error, context);
  }
}

// --- Bulk Data Fetching Functions ---

/**
 * ✨ UPDATED: Fetches connection details in bulk using cursor-based pagination.
 * The request payload can include limit, cursor, and crossSystemOnly parameters.
 * @param payload - The request body, including items and pagination options.
 * @param opinionName - The name of the opinion to use.
 * @returns A promise that resolves to the paginated connection data.
 */
export async function getBulkConnections(
  payload: BulkConnectionsRequest,
  opinionName?: string,
  signal?: AbortSignal
): Promise<PaginatedBulkConnectionsResponse> {
  const context = `getBulkConnections (${payload.items.length} items)`;
  const url = `${API_BASE_URL}/bulk-connections`;

  // Ensure payload has the correct structure with defaults
  const requestPayload: BulkConnectionsRequest = {
    items: payload.items,
    limit: payload.limit || 50,
    cursor: payload.cursor || undefined,
    crossSystemOnly: payload.crossSystemOnly || false,
  };

  try {
    console.log(`[API_CLIENT] 🔄 ${context}:`, {
      itemCount: requestPayload.items.length,
      crossSystemOnly: requestPayload.crossSystemOnly,
      hasCursor: !!requestPayload.cursor
    });

    const response = await fetch(url, {
      method: "POST",
      headers: getApiHeaders(opinionName),
      body: JSON.stringify(requestPayload),
      signal,
    });

    const result = await validateResponse<PaginatedBulkConnectionsResponse>(
      response,
      context
    );

    console.log(`[API_CLIENT] ✅ ${context}:`, {
      connectionsReturned: result.connections.length,
      hasMore: result.hasMore,
      nextCursor: result.nextCursor ? "present" : "null",
    });

    return result;
  } catch (error) {
    if (error instanceof Error && error.name === 'AbortError') {
      console.log(`🛑 [API_CLIENT] ${context} - Request cancelled`);
      throw error;
    }
    return handleApiError(error, context);
  }
}

export async function getBulkVisualizations(
  payload: BulkVisualizationsRequest,
  opinionName?: string,
  signal?: AbortSignal
): Promise<BulkVisualizationsResponse> {
  const context = `getBulkVisualizations, opinion: ${opinionName || "none"}`;
  console.log("[API_CLIENT] Requesting bulk visualizations:", payload);
  const url = `${API_BASE_URL}/bulk-visualizations`;
  try {
    const response = await fetch(url, {
      method: "POST",
      headers: getApiHeaders(opinionName),
      body: JSON.stringify(payload),
      signal,
    });
    return await validateResponse<BulkVisualizationsResponse>(
      response,
      context
    );
  } catch (error) {
    if (error instanceof Error && error.name === "AbortError") {
      console.log(`[API_CLIENT] Request aborted: ${context}`);
      throw error;
    }
    return handleApiError(error, context);
  }
}

// --- Opinion Preferences Functions ---

export async function getOpinionPreferences(
  opinionName?: string,
  signal?: AbortSignal
): Promise<GetOpinionPreferencesResponse> {
  const context = `getOpinionPreferences, opinion: ${opinionName || "default"}`;
  const url = `${API_BASE_URL}/opinion-preferences`;
  try {
    const response = await fetch(url, {
      headers: getApiHeaders(opinionName),
      signal,
    });
    return await validateResponse<GetOpinionPreferencesResponse>(
      response,
      context
    );
  } catch (error) {
    if (error instanceof Error && error.name === "AbortError") {
      console.log(`[API_CLIENT] Request aborted: ${context}`);
      throw error;
    }
    return handleApiError(error, context);
  }
}

export async function updateOpinionPreferences(
  payload: UpdateOpinionPreferencesRequest,
  opinionName?: string,
  signal?: AbortSignal
): Promise<UpdateOpinionPreferencesResponse> {
  const context = `updateOpinionPreferences, opinion: ${
    opinionName || "default"
  }`;
  const url = `${API_BASE_URL}/opinion-preferences`;
  try {
    console.log("[API_CLIENT] Updating opinion preferences:", payload);
    const response = await fetch(url, {
      method: "POST",
      headers: getApiHeaders(opinionName),
      body: JSON.stringify(payload),
      signal,
    });
    return await validateResponse<UpdateOpinionPreferencesResponse>(
      response,
      context
    );
  } catch (error) {
    if (error instanceof Error && error.name === "AbortError") {
      console.log(`[API_CLIENT] Request aborted: ${context}`);
      throw error;
    }
    return handleApiError(error, context);
  }
}

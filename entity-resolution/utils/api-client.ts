// utils/api-client.ts - ADD Cluster Progress Function

import type {
  EntityConnectionDataResponse,
  EntityVisualizationDataResponse,
  EdgeReviewApiPayload,
  EdgeReviewApiResponse,
  DisconnectDependentServicesRequest,
  DisconnectDependentServicesResponse,
  NodeDetailResponse,
  BulkNodeDetailsRequest,
  BulkConnectionsRequest,
  BulkConnectionsResponse,
  BulkVisualizationsRequest,
  BulkVisualizationsResponse,
  PaginatedClustersResponse,
  EntityCluster,
  ClusterFilterStatus,
  WorkflowFilter, // ✨ ADD: Import WorkflowFilter
  ClusterProgressResponse, // ✨ ADD: Import ClusterProgressResponse
  OpinionPreferences,
  GetOpinionPreferencesResponse,
  UpdateOpinionPreferencesRequest,
  UpdateOpinionPreferencesResponse,
} from "@/types/entity-resolution";

const API_BASE_URL = "/api";

// Helper function to create headers with opinion support
function getApiHeaders(opinionName?: string): HeadersInit {
  const headers: HeadersInit = {
    'Content-Type': 'application/json',
  };
  
  if (opinionName) {
    headers['X-Opinion-Name'] = opinionName;
  }
  
  return headers;
}

// Handles API errors by logging them and throwing a new error
function handleApiError(error: unknown, context?: string): never {
  const contextMsg = context ? ` (${context})` : "";
  let errorMessage = "An unknown error occurred";
  if (error instanceof Error) {
    errorMessage = error.message;
  } else if (typeof error === "string") {
    errorMessage = error;
  }
  console.error(`[API_CLIENT] API Error${contextMsg}:`, errorMessage, error);
  throw new Error(errorMessage);
}

// Validates the HTTP response and parses it as JSON
async function validateResponse<T>(
  response: Response,
  context?: string
): Promise<T> {
  if (!response.ok) {
    let errorData;
    try {
      errorData = await response.json();
    } catch (e) {
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
  if (response.status === 204) {
    return {} as T;
  }
  const responseData = await response.json();
  console.log(`[API_CLIENT] Successful response for ${context}:`, responseData);
  return responseData as T;
}

// ✨ NEW: Cluster Progress API Function
export async function getClusterProgress(
  page: number = 1,
  limit: number = 10,
  reviewStatus: ClusterFilterStatus = "unreviewed",
  workflowFilter: WorkflowFilter = "all",
  type: "entity" | "service" = "entity",
  opinionName?: string
): Promise<ClusterProgressResponse> {
  const context = `getClusterProgress (page: ${page}, limit: ${limit}, reviewStatus: ${reviewStatus}, workflowFilter: ${workflowFilter}, type: ${type}, opinion: ${opinionName || 'none'})`;
  
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
    });
    
    return await validateResponse<ClusterProgressResponse>(response, context);
  } catch (error) {
    return handleApiError(error, context);
  }
}

// --- Existing Entity Specific Functions ---

export async function getOrganizationClusters(
  page: number = 1,
  limit: number = 10,
  reviewStatus: ClusterFilterStatus = "unreviewed",
  opinionName?: string
): Promise<PaginatedClustersResponse<EntityCluster>> {
  const url = `${API_BASE_URL}/clusters?type=entity&page=${page}&limit=${limit}&review_status=${reviewStatus}`;
  const context = `getEntityClusters (url: ${url}, opinion: ${opinionName || 'none'})`;
  try {
    const response = await fetch(url, {
      headers: getApiHeaders(opinionName),
    });
    return await validateResponse<PaginatedClustersResponse<EntityCluster>>(
      response,
      context
    );
  } catch (error) {
    return handleApiError(error, context);
  }
}

export async function getOrganizationConnectionData(
  edgeId: string,
  opinionName?: string
): Promise<EntityConnectionDataResponse> {
  const context = `getEntityConnectionData for edge ${edgeId}, opinion: ${opinionName || 'none'}`;
  const url = `${API_BASE_URL}/connections/${edgeId}?type=entity`;
  try {
    const response = await fetch(url, {
      headers: getApiHeaders(opinionName),
    });
    return await validateResponse<EntityConnectionDataResponse>(
      response,
      context
    );
  } catch (error) {
    return handleApiError(error, context);
  }
}

export async function getServiceConnectionData(
  edgeId: string,
  opinionName?: string
): Promise<EntityConnectionDataResponse> {
  const context = `getServiceConnectionData for edge ${edgeId}, opinion: ${opinionName || 'none'}`;
  const url = `${API_BASE_URL}/connections/${edgeId}?type=service`;
  try {
    const response = await fetch(url, {
      headers: getApiHeaders(opinionName),
    });
    return await validateResponse<EntityConnectionDataResponse>(
      response,
      context
    );
  } catch (error) {
    return handleApiError(error, context);
  }
}

export async function getEntityVisualizationData(
  clusterId: string,
  opinionName?: string
): Promise<EntityVisualizationDataResponse> {
  const context = `getEntityVisualizationData for cluster ${clusterId}, opinion: ${opinionName || 'none'}`;
  const url = `${API_BASE_URL}/visualization/${clusterId}?type=entity`;
  try {
    const response = await fetch(url, {
      headers: getApiHeaders(opinionName),
    });
    return await validateResponse<EntityVisualizationDataResponse>(
      response,
      context
    );
  } catch (error) {
    return handleApiError(error, context);
  }
}

export async function getServiceVisualizationData(
  clusterId: string,
  opinionName?: string
): Promise<EntityVisualizationDataResponse> {
  const context = `getServiceVisualizationData for cluster ${clusterId}, opinion: ${opinionName || 'none'}`;
  const url = `${API_BASE_URL}/visualization/${clusterId}?type=service`;
  try {
    const response = await fetch(url, {
      headers: getApiHeaders(opinionName),
    });
    return await validateResponse<EntityVisualizationDataResponse>(
      response,
      context
    );
  } catch (error) {
    return handleApiError(error, context);
  }
}

export async function postEdgeReview(
  edgeId: string,
  payload: EdgeReviewApiPayload,
  opinionName?: string
): Promise<EdgeReviewApiResponse> {
  const context = `postEdgeReview for edge ${edgeId}, opinion: ${opinionName || 'none'}`;
  const url = `${API_BASE_URL}/edge-visualizations/${edgeId}/review`;
  try {
    const response = await fetch(url, {
      method: "POST",
      headers: getApiHeaders(opinionName),
      body: JSON.stringify(payload),
    });
    console.log("[API_CLIENT] Response for postEdgeReview:", response);
    return await validateResponse<EdgeReviewApiResponse>(response, context);
  } catch (error) {
    return handleApiError(error, context);
  }
}

export async function postDisconnectDependentServices(
  payload: DisconnectDependentServicesRequest,
  opinionName?: string
): Promise<DisconnectDependentServicesResponse> {
  const context = `postDisconnectDependentServices, opinion: ${opinionName || 'none'}`;
  const url = `${API_BASE_URL}/disconnect-dependent-service-matches`;
  try {
    console.log("[API_CLIENT] Requesting bulk disconnect of dependent services:", payload);
    const response = await fetch(url, {
      method: "POST",
      headers: getApiHeaders(opinionName),
      body: JSON.stringify(payload),
    });
    return await validateResponse<DisconnectDependentServicesResponse>(response, context);
  } catch (error) {
    return handleApiError(error, context);
  }
}

// --- Service Specific Functions ---

export async function getServiceClusters(
  page: number = 1,
  limit: number = 10,
  reviewStatus: ClusterFilterStatus = "unreviewed",
  opinionName?: string
): Promise<PaginatedClustersResponse<EntityCluster>> {
  const context = `getServiceClusters (page: ${page}, limit: ${limit}, reviewStatus: ${reviewStatus}, opinion: ${opinionName || 'none'})`;
  const url = `${API_BASE_URL}/clusters?type=service&page=${page}&limit=${limit}&review_status=${reviewStatus}`;
  try {
    const response = await fetch(url, {
      headers: getApiHeaders(opinionName),
    });
    return await validateResponse<PaginatedClustersResponse<EntityCluster>>(
      response,
      context
    );
  } catch (error) {
    return handleApiError(error, context);
  }
}

// --- Generic Node Detail Functions ---

export async function getBulkNodeDetails(
  payload: BulkNodeDetailsRequest,
  opinionName?: string
): Promise<NodeDetailResponse[]> {
  const context = `getBulkNodeDetails, opinion: ${opinionName || 'none'}`;
  const url = `${API_BASE_URL}/bulk-node-details`;
  try {
    console.log(`[API_CLIENT] Requesting bulk node details:`, payload);
    const response = await fetch(url, {
      method: "POST",
      headers: getApiHeaders(opinionName),
      body: JSON.stringify(payload),
    });
    return await validateResponse<NodeDetailResponse[]>(
      response,
      context
    );
  } catch (error) {
    return handleApiError(error, context);
  }
}

// --- Bulk Data Fetching Functions ---

export async function getBulkConnections(
  payload: BulkConnectionsRequest,
  opinionName?: string
): Promise<BulkConnectionsResponse> {
  const context = `getBulkConnections, opinion: ${opinionName || 'none'}`;
  const url = `${API_BASE_URL}/bulk-connections`;
  try {
    console.log(`[API_CLIENT] Requesting bulk connections:`, payload);
    const response = await fetch(url, {
      method: "POST",
      headers: getApiHeaders(opinionName),
      body: JSON.stringify(payload),
    });
    return await validateResponse<BulkConnectionsResponse>(
      response,
      context
    );
  } catch (error) {
    return handleApiError(error, context);
  }
}

export async function getBulkVisualizations(
  payload: BulkVisualizationsRequest,
  opinionName?: string
): Promise<BulkVisualizationsResponse> {
  const context = `getBulkVisualizations, opinion: ${opinionName || 'none'}`;
  console.log("[API_CLIENT] Requesting bulk visualizations:", payload);
  const url = `${API_BASE_URL}/bulk-visualizations`;
  try {
    const response = await fetch(url, {
      method: "POST",
      headers: getApiHeaders(opinionName),
      body: JSON.stringify(payload),
    });
    return await validateResponse<BulkVisualizationsResponse>(
      response,
      context
    );
  } catch (error) {
    return handleApiError(error, context);
  }
}

// --- Opinion Preferences Functions ---

export async function getOpinionPreferences(
  opinionName?: string
): Promise<GetOpinionPreferencesResponse> {
  const context = `getOpinionPreferences, opinion: ${opinionName || 'default'}`;
  const url = `${API_BASE_URL}/opinion-preferences`;
  try {
    const response = await fetch(url, {
      headers: getApiHeaders(opinionName),
    });
    return await validateResponse<GetOpinionPreferencesResponse>(
      response,
      context
    );
  } catch (error) {
    return handleApiError(error, context);
  }
}

export async function updateOpinionPreferences(
  payload: UpdateOpinionPreferencesRequest,
  opinionName?: string
): Promise<UpdateOpinionPreferencesResponse> {
  const context = `updateOpinionPreferences, opinion: ${opinionName || 'default'}`;
  const url = `${API_BASE_URL}/opinion-preferences`;
  try {
    console.log("[API_CLIENT] Updating opinion preferences:", payload);
    const response = await fetch(url, {
      method: "POST",
      headers: getApiHeaders(opinionName),
      body: JSON.stringify(payload),
    });
    return await validateResponse<UpdateOpinionPreferencesResponse>(
      response,
      context
    );
  } catch (error) {
    return handleApiError(error, context);
  }
}
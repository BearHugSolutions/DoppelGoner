// utils/api-client.ts
import type {
  EntityConnectionDataResponse,
  EntityVisualizationDataResponse,
  GroupReviewApiResponse,
  NodeDetailResponse,
  BulkNodeDetailsRequest,
  BulkConnectionsRequest,
  BulkConnectionsResponse,
  BulkVisualizationsRequest,
  BulkVisualizationsResponse,
  PaginatedClustersResponse,
  EntityCluster,
} from "@/types/entity-resolution";

const API_BASE_URL = "/api";

/**
 * Handles API errors by logging them and throwing a new error.
 * @param error - The error object.
 * @param context - Optional context for the error message.
 * @throws Error with a descriptive message.
 */
function handleApiError(error: unknown, context?: string): never {
  const contextMsg = context ? ` (${context})` : "";
  let errorMessage = "An unknown error occurred";
  if (error instanceof Error) {
    errorMessage = error.message;
  } else if (typeof error === "string") {
    errorMessage = error;
  }
  // Centralized logging for all API errors
  console.error(`[API_CLIENT] API Error${contextMsg}:`, errorMessage, error);
  throw new Error(errorMessage);
}

/**
 * Validates the HTTP response and parses it as JSON.
 * @param response - The fetch Response object.
 * @param context - Optional context for error messages.
 * @returns Promise resolving to the parsed JSON data.
 * @throws Error if the response is not ok.
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

// --- Entity Specific Functions ---

export async function getOrganizationClusters(
  page: number = 1,
  limit: number = 10
): Promise<PaginatedClustersResponse<EntityCluster>> {
  const url = `${API_BASE_URL}/clusters?type=entity&page=${page}&limit=${limit}`;
  const context = `getEntityClusters (url: ${url})`;
  try {
    const response = await fetch(url);
    return await validateResponse<PaginatedClustersResponse<EntityCluster>>(
      response,
      context
    );
  } catch (error) {
    return handleApiError(error, context);
  }
}

export async function getOrganizationConnectionData(
  edgeId: string
): Promise<EntityConnectionDataResponse> {
    const context = `getEntityConnectionData for edge ${edgeId}`;
  const url = `${API_BASE_URL}/connections/${edgeId}?type=entity`;
  try {
    const response = await fetch(url);
    return await validateResponse<EntityConnectionDataResponse>(
      response,
      context
    );
  } catch (error) {
    return handleApiError(error, context);
  }
}

export async function getServiceConnectionData(
  edgeId: string
): Promise<EntityConnectionDataResponse> {
  const context = `getServiceConnectionData for edge ${edgeId}`;
  const url = `${API_BASE_URL}/connections/${edgeId}?type=service`;
  try {
    const response = await fetch(url);
    return await validateResponse<EntityConnectionDataResponse>(
      response,
      context
    );
  } catch (error) {
    return handleApiError(
      error,
      context
    );
  }
}

export async function getEntityVisualizationData(
  clusterId: string
): Promise<EntityVisualizationDataResponse> {
  const context = `getEntityVisualizationData for cluster ${clusterId}`;
  const url = `${API_BASE_URL}/visualization/${clusterId}?type=entity`;
  try {
    const response = await fetch(url);
    return await validateResponse<EntityVisualizationDataResponse>(
      response,
      context
    );
  } catch (error) {
    return handleApiError(
      error,
      context
    );
  }
}

export async function getServiceVisualizationData(
  clusterId: string
): Promise<EntityVisualizationDataResponse> {
  const context = `getServiceVisualizationData for cluster ${clusterId}`;
  const url = `${API_BASE_URL}/visualization/${clusterId}?type=service`;
  try {
    const response = await fetch(url);
    return await validateResponse<EntityVisualizationDataResponse>(
      response,
      context
    );
  } catch (error) {
    return handleApiError(
      error,
      context
    );
  }
}

export async function postEntityGroupFeedback(
  groupId: string,
  payload: any
): Promise<GroupReviewApiResponse> {
  const context = `postEntityGroupFeedback for group ${groupId}`;
  const url = `${API_BASE_URL}/entity-groups/${groupId}/review`;
  try {
    const response = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    return await validateResponse<GroupReviewApiResponse>(
      response,
      context
    );
  } catch (error) {
    return handleApiError(
      error,
      context
    );
  }
}

export async function postServiceGroupFeedback(
  groupId: string,
  payload: any
): Promise<GroupReviewApiResponse> {
  const context = `postServiceGroupFeedback for group ${groupId}`;
  const url = `${API_BASE_URL}/service-groups/${groupId}/review`;
  try {
    const response = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    return await validateResponse<GroupReviewApiResponse>(
      response,
      context
    );
  } catch (error) {
    return handleApiError(
      error,
      context
    );
  }
}

// --- Service Specific Functions ---

export async function getServiceClusters(
  page: number = 1,
  limit: number = 10
): Promise<PaginatedClustersResponse<EntityCluster>> {
  const context = `getServiceClusters (page: ${page}, limit: ${limit})`;
  const url = `${API_BASE_URL}/clusters?type=service&page=${page}&limit=${limit}`;
  try {
    const response = await fetch(url);
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
  payload: BulkNodeDetailsRequest
): Promise<NodeDetailResponse[]> {
  const context = 'getBulkNodeDetails';
  const url = `${API_BASE_URL}/bulk-node-details`;
  try {
    console.log(`[API_CLIENT] Requesting bulk node details:`, payload);
    const response = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
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

// --- NEW Bulk Data Fetching Functions ---

export async function getBulkConnections(
  payload: BulkConnectionsRequest
): Promise<BulkConnectionsResponse> {
    const context = 'getBulkConnections';
  const url = `${API_BASE_URL}/bulk-connections`;
  try {
    console.log(`[API_CLIENT] Requesting bulk connections:`, payload);
    const response = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
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
  payload: BulkVisualizationsRequest
): Promise<BulkVisualizationsResponse> {
    const context = 'getBulkVisualizations';
    console.log("[API_CLIENT] Requesting bulk visualizations:", payload);
  const url = `${API_BASE_URL}/bulk-visualizations`;
  try {
    const response = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
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

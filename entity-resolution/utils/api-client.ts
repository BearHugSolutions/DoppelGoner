// utils/api-client.ts
import type {
  EntityConnectionDataResponse,
  EntityVisualizationDataResponse,
  GroupReviewApiResponse,
  ClusterFinalizationStatusResponse,
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
  console.error(`API Error${contextMsg}:`, errorMessage, error);
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
      `API Response Error: ${response.status} ${response.statusText}`,
      errorData
    );
    throw new Error(fullContext);
  }
  if (response.status === 204) {
    return {} as T;
  }
  return response.json() as Promise<T>;
}

// --- Entity Specific Functions ---

export async function getOrganizationClusters(
  page: number = 1,
  limit: number = 10
): Promise<PaginatedClustersResponse<EntityCluster>> {
  const url = `${API_BASE_URL}/clusters?type=entity&page=${page}&limit=${limit}`;
  try {
    const response = await fetch(url);
    return await validateResponse<PaginatedClustersResponse<EntityCluster>>(
      response,
      "getEntityClusters"
    );
  } catch (error) {
    return handleApiError(error, `getEntityClusters (url: ${url})`);
  }
}

export async function getOrganizationConnectionData(
  edgeId: string
): Promise<EntityConnectionDataResponse> {
  const url = `${API_BASE_URL}/connections/${edgeId}?type=entity`;
  try {
    const response = await fetch(url);
    return await validateResponse<EntityConnectionDataResponse>(
      response,
      `getEntityConnectionData for edge ${edgeId}`
    );
  } catch (error) {
    return handleApiError(error, `getEntityConnectionData (edgeId: ${edgeId})`);
  }
}

export async function getServiceConnectionData(
  edgeId: string
): Promise<EntityConnectionDataResponse> {
  const url = `${API_BASE_URL}/connections/${edgeId}?type=service`;
  try {
    const response = await fetch(url);
    return await validateResponse<EntityConnectionDataResponse>(
      response,
      `getServiceConnectionData for edge ${edgeId}`
    );
  } catch (error) {
    return handleApiError(
      error,
      `getServiceConnectionData (edgeId: ${edgeId})`
    );
  }
}

export async function getEntityVisualizationData(
  clusterId: string
): Promise<EntityVisualizationDataResponse> {
  const url = `${API_BASE_URL}/visualization/${clusterId}?type=entity`;
  try {
    const response = await fetch(url);
    return await validateResponse<EntityVisualizationDataResponse>(
      response,
      `getEntityVisualizationData for cluster ${clusterId}`
    );
  } catch (error) {
    return handleApiError(
      error,
      `getEntityVisualizationData (clusterId: ${clusterId})`
    );
  }
}

export async function getServiceVisualizationData(
  clusterId: string
): Promise<EntityVisualizationDataResponse> {
  const url = `${API_BASE_URL}/visualization/${clusterId}?type=service`;
  try {
    const response = await fetch(url);
    return await validateResponse<EntityVisualizationDataResponse>(
      response,
      `getServiceVisualizationData for cluster ${clusterId}`
    );
  } catch (error) {
    return handleApiError(
      error,
      `getServiceVisualizationData (clusterId: ${clusterId})`
    );
  }
}

export async function postEntityGroupFeedback(
  groupId: string,
  payload: any
): Promise<GroupReviewApiResponse> {
  const url = `${API_BASE_URL}/entity-groups/${groupId}/review`;
  try {
    const response = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    return await validateResponse<GroupReviewApiResponse>(
      response,
      `postEntityGroupFeedback for group ${groupId}`
    );
  } catch (error) {
    return handleApiError(
      error,
      `postEntityGroupFeedback (groupId: ${groupId})`
    );
  }
}

export async function postServiceGroupFeedback(
  groupId: string,
  payload: any
): Promise<GroupReviewApiResponse> {
  const url = `${API_BASE_URL}/service-groups/${groupId}/review`;
  try {
    const response = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    return await validateResponse<GroupReviewApiResponse>(
      response,
      `postServiceGroupFeedback for group ${groupId}`
    );
  } catch (error) {
    return handleApiError(
      error,
      `postServiceGroupFeedback (groupId: ${groupId})`
    );
  }
}

export async function triggerEntityClusterFinalization(
  clusterId: string
): Promise<ClusterFinalizationStatusResponse> {
  const url = `${API_BASE_URL}/clusters/${clusterId}/finalize-review?type=entity`;
  try {
    const response = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
    });
    return await validateResponse<ClusterFinalizationStatusResponse>(
      response,
      `triggerEntityClusterFinalization for cluster ${clusterId}`
    );
  } catch (error) {
    return handleApiError(
      error,
      `triggerEntityClusterFinalization (clusterId: ${clusterId})`
    );
  }
}

// --- Service Specific Functions ---

// UPDATED: Now returns a promise with EntityCluster, matching the unified API response.
export async function getServiceClusters(
  page: number = 1,
  limit: number = 10
): Promise<PaginatedClustersResponse<EntityCluster>> {
  const url = `${API_BASE_URL}/clusters?type=service&page=${page}&limit=${limit}`;
  try {
    const response = await fetch(url);
    return await validateResponse<PaginatedClustersResponse<EntityCluster>>(
      response,
      "getServiceClusters"
    );
  } catch (error) {
    return handleApiError(error, `getServiceClusters (url: ${url})`);
  }
}

export async function triggerServiceClusterFinalization(
  clusterId: string
): Promise<ClusterFinalizationStatusResponse> {
  const url = `${API_BASE_URL}/clusters/${clusterId}/finalize-review?type=service`;
  try {
    const response = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
    });
    return await validateResponse<ClusterFinalizationStatusResponse>(
      response,
      `triggerServiceClusterFinalization for cluster ${clusterId}`
    );
  } catch (error) {
    return handleApiError(
      error,
      `triggerServiceClusterFinalization (clusterId: ${clusterId})`
    );
  }
}

// --- Generic Node Detail Functions ---

export async function getBulkNodeDetails(
  payload: BulkNodeDetailsRequest
): Promise<NodeDetailResponse[]> {
  const url = `${API_BASE_URL}/bulk-node-details`;
  try {
    const response = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    return await validateResponse<NodeDetailResponse[]>(
      response,
      "getBulkNodeDetails"
    );
  } catch (error) {
    return handleApiError(error, "getBulkNodeDetails");
  }
}

// --- NEW Bulk Data Fetching Functions ---

export async function getBulkConnections(
  payload: BulkConnectionsRequest
): Promise<BulkConnectionsResponse> {
  const url = `${API_BASE_URL}/bulk-connections`;
  try {
    const response = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });

    // console.log("Bulk connections response:", response.json());

    return await validateResponse<BulkConnectionsResponse>(
      response,
      "getBulkConnections"
    );
  } catch (error) {
    return handleApiError(error, "getBulkConnections");
  }
}

export async function getBulkVisualizations(
  payload: BulkVisualizationsRequest
): Promise<BulkVisualizationsResponse> {
  console.log("Bulk visualizations payload:", payload);
  const url = `${API_BASE_URL}/bulk-visualizations`;
  try {
    const response = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });

    // console.log("Bulk visualizations response:", response.json());

    return await validateResponse<BulkVisualizationsResponse>(
      response,
      "getBulkVisualizations"
    );
  } catch (error) {
    return handleApiError(error, "getBulkVisualizations");
  }
}

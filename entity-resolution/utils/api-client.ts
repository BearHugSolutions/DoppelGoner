// utils/api-client.ts
import type {
  // ResolutionMode, // Not directly used in this file but good for context
  EntityCluster,
  ServiceCluster,
  EntityGroup,
  ServiceGroup,
  EntityNode,
  ServiceNode,
  EntityLink,
  ServiceLink,
  VisualizationEntityEdge,
  VisualizationServiceEdge,
  // MatchDecisionDetails, // Not directly used in current API client functions' return types
  Entity,
  Service,
  EntityGroupReviewApiPayload,
  ServiceGroupReviewApiPayload,
  GroupReviewApiResponse,
  ClusterFinalizationStatusResponse,
  EntityConnectionDataResponse,
  ServiceConnectionDataResponse,
  EntityClustersResponse,
  ServiceClustersResponse,
  EntityVisualizationDataResponse,
  ServiceVisualizationDataResponse,
} from "@/types/entity-resolution";

const API_BASE_URL = '/api';

/**
 * Handles API errors by logging them and throwing a new error.
 * @param error - The error object.
 * @param context - Optional context for the error message.
 * @throws Error with a descriptive message.
 */
function handleApiError(error: unknown, context?: string): never {
  const contextMsg = context ? ` (${context})` : '';
  let errorMessage = 'An unknown error occurred';
  if (error instanceof Error) {
    errorMessage = error.message;
  } else if (typeof error === 'string') {
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
      // If response is not JSON, use status text or a generic message
      errorData = { message: response.statusText || `Request failed with status ${response.status}` };
    }
    const errorMessage = errorData.message || errorData.error || `Request failed with status ${response.status}`;
    const fullContext = context ? `${context}: ${errorMessage}` : errorMessage;
    console.error(`API Response Error: ${response.status} ${response.statusText}`, errorData);
    throw new Error(fullContext);
  }
  return response.json() as Promise<T>;
}

// --- Entity Specific Functions ---

/**
 * Fetches a paginated list of entity clusters.
 * @param page - The page number to fetch.
 * @param limit - The number of items per page.
 * @returns Promise resolving to EntityClustersResponse.
 */
export async function getEntityClusters(page: number = 1, limit: number = 10): Promise<EntityClustersResponse> {
  const url = `${API_BASE_URL}/clusters?type=entity&page=${page}&limit=${limit}`;
  try {
    const response = await fetch(url);
    console.log("getEntityClusters response:", JSON.stringify(response));
    return await validateResponse<EntityClustersResponse>(response, 'getEntityClusters');
  } catch (error) {
    return handleApiError(error, `getEntityClusters (url: ${url})`);
  }
}

/**
 * Fetches detailed data for a specific entity connection (edge).
 * @param edgeId - The ID of the edge.
 * @returns Promise resolving to EntityConnectionDataResponse.
 */
export async function getEntityConnectionData(edgeId: string): Promise<EntityConnectionDataResponse> {
  const url = `${API_BASE_URL}/connections/${edgeId}?type=entity`;
  try {
    const response = await fetch(url);
    return await validateResponse<EntityConnectionDataResponse>(response, `getEntityConnectionData for edge ${edgeId}`);
  } catch (error) {
    return handleApiError(error, `getEntityConnectionData (edgeId: ${edgeId})`);
  }
}

/**
 * Fetches visualization data (nodes and links) for a specific entity cluster.
 * @param clusterId - The ID of the cluster.
 * @returns Promise resolving to EntityVisualizationDataResponse.
 */
export async function getEntityVisualizationData(clusterId: string): Promise<EntityVisualizationDataResponse> {
  const url = `${API_BASE_URL}/visualization/${clusterId}?type=entity`;
  try {
    const response = await fetch(url);
    // console.log("getEntityVisualizationData response:", JSON.stringify(response));
    return await validateResponse<EntityVisualizationDataResponse>(response, `getEntityVisualizationData for cluster ${clusterId}`);
  } catch (error) {
    return handleApiError(error, `getEntityVisualizationData (clusterId: ${clusterId})`);
  }
}

/**
 * Submits feedback for an entity group.
 * @param groupId - The ID of the entity group.
 * @param payload - The review payload.
 * @returns Promise resolving to GroupReviewApiResponse.
 */
export async function postEntityGroupFeedback(
  groupId: string,
  payload: EntityGroupReviewApiPayload
): Promise<GroupReviewApiResponse> {
  const url = `${API_BASE_URL}/entity-groups/${groupId}/review`;
  try {
    const response = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    return await validateResponse<GroupReviewApiResponse>(response, `postEntityGroupFeedback for group ${groupId}`);
  } catch (error) {
    return handleApiError(error, `postEntityGroupFeedback (groupId: ${groupId})`);
  }
}

/**
 * Triggers the finalization process for an entity cluster.
 * @param clusterId - The ID of the cluster to finalize.
 * @returns Promise resolving to ClusterFinalizationStatusResponse.
 */
export async function triggerEntityClusterFinalization(
  clusterId: string
): Promise<ClusterFinalizationStatusResponse> {
  const url = `${API_BASE_URL}/clusters/${clusterId}/finalize-review?type=entity`;
  try {
    const response = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      // No body needed if the API doesn't expect one for this POST
    });
    return await validateResponse<ClusterFinalizationStatusResponse>(response, `triggerEntityClusterFinalization for cluster ${clusterId}`);
  } catch (error) {
    return handleApiError(error, `triggerEntityClusterFinalization (clusterId: ${clusterId})`);
  }
}


// --- Service Specific Functions ---

/**
 * Fetches a paginated list of service clusters.
 * @param page - The page number to fetch.
 * @param limit - The number of items per page.
 * @returns Promise resolving to ServiceClustersResponse.
 */
export async function getServiceClusters(page: number = 1, limit: number = 10): Promise<ServiceClustersResponse> {
  const url = `${API_BASE_URL}/clusters?type=service&page=${page}&limit=${limit}`;
  try {
    const response = await fetch(url);
    return await validateResponse<ServiceClustersResponse>(response, 'getServiceClusters');
  } catch (error) {
    return handleApiError(error, `getServiceClusters (url: ${url})`);
  }
}

/**
 * Fetches detailed data for a specific service connection (edge).
 * @param edgeId - The ID of the edge.
 * @returns Promise resolving to ServiceConnectionDataResponse.
 */
export async function getServiceConnectionData(edgeId: string): Promise<ServiceConnectionDataResponse> {
  const url = `${API_BASE_URL}/connections/${edgeId}?type=service`;
  try {
    const response = await fetch(url);
    return await validateResponse<ServiceConnectionDataResponse>(response, `getServiceConnectionData for edge ${edgeId}`);
  } catch (error) {
    return handleApiError(error, `getServiceConnectionData (edgeId: ${edgeId})`);
  }
}

/**
 * Fetches visualization data (nodes and links) for a specific service cluster.
 * @param clusterId - The ID of the cluster.
 * @returns Promise resolving to ServiceVisualizationDataResponse.
 */
export async function getServiceVisualizationData(clusterId: string): Promise<ServiceVisualizationDataResponse> {
  const url = `${API_BASE_URL}/visualization/${clusterId}?type=service`;
  try {
    const response = await fetch(url);
    return await validateResponse<ServiceVisualizationDataResponse>(response, `getServiceVisualizationData for cluster ${clusterId}`);
  } catch (error) {
    return handleApiError(error, `getServiceVisualizationData (clusterId: ${clusterId})`);
  }
}

/**
 * Submits feedback for a service group.
 * @param groupId - The ID of the service group.
 * @param payload - The review payload.
 * @returns Promise resolving to GroupReviewApiResponse.
 */
export async function postServiceGroupFeedback(
  groupId: string,
  payload: ServiceGroupReviewApiPayload
): Promise<GroupReviewApiResponse> {
  const url = `${API_BASE_URL}/service-groups/${groupId}/review`; // Uses the new service-specific endpoint
  try {
    const response = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    return await validateResponse<GroupReviewApiResponse>(response, `postServiceGroupFeedback for group ${groupId}`);
  } catch (error) {
    return handleApiError(error, `postServiceGroupFeedback (groupId: ${groupId})`);
  }
}

/**
 * Triggers the finalization process for a service cluster.
 * @param clusterId - The ID of the cluster to finalize.
 * @returns Promise resolving to ClusterFinalizationStatusResponse.
 */
export async function triggerServiceClusterFinalization(
  clusterId: string
): Promise<ClusterFinalizationStatusResponse> {
  const url = `${API_BASE_URL}/clusters/${clusterId}/finalize-review?type=service`;
  try {
    const response = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      // No body needed if the API doesn't expect one for this POST
    });
    return await validateResponse<ClusterFinalizationStatusResponse>(response, `triggerServiceClusterFinalization for cluster ${clusterId}`);
  } catch (error) {
    return handleApiError(error, `triggerServiceClusterFinalization (clusterId: ${clusterId})`);
  }
}

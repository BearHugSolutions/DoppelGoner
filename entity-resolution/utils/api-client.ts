// utils/api-client.ts
import type {
  EntityClustersResponse,
  ServiceClustersResponse,
  EntityConnectionDataResponse,
  ServiceConnectionDataResponse,
  EntityVisualizationDataResponse,
  ServiceVisualizationDataResponse,
  EntityGroupReviewApiPayload,
  ServiceGroupReviewApiPayload,
  GroupReviewApiResponse,
  ClusterFinalizationStatusResponse,
  NodeDetailResponse,
  BulkNodeDetailsRequest, // New import
  NodeIdentifier, // New import (though not directly used as param type here, good for context)
} from "@/types/entity-resolution";

const API_BASE_URL = '/api'; // Assuming this is your Next.js API proxy

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

export async function getEntityClusters(page: number = 1, limit: number = 10): Promise<EntityClustersResponse> {
  const url = `${API_BASE_URL}/clusters?type=entity&page=${page}&limit=${limit}`;
  try {
    const response = await fetch(url);
    return await validateResponse<EntityClustersResponse>(response, 'getEntityClusters');
  } catch (error) {
    return handleApiError(error, `getEntityClusters (url: ${url})`);
  }
}

export async function getEntityConnectionData(edgeId: string): Promise<EntityConnectionDataResponse> {
  const url = `${API_BASE_URL}/connections/${edgeId}?type=entity`;
  try {
    const response = await fetch(url);
    return await validateResponse<EntityConnectionDataResponse>(response, `getEntityConnectionData for edge ${edgeId}`);
  } catch (error) {
    return handleApiError(error, `getEntityConnectionData (edgeId: ${edgeId})`);
  }
}

export async function getEntityVisualizationData(clusterId: string): Promise<EntityVisualizationDataResponse> {
  const url = `${API_BASE_URL}/visualization/${clusterId}?type=entity`;
  try {
    const response = await fetch(url);
    return await validateResponse<EntityVisualizationDataResponse>(response, `getEntityVisualizationData for cluster ${clusterId}`);
  } catch (error) {
    return handleApiError(error, `getEntityVisualizationData (clusterId: ${clusterId})`);
  }
}

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

export async function triggerEntityClusterFinalization(
  clusterId: string
): Promise<ClusterFinalizationStatusResponse> {
  const url = `${API_BASE_URL}/clusters/${clusterId}/finalize-review?type=entity`;
  try {
    const response = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
    });
    return await validateResponse<ClusterFinalizationStatusResponse>(response, `triggerEntityClusterFinalization for cluster ${clusterId}`);
  } catch (error) {
    return handleApiError(error, `triggerEntityClusterFinalization (clusterId: ${clusterId})`);
  }
}


// --- Service Specific Functions ---

export async function getServiceClusters(page: number = 1, limit: number = 10): Promise<ServiceClustersResponse> {
  const url = `${API_BASE_URL}/clusters?type=service&page=${page}&limit=${limit}`;
  try {
    const response = await fetch(url);
    return await validateResponse<ServiceClustersResponse>(response, 'getServiceClusters');
  } catch (error) {
    return handleApiError(error, `getServiceClusters (url: ${url})`);
  }
}

export async function getServiceConnectionData(edgeId: string): Promise<ServiceConnectionDataResponse> {
  const url = `${API_BASE_URL}/connections/${edgeId}?type=service`;
  try {
    const response = await fetch(url);
    return await validateResponse<ServiceConnectionDataResponse>(response, `getServiceConnectionData for edge ${edgeId}`);
  } catch (error) {
    return handleApiError(error, `getServiceConnectionData (edgeId: ${edgeId})`);
  }
}

export async function getServiceVisualizationData(clusterId: string): Promise<ServiceVisualizationDataResponse> {
  const url = `${API_BASE_URL}/visualization/${clusterId}?type=service`;
  try {
    const response = await fetch(url);
    return await validateResponse<ServiceVisualizationDataResponse>(response, `getServiceVisualizationData for cluster ${clusterId}`);
  } catch (error) {
    return handleApiError(error, `getServiceVisualizationData (clusterId: ${clusterId})`);
  }
}

export async function postServiceGroupFeedback(
  groupId: string,
  payload: ServiceGroupReviewApiPayload
): Promise<GroupReviewApiResponse> {
  const url = `${API_BASE_URL}/service-groups/${groupId}/review`;
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

export async function triggerServiceClusterFinalization(
  clusterId: string
): Promise<ClusterFinalizationStatusResponse> {
  const url = `${API_BASE_URL}/clusters/${clusterId}/finalize-review?type=service`;
  try {
    const response = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
    });
    return await validateResponse<ClusterFinalizationStatusResponse>(response, `triggerServiceClusterFinalization for cluster ${clusterId}`);
  } catch (error) {
    return handleApiError(error, `triggerServiceClusterFinalization (clusterId: ${clusterId})`);
  }
}

// --- Generic Node Detail Functions ---

/**
 * Fetches detailed information for a specific entity or service node.
 * @param nodeId - The ID of the node (entity or service).
 * @param nodeType - The type of the node ('entity' or 'service').
 * @returns Promise resolving to NodeDetailResponse.
 */
export async function getNodeDetails(nodeId: string, nodeType: 'entity' | 'service'): Promise<NodeDetailResponse> {
  const url = `${API_BASE_URL}/nodeData/${nodeId}?type=${nodeType}`;
  try {
    const response = await fetch(url);
    return await validateResponse<NodeDetailResponse>(response, `getNodeDetails for node ${nodeId} (type: ${nodeType})`);
  } catch (error) {
    return handleApiError(error, `getNodeDetails (nodeId: ${nodeId}, nodeType: ${nodeType})`);
  }
}

/**
 * Fetches detailed information for multiple entity or service nodes in bulk.
 * @param payload - Object containing an array of NodeIdentifier { id: string, nodeType: 'entity' | 'service' }.
 * @returns Promise resolving to an array of NodeDetailResponse.
 */
export async function getBulkNodeDetails(payload: BulkNodeDetailsRequest): Promise<NodeDetailResponse[]> {
  const url = `${API_BASE_URL}/bulk-node-details`;
  try {
    const response = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    return await validateResponse<NodeDetailResponse[]>(response, 'getBulkNodeDetails');
  } catch (error) {
    return handleApiError(error, 'getBulkNodeDetails');
  }
}
// utils/api-client.ts
import type {
  Cluster,
  EntityGroup,
  EntityNode,
  EntityLink,
  VisualizationEntityEdge,
  MatchDecisionDetails,
  Entity,
  EntityGroupReviewApiPayload,
  EntityGroupReviewApiResponse,
  ClusterFinalizationStatusResponse,
  ConnectionDataResponse,
  ClustersResponse,
  VisualizationDataResponse,
} from "@/types/entity-resolution";

const API_BASE_URL = '/api';

/**
 * Fetches a paginated list of clusters from the user's schema.
 * @param page The page number to fetch.
 * @param limit The number of clusters per page.
 * @returns A promise that resolves to the clusters response.
 */
export async function getClusters(page: number = 1, limit: number = 10): Promise<ClustersResponse> {
  const response = await fetch(`${API_BASE_URL}/clusters?page=${page}&limit=${limit}`);
  if (!response.ok) {
    const errorData = await response.json().catch(() => ({ message: 'Failed to fetch clusters and parse error response.' }));
    console.error("getClusters error:", errorData);
    throw new Error(errorData.message || 'Failed to fetch clusters');
  }
  return response.json();
}

/**
 * Fetches detailed data for a specific connection (edge).
 * @param edgeId The ID of the `entity_edge_visualization` record.
 * @returns A promise that resolves to the connection data.
 */
export async function getConnectionData(edgeId: string): Promise<ConnectionDataResponse> {
  const response = await fetch(`${API_BASE_URL}/connections/${edgeId}`);
  if (!response.ok) {
    const errorData = await response.json().catch(() => ({ message: `Failed to fetch connection data for edge ${edgeId} and parse error response.` }));
    console.error(`getConnectionData error for edge ${edgeId}:`, errorData);
    throw new Error(errorData.message || `Failed to fetch connection data for edge ${edgeId}`);
  }
  return response.json();
}

/**
 * Fetches nodes and links for visualizing a specific cluster.
 * @param clusterId The ID of the cluster.
 * @returns A promise that resolves to the visualization data.
 */
export async function getVisualizationData(clusterId: string): Promise<VisualizationDataResponse> {
  const response = await fetch(`${API_BASE_URL}/visualization/${clusterId}`);
  if (!response.ok) {
    const errorData = await response.json().catch(() => ({ message: `Failed to fetch visualization data for cluster ${clusterId} and parse error response.` }));
    console.error(`getVisualizationData error for cluster ${clusterId}:`, errorData);
    throw new Error(errorData.message || `Failed to fetch visualization data for cluster ${clusterId}`);
  }
  return response.json();
}

/**
 * Submits feedback for a specific entity group.
 * @param groupId The ID of the `entity_group` being reviewed.
 * @param payload The review decision, reviewer ID (from session), and optional notes.
 * @returns A promise that resolves to the API response.
 */
export async function postEntityGroupFeedback(
  groupId: string,
  payload: EntityGroupReviewApiPayload
): Promise<EntityGroupReviewApiResponse> {
  const response = await fetch(`${API_BASE_URL}/entity-groups/${groupId}/review`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload),
  });

  if (!response.ok) {
    const errorData = await response.json().catch(() => ({ message: `Failed to submit feedback for group ${groupId} and parse error response.` }));
    console.error(`postEntityGroupFeedback error for group ${groupId}:`, errorData);
    throw new Error(errorData.message || `Failed to submit feedback for group ${groupId}`);
  }
  return response.json();
}

/**
 * Triggers the backend process to finalize the review of a cluster.
 * This checks if all groups are reviewed and handles potential splits.
 * @param clusterId The ID of the cluster to finalize.
 * @returns A promise that resolves to the status of the finalization process.
 */
export async function triggerClusterFinalization(
  clusterId: string
): Promise<ClusterFinalizationStatusResponse> {
  const response = await fetch(`${API_BASE_URL}/clusters/${clusterId}/finalize-review`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    // No body is typically needed if the clusterId is in the URL,
    // but ensure this matches your backend expectation.
    // The provided backend route does not expect a body.
  });

  if (!response.ok) {
    // Attempt to parse error response, provide a fallback if parsing fails
    let errorPayload: ClusterFinalizationStatusResponse | { message: string };
    try {
      errorPayload = await response.json();
    } catch (e) {
      errorPayload = { 
        status: 'ERROR', // Fallback status
        message: `Failed to trigger cluster finalization for cluster ${clusterId}. Server responded with ${response.status}.`,
        originalClusterId: clusterId 
      };
    }
    console.error(`triggerClusterFinalization error for cluster ${clusterId}:`, errorPayload);
    // Ensure the thrown error has a message property
    const errorMessage = (errorPayload as any).message || `Failed to trigger cluster finalization for cluster ${clusterId}`;
    throw new Error(errorMessage);
  }
  return response.json() as Promise<ClusterFinalizationStatusResponse>;
}

/**
 * Utility function to handle API errors consistently across the application.
 * @param error The error object from a failed API call.
 * @param context Optional context string for debugging.
 */
export function handleApiError(error: unknown, context?: string): never {
  const contextMsg = context ? ` (${context})` : '';
  
  if (error instanceof Error) {
    console.error(`API Error${contextMsg}:`, error.message);
    throw new Error(error.message);
  }
  
  console.error(`Unknown API Error${contextMsg}:`, error);
  throw new Error('An unknown error occurred');
}

/**
 * Validates that a response is successful and returns the parsed JSON.
 * @param response The fetch response object.
 * @param context Optional context for error messages.
 */
export async function validateResponse<T>(
  response: Response, 
  context?: string
): Promise<T> {
  if (!response.ok) {
    let errorMessage: string;
    try {
      const errorData = await response.json();
      errorMessage = errorData.message || errorData.error || `Request failed with status ${response.status}`;
    } catch {
      errorMessage = `Request failed with status ${response.status}`;
    }
    
    if (context) {
      errorMessage = `${context}: ${errorMessage}`;
    }
    
    throw new Error(errorMessage);
  }
  
  return response.json();
}
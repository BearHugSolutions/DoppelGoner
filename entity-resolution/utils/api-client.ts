import type { Cluster, EntityGroup, EntityNode, EntityLink, VisualizationEntityEdge, MatchDecisionDetails, EntityDetails, SuggestedAction, Entity } from "@/types/entity-resolution"

const API_BASE_URL = '/api'

export interface ClustersResponse {
  clusters: Cluster[]
  total: number
}

export async function getClusters(page: number = 1, limit: number = 10): Promise<ClustersResponse> {
  const response = await fetch(`${API_BASE_URL}/clusters?page=${page}&limit=${limit}`)
  if (!response.ok) {
    throw new Error('Failed to fetch clusters')
  }
  return response.json()
}

export interface ConnectionDataResponse {
  edge: VisualizationEntityEdge
  entity1: Entity
  entity2: Entity
  entityGroups: EntityGroup[]
  matchDecisions: MatchDecisionDetails[]
}
export async function getConnectionData(edgeId: string): Promise<ConnectionDataResponse> {
  const response = await fetch(`${API_BASE_URL}/connections/${edgeId}`)
  
  if (!response.ok) {
    throw new Error(`Failed to fetch connection data for edge ${edgeId}`)
  }
  
  // Read the response once and store it
  const data = await response.json();
  console.log("getConnectionData Res: ", JSON.stringify(data));
  
  return data;
}

export interface VisualizationDataResponse {
  nodes: EntityNode[]
  links: EntityLink[]
  entityGroups: EntityGroup[]
}

export interface VisualizationDataResponse {
  nodes: EntityNode[]
  links: EntityLink[]
  entityGroups: EntityGroup[]
}

export async function getVisualizationData(clusterId: string): Promise<VisualizationDataResponse> {
  const response = await fetch(`${API_BASE_URL}/visualization/${clusterId}`)
  if (!response.ok) {
    throw new Error(`Failed to fetch visualization data for cluster ${clusterId}`)
  }
  console.log("b:"+JSON.stringify(response))
  return response.json()
}

export async function submitFeedback(edgeId: string, isMatch: boolean, reviewerId: string, notes: string = ''): Promise<void> {
  const response = await fetch(`${API_BASE_URL}/connections/${edgeId}/feedback`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      isMatch,
      reviewerId,
      notes,
    }),
  })

  if (!response.ok) {
    throw new Error('Failed to submit feedback')
  }
}

export async function updateEntityGroupStatus(groupId: string, status: 'ACCEPTED' | 'REJECTED', reviewerId: string): Promise<void> {
  const response = await fetch(`${API_BASE_URL}/entity-groups/${groupId}/status`, {
    method: 'PUT',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      status,
      reviewerId,
    }),
  })

  if (!response.ok) {
    throw new Error('Failed to update entity group status')
  }
}

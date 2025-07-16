/*
================================================================================
|
|   File: /types/entity-resolution.ts
|
|   Description: Centralized TypeScript types for the application.
|   - This file integrates new user/opinion types with the existing application types.
|
================================================================================
*/

import { Node } from "reactflow";

// --- User and Opinion Types ---
// Represents a single opinion available to a user
export interface OpinionInfo {
  name: string;
  displayName: string;
  isDefault: boolean;
}

// ✨ NEW: Opinion preferences stored in the database
export interface OpinionPreferences {
  disconnectDependentServices: boolean;
  // Add other preferences here as needed
}

// ✨ NEW: API response types for opinion preferences
export interface GetOpinionPreferencesResponse {
  preferences: OpinionPreferences;
  opinionName: string;
}

export interface UpdateOpinionPreferencesRequest {
  disconnectDependentServices?: boolean;
  // Add other preferences here as needed
}

export interface UpdateOpinionPreferencesResponse {
  message: string;
  preferences: OpinionPreferences;
  opinionName: string;
}

// Represents the authenticated user, updated with opinions
export interface User {
  id: string;
  username: string;
  email?: string;
  teamId: string;
  teamName: string;
  teamSchema: string;
  userPrefix: string;
  opinions: OpinionInfo[]; // List of available opinions
}

// Represents the user session data stored on the server
export interface UserSessionData {
  userId: string;
  sessionId: string;
  username: string;
  email?: string;
  teamId: string;
  teamName: string;
  teamSchema: string;
  userPrefix: string;
  isLoggedIn: true;
  opinions: OpinionInfo[]; // Store all available opinions in the session
}

// --- Generic and Graph-Specific Types ---
// Generic paginated response structure from the API
export interface PaginatedResponse<T> {
  items: T[];
  total: number;
  page: number;
  limit: number;
  totalPages: number;
}

// Node and Edge types for graph visualization (React Flow)
export interface BaseNodeData {
  id: string;
  name?: string;
  source_system?: string;
  source_id?: string;
  organization_id?: string;
  contributor_id?: string;
}

export type GraphNode = Node<BaseNodeData>;

export interface GraphLink {
  id:string;
  source: string;
  target: string;
  weight: number;
  status?: string;
  details?: any;
  created_at?: string;
  cluster_id: string;
  was_reviewed?: boolean;
}

// --- Core Application Types ---

export type ResolutionMode = "entity" | "service";
export type ClusterFilterStatus = "all" | "reviewed" | "unreviewed";

// --- Structs for Bulk API Requests ---
export interface NodeIdentifier {
  id: string;
  nodeType: ResolutionMode; // 'entity' or 'service'
}

export interface BulkNodeDetailsRequest {
  items: NodeIdentifier[];
}

export interface BulkConnectionRequestItem {
  edgeId: string;
  itemType: ResolutionMode; // 'entity' or 'service'
}

export interface BulkConnectionsRequest {
  items: BulkConnectionRequestItem[];
}

export interface BulkVisualizationRequestItem {
  clusterId: string;
  itemType: ResolutionMode; // 'entity' or 'service'
}

export interface BulkVisualizationsRequest {
  items: BulkVisualizationRequestItem[];
}

// --- Bulk Disconnect Types ---
export interface DisconnectDependentServicesRequest {
  reviewerId: string;
  notes?: string;
  asyncProcessing?: boolean; // Default: true
}

export interface DisconnectDependentServicesResponse {
  message: string;
  entityPairsFound: number;
  serviceEdgesIdentified: number;
  serviceEdgesProcessed: number;
  clustersFinalized: number;
  processingMode: "synchronous" | "asynchronous";
  jobId?: string;
  processingTimeMs: number;
}

// --- Base Entity and Data Structure Types ---
export interface Organization {
  id: string;
  organizationId: string | null;
  name: string | null;
  createdAt: string | null;
  updatedAt: string | null;
  sourceSystem: string | null;
  sourceId: string | null;
  url?: string | null;
}

export interface Service {
  id: string;
  name: string | null;
  organizationId?: string | null;
  createdAt: string | null;
  updatedAt: string | null;
  sourceSystem: string | null;
  sourceId: string | null;
  url?: string | null;
}

export interface NodePhone {
  id: string;
  locationId: string | null;
  serviceId: string | null;
  organizationId: string | null;
  contactId: string | null;
  serviceAtLocationId: string | null;
  number: string;
  extension: string | null;
  type: string;
  language: string;
  description: string | null;
  priority: number | null;
  lastModified: string;
  created: string;
  originalId: string;
  originalTranslationsId: string;
  contributorId: string | null;
}

export interface NodeServiceAttribute {
  id: string;
  name: string;
  organizationId: string;
  createdAt: string;
  updatedAt: string;
  sourceSystem: string;
  url?: string | null;
}

export interface LocationAndAddress {
  id: string;
  organizationId: string;
  name: string | null;
  alternateName: string | null;
  description: string | null;
  shortDescription: string | null;
  transportation: string | null;
  latitude: number | null;
  longitude: number | null;
  locationType: string | null;
  lastModified: string | null;
  created: string | null;
  originalId: string | null;
  originalTranslationsId: string | null;
  contributorId: string | null;
  attention: string | null;
  address1: string | null;
  address2: string | null;
  city: string | null;
  region: string | null;
  stateProvince: string | null;
  postalCode: string | null;
  country: string | null;
  addressType: string | null;
}

export interface MatchValues {
  type?: string;
  values: Record<string, any>;
}

export interface EntityGroup {
  id: string;
  entityId1: string;
  entityId2: string;
  confidenceScore: number | null;
  preRlConfidenceScore: number | null;
  methodType: string;
  matchValues: MatchValues | null;
  confirmedStatus:
    | "PENDING_REVIEW"
    | "CONFIRMED_MATCH"
    | "CONFIRMED_NON_MATCH"
    | "DENIED"
    | string;
  createdAt: string | null;
  updatedAt: string | null;
  groupClusterId?: string | null;
  reviewedAt?: string | null;
  reviewerId?: string | null;
  notes?: string | null;
}

// --- Cluster Types ---
export interface BaseCluster {
  id: string;
  name?: string | null;
  description?: string | null;
  averageCoherenceScore: number | null;
  createdAt?: string | null;
  updatedAt?: string | null;
  wasReviewed?: boolean | null;
}

export interface EntityCluster extends BaseCluster {
  entityCount?: number | null;
  groupCount?: number | null;
}

export interface ServiceCluster extends BaseCluster {
  service_count?: number;
  service_group_count?: number;
}

export interface PaginatedClustersResponse<T> {
  clusters: T[];
  total: number;
  page: number;
  limit: number;
  totalPages: number;
}

// --- Match Decision and Feedback Types ---
export interface MatchDecisionDetails {
  id: string;
  groupId: string;
  pipelineRunId: string | null;
  snapshottedFeatures: Record<string, unknown>;
  methodTypeAtDecision: string;
  preRlConfidenceAtDecision: number;
  tunedConfidenceAtDecision: number;
  confidenceTunerVersionAtDecision: number | null;
  createdAt: string;
}

export interface HumanFeedbackBase {
  reviewerId: string;
  feedbackTimestamp: string;
  isMatchCorrect: boolean;
  notes: string | null;
  processedForTunerUpdateAt: string | null;
  matchDecisionId: string;
}

export interface EntityHumanFeedback extends HumanFeedbackBase {
  id: string;
  entityGroupId: string;
}

export interface ServiceMatchHumanFeedback extends HumanFeedbackBase {
  serviceGroupId: string;
}

// --- Visualization and Connection Types ---
export interface BaseNode {
  id: string;
  name: string | null;
  sourceSystem?: string | null;
  sourceId?: string | null;
  organizationId?: string | null;
  contributorId?: string | null;
}

export interface BaseLink {
  id: string;
  source: string;
  target: string;
  weight: number;
  status?:
    | "PENDING_REVIEW"
    | "CONFIRMED_MATCH"
    | "CONFIRMED_NON_MATCH"
    | string;
  details?: Record<string, any> | null;
  createdAt?: string | null;
  clusterId: string;
  wasReviewed?: boolean | null;
}

export interface VisualizationEntityEdge {
  id: string;
  clusterId: string;
  entityId1: string;
  entityId2: string;
  edgeWeight: number;
  details: Record<string, any> | null;
  pipelineRunId: string | null;
  createdAt: string;
  confirmedStatus: string | null;
  entity1Name: string | null;
  entity2Name: string | null;
  status: string | null;
  displayWeight: number | null;
  color: string | null;
  wasReviewed?: boolean | null;
}

export interface VisualizationData {
  clusterId: string;
  nodes: BaseNode[];
  links: BaseLink[];
  groups: EntityGroup[] | Record<string, any>;
}
export type EntityVisualizationDataResponse = VisualizationData;
export type BulkVisualizationsResponse = VisualizationData[];


export interface EntityConnectionDataResponse {
  edge: VisualizationEntityEdge;
  entity1: Organization | Service;
  entity2: Organization | Service;
  entityGroups: EntityGroup[];
  clusterId: string;
  matchDecisions?: MatchDecisionDetails[] | null;
}

export type BulkConnectionResponseItem = EntityConnectionDataResponse;
export type BulkConnectionsResponse = BulkConnectionResponseItem[];

export function isEntityConnectionData(
  data: BulkConnectionResponseItem,
  mode?: ResolutionMode
): data is EntityConnectionDataResponse {
  return "entity1" in data && "entityGroups" in data;
}


// --- Review and Action Types ---
export type EdgeDecision = "ACCEPTED" | "REJECTED";

export interface EdgeReviewApiPayload {
  decision: EdgeDecision;
  reviewerId: string;
  notes?: string;
  type: ResolutionMode;
  disconnectDependentServices?: boolean;
}

export interface EdgeReviewApiResponse {
  message: string;
  edgeId: string;
  newStatus: string;
  clusterId: string;
  clusterFinalized: boolean;
  dependentServicesDisconnected?: number;
  dependentClustersFinalized?: number;
}

export type GroupReviewDecision = "ACCEPTED" | "REJECTED" | string;


export interface SuggestedAction {
  id: string;
  pipelineRunId: string | null;
  actionType: string;
  entityId: string | null;
  groupId1: string | null;
  groupId2: string | null;
  clusterId: string | null;
  triggeringConfidence: number | null;
  details: Record<string, unknown> | null;
  reasonCode: string | null;
  reasonMessage: string | null;
  priority: number;
  status: string;
  reviewerId: string | null;
  reviewedAt: string | null;
  reviewNotes: string | null;
  createdAt: string;
  updatedAt: string;
}

export interface ReviewOperationBase {
  groupId: string;
  originalGroupStatus:
    | "PENDING_REVIEW"
    | "CONFIRMED_MATCH"
    | "CONFIRMED_NON_MATCH"
    | string;
}

export interface QueuedReviewBatch {
  batchId: string;
  edgeId: string;
  clusterId: string;
  decision: GroupReviewDecision;
  reviewerId: string;
  operations: ReviewOperationBase[];
  originalEdgeStatus:
    | "PENDING_REVIEW"
    | "CONFIRMED_MATCH"
    | "CONFIRMED_NON_MATCH"
    | string;
  optimisticEdgeStatus:
    | "PENDING_REVIEW"
    | "CONFIRMED_MATCH"
    | "CONFIRMED_NON_MATCH"
    | string;
  optimisticGroupStatus:
    | "PENDING_REVIEW"
    | "CONFIRMED_MATCH"
    | "CONFIRMED_NON_MATCH"
    | string;
  attempt: number;
  error?: string;
  processedOperations: Set<string>;
  failedOperations: Set<string>;
  isTerminalFailure?: boolean;
  mode: ResolutionMode;
}

export interface ClusterReviewProgress {
  totalEdges: number;
  reviewedEdges: number;
  progressPercentage: number;
  isComplete: boolean;
  pendingEdges: number;
  confirmedMatches: number;
  confirmedNonMatches: number;
}

export interface NodeDetailResponse {
  id: string;
  nodeType: "entity" | "service";
  baseData: Organization | Service;
  attributes: {
    phones?: NodePhone[];
    services?: NodeServiceAttribute[];
    locations?: LocationAndAddress[];
  };
}

// --- UI State Management Types ---
export interface ClustersState {
  data: EntityCluster[];
  total: number;
  page: number;
  limit: number;
  loading: boolean;
  error: string | null;
}

export interface VisualizationState {
  data: EntityVisualizationDataResponse | null;
  loading: boolean;
  error: string | null;
  lastUpdated: number | null;
}

export interface ConnectionState {
  data: EntityConnectionDataResponse | null;
  loading: boolean;
  error: string | null;
  lastUpdated: number | null;
}

export interface EdgeSelectionInfo {
  currentEdgeId: string | null;
  nextUnreviewedEdgeId: string | null;
  hasUnreviewedEdges: boolean;
  currentEdgeIndex: number;
  totalEdges: number;
  totalUnreviewedEdgesInCluster: number;
  currentUnreviewedEdgeIndexInCluster: number;
  totalEdgesInEntireCluster: number;
}
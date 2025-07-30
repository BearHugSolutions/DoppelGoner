/*
================================================================================
|
|   File: /types/entity-resolution.ts
|
|   Description: Centralized TypeScript types for the application.
|   - This file integrates new user/opinion types with the existing application types.
|   - UPDATED: Added types for paginated connection fetching.
|
================================================================================
*/

import { Node } from "reactflow";
import type { SimulationNodeDatum } from "d3";

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
  opinionName:string;
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
  id: string;
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

// ✨ NEW: Workflow filter type for cross-source system filtering
export type WorkflowFilter = "all" | "cross-source-only";

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

// ✨ UPDATED with pagination and filtering fields
export interface BulkConnectionsRequest {
  items: BulkConnectionRequestItem[];
  limit?: number;
  cursor?: string;
  crossSystemOnly?: boolean; // Note: camelCase for JSON payload
}

export interface BulkVisualizationRequestItem {
  clusterId: string;
  itemType: ResolutionMode; // 'entity' or 'service'
  limit?: number;
  cursor?: string;
  crossSystemOnly?: boolean;
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
export interface BaseNode extends SimulationNodeDatum {
  id: string;
  name: string | null;
  sourceSystem?: string | null;
  sourceId?: string | null;
  organizationId?: string | null;
  contributorId?: string | null;
}

export interface BaseLink {
  id: string;
  source: string | BaseNode;
  target: string | BaseNode;
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

export interface PaginatedLinks<T> {
  links: T[];
  next_cursor: string | null;
  has_more: boolean;
}

export interface VisualizationDataResponse {
  clusterId: string;
  nodes: BaseNode[];
  links: PaginatedLinks<BaseLink>;
  groups: EntityGroup[] | Record<string, any>;
  totalConnections: number;
  crossSystemConnections: number;
}

export type BulkVisualizationsResponse = VisualizationDataResponse[];

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

export interface EntityConnectionDataResponse {
  edge: VisualizationEntityEdge;
  entity1: Organization | Service;
  entity2: Organization | Service;
  entityGroups: EntityGroup[];
  clusterId: string;
  matchDecisions?: MatchDecisionDetails[] | null;
}

export type BulkConnectionResponseItem = EntityConnectionDataResponse;

// ✨ NEW: The paginated response type for bulk connections
export interface PaginatedBulkConnectionsResponse {
  connections: EntityConnectionDataResponse[];
  nextCursor: string | null;
  hasMore: boolean;
}

// The old response type is a simple array, preserved for compatibility.
export type BulkConnectionsResponse = BulkConnectionResponseItem[];


export function isEntityConnectionData(
  data: BulkConnectionResponseItem,
  mode?: ResolutionMode
): data is EntityConnectionDataResponse {
  // This type guard checks if the object has the properties of EntityConnectionDataResponse
  return "edge" in data && "entity1" in data && "entity2" in data && "entityGroups" in data;
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

export interface ProgressData {
  totalEdges: number;
  reviewedEdges: number;
  pendingEdges: number;
  confirmedMatches: number;
  confirmedNonMatches: number;
  progressPercentage: number;
  isComplete: boolean;
}

export interface CurrentViewProgress extends ProgressData {
  filterApplied: WorkflowFilter;
}

export interface ClusterProgress {
  // Unfiltered progress (all edges in cluster)
  allEdges: ProgressData;

  // Cross-source filtered progress (only cross-source edges)
  crossSourceEdges: ProgressData;

  // Current view progress (based on workflow_filter parameter)
  currentView: CurrentViewProgress;
}

export interface ClusterWithProgress {
  // Cluster basic information (flattened) - matching EntityCluster/BaseCluster pattern
  id: string;
  name?: string | null;
  description?: string | null;
  entityCount?: number | null;
  groupCount?: number | null;
  averageCoherenceScore: number | null;
  createdAt?: string | null;
  updatedAt?: string | null;
  wasReviewed?: boolean | null;

  // Progress information
  progress: ClusterProgress;
}

export interface OverallProgressData {
  totalPendingDecisions: number;
  totalCompletedDecisions: number;
  overallProgressPercentage: number;
  clustersComplete: number;
  totalClusters: number;
}

export interface CurrentViewOverallProgress extends OverallProgressData {
  filterApplied: WorkflowFilter;
}

export interface OverallProgress {
  // Unfiltered totals across all clusters
  all: OverallProgressData;

  // Cross-source filtered totals across all clusters
  crossSourceOnly: OverallProgressData;

  // Current view (based on workflow_filter parameter)
  currentView: CurrentViewOverallProgress;
}

export interface ClusterProgressResponse {
  clusters: ClusterWithProgress[];
  total: number;
  page: number;
  limit: number;
  totalPages: number;
  overallProgress: OverallProgress;
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
// This is the shape of the data stored in the context, not the direct API response.
// The `links` array here will be the ACCUMULATED list of links.
export interface VisualizationData {
  clusterId: string;
  nodes: BaseNode[];
  links: BaseLink[];
  groups: EntityGroup[] | Record<string, any>;
  totalConnections: number | null;
  crossSystemConnections: number | null;
}

// ✨ NEW: State slice for managing paginated bulk connection data
export interface BulkConnectionDataState {
  connections: Record<string, EntityConnectionDataResponse>; // Store by edge ID for easy access
  connectionOrder: string[]; // Maintain the order of connections
  isLoading: boolean;
  isLoadingMore: boolean;
  error: string | null;
  nextCursor: string | null;
  hasMore: boolean;
  filter: WorkflowFilter;
}

export interface ClustersState {
  data: EntityCluster[];
  total: number;
  page: number;
  limit: number;
  loading: boolean;
  error: string | null;
}

export interface VisualizationState {
  data: VisualizationData | null;
  loading: boolean;
  error: string | null;
  lastUpdated: number | null;
  // Pagination state
  nextCursor: string | null;
  hasMore: boolean;
  isLoadingMore: boolean;
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
  loadedLinksCount: number; // NEW: Track loaded links
  totalConnectionsInFilter: number | null; // NEW: Track total connections for the current filter
}

export interface EntityResolutionContextType {
  resolutionMode: ResolutionMode;
  selectedClusterId: string | null;
  selectedEdgeId: string | null;
  reviewerId: string;
  lastReviewedEdgeId: string | null;
  refreshTrigger: number;
  isAutoAdvanceEnabled: boolean;
  isReviewToolsMaximized: boolean;
  clusterFilterStatus: ClusterFilterStatus;
  disconnectDependentServicesEnabled: boolean;
  workflowFilter: WorkflowFilter; // ✨ NEW: Workflow filter state

  clusters: ClustersState;
  visualizationData: Record<string, VisualizationState>;
  connectionData: Record<string, ConnectionState>;
  bulkConnectionData: BulkConnectionDataState; // ✨ NEW: State for paginated connections
  nodeDetails: Record<string, NodeDetailResponse | null | "loading" | "error">;

  clusterProgress: Record<string, ClusterReviewProgress>;
  edgeSelectionInfo: EdgeSelectionInfo;

  currentVisualizationData: VisualizationData | null; // Note: this is the accumulated data
  currentConnectionData: EntityConnectionDataResponse | null;
  selectedClusterDetails: EntityCluster | null;

  actions: {
    setResolutionMode: (mode: ResolutionMode) => void;
    setSelectedClusterId: (id: string | null) => void;
    setSelectedEdgeId: (id: string | null) => void;
    setReviewerId: (id: string) => void;
    setLastReviewedEdgeId: (id: string | null) => void;
    setIsReviewToolsMaximized: (isMaximized: boolean) => void;
    setClusterFilterStatus: (status: ClusterFilterStatus) => void;
    setDisconnectDependentServicesEnabled: (enabled: boolean) => void;
    setWorkflowFilter: (filter: WorkflowFilter) => void; // ✨ NEW: Action to set workflow filter
    enableDisconnectDependentServices: () => Promise<void>;
    triggerRefresh: (
      target?:
        | "all"
        | "clusters"
        | "current_visualization"
        | "current_connection"
    ) => void;
    loadClusters: (page: number, limit?: number) => Promise<void>;
    loadBulkNodeDetails: (nodesToFetch: NodeIdentifier[]) => Promise<void>;
    loadSingleConnectionData: (
      edgeId: string
    ) => Promise<EntityConnectionDataResponse | null>;
    invalidateVisualizationData: (clusterId: string) => Promise<void>;
    invalidateConnectionData: (edgeId: string) => Promise<void>;
    clearAllData: () => void;
    selectNextUnreviewedEdge: (afterEdgeId?: string | null) => void;
    advanceToNextCluster: () => Promise<void>;
    checkAndAdvanceIfComplete: (clusterIdToCheck?: string) => Promise<void>;
    submitEdgeReview: (
      edgeId: string,
      decision: GroupReviewDecision,
      notes?: string
    ) => Promise<void>;
    setIsAutoAdvanceEnabled: (enabled: boolean) => void;
    selectPreviousUnreviewedInCluster: () => void;
    selectNextUnreviewedInCluster: () => void;
    
    initializeLargeClusterConnectionPaging: (clusterId: string) => Promise<void>;
    loadMoreConnectionsForCluster: (clusterId: string) => Promise<void>;
    
    // ✨ NEW: Actions for paginated bulk connections
    loadInitialBulkConnections: (items: BulkConnectionRequestItem[], filter: WorkflowFilter) => Promise<void>;
    loadMoreBulkConnections: () => Promise<void>;


    performThreeClusterCleanup: () => void;
    getCacheStats: () => any;
  };

  queries: {
    isVisualizationDataLoaded: (clusterId: string) => boolean;
    isVisualizationDataLoading: (clusterId: string) => boolean;
    isConnectionDataLoaded: (edgeId: string) => boolean;
    isConnectionDataLoading: (edgeId: string) => boolean;
    getVisualizationError: (clusterId: string) => string | null;
    getConnectionError: (edgeId: string) => string | null;
    getClusterProgress: (clusterId: string) => ClusterReviewProgress;
    canAdvanceToNextCluster: () => boolean;
    isEdgeReviewed: (edgeId: string) => boolean;
    getEdgeStatus: (edgeId: string) => BaseLink["status"] | null;
    getEdgeSubmissionStatus: (edgeId: string) => {
      isSubmitting: boolean;
      error: string | null;
    };
    getClusterById: (clusterId: string) => EntityCluster | undefined;
    getNodeDetail: (
      nodeId: string
    ) => NodeDetailResponse | null | "loading" | "error";
    
    getClusterPaginationState: (clusterId: string) => { 
      hasMore: boolean; 
      isLoadingMore: boolean; 
      nextCursor: string | null 
    };
  };
}

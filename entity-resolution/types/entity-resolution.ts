// types/entity-resolution.ts

export type ResolutionMode = "entity" | "service";

// Base entity and cluster types
export interface Entity {
  id: string;
  organizationId: string | null;
  name: string | null;
  createdAt: Date | string | null;
  updatedAt: Date | string | null;
  source_system: string | null;
  source_id: string | null;
}

export interface Service {
  id: string;
  name: string | null;
  createdAt: Date | string | null;
  updatedAt: Date | string | null;
  source_system: string | null;
  source_id: string | null;
}

export interface MatchValues {
  type?: string;
  values: Record<string, any>;
}

export interface EntityGroup {
  id: string;
  entityId1: string;
  entityId2: string;
  rl_confidence: number | null;
  pre_rl_confidence: number | null;
  methodType: string;
  matchValues: MatchValues | null;
  confirmedStatus: 'PENDING_REVIEW' | 'CONFIRMED_MATCH' | 'CONFIRMED_NON_MATCH' | 'DENIED' | string; // Added DENIED based on backend
  createdAt: Date | string | null;
  updatedAt: Date | string | null;
  groupClusterId?: string | null; // This can become null
  reviewedAt?: Date | string | null;
  reviewerId?: string | null;
  notes?: string | null;
}

export interface ServiceGroup {
  id: string;
  serviceId1: string;
  serviceId2: string;
  rl_confidence: number | null;
  pre_rl_confidence: number | null;
  methodType: string;
  matchValues: MatchValues | null;
  confirmedStatus: 'PENDING_REVIEW' | 'CONFIRMED_MATCH' | 'CONFIRMED_NON_MATCH' | 'DENIED' | string; // Added DENIED based on backend
  createdAt: Date | string | null;
  updatedAt: Date | string | null;
  groupClusterId?: string | null; // This can become null
  reviewedAt?: Date | string | null;
  reviewerId?: string | null;
  notes?: string | null;
}

export interface BaseCluster {
  id: string;
  name: string | null;
  description: string | null;
  averageCoherenceScore: number | null;
  createdAt: Date | string | null;
  updatedAt: Date | string | null;
  wasSplit?: boolean | null;
}

export interface EntityCluster extends BaseCluster {
  entityCount: number | null;
  groupCount: number | null;
}

export interface ServiceCluster extends BaseCluster {
  serviceCount: number | null;
  serviceGroupCount: number | null;
}

export interface MatchDecisionDetails {
  id: string;
  groupId: string;
  pipelineRunId: string | null;
  snapshottedFeatures: Record<string, unknown>;
  methodTypeAtDecision: string;
  preRlConfidenceAtDecision: number;
  tunedConfidenceAtDecision: number;
  confidenceTunerVersionAtDecision: number | null;
  createdAt: Date | string;
}

export interface HumanFeedbackBase {
  reviewerId: string;
  feedbackTimestamp: Date | string;
  isMatchCorrect: boolean;
  notes: string | null;
  processedForTunerUpdateAt: Date | string | null;
  matchDecisionId: string;
}

export interface EntityHumanFeedback extends HumanFeedbackBase {
  id: string;
  entityGroupId: string;
}

export interface ServiceMatchHumanFeedback extends HumanFeedbackBase {
  serviceGroupId: string;
}

export interface BaseNode {
  id: string;
  name: string | null;
  sourceSystem?: string | null;
  sourceId?: string | null;
  organizationId?: string | null;
  contributorId?: string | null;
}
export type EntityNode = BaseNode;
export type ServiceNode = BaseNode;

export interface BaseLink {
  id: string;
  source: string;
  target: string;
  weight: number;
  status?: 'PENDING_REVIEW' | 'CONFIRMED_MATCH' | 'CONFIRMED_NON_MATCH' | string;
  details?: Record<string, any> | null;
  createdAt?: Date | string | null;
  clusterId: string; // Added clusterId to BaseLink for easier cache invalidation
}
export type EntityLink = BaseLink;
export type ServiceLink = BaseLink;

export interface VisualizationEntityEdgeDetails {
    methods: Array<{
      method_type: string;
      pre_rl_confidence: number;
      rl_confidence: number;
      combined_confidence: number;
    }>;
    method_count?: number;
    rl_weight_factor?: number;
}

export interface VisualizationEntityEdge extends EntityLink { // Inherits clusterId
  entityId1: string;
  entityId2: string;
  edgeWeight: number;
  details: VisualizationEntityEdgeDetails | null;
  pipelineRunId: string | null;
  // createdAt is in BaseLink
  confirmedStatus?: EntityGroup['confirmedStatus'] | string;
  entity1Name?: string | null;
  entity2Name?: string | null;
  displayWeight?: number | null;
  color?: string | null;
}

export interface VisualizationServiceEdge extends ServiceLink { // Inherits clusterId
  serviceId1: string;
  serviceId2: string;
  edgeWeight: number;
  details: Record<string, unknown> | null; // For services, details might be more generic
  pipelineRunId: string;
  // createdAt is in BaseLink
  // confirmedStatus is not in the backend model for service_edge_visualization
  service1Name?: string | null;
  service2Name?: string | null;
  displayWeight?: number | null;
  color?: string | null;
}

export interface GroupReviewApiPayloadBase {
  decision: GroupReviewDecision;
  reviewerId: string;
  notes?: string;
}
export type EntityGroupReviewApiPayload = GroupReviewApiPayloadBase;
export type ServiceGroupReviewApiPayload = GroupReviewApiPayloadBase;

export interface GroupReviewApiResponse {
  message: string;
  updatedGroupId?: string;
  newStatus?: string;
  updatedEdgesInUserSchema?: number | null;
}

export type GroupReviewDecision = 'ACCEPTED' | 'REJECTED' | string;

// Updated ClusterFinalizationStatusResponse
export interface ClusterFinalizationStatusResponse {
  status: 'COMPLETED_NO_SPLIT_NEEDED' | 'COMPLETED_SPLIT_DETECTED' | 'PENDING_FULL_REVIEW' | 'CLUSTER_NOT_FOUND' | 'ERROR' | string;
  message: string;
  originalClusterId: string;
  newClusterIds?: string[]; // Keep as optional, though we don't expect it now
}

export interface ClustersResponse<TCluster extends BaseCluster> {
  clusters: TCluster[];
  total: number;
  page: number;
  limit: number;
  totalPages?: number;
}
export type EntityClustersResponse = ClustersResponse<EntityCluster>;
export type ServiceClustersResponse = ClustersResponse<ServiceCluster>;

// VisualizationDataResponse now includes clusterId for context
export interface VisualizationDataResponse<TNode extends BaseNode, TLink extends BaseLink, TGroup> {
  clusterId: string; // Added to know which cluster this data belongs to
  nodes: TNode[];
  links: TLink[];
  entityGroups: TGroup[] | Record<string, any>; // entityGroups might be specific to the cluster
}
export type EntityVisualizationDataResponse = VisualizationDataResponse<EntityNode, EntityLink, EntityGroup>;
export type ServiceVisualizationDataResponse = VisualizationDataResponse<ServiceNode, ServiceLink, ServiceGroup>;


// ConnectionDataResponse now includes clusterId for context
export interface ConnectionDataResponse<TEdge, TGroup, TEntityOrService extends Entity | Service> {
  edge: TEdge;
  entityGroups: TGroup[];
  matchDecisions?: MatchDecisionDetails[] | null;
  entity1: TEntityOrService;
  entity2: TEntityOrService;
  clusterId: string; // Added to know which cluster this connection data belongs to
}
export type EntityConnectionDataResponse = ConnectionDataResponse<VisualizationEntityEdge, EntityGroup, Entity>;
export type ServiceConnectionDataResponse = ConnectionDataResponse<VisualizationServiceEdge, ServiceGroup, Service>;

export function isEntityConnectionData(
  data: EntityConnectionDataResponse | ServiceConnectionDataResponse,
  mode: ResolutionMode
): data is EntityConnectionDataResponse {
  return mode === 'entity';
}

export function isServiceConnectionData(
  data: EntityConnectionDataResponse | ServiceConnectionDataResponse,
  mode: ResolutionMode
): data is ServiceConnectionDataResponse {
  return mode === 'service';
}

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
  reviewedAt: Date | string | null;
  reviewNotes: string | null;
  createdAt: Date | string;
  updatedAt: Date | string;
}

export interface ReviewOperationBase {
  groupId: string;
  originalGroupStatus: 'PENDING_REVIEW' | 'CONFIRMED_MATCH' | 'CONFIRMED_NON_MATCH' | string;
}

export interface QueuedReviewBatch {
  batchId: string;
  edgeId: string;
  clusterId: string;
  decision: GroupReviewDecision;
  reviewerId: string;
  operations: ReviewOperationBase[];
  originalEdgeStatus: 'PENDING_REVIEW' | 'CONFIRMED_MATCH' | 'CONFIRMED_NON_MATCH' | string;
  optimisticEdgeStatus: 'PENDING_REVIEW' | 'CONFIRMED_MATCH' | 'CONFIRMED_NON_MATCH' | string;
  optimisticGroupStatus: 'PENDING_REVIEW' | 'CONFIRMED_MATCH' | 'CONFIRMED_NON_MATCH' | string;
  attempt: number;
  error?: string;
  processedOperations: Set<string>;
  failedOperations: Set<string>;
  isTerminalFailure?: boolean;
  mode: ResolutionMode;
}

// Progress tracking for each cluster
export interface ClusterReviewProgress {
  totalEdges: number;
  reviewedEdges: number;
  progressPercentage: number;
  isComplete: boolean;
  // Optional: Store individual edge statuses within this cluster if needed for quick lookups
  // edgeStatuses?: Record<string, 'PENDING_REVIEW' | 'CONFIRMED_MATCH' | 'CONFIRMED_NON_MATCH'>;
}

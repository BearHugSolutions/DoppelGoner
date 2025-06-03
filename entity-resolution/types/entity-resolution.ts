// types/entity-resolution.ts

export type ResolutionMode = "entity" | "service";

// --- Structs for Bulk Node Details ---
export interface NodeIdentifier {
  id: string;
  nodeType: ResolutionMode; // 'entity' or 'service'
}

export interface BulkNodeDetailsRequest {
  items: NodeIdentifier[];
}

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
  // Assuming organizationId might be present based on backend models
  organizationId?: string | null;
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
  confirmedStatus: 'PENDING_REVIEW' | 'CONFIRMED_MATCH' | 'CONFIRMED_NON_MATCH' | 'DENIED' | string;
  createdAt: Date | string | null;
  updatedAt: Date | string | null;
  groupClusterId?: string | null;
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
  confirmedStatus: 'PENDING_REVIEW' | 'CONFIRMED_MATCH' | 'CONFIRMED_NON_MATCH' | 'DENIED' | string;
  createdAt: Date | string | null;
  updatedAt: Date | string | null;
  groupClusterId?: string | null;
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
  serviceGroupCount: number | null; // Corrected from groupCount
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
  sourceId?: string | null; // previously contributor_id for service, now consistently source_id
  organizationId?: string | null;
  // contributorId is deprecated in favor of sourceId for consistency
}
export type EntityNode = BaseNode;
export type ServiceNode = BaseNode;

export interface BaseLink {
  id: string;
  source: string; // node ID
  target: string; // node ID
  weight: number;
  status?: 'PENDING_REVIEW' | 'CONFIRMED_MATCH' | 'CONFIRMED_NON_MATCH' | string;
  details?: Record<string, any> | null;
  createdAt?: Date | string | null; // NaiveDateTime from Rust, so string or Date
  clusterId: string;
}
export type EntityLink = BaseLink; // Can be extended if entity links have unique props not in BaseLink
export type ServiceLink = BaseLink; // Can be extended if service links have unique props

// Specific edge types for connection data (if different from generic links)
export interface VisualizationEntityEdge extends BaseLink {
  entityId1: string; // Maps to source
  entityId2: string; // Maps to target
  edgeWeight: number; // Maps to weight
  // details: VisualizationEntityEdgeDetails | null; // Already in BaseLink
  pipelineRunId: string | null;
  confirmedStatus?: EntityGroup['confirmedStatus'] | string; // Maps to status
  entity1Name?: string | null;
  entity2Name?: string | null;
  displayWeight?: number | null;
  color?: string | null;
}

export interface VisualizationServiceEdge extends BaseLink {
  serviceId1: string; // Maps to source
  serviceId2: string; // Maps to target
  edgeWeight: number; // Maps to weight
  // details: Record<string, unknown> | null; // Already in BaseLink
  pipelineRunId: string; // Note: Rust model had this as non-optional
  // confirmedStatus is not directly on service_edge_visualization, status comes from groups
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

export interface ClusterFinalizationStatusResponse {
  status: 'COMPLETED_NO_SPLIT_NEEDED' | 'COMPLETED_SPLIT_DETECTED' | 'PENDING_FULL_REVIEW' | 'CLUSTER_NOT_FOUND' | 'ERROR' | string;
  message: string;
  originalClusterId: string;
  newClusterIds?: string[];
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

export interface VisualizationDataResponse<TNode extends BaseNode, TLink extends BaseLink> {
  clusterId: string;
  nodes: TNode[];
  links: TLink[]; // These are generic links, not specific VisualizationEntityEdge/ServiceEdge
  groups: EntityGroup[] | ServiceGroup[] | Record<string, any>; // Changed from entityGroups
}
export type EntityVisualizationDataResponse = VisualizationDataResponse<EntityNode, EntityLink>;
export type ServiceVisualizationDataResponse = VisualizationDataResponse<ServiceNode, ServiceLink>;


export interface ConnectionDataResponse<TEdge, TGroup, TEntityOrService extends Entity | Service> {
  edge: TEdge; // This will be VisualizationEntityEdge or VisualizationServiceEdge
  entityGroups: TGroup[]; // For entity mode, TGroup is EntityGroup. For service, ServiceGroup.
  matchDecisions?: MatchDecisionDetails[] | null;
  entity1: TEntityOrService; // For entity mode, TEntityOrService is Entity. For service, Service.
  entity2: TEntityOrService;
  clusterId: string;
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

export interface ClusterReviewProgress {
  totalEdges: number;
  reviewedEdges: number;
  progressPercentage: number;
  isComplete: boolean;
}

export interface NodeDetailResponse {
  id: string;
  nodeType: 'entity' | 'service';
  baseData: Record<string, any>; // Entity or Service (serialized as JSON from backend)
  attributes: Record<string, any[]>; // e.g., {"address": [{}, {}], "phone": [{}]}
}
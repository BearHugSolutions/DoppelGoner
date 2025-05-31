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
  confirmedStatus: 'PENDING_REVIEW' | 'CONFIRMED_MATCH' | 'CONFIRMED_NON_MATCH' | string;
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
  confirmedStatus: 'PENDING_REVIEW' | 'CONFIRMED_MATCH' | 'CONFIRMED_NON_MATCH' | string;
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
}
export type EntityLink = BaseLink;
export type ServiceLink = BaseLink;

// Details structure for VisualizationEntityEdge based on previous TS definition
// Note: The provided sample JSON for edge.details includes method_count and rl_weight_factor
// which are not in this interface. Consider adding them if they are consistently present.
export interface VisualizationEntityEdgeDetails {
    methods: Array<{
      method_type: string;
      pre_rl_confidence: number;
      rl_confidence: number;
      combined_confidence: number;
    }>;
    method_count?: number; // Added based on sample JSON
    rl_weight_factor?: number; // Added based on sample JSON
}

export interface VisualizationEntityEdge {
  id: string;
  clusterId: string;
  entityId1: string;
  entityId2: string;
  edgeWeight: number;
  details: VisualizationEntityEdgeDetails | null;
  pipelineRunId: string | null;
  createdAt: Date | string;
  status?: EntityLink['status'];
  confirmedStatus?: EntityGroup['confirmedStatus'] | string;
  entity1Name?: string | null;
  entity2Name?: string | null;
  displayWeight?: number | null;
  color?: string | null;
}

export interface VisualizationServiceEdge {
  id: string;
  clusterId: string;
  serviceId1: string;
  serviceId2: string;
  edgeWeight: number;
  details: Record<string, unknown> | null; // For services, details might be more generic or have a different structure
  pipelineRunId: string;
  createdAt: Date | string | null;
  status?: ServiceLink['status'];
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
  status: 'COMPLETED_NO_SPLIT_NEEDED' | 'COMPLETED_SPLIT_OCCURRED' | 'PENDING_FULL_REVIEW' | 'CLUSTER_NOT_FOUND' | 'ERROR' | string;
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

export interface VisualizationDataResponse<TNode extends BaseNode, TLink extends BaseLink, TGroup> {
  nodes: TNode[];
  links: TLink[];
  entityGroups: TGroup[] | Record<string, any>;
}
export type EntityVisualizationDataResponse = VisualizationDataResponse<EntityNode, EntityLink, EntityGroup>;
export type ServiceVisualizationDataResponse = VisualizationDataResponse<ServiceNode, ServiceLink, ServiceGroup>;


export interface ConnectionDataResponse<TEdge, TGroup, TEntityOrService extends Entity | Service> {
  edge: TEdge;
  // The key from the API is 'entityGroups' for both entity and service resolution modes.
  // The type of the items in the array (TGroup) will be EntityGroup or ServiceGroup.
  entityGroups: TGroup[];
  matchDecisions?: MatchDecisionDetails[] | null;
  entity1: TEntityOrService; // Represents either the first entity or first service
  entity2: TEntityOrService; // Represents either the second entity or second service
}
export type EntityConnectionDataResponse = ConnectionDataResponse<VisualizationEntityEdge, EntityGroup, Entity>;
export type ServiceConnectionDataResponse = ConnectionDataResponse<VisualizationServiceEdge, ServiceGroup, Service>;

// Type guards to help differentiate between EntityConnectionDataResponse and ServiceConnectionDataResponse
export function isEntityConnectionData(
  data: EntityConnectionDataResponse | ServiceConnectionDataResponse,
  mode: ResolutionMode
): data is EntityConnectionDataResponse {
  // This guard relies on the external 'mode' to discriminate.
  // It assumes that if mode is 'entity', the data structure will match EntityConnectionDataResponse.
  return mode === 'entity';
}

export function isServiceConnectionData(
  data: EntityConnectionDataResponse | ServiceConnectionDataResponse,
  mode: ResolutionMode
): data is ServiceConnectionDataResponse {
  // Similar to isEntityConnectionData, relies on 'mode'.
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

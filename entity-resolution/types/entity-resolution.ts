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

// --- Structs for Bulk Connections ---
export interface BulkConnectionRequestItem {
  edgeId: string;
  itemType: ResolutionMode; // 'entity' or 'service'
}

export interface BulkConnectionsRequest {
  items: BulkConnectionRequestItem[];
}

// --- Structs for Bulk Visualizations ---
export interface BulkVisualizationRequestItem {
  clusterId: string;
  itemType: ResolutionMode; // 'entity' or 'service'
}

export interface BulkVisualizationsRequest {
  items: BulkVisualizationRequestItem[];
}


// Base entity and cluster types
export interface Entity {
  id: string;
  organizationId: string | null;
  name: string | null;
  createdAt: Date | string | null; // NaiveDateTime from Rust becomes string
  updatedAt: Date | string | null; // NaiveDateTime from Rust becomes string
  source_system: string | null;
  source_id: string | null;
}

export interface Service {
  id: string;
  name: string | null;
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
  confidenceScore: number | null; 
  preRlConfidenceScore: number | null;
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
  confidenceScore: number | null;
  preRlConfidenceScore: number | null;
  methodType: string;
  matchValues: MatchValues | null; 
  confirmedStatus: 'PENDING_REVIEW' | 'CONFIRMED_MATCH' | 'CONFIRMED_NON_MATCH' | 'REJECTED' | string; 
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
  clusterId: string;
}
export type EntityLink = BaseLink;
export type ServiceLink = BaseLink;

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
}

export interface VisualizationServiceEdge {
  id: string;
  clusterId: string; 
  serviceId1: string;
  serviceId2: string;
  edgeWeight: number;
  details: Record<string, any> | null; 
  pipelineRunId: string | null; 
  createdAt: string; 
  service1Name?: string | null;
  service2Name?: string | null;
  status?: string | null; 
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

export interface VisualizationData {
  clusterId: string;
  nodes: BaseNode[]; 
  links: BaseLink[]; 
  groups: EntityGroup[] | ServiceGroup[] | Record<string, any>; 
}
export type EntityVisualizationDataResponse = VisualizationData;
export type ServiceVisualizationDataResponse = VisualizationData;
export type BulkVisualizationsResponse = VisualizationData[];

export interface EntityConnectionDataResponse {
  edge: VisualizationEntityEdge;
  entity1: Entity;
  entity2: Entity;
  entityGroups: EntityGroup[];
  clusterId: string;
  matchDecisions?: MatchDecisionDetails[] | null;
}

export interface ServiceConnectionDataResponse {
  edge: VisualizationServiceEdge;
  service1: Service; 
  service2: Service; 
  serviceGroups: ServiceGroup[]; 
  clusterId: string;
  matchDecisions?: MatchDecisionDetails[] | null;
}

export type BulkConnectionResponseItem = EntityConnectionDataResponse | ServiceConnectionDataResponse;
export type BulkConnectionsResponse = BulkConnectionResponseItem[];


export function isEntityConnectionData(
  data: BulkConnectionResponseItem,
  mode?: ResolutionMode 
): data is EntityConnectionDataResponse {
  return 'entity1' in data && 'entityGroups' in data;
}

export function isServiceConnectionData(
  data: BulkConnectionResponseItem,
  mode?: ResolutionMode
): data is ServiceConnectionDataResponse {
  return 'service1' in data && 'serviceGroups' in data;
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
  totalEdges: number; // -1 if unknown (e.g. large cluster, viz not loaded)
  reviewedEdges: number;
  progressPercentage: number; // -1 if unknown
  isComplete: boolean;
  pendingEdges: number; // Added
  confirmedMatches: number; // Added
  confirmedNonMatches: number; // Added
}

export interface NodeDetailResponse {
  id: string;
  nodeType: 'entity' | 'service';
  baseData: Record<string, any>; 
  attributes: Record<string, any[]>; 
}

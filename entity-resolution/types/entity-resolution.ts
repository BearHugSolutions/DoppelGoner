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
export interface Organization {
  id: string;
  organizationId: string | null;
  name: string | null;
  createdAt: string | null; // NaiveDateTime from Rust becomes string
  updatedAt: string | null; // NaiveDateTime from Rust becomes string
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

/**
 * Represents a phone number associated with a node, as seen in the 'Node phone' log.
 */
export interface NodePhone {
  id: string;
  locationId: string | null;
  serviceId: string | null;
  organizationId: string | null;
  contactId: string | null;
  serviceAtLocationId: string | null; // Based on logs, can be a string
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

/**
 * Represents a service listed in the attributes of a node, from the 'Node services' log.
 * This is different from the top-level Service type.
 */
export interface NodeServiceAttribute {
  id: string;
  name: string;
  organizationId: string;
  createdAt: string;
  updatedAt: string;
  sourceSystem: string;
  url?: string | null;
}

/**
 * Represents a location associated with a node, from the 'Node locations' log.
 */
export interface NodeLocation {
  id: string;
  organizationId: string;
  name: string;
  alternateName: string | null;
  description: string | null;
  shortDescription: string | null;
  transportation: string | null;
  latitude: number;
  longitude: number;
  locationType: string;
  lastModified: string;
  created: string;
  originalId: string;
  originalTranslationsId: string;
  contributorId: string | null;
}

/**
 * Represents a postal address associated with a location, from the 'Node addresses' log.
 */
export interface NodeAddress {
  id: string;
  locationId: string;
  attention: string | null;
  address1: string;
  address2: string | null;
  city: string;
  region: string | null;
  stateProvince: string;
  postalCode: string;
  country: string;
  addressType: string;
  lastModified: string;
  created: string;
  originalId: string;
  contributorId: string | null;
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

// More accurately reflects optional fields from Rust backend
export interface BaseCluster {
  id: string;
  name?: string | null;
  description?: string | null;
  averageCoherenceScore: number | null;
  createdAt?: string | null; // NaiveDateTime serializes to string
  updatedAt?: string | null;
  wasSplit?: boolean | null;
}

// This now perfectly matches the Rust `EntityClusterItem` and is used for both modes
export interface EntityCluster extends BaseCluster {
  entityCount?: number | null;
  groupCount?: number | null;
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
}

export interface GroupReviewApiPayloadBase {
  decision: GroupReviewDecision;
  reviewerId: string;
  notes?: string;
}

export interface GroupReviewApiResponse {
  message: string;
  updatedGroupId?: string;
  newStatus?: string;
  updatedEdgesInUserSchema?: number | null;
}

export type GroupReviewDecision = "ACCEPTED" | "REJECTED" | string;

// Generic response type to match Rust's `TypedClusterListResponse<T>`
export interface PaginatedClustersResponse<T> {
  clusters: T[];
  total: number;
  page: number;
  limit: number;
  totalPages: number;
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
  nodeType: "entity" | "service";
  baseData: Organization | Service; // This remains flexible for both node types
  attributes: {
    // These properties are optional because they are not present in every node response
    phones?: NodePhone[];
    services?: NodeServiceAttribute[];
    locations?: NodeLocation[];
    addresses?: NodeAddress[];
  };
}

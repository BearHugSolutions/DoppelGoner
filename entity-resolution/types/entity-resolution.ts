// types/entity-resolution.ts

// Base entity and cluster types
export interface Entity {
  id: string;
  organization_id: string;
  name: string | null;
  created_at: Date | null;
  updated_at: Date | null;
  source_system: string | null;
  source_id: string | null;
}

export interface EntityGroup {
  id: string;
  entity_id_1: string;
  entity_id_2: string;
  confidence_score: number | null;
  pre_rl_confidence_score: number | null;
  method_type: string;
  match_values: MatchValues;
  confirmed_status: 'PENDING_REVIEW' | 'CONFIRMED_MATCH' | 'CONFIRMED_NON_MATCH';
  created_at: Date | null;
  updated_at: Date | null;
  group_cluster_id?: string | null;
  reviewed_at?: Date | null;
  reviewer_id?: string | null;
}

export interface Cluster {
  id: string;
  name: string | null;
  description: string | null;
  entity_count: number;
  group_count: number;
  average_coherence_score: number | null;
  created_at: Date | null;
  updated_at: Date | null;
}

// Match decision and feedback types
export interface MatchDecisionDetails {
  id: string;
  entity_group_id: string;
  pipeline_run_id: string | null;
  snapshotted_features: Record<string, unknown>;
  method_type_at_decision: string;
  pre_rl_confidence_at_decision: number;
  tuned_confidence_at_decision: number;
  confidence_tuner_version_at_decision: number | null;
  created_at: Date;
}

export interface HumanFeedback {
  id: string;
  entity_group_id: string;
  reviewer_id: string;
  feedback_timestamp: Date;
  is_match_correct: boolean;
  notes: string | null;
  processed_for_tuner_update_at: Date | null;
  match_decision_id: string;
}

// Visualization types
export interface EntityNode {
  id: string;
  name: string;
}

export interface EntityLink {
  id: string;
  source: string; // entity_id_1
  target: string; // entity_id_2
  weight: number; // edge_weight
  status?: 'PENDING_REVIEW' | 'CONFIRMED_MATCH' | 'CONFIRMED_NON_MATCH';
}

export interface VisualizationEntityEdge {
  id: string;
  cluster_id: string;
  entity_id_1: string;
  entity_id_2: string;
  edge_weight: number;
  details: {
    methods: Array<{
      method_type: string;
      pre_rl_confidence: number;
      rl_confidence: number;
      combined_confidence: number;
    }>;
  };
  pipeline_run_id: string | null;
  created_at: Date;
  // FIX: Added status and confirmed_status to align with usage in context
  status?: EntityLink['status'];
  confirmed_status?: EntityGroup['confirmed_status']; // Or more specific if needed
}

// Match values structure
export interface MatchValues {
  type?: string;
  values: Record<string, any>; // Consider defining more specific types if possible
}

// API request/response types
export interface EntityGroupReviewApiPayload {
  decision: EntityGroupReviewDecision;
  reviewerId: string;
  notes?: string;
}

export interface EntityGroupReviewApiResponse {
  message: string;
}

export type EntityGroupReviewDecision = 'ACCEPTED' | 'REJECTED';

// Cluster finalization types
export interface ClusterFinalizationStatusResponse {
  status: 'COMPLETED_NO_SPLIT_NEEDED' | 'COMPLETED_SPLIT_OCCURRED' | 'PENDING_FULL_REVIEW' | 'CLUSTER_NOT_FOUND' | 'ERROR';
  message: string;
  originalClusterId: string;
  newClusterIds?: string[];
}

// API response types
export interface ClustersResponse {
  clusters: Cluster[];
  total: number;
  page: number;
  limit: number;
}

export interface VisualizationDataResponse {
  nodes: EntityNode[];
  links: EntityLink[]; // Represents the edges in the graph visualization
  entityGroups: EntityGroup[]; // Underlying groups, potentially for all links in visualization
}

export interface ConnectionDataResponse {
  edge: VisualizationEntityEdge; // The specific edge being reviewed
  entityGroups: EntityGroup[]; // EntityGroups specifically related to this edge
  matchDecisions: MatchDecisionDetails[];
  entity1: Entity;
  entity2: Entity;
}

// Suggested actions type
export interface SuggestedAction {
  id: string;
  pipeline_run_id: string | null;
  action_type: string;
  entity_id: string | null;
  group_id_1: string | null;
  group_id_2: string | null;
  cluster_id: string | null;
  triggering_confidence: number | null;
  details: Record<string, unknown> | null;
  reason_code: string | null;
  reason_message: string | null;
  priority: number;
  status: string;
  reviewer_id: string | null;
  reviewed_at: Date | null;
  review_notes: string | null;
  created_at: Date;
  updated_at: Date;
}

// Optimistic Update Queue Types (moved from context file for better organization)
export interface ReviewOperation {
  groupId: string;
  originalGroupStatus: EntityGroup['confirmed_status'];
}

export interface QueuedReviewBatch {
  batchId: string;
  edgeId: string;
  clusterId: string;
  decision: EntityGroupReviewDecision;
  reviewerId: string;
  operations: ReviewOperation[];
  originalEdgeStatus: EntityLink['status']; // Status of the edge before optimistic update
  optimisticEdgeStatus: EntityLink['status']; // Status applied optimistically to the edge
  optimisticGroupStatus: EntityGroup['confirmed_status']; // Status applied optimistically to groups
  attempt: number;
  error?: string;
  processedOperations: Set<string>; // groupIds that succeeded in this batch
  failedOperations: Set<string>; // groupIds that failed in the current attempt for this batch
  isTerminalFailure?: boolean;
}

// Service types (for future use) - Keep as is if not directly related to errors
export interface ServiceGroup {
  id: string;
  service_id_1: string;
  service_id_2: string;
  confidence_score: number | null;
  pre_rl_confidence_score: number | null;
  method_type: string;
  match_values: MatchValues;
  confirmed_status: 'PENDING_REVIEW' | 'CONFIRMED_MATCH' | 'CONFIRMED_NON_MATCH';
  created_at: Date | null;
  updated_at: Date | null;
  group_cluster_id?: string | null;
  reviewed_at?: Date | null;
  reviewer_id?: string | null;
}

export interface ServiceGroupCluster {
  id: string;
  name: string | null;
  description: string | null;
  service_count: number;
  service_group_count: number;
  average_coherence_score: number | null;
  created_at: Date | null;
  updated_at: Date | null;
}

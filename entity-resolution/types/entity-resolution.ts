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
  source: string;
  target: string;
  weight: number;
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
}

// Match values structure
export interface MatchValues {
  type?: string;
  values: Record<string, any>;
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
  links: EntityLink[];
  entityGroups: EntityGroup[];
}

export interface ConnectionDataResponse {
  edge: VisualizationEntityEdge;
  entityGroups: EntityGroup[];
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

// Service types (for future use)
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
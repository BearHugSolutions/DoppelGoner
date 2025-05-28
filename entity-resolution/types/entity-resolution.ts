// CLUSTER SELECTOR TYPES
export interface Cluster {
  id: string
  name: string | null
  description: string | null
  entity_count: number
  group_count: number
  average_coherence_score: number | null
  created_at: Date
  updated_at: Date
}

export enum ActionType {
  REVIEW_ENTITY_IN_GROUP = "REVIEW_ENTITY_IN_GROUP",
  REVIEW_INTER_GROUP_LINK = "REVIEW_INTER_GROUP_LINK",
  SUGGEST_SPLIT_CLUSTER = "SUGGEST_SPLIT_CLUSTER",
  SUGGEST_MERGE_CLUSTERS = "SUGGEST_MERGE_CLUSTERS",
}

export interface SuggestedAction {
  id: string
  action_type: ActionType
  cluster_id: string | null
  entity_id: string | null
  group_id_1: string | null
  group_id_2: string | null
  status: string // Default 'PENDING_REVIEW'
  priority: number
  reason_message: string | null
}

// GRAPH VISUALIZER TYPES
export interface Entity {
  id: string
  organization_id: string
  name: string
  created_at: string
  updated_at: string
  source_system: string
  source_id: string
}

export interface MatchingMethodDetail {
  method_type: string
  pre_rl_confidence: number
  rl_confidence: number
  combined_confidence: number
}

export interface VisualizationEdgeDetails {
  methods: MatchingMethodDetail[]
  rl_weight_factor: number
  method_count: number
}

export interface VisualizationEntityEdge {
  id: string
  cluster_id: string
  entity_id_1: string
  entity_id_2: string
  edge_weight: number
  details: VisualizationEdgeDetails
  pipeline_run_id: string
  created_at: string
  entity1_name: string
  entity2_name: string
  status: string
  display_weight: number
  color: string | null
}

// For D3 visualization
export interface EntityNode {
  id: string
  name: string | null
  organization_id: string
  // D3 properties added during simulation
  x?: number
  y?: number
}

export interface EntityLink {
  source: string // entity_id_1
  target: string // entity_id_2
  id: string // visualization_entity_edge id
  weight: number // edge_weight
  status?: string // 'PENDING_REVIEW', 'ACCEPTED', or 'REJECTED'
}

// CONNECTION REVIEW TYPES
export interface MatchValues {
  type: string
  values: {
    // For Email type:
    original_email1?: string
    original_email2?: string
    normalized_shared_email?: string
    
    // For Name type:
    original_name1?: string
    original_name2?: string
    normalized_name1?: string
    normalized_name2?: string
    pre_rl_match_type?: string
    
    // For Url type:
    original_url1?: string
    original_url2?: string
    matching_slug_count?: number
    normalized_shared_domain?: string
    
    // Generic fallback
    [key: string]: any
  }
}
export interface EntityGroup {
  id: string
  confidence_score: number
  method_type: string
  match_values: MatchValues
  pre_rl_confidence_score: number
  confirmed_status: string
  reviewed_at: string | null
  reviewer_id: string | null
}


export interface MatchDecisionDetails {
  id: string
  entity_group_id: string
  pipeline_run_id: string | null
  method_type_at_decision: string
  pre_rl_confidence_at_decision: number
  tuned_confidence_at_decision: number
}

export interface HumanFeedback {
  id: string
  entity_group_id: string
  reviewer_id: string
  is_match_correct: boolean
  notes: string | null
  match_decision_id: string
}

// Entity details for hover view
export interface EntityDetails {
  edge: {
    id: string
    cluster_id: string
    color: string | null
    created_at: string
    details: {
      methods: any[]
      method_count: number
      rl_weight_factor: number
    }
    display_weight: number
    edge_weight: number
    entity1_name: string
    entity2_name: string
    entity_id_1: string
    entity_id_2: string
    pipeline_run_id: string
    status: string
  }
  entity1: {
    id: string
    name: string
    organization_id: string
    source_id: string
    source_system: string
    created_at: string
    updated_at: string
  }
  entity2: {
    id: string
    name: string
    organization_id: string
    source_id: string
    source_system: string
    created_at: string
    updated_at: string
  }
  entityGroups: EntityGroup[]
  matchDecisions: MatchDecisionDetails
}
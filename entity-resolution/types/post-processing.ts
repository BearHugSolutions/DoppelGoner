/*
================================================================================
|
|   File: /types/post-processing.ts
|
|   Description: TypeScript types for the Post-Processing Decision Audit System
|   - API request/response types for audit endpoints
|   - UI state management types for audit mode
|   - Integration types for existing entity resolution system
|
================================================================================
*/

import { WorkflowFilter } from "./entity-resolution";

// --- Core Post-Processing Decision Types ---

export interface PostProcessingDecision {
    id: string;
    entityType: "entity" | "service";
    clusterId: string;
    edgeId: string;
    postProcFilter: string;
    originalStatus: string | null;
    newStatus: string;
    createdAt: string;
    reviewedByHuman: boolean;
    reviewedAt: string | null;
  }
  
  // --- API Request/Response Types ---
  
  export interface PostProcessingAuditParams {
    entityType?: "entity" | "service";
    postProcFilter?: string;
    reviewedByHuman?: boolean;
    clusterId?: string;
    page?: number;
    limit?: number;
    workflowFilter?: WorkflowFilter;
  }
  
  export interface PostProcessingAuditResponse {
    decisions: PostProcessingDecision[];
    total: number;
    page: number;
    limit: number;
    _clusterSpecific?: string;
  }
  
  // --- Cluster-Centric View Types ---
  
  export interface ClusterWithAuditDecisions {
    id: string;
    entityType: "entity" | "service";
    unreviewedDecisionsCount: number;
    totalDecisionsCount: number;
    lastDecisionAt: string;
  }
  
  export interface AuditCounts {
    totalUnreviewed: number;
    totalReviewed: number;
    byFilter: Record<string, {
      unreviewed: number;
      reviewed: number;
    }>;
  }
  
  export interface ClustersWithPostProcessingResponse {
    clusters: ClusterWithAuditDecisions[];
    auditCounts: AuditCounts;
    total: number;
    page: number;
    limit: number;
  }
  
  // --- Bulk Operations Types ---
  
  export interface BulkMarkReviewedRequest {
    decisionIds: string[];
    reviewerId: string;
  }
  
  export interface BulkMarkReviewedResponse {
    message: string;
    markedCount: number;
    failedCount: number;
  }
  
  // --- UI State Types ---
  
  export type AuditMode = "normal" | "post_processing_audit";
  
  export interface PostProcessingAuditState {
    data: PostProcessingAuditResponse | null;
    loading: boolean;
    error: string | null;
    lastUpdated: number | null;
  }
  
  export interface ClustersWithAuditState {
    data: ClustersWithPostProcessingResponse | null;
    loading: boolean;
    error: string | null;
    lastUpdated: number | null;
  }
  
  // --- Enhanced Context Types ---
  
  export interface AuditStateExtension {
    auditMode: AuditMode;
    postProcessingFilter: string | null;
  }
  
  export interface AuditActionsExtension {
    setAuditMode: (mode: AuditMode) => void;
    setPostProcessingFilter: (filter: string | null) => void;
  }
  
  export interface AuditDataExtension {
    postProcessingAuditData: PostProcessingAuditState;
    clustersWithAuditData: ClustersWithAuditState;
    loadPostProcessingAuditData: (params: PostProcessingAuditParams) => Promise<void>;
    loadClustersWithAuditData: (params: PostProcessingAuditParams) => Promise<void>;
    bulkMarkDecisionsReviewed: (decisionIds: string[], reviewerId: string) => Promise<void>;
  }
  
  // --- Filter Types ---
  
  export interface PostProcessingFilterInfo {
    key: string;
    displayName: string;
    description: string;
  }
  
  export const POST_PROCESSING_FILTERS: Record<string, PostProcessingFilterInfo> = {
    disconnectDependentServices: {
      key: "disconnectDependentServices",
      displayName: "Dependent Services",
      description: "Automatically disconnect service matches when parent entities are marked as non-matches"
    }
    // Add more filters here as they are implemented
  };
  
  // --- Utility Types ---
  
  export interface AuditNavigationState {
    currentClusterIndex: number;
    hasMoreClusters: boolean;
    canNavigateToNext: boolean;
    canNavigateToPrevious: boolean;
  }
  
  export interface AuditProgressSummary {
    totalDecisions: number;
    reviewedDecisions: number;
    pendingDecisions: number;
    progressPercentage: number;
    clusterStats: {
      totalClusters: number;
      clustersWithDecisions: number;
      clustersFullyReviewed: number;
    };
  }
  
  // --- Component Props Types ---
  
  export interface AuditModeToggleProps {
    auditMode: AuditMode;
    onModeChange: (mode: AuditMode) => void;
    unviewedCount?: number;
  }
  
  export interface PostProcessingFilterSelectorProps {
    selectedFilter: string | null;
    onFilterChange: (filter: string | null) => void;
    auditCounts?: AuditCounts;
  }
  
  export interface AuditDecisionCardProps {
    decision: PostProcessingDecision;
    onMarkReviewed: (decisionId: string) => Promise<void>;
    isMarking?: boolean;
  }
  
  export interface AuditClusterListProps {
    clusters: ClusterWithAuditDecisions[];
    selectedClusterId: string | null;
    onClusterSelect: (clusterId: string) => void;
    loading?: boolean;
    error?: string | null;
  }
  
  // --- Error Types ---
  
  export interface AuditError {
    code: string;
    message: string;
    details?: Record<string, any>;
  }
  
  export class PostProcessingAuditError extends Error {
    constructor(
      message: string,
      public code: string,
      public details?: Record<string, any>
    ) {
      super(message);
      this.name = 'PostProcessingAuditError';
    }
  }
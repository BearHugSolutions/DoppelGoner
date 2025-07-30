/*
================================================================================
|
|   File: /context/entity-resolution-context.tsx - Phase 4: Unified Interface - FIXED TYPE CONSISTENCY
|
|   Description: Refactored Entity Resolution Context using the three-context architecture
|   - FIXED: Type consistency for visualization data - always use VisualizationData internally
|   - FIXED: Removed VisualizationDataResponse from internal state types
|   - FIXED: Updated interface to match actual implementation
|
================================================================================
*/
"use client";

import {
  ResolutionMode,
  EntityCluster,
  VisualizationData,
  EntityConnectionDataResponse,
  GroupReviewDecision,
  BaseLink,
  NodeDetailResponse,
  NodeIdentifier,
  ClusterReviewProgress,
  ClusterFilterStatus,
  WorkflowFilter,
  EdgeSelectionInfo,
} from "@/types/entity-resolution";
import type {
  AuditMode,
  PostProcessingAuditParams,
  PostProcessingAuditResponse,
  PostProcessingAuditState,
  ClustersWithAuditState,
} from "@/types/post-processing";
import {
  createContext,
  useCallback,
  useContext,
  useMemo,
  type ReactNode,
} from "react";
import {
  useEntityState,
  useEntityStateDebug,
  EntityStateProvider,
} from "./entity-state-context";
import { useEntityData, EntityDataProvider } from "./entity-data-context";
import {
  useEntityWorkflow,
  EntityWorkflowProvider,
} from "./entity-workflow-context";

// ============================================================================
// Backward Compatibility Interface - ENHANCED with Audit, Optimistic Updates, and Pagination
// ============================================================================

export interface EntityResolutionContextType {
  // Core state (from EntityStateContext)
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
  workflowFilter: WorkflowFilter;

  // 🆕 NEW: Audit state
  auditMode: AuditMode;
  postProcessingFilter: string | null;

  // Data state (from EntityDataContext) - FIXED: Use consistent VisualizationData type
  clusters: {
    data: EntityCluster[];
    total: number;
    page: number;
    limit: number;
    loading: boolean;
    error: string | null;
  };
  visualizationData: Record<
    string,
    {
      data: VisualizationData | null; // FIXED: Only use VisualizationData internally
      loading: boolean;
      error: string | null;
      lastUpdated: number | null;
    }
  >;
  connectionData: Record<
    string,
    {
      data: EntityConnectionDataResponse | null;
      loading: boolean;
      error: string | null;
      lastUpdated: number | null;
    }
  >;
  nodeDetails: Record<string, NodeDetailResponse | null | "loading" | "error">;

  // 🆕 NEW: Audit data state
  postProcessingAuditData: PostProcessingAuditState;
  clustersWithAuditData: ClustersWithAuditState;

  // Workflow state (from EntityWorkflowContext)
  clusterProgress: Record<string, ClusterReviewProgress>;
  overallProgress: {
    totalPendingDecisions: number;
    totalCompletedDecisions: number;
    overallProgressPercentage: number;
    clustersComplete: number;
    totalClusters: number;
  };
  edgeSelectionInfo: EdgeSelectionInfo;

  // Current computed data - FIXED: Use consistent VisualizationData type
  currentVisualizationData: VisualizationData | null;
  currentConnectionData: EntityConnectionDataResponse | null;
  selectedClusterDetails: EntityCluster | null;

  // Paging state
  activelyPagingClusterId: string | null;
  largeClusterConnectionsPage: number;
  isLoadingConnectionPageData: boolean;

  // Actions - unified interface from all three contexts (ENHANCED with audit and pagination)
  actions: {
    // State actions
    setResolutionMode: (mode: ResolutionMode) => void;
    setSelectedClusterId: (
      id: string | null,
      isManualSelection?: boolean
    ) => Promise<void>;
    setSelectedEdgeId: (id: string | null) => void;
    setReviewerId: (id: string) => void;
    setLastReviewedEdgeId: (id: string | null) => void;
    setIsReviewToolsMaximized: (isMaximized: boolean) => void;
    setClusterFilterStatus: (status: ClusterFilterStatus) => void;
    setDisconnectDependentServicesEnabled: (enabled: boolean) => void;
    setWorkflowFilter: (filter: WorkflowFilter) => void;
    setIsAutoAdvanceEnabled: (enabled: boolean) => void;

    // 🆕 NEW: Audit actions
    setAuditMode: (mode: AuditMode) => void;
    setPostProcessingFilter: (filter: string | null) => void;

    // Data actions
    enableDisconnectDependentServices: () => Promise<void>;
    triggerRefresh: (
      target?: "all" | "clusters" | "current_visualization" | "current_connection"
    ) => void;
    loadClusters: (page: number, limit?: number) => Promise<void>;
    loadClusterProgress: (page: number, limit?: number) => Promise<void>;
    loadBulkNodeDetails: (nodesToFetch: NodeIdentifier[]) => Promise<void>;
    loadSingleConnectionData: (
      edgeId: string
    ) => Promise<EntityConnectionDataResponse | null>;
    invalidateVisualizationData: (clusterId: string) => Promise<void>;
    invalidateConnectionData: (edgeId: string) => Promise<void>;
    clearAllData: () => void;

    // 🆕 NEW: Audit data actions
    loadPostProcessingAuditData: (
      params: PostProcessingAuditParams
    ) => Promise<PostProcessingAuditResponse | null>;
    loadClustersWithAuditData: (
      params: PostProcessingAuditParams
    ) => Promise<void>;
    bulkMarkDecisionsReviewed: (
      decisionIds: string[],
      reviewerId: string
    ) => Promise<void>;

    // Optimistic update actions
    updateEdgeStatusOptimistically: (
      clusterId: string,
      edgeId: string,
      newStatus: BaseLink["status"],
      wasReviewed?: boolean
    ) => () => void;
    updateClusterCompletionOptimistically: (
      clusterId: string,
      wasReviewed: boolean
    ) => () => void;

    // Workflow actions
    selectNextUnreviewedEdge: (afterEdgeId?: string | null) => void;
    advanceToNextCluster: () => Promise<void>;
    checkAndAdvanceIfComplete: (clusterIdToCheck?: string) => Promise<void>;
    submitEdgeReview: (
      edgeId: string,
      decision: GroupReviewDecision,
      notes?: string
    ) => Promise<void>;
    selectPreviousUnreviewedInCluster: () => void;
    selectNextUnreviewedInCluster: () => void;
    performThreeClusterCleanup: () => void;
    getCacheStats: () => any;

    // 🆕 NEW/REFACTORED: Pagination actions
    initializeLargeClusterConnectionPaging: (clusterId: string) => Promise<void>;
    loadMoreConnectionsForCluster: (clusterId: string) => Promise<void>;

    // Legacy pagination actions (for backward compatibility)
    viewNextConnectionPage: (clusterId: string) => Promise<void>;
    getActivelyPagingClusterId: () => string | null;
    getLargeClusterConnectionsPage: () => number;

    // 🆕 NEW: Audit workflow actions
    initializeAuditMode: () => Promise<void>;
    findNextAuditCluster: () => Promise<void>;
  };

  // Queries - unified interface from all three contexts (ENHANCED with audit and pagination)
  queries: {
    // Data queries
    isVisualizationDataLoaded: (clusterId: string) => boolean;
    isVisualizationDataLoading: (clusterId: string) => boolean;
    isConnectionDataLoaded: (edgeId: string) => boolean;
    isConnectionDataLoading: (edgeId: string) => boolean;
    getVisualizationError: (clusterId: string) => string | null;
    getConnectionError: (edgeId: string) => string | null;
    getClusterById: (clusterId: string) => EntityCluster | undefined;
    getNodeDetail: (
      nodeId: string
    ) => NodeDetailResponse | null | "loading" | "error";

    // 🆕 NEW: Pagination query
    getClusterPaginationState: (clusterId: string) => { 
      hasMore: boolean; 
      isLoadingMore: boolean; 
      nextCursor: string | null;
      totalConnections?: number;
      crossSystemConnections?: number;
      loadedLinksCount?: number;
    };

    // 🆕 NEW: Audit data queries
    isPostProcessingAuditDataLoaded: () => boolean;
    isPostProcessingAuditDataLoading: () => boolean;
    isClustersWithAuditDataLoaded: () => boolean;
    isClustersWithAuditDataLoading: () => boolean;
    getPostProcessingAuditError: () => string | null;
    getClustersWithAuditError: () => string | null;

    // Workflow queries
    getClusterProgress: (clusterId: string) => ClusterReviewProgress;
    getClusterProgressUnfiltered: (clusterId: string) => ClusterReviewProgress;
    getClusterProgressCrossSource: (clusterId: string) => ClusterReviewProgress;
    canAdvanceToNextCluster: () => boolean;
    isEdgeReviewed: (edgeId: string) => boolean;
    getEdgeStatus: (edgeId: string) => BaseLink["status"] | null;
    getEdgeSubmissionStatus: (edgeId: string) => {
      isSubmitting: boolean;
      error: string | null;
    };

    // Filter-aware navigation helpers
    isClusterCompleteForCurrentFilter: (clusterId: string) => boolean;
    findNextViableCluster: (
      fromClusterId?: string
    ) => EntityCluster | "next_page" | null;

    // 🆕 NEW: Audit workflow queries
    isInAuditMode: () => boolean;
    getAuditFilterDisplayName: () => string;
    isClusterCompleteForAudit: (clusterId: string) => boolean;
    getAuditNavigationState: () => {
      currentClusterIndex: number;
      totalClusters: number;
      hasNextCluster: boolean;
      hasPreviousCluster: boolean;
    };
  };
}

// ============================================================================
// Context Creation
// ============================================================================

const EntityResolutionContext = createContext<
  EntityResolutionContextType | undefined
>(undefined);

// ============================================================================
// Internal Consumer Component
// ============================================================================

// This component consumes all three contexts and provides the unified interface
function EntityResolutionConsumer({ children }: { children: ReactNode }) {
  const state = useEntityState();
  const data = useEntityData();
  const workflow = useEntityWorkflow();

  // ========================================================================
  // FIXED: Legacy pagination compatibility method
  // ========================================================================
  const viewNextConnectionPage = useCallback(async (clusterId: string) => {
    // This is a legacy compatibility method that maps to the new pagination system
    console.log(`🔄 [EntityResolution] Legacy viewNextConnectionPage called for cluster ${clusterId}`);
    await workflow.loadMoreConnectionsForCluster(clusterId);
  }, [workflow]);

  // ========================================================================
  // Unified Actions - ENHANCED with Audit, Optimistic Updates, and Pagination
  // ========================================================================

  const actions = useMemo(
    () => ({
      // State actions
      setResolutionMode: state.actions.setResolutionMode,
      setSelectedClusterId: (id: string | null, isManualSelection?: boolean) =>
        workflow.handleSetSelectedClusterId(id, isManualSelection ?? true),
      setSelectedEdgeId: workflow.setSelectedEdgeIdAction,
      setReviewerId: state.actions.setReviewerId,
      setLastReviewedEdgeId: state.actions.setLastReviewedEdgeId,
      setIsReviewToolsMaximized: state.actions.setIsReviewToolsMaximized,
      setClusterFilterStatus: state.actions.setClusterFilterStatus,
      setDisconnectDependentServicesEnabled:
        state.actions.setDisconnectDependentServicesEnabled,
      setWorkflowFilter: state.actions.setWorkflowFilter,
      setIsAutoAdvanceEnabled: state.actions.setIsAutoAdvanceEnabled,

      // 🆕 NEW: Audit actions
      setAuditMode: state.actions.setAuditMode,
      setPostProcessingFilter: state.actions.setPostProcessingFilter,

      // Data actions
      enableDisconnectDependentServices: workflow.enableDisconnectDependentServices,
      triggerRefresh: state.actions.triggerRefresh,
      loadClusters: data.loadClusters,
      loadClusterProgress: data.loadClusterProgress,
      loadBulkNodeDetails: data.loadBulkNodeDetails,
      loadSingleConnectionData: data.loadSingleConnectionData,
      invalidateVisualizationData: data.invalidateVisualizationData,
      invalidateConnectionData: data.invalidateConnectionData,
      clearAllData: data.clearAllData,

      // 🆕 NEW: Audit data actions
      loadPostProcessingAuditData: data.loadPostProcessingAuditData,
      loadClustersWithAuditData: data.loadClustersWithAuditData,
      bulkMarkDecisionsReviewed: data.bulkMarkDecisionsReviewed,

      // Optimistic update actions
      updateEdgeStatusOptimistically: data.updateEdgeStatusOptimistically,
      updateClusterCompletionOptimistically:
        data.updateClusterCompletionOptimistically,

      // Workflow actions
      selectNextUnreviewedEdge: workflow.selectNextUnreviewedEdge,
      advanceToNextCluster: workflow.advanceToNextCluster,
      checkAndAdvanceIfComplete: workflow.checkAndAdvanceIfComplete,
      submitEdgeReview: workflow.submitEdgeReview,
      selectPreviousUnreviewedInCluster:
        workflow.selectPreviousUnreviewedInCluster,
      selectNextUnreviewedInCluster: workflow.selectNextUnreviewedInCluster,
      performThreeClusterCleanup: data.performThreeClusterCleanup,
      getCacheStats: data.getCacheStats,

      // 🆕 NEW/REFACTORED: Pagination actions
      initializeLargeClusterConnectionPaging:
        workflow.initializeLargeClusterConnectionPaging,
      loadMoreConnectionsForCluster: workflow.loadMoreConnectionsForCluster,

      // FIXED: Legacy pagination actions (for backward compatibility)
      viewNextConnectionPage,
      getActivelyPagingClusterId: () => state.activelyPagingClusterId,
      getLargeClusterConnectionsPage: () => state.largeClusterConnectionsPage,

      // 🆕 NEW: Audit workflow actions
      initializeAuditMode: workflow.initializeAuditMode,
      findNextAuditCluster: workflow.findNextAuditCluster,
    }),
    [state, data, workflow, viewNextConnectionPage]
  );

  // ========================================================================
  // Unified Queries - ENHANCED with Pagination - FIXED compatibility issues
  // ========================================================================

  const queries = useMemo(
    () => ({
      // Data queries
      isVisualizationDataLoaded: data.isVisualizationDataLoaded,
      isVisualizationDataLoading: data.isVisualizationDataLoading,
      isConnectionDataLoaded: data.isConnectionDataLoaded,
      isConnectionDataLoading: data.isConnectionDataLoading,
      getVisualizationError: data.getVisualizationError,
      getConnectionError: data.getConnectionError,
      getClusterById: data.getClusterById,
      getNodeDetail: data.getNodeDetail,

      // 🆕 NEW: Pagination query - FIXED to use data context method directly
      getClusterPaginationState: data.getClusterPaginationState,

      // 🆕 NEW: Audit data queries
      isPostProcessingAuditDataLoaded: data.isPostProcessingAuditDataLoaded,
      isPostProcessingAuditDataLoading: data.isPostProcessingAuditDataLoading,
      isClustersWithAuditDataLoaded: data.isClustersWithAuditDataLoaded,
      isClustersWithAuditDataLoading: data.isClustersWithAuditDataLoading,
      getPostProcessingAuditError: data.getPostProcessingAuditError,
      getClustersWithAuditError: data.getClustersWithAuditError,

      // Workflow queries
      getClusterProgress: workflow.getClusterProgress,
      getClusterProgressUnfiltered: workflow.getClusterProgressUnfiltered,
      getClusterProgressCrossSource: workflow.getClusterProgressCrossSource,
      canAdvanceToNextCluster: workflow.canAdvanceToNextCluster,
      isEdgeReviewed: workflow.isEdgeReviewed,
      getEdgeStatus: workflow.getEdgeStatus,
      getEdgeSubmissionStatus: workflow.getEdgeSubmissionStatus,

      // Filter-aware navigation helpers
      isClusterCompleteForCurrentFilter:
        workflow.isClusterCompleteForCurrentFilter,
      findNextViableCluster: workflow.findNextViableCluster,

      // 🆕 NEW: Audit workflow queries
      isInAuditMode: state.queries.isInAuditMode,
      getAuditFilterDisplayName: state.queries.getAuditFilterDisplayName,
      isClusterCompleteForAudit: workflow.isClusterCompleteForAudit,
      getAuditNavigationState: workflow.getAuditNavigationState,
    }),
    [data, workflow, state]
  );

  // ========================================================================
  // Unified Context Value
  // ========================================================================

  const contextValue: EntityResolutionContextType = {
    // Core state
    resolutionMode: state.resolutionMode,
    selectedClusterId: state.selectedClusterId,
    selectedEdgeId: state.selectedEdgeId,
    reviewerId: state.reviewerId,
    lastReviewedEdgeId: state.lastReviewedEdgeId,
    refreshTrigger: state.refreshTrigger,
    isAutoAdvanceEnabled: state.isAutoAdvanceEnabled,
    isReviewToolsMaximized: state.isReviewToolsMaximized,
    clusterFilterStatus: state.clusterFilterStatus,
    disconnectDependentServicesEnabled:
      state.disconnectDependentServicesEnabled,
    workflowFilter: state.workflowFilter,

    // 🆕 NEW: Audit state
    auditMode: state.auditMode,
    postProcessingFilter: state.postProcessingFilter,

    // Data state
    clusters: data.clusters,
    visualizationData: data.visualizationData,
    connectionData: data.connectionData,
    nodeDetails: data.nodeDetails,

    // 🆕 NEW: Audit data state
    postProcessingAuditData: data.postProcessingAuditData,
    clustersWithAuditData: data.clustersWithAuditData,

    // Workflow state
    clusterProgress: workflow.clusterProgress,
    overallProgress: workflow.overallProgress,
    edgeSelectionInfo: workflow.edgeSelectionInfo,

    // Current computed data
    currentVisualizationData: workflow.currentVisualizationData,
    currentConnectionData: workflow.currentConnectionData,
    selectedClusterDetails: workflow.selectedClusterDetails,

    // Paging state
    activelyPagingClusterId: state.activelyPagingClusterId,
    largeClusterConnectionsPage: state.largeClusterConnectionsPage,
    isLoadingConnectionPageData: state.isLoadingConnectionPageData,

    // Unified actions and queries
    actions,
    queries,
  };

  return (
    <EntityResolutionContext.Provider value={contextValue}>
      {children}
    </EntityResolutionContext.Provider>
  );
}

// ============================================================================
// Main Provider Component
// ============================================================================

export function EntityResolutionProvider({
  children,
}: {
  children: ReactNode;
}) {
  return (
    <EntityStateProvider>
      <EntityDataProvider>
        <EntityWorkflowProvider>
          <EntityResolutionConsumer>{children}</EntityResolutionConsumer>
        </EntityWorkflowProvider>
      </EntityDataProvider>
    </EntityStateProvider>
  );
}

// ============================================================================
// Main Hook - Backward Compatibility
// ============================================================================

export function useEntityResolution(): EntityResolutionContextType {
  const context = useContext(EntityResolutionContext);

  if (context === undefined) {
    throw new Error(
      "useEntityResolution must be used within an EntityResolutionProvider"
    );
  }

  return context;
}

// ============================================================================
// Migration Utility Hooks
// ============================================================================

/**
 * Hook for components migrating to use specific contexts
 * Returns all three context hooks for gradual migration
 */
export function useEntityResolutionMigration() {
  const state = useEntityState();
  const data = useEntityData();
  const workflow = useEntityWorkflow();
  const unified = useEntityResolution();

  return {
    state,
    data,
    workflow,
    unified,
  };
}

/**
 * Utility to help identify which context a component should use
 */
export function analyzeComponentUsage(componentName: string) {
  console.group(`🔍 [Migration] Analyzing component: ${componentName}`);

  const recommendations = {
    useEntityState: [],
    useEntityData: [],
    useEntityWorkflow: [],
    keepUnified: [],
  };

  // This would be enhanced with actual usage analysis
  console.log("Run this in your component to see what it uses:");
  console.log(`
    const context = useEntityResolution();
    console.log("Used properties:", Object.keys(context).filter(key => 
      // Add logic to track which properties are actually used
    ));
  `);

  console.groupEnd();
  return recommendations;
}

// ============================================================================
// Development Utilities
// ============================================================================

/**
 * Hook for debugging the three-context architecture
 */
export function useEntityResolutionDebug() {
  const state = useEntityState();
  const data = useEntityData();
  const workflow = useEntityWorkflow();

  // Import the specific debug hook for state context
  const stateDebug = useEntityStateDebug();

  const logAllContexts = useCallback(() => {
    console.group("🐛 [EntityResolution] All Context States");

    console.group("📊 State Context");
    console.log("Selection:", {
      clusterId: state.selectedClusterId,
      edgeId: state.selectedEdgeId,
      mode: state.resolutionMode,
    });
    console.log("Filters:", {
      workflow: state.workflowFilter,
      cluster: state.clusterFilterStatus,
    });
    console.log("UI:", {
      autoAdvance: state.isAutoAdvanceEnabled,
      toolsMaximized: state.isReviewToolsMaximized,
    });
    console.log("Audit Mode:", {
      mode: state.auditMode,
      filter: state.postProcessingFilter,
    });
    console.groupEnd();

    console.group("💾 Data Context");
    console.log("Clusters:", {
      count: data.clusters.data.length,
      loading: data.clusters.loading,
      page: data.clusters.page,
    });
    console.log("Cache:", {
      visualizations: Object.keys(data.visualizationData).length,
      connections: Object.keys(data.connectionData).length,
      nodes: Object.keys(data.nodeDetails).length,
    });
    console.log("Audit Data:", {
      decisionsLoaded: !!data.postProcessingAuditData.data,
      clustersWithAuditLoaded: !!data.clustersWithAuditData.data,
    });
    console.groupEnd();

    console.group("⚡ Workflow Context");
    console.log("Progress:", {
      overallPercent: workflow.overallProgress.overallProgressPercentage,
      clustersComplete: workflow.overallProgress.clustersComplete,
      totalClusters: workflow.overallProgress.totalClusters,
    });
    console.log("Edge Selection:", {
      current: workflow.edgeSelectionInfo.currentEdgeId,
      hasUnreviewed: workflow.edgeSelectionInfo.hasUnreviewedEdges,
      totalInView: workflow.edgeSelectionInfo.totalEdgesInEntireCluster,
    });
    console.groupEnd();

    console.groupEnd();
  }, [state, data, workflow]);

  const validateContextIntegrity = useCallback(() => {
    const issues = [];

    // Check for state consistency
    if (
      state.selectedClusterId &&
      !data.clusters.data.find((c: EntityCluster) => c.id === state.selectedClusterId)
    ) {
      if (state.auditMode !== "post_processing_audit") {
        // In audit mode, cluster may not be in the main list
        issues.push("Selected cluster not found in clusters data");
      }
    }

    if (state.selectedEdgeId && !data.connectionData[state.selectedEdgeId]) {
      issues.push("Selected edge not found in connection data");
    }

    // Check for data consistency
    if (
      state.selectedClusterId &&
      data.visualizationData[state.selectedClusterId]?.data
    ) {
      const vizData = data.visualizationData[state.selectedClusterId].data;
      if (
        state.selectedEdgeId &&
        vizData &&
        'links' in vizData &&
        !vizData.links.find((l: any) => l.id === state.selectedEdgeId)
      ) {
        issues.push("Selected edge not found in visualization data");
      }
    }

    if (issues.length > 0) {
      console.group("⚠️ [EntityResolution] Context Integrity Issues");
      issues.forEach((issue) => console.warn(issue));
      console.groupEnd();
    } else {
      console.log("✅ [EntityResolution] Context integrity validated");
    }

    return issues;
  }, [state, data]);

  return {
    logAllContexts,
    validateContextIntegrity,
    logStateContext: stateDebug.logCurrentState,
  };
}

// ============================================================================
// Performance Monitoring
// ============================================================================

/**
 * Hook to monitor performance of the three-context architecture
 */
export function useEntityResolutionPerformance() {
  const startTime = useMemo(() => Date.now(), []);

  const measureRenderTime = useCallback(
    (componentName: string) => {
      const renderTime = Date.now() - startTime;
      if (renderTime > 100) {
        // Log slow renders
        console.warn(
          `🐌 [Performance] Slow render in ${componentName}: ${renderTime}ms`
        );
      }
    },
    [startTime]
  );

  return { measureRenderTime };
}

// ============================================================================
// Export All Context Hooks for Specific Usage
// ============================================================================

// Re-export specific context hooks for components that want to migrate
export {
  useEntityState,
  useEntitySelection,
  useEntityFilters,
  useEntityUI,
  useEntityStateDebug,
} from "./entity-state-context";

export {
  useEntityData,
  useClusterData,
  useVisualizationData,
  useConnectionData,
} from "./entity-data-context";

export {
  useEntityWorkflow,
  useEntityProgress,
  useEdgeSelection,
  useEntityReview,
  useCurrentEntityData,
} from "./entity-workflow-context";

// ============================================================================
// Migration Guide Comment
// ============================================================================

/*
MIGRATION GUIDE:

✅ FIXES APPLIED:
1. FIXED: Type consistency - visualization data now always uses VisualizationData internally
2. FIXED: Removed VisualizationDataResponse from internal state types
3. FIXED: Updated interface to match actual implementation
4. FIXED: All TypeScript compilation errors resolved

🔧 TECHNICAL DETAILS:
- The data context always stores the accumulated format (VisualizationData), not API response format
- Components can safely use array methods on viz.data.links since it's always BaseLink[]
- The pagination properties are stored separately on VisualizationState
- All type names corrected to match the actual exported types

Phase 1: Ensure the refactored context works (COMPLETED ✅)
✅ Keep existing useEntityResolution() calls working
✅ No breaking changes to components
✅ Test all existing functionality
✅ FIXED: All TypeScript compilation errors resolved
✅ FIXED: Restored optimistic updates for immediate UI feedback
✨ NEW: Added support for Post-Processing Audit System
✨ NEW: Added cursor-based pagination support for large cluster connections

Phase 2: Identify component usage patterns
🔄 Use analyzeComponentUsage() to understand what each component needs
🔄 Categorize components by their usage patterns
🔄 Create migration plan for each component

Phase 3: Migrate components gradually
📋 Start with simple components that only need one context
📋 Use specific hooks (useEntityState, useEntityData, useEntityWorkflow)
📋 Update components one by one
📋 Test thoroughly after each migration

Phase 4: Clean up
🧹 Remove backward compatibility layer once all components migrated
🧹 Remove unused imports and dependencies
🧹 Optimize provider structure if needed
*/
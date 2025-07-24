/*
================================================================================
|
|   File: /context/entity-resolution-context.tsx - Phase 4: Unified Interface
|
|   Description: Refactored Entity Resolution Context using the three-context architecture
|   - Provides backward compatibility with existing components
|   - Combines EntityStateContext, EntityDataContext, and EntityWorkflowContext
|   - Maintains the same interface as the original monolithic context
|   - Enables gradual migration of components to use specific contexts
|   - 🔧 FIXED: Restored optimistic updates through unified actions interface
|
================================================================================
*/
"use client";

import {
  ResolutionMode,
  EntityCluster,
  EntityVisualizationDataResponse,
  EntityConnectionDataResponse,
  GroupReviewDecision,
  BaseLink,
  NodeDetailResponse,
  NodeIdentifier,
  ClusterReviewProgress,
  ClusterFilterStatus,
  WorkflowFilter,
} from "@/types/entity-resolution";
import {
  createContext,
  useCallback,
  useContext,
  useMemo,
  type ReactNode,
} from "react";
import { useEntityState, useEntityStateDebug, EntityStateProvider } from "./entity-state-context";
import { useEntityData, EntityDataProvider } from "./entity-data-context";
import { useEntityWorkflow, EntityWorkflowProvider } from "./entity-workflow-context";

// ============================================================================
// Backward Compatibility Interface - ENHANCED with Optimistic Updates
// ============================================================================

// This interface maintains compatibility with the original EntityResolutionContextType
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

  // Data state (from EntityDataContext)
  clusters: {
    data: EntityCluster[];
    total: number;
    page: number;
    limit: number;
    loading: boolean;
    error: string | null;
  };
  visualizationData: Record<string, {
    data: EntityVisualizationDataResponse | null;
    loading: boolean;
    error: string | null;
    lastUpdated: number | null;
  }>;
  connectionData: Record<string, {
    data: EntityConnectionDataResponse | null;
    loading: boolean;
    error: string | null;
    lastUpdated: number | null;
  }>;
  nodeDetails: Record<string, NodeDetailResponse | null | "loading" | "error">;

  // Workflow state (from EntityWorkflowContext)
  clusterProgress: Record<string, ClusterReviewProgress>;
  overallProgress: {
    totalPendingDecisions: number;
    totalCompletedDecisions: number;
    overallProgressPercentage: number;
    clustersComplete: number;
    totalClusters: number;
  };
  edgeSelectionInfo: {
    currentEdgeId: string | null;
    nextUnreviewedEdgeId: string | null;
    hasUnreviewedEdges: boolean;
    currentEdgeIndex: number;
    totalEdgesInView: number;
    totalUnreviewedEdgesInCluster: number;
    currentUnreviewedEdgeIndexInCluster: number;
    totalEdgesInEntireCluster: number;
  };

  // Current computed data
  currentVisualizationData: EntityVisualizationDataResponse | null;
  currentConnectionData: EntityConnectionDataResponse | null;
  selectedClusterDetails: EntityCluster | null;

  // Paging state
  activelyPagingClusterId: string | null;
  largeClusterConnectionsPage: number;
  isLoadingConnectionPageData: boolean;

  // Actions - unified interface from all three contexts (ENHANCED)
  actions: {
    // State actions
    setResolutionMode: (mode: ResolutionMode) => void;
    setSelectedClusterId: (id: string | null, isManualSelection?: boolean) => Promise<void>;
    setSelectedEdgeId: (id: string | null) => void;
    setReviewerId: (id: string) => void;
    setLastReviewedEdgeId: (id: string | null) => void;
    setIsReviewToolsMaximized: (isMaximized: boolean) => void;
    setClusterFilterStatus: (status: ClusterFilterStatus) => void;
    setDisconnectDependentServicesEnabled: (enabled: boolean) => void;
    setWorkflowFilter: (filter: WorkflowFilter) => void;
    setIsAutoAdvanceEnabled: (enabled: boolean) => void;

    // Data actions
    enableDisconnectDependentServices: () => Promise<void>;
    triggerRefresh: (target?: "all" | "clusters" | "current_visualization" | "current_connection") => void;
    loadClusters: (page: number, limit?: number) => Promise<void>;
    loadClusterProgress: (page: number, limit?: number) => Promise<void>;
    loadBulkNodeDetails: (nodesToFetch: NodeIdentifier[]) => Promise<void>;
    loadSingleConnectionData: (edgeId: string) => Promise<EntityConnectionDataResponse | null>;
    invalidateVisualizationData: (clusterId: string) => Promise<void>;
    invalidateConnectionData: (edgeId: string) => Promise<void>;
    clearAllData: () => void;

    // 🔧 NEW: Optimistic update actions
    updateEdgeStatusOptimistically: (
      clusterId: string, 
      edgeId: string, 
      newStatus: BaseLink["status"],
      wasReviewed?: boolean
    ) => () => void; // Returns revert function
    updateClusterCompletionOptimistically: (
      clusterId: string, 
      wasReviewed: boolean
    ) => () => void; // Returns revert function

    // Workflow actions
    selectNextUnreviewedEdge: (afterEdgeId?: string | null) => void;
    advanceToNextCluster: () => Promise<void>;
    checkAndAdvanceIfComplete: (clusterIdToCheck?: string) => Promise<void>;
    submitEdgeReview: (edgeId: string, decision: GroupReviewDecision, notes?: string) => Promise<void>;
    selectPreviousUnreviewedInCluster: () => void;
    selectNextUnreviewedInCluster: () => void;
    initializeLargeClusterConnectionPaging: (clusterId: string) => Promise<void>;
    viewNextConnectionPage: (clusterId: string) => Promise<void>;
    getActivelyPagingClusterId: () => string | null;
    getLargeClusterConnectionsPage: () => number;
    performThreeClusterCleanup: () => void;
    getCacheStats: () => any;
  };

  // Queries - unified interface from all three contexts
  queries: {
    // Data queries
    isVisualizationDataLoaded: (clusterId: string) => boolean;
    isVisualizationDataLoading: (clusterId: string) => boolean;
    isConnectionDataLoaded: (edgeId: string) => boolean;
    isConnectionDataLoading: (edgeId: string) => boolean;
    getVisualizationError: (clusterId: string) => string | null;
    getConnectionError: (edgeId: string) => string | null;
    getClusterById: (clusterId: string) => EntityCluster | undefined;
    getNodeDetail: (nodeId: string) => NodeDetailResponse | null | "loading" | "error";

    // Workflow queries
    getClusterProgress: (clusterId: string) => ClusterReviewProgress;
    getClusterProgressUnfiltered: (clusterId: string) => ClusterReviewProgress;
    getClusterProgressCrossSource: (clusterId: string) => ClusterReviewProgress;
    canAdvanceToNextCluster: () => boolean;
    isEdgeReviewed: (edgeId: string) => boolean;
    getEdgeStatus: (edgeId: string) => BaseLink["status"] | null;
    getEdgeSubmissionStatus: (edgeId: string) => { isSubmitting: boolean; error: string | null };

    // 🔧 NEW: Filter-aware navigation helpers
    isClusterCompleteForCurrentFilter: (clusterId: string) => boolean;
    findNextViableCluster: (fromClusterId?: string) => EntityCluster | "next_page" | null;
  };
}

// ============================================================================
// Context Creation
// ============================================================================

const EntityResolutionContext = createContext<EntityResolutionContextType | undefined>(
  undefined
);

// ============================================================================
// Internal Consumer Component
// ============================================================================

// This component consumes all three contexts and provides the unified interface
function EntityResolutionConsumer({ children }: { children: ReactNode }) {
  const state = useEntityState();
  const data = useEntityData();
  const workflow = useEntityWorkflow();

  // ========================================================================
  // Unified Actions - ENHANCED with Optimistic Updates
  // ========================================================================

const actions = useMemo(() => ({
  // State actions
  setResolutionMode: state.actions.setResolutionMode,
  setSelectedClusterId: (id: string | null, isManualSelection?: boolean) => 
    workflow.handleSetSelectedClusterId(id, isManualSelection ?? true),
  setSelectedEdgeId: workflow.setSelectedEdgeIdAction,
  setReviewerId: state.actions.setReviewerId,
  setLastReviewedEdgeId: state.actions.setLastReviewedEdgeId,
  setIsReviewToolsMaximized: state.actions.setIsReviewToolsMaximized,
  setClusterFilterStatus: state.actions.setClusterFilterStatus,
  setDisconnectDependentServicesEnabled: state.actions.setDisconnectDependentServicesEnabled,
  setWorkflowFilter: state.actions.setWorkflowFilter,
  setIsAutoAdvanceEnabled: state.actions.setIsAutoAdvanceEnabled,

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

  // Optimistic update actions
  updateEdgeStatusOptimistically: data.updateEdgeStatusOptimistically,
  updateClusterCompletionOptimistically: data.updateClusterCompletionOptimistically,

  // Workflow actions
  selectNextUnreviewedEdge: workflow.selectNextUnreviewedEdge,
  advanceToNextCluster: workflow.advanceToNextCluster,
  checkAndAdvanceIfComplete: workflow.checkAndAdvanceIfComplete,
  submitEdgeReview: workflow.submitEdgeReview,
  selectPreviousUnreviewedInCluster: workflow.selectPreviousUnreviewedInCluster,
  selectNextUnreviewedInCluster: workflow.selectNextUnreviewedInCluster,
  initializeLargeClusterConnectionPaging: workflow.initializeLargeClusterConnectionPaging,
  viewNextConnectionPage: workflow.viewNextConnectionPage,
  getActivelyPagingClusterId: () => state.activelyPagingClusterId,
  getLargeClusterConnectionsPage: () => state.largeClusterConnectionsPage,
  performThreeClusterCleanup: data.performThreeClusterCleanup,
  getCacheStats: data.getCacheStats,
}), [state, data, workflow]);

  // ========================================================================
  // Unified Queries
  // ========================================================================

  const queries = useMemo(() => ({
    // Data queries
    isVisualizationDataLoaded: data.isVisualizationDataLoaded,
    isVisualizationDataLoading: data.isVisualizationDataLoading,
    isConnectionDataLoaded: data.isConnectionDataLoaded,
    isConnectionDataLoading: data.isConnectionDataLoading,
    getVisualizationError: data.getVisualizationError,
    getConnectionError: data.getConnectionError,
    getClusterById: data.getClusterById,
    getNodeDetail: data.getNodeDetail,

    // Workflow queries
    getClusterProgress: workflow.getClusterProgress,
    getClusterProgressUnfiltered: workflow.getClusterProgressUnfiltered,
    getClusterProgressCrossSource: workflow.getClusterProgressCrossSource,
    canAdvanceToNextCluster: workflow.canAdvanceToNextCluster,
    isEdgeReviewed: workflow.isEdgeReviewed,
    getEdgeStatus: workflow.getEdgeStatus,
    getEdgeSubmissionStatus: workflow.getEdgeSubmissionStatus,

    // 🔧 NEW: Filter-aware navigation helpers
    isClusterCompleteForCurrentFilter: workflow.isClusterCompleteForCurrentFilter,
    findNextViableCluster: workflow.findNextViableCluster,
  }), [data, workflow]);

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
    disconnectDependentServicesEnabled: state.disconnectDependentServicesEnabled,
    workflowFilter: state.workflowFilter,

    // Data state
    clusters: data.clusters,
    visualizationData: data.visualizationData,
    connectionData: data.connectionData,
    nodeDetails: data.nodeDetails,

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

export function EntityResolutionProvider({ children }: { children: ReactNode }) {
  return (
    <EntityStateProvider>
      <EntityDataProvider>
        <EntityWorkflowProvider>
          <EntityResolutionConsumer>
            {children}
          </EntityResolutionConsumer>
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
      totalInView: workflow.edgeSelectionInfo.totalEdgesInView,
    });
    console.groupEnd();

    console.groupEnd();
  }, [state, data, workflow]);

  const validateContextIntegrity = useCallback(() => {
    const issues = [];

    // Check for state consistency
    if (state.selectedClusterId && !data.clusters.data.find(c => c.id === state.selectedClusterId)) {
      issues.push("Selected cluster not found in clusters data");
    }

    if (state.selectedEdgeId && !data.connectionData[state.selectedEdgeId]) {
      issues.push("Selected edge not found in connection data");
    }

    // Check for data consistency
    if (state.selectedClusterId && data.visualizationData[state.selectedClusterId]?.data) {
      const vizData = data.visualizationData[state.selectedClusterId].data;
      if (state.selectedEdgeId && !vizData?.links.find(l => l.id === state.selectedEdgeId)) {
        issues.push("Selected edge not found in visualization data");
      }
    }

    if (issues.length > 0) {
      console.group("⚠️ [EntityResolution] Context Integrity Issues");
      issues.forEach(issue => console.warn(issue));
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

  const measureRenderTime = useCallback((componentName: string) => {
    const renderTime = Date.now() - startTime;
    if (renderTime > 100) { // Log slow renders
      console.warn(`🐌 [Performance] Slow render in ${componentName}: ${renderTime}ms`);
    }
  }, [startTime]);

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

Phase 1: Ensure the refactored context works (CURRENT)
✅ Keep existing useEntityResolution() calls working
✅ No breaking changes to components
✅ Test all existing functionality
✅ FIXED: Restored optimistic updates for immediate UI feedback

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

Example Migration:
// Before
const { selectedClusterId, setSelectedClusterId } = useEntityResolution();

// After (if component only needs selection)
const { selectedClusterId, setSelectedClusterId } = useEntitySelection();

// Or (if component needs multiple state pieces)
const { selectedClusterId } = useEntityState();
const { setSelectedClusterId } = useEntityWorkflow(); // enhanced version

🔧 OPTIMISTIC UPDATES RESTORED:
The refactored context now properly supports optimistic updates through:
- updateEdgeStatusOptimistically() - immediately updates UI on review submission
- updateClusterCompletionOptimistically() - immediately updates cluster status
- Enhanced submitEdgeReview() - applies optimistic updates and reverts on error
- Proper error handling with revert functions
*/
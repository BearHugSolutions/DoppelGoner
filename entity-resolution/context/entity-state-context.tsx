/*
================================================================================
|
|   File: /context/entity-state-context.tsx - Phase 1: Core State Management
|
|   Description: Foundation context for Entity Resolution state management
|   - Manages core selections (cluster, edge, mode)
|   - Handles filters and preferences
|   - Controls UI state and configuration
|   - Provides foundation for EntityDataContext and EntityWorkflowContext
|   - Clean, focused state management with clear responsibilities
|   - 🔧 FIXED: Workflow filter changes no longer trigger unnecessary data clearing
|
================================================================================
*/
"use client";

import {
  ResolutionMode,
  ClusterFilterStatus,
  WorkflowFilter,
} from "@/types/entity-resolution";
import {
  createContext,
  useCallback,
  useContext,
  useState,
  useEffect,
  useMemo,
  type ReactNode,
} from "react";
import { useAuth } from "./auth-context";

// ============================================================================
// Types and Interfaces
// ============================================================================

export interface EntityStateContextType {
  // Core selection state
  selectedClusterId: string | null;
  selectedEdgeId: string | null;
  resolutionMode: ResolutionMode;
  reviewerId: string;
  lastReviewedEdgeId: string | null;

  // Filter and preference state
  workflowFilter: WorkflowFilter;
  clusterFilterStatus: ClusterFilterStatus;
  disconnectDependentServicesEnabled: boolean;

  // UI configuration state
  isAutoAdvanceEnabled: boolean;
  isReviewToolsMaximized: boolean;

  // Paging state for large clusters
  activelyPagingClusterId: string | null;
  largeClusterConnectionsPage: number;
  isLoadingConnectionPageData: boolean;

  // Utility state
  refreshTrigger: number;

  // Core actions
  actions: {
    // Selection actions
    setSelectedClusterId: (id: string | null) => void;
    setSelectedEdgeId: (id: string | null) => void;
    setResolutionMode: (mode: ResolutionMode) => void;
    setReviewerId: (id: string) => void;
    setLastReviewedEdgeId: (id: string | null) => void;

    // Filter and preference actions
    setWorkflowFilter: (filter: WorkflowFilter) => void;
    setClusterFilterStatus: (status: ClusterFilterStatus) => void;
    setDisconnectDependentServicesEnabled: (enabled: boolean) => void;

    // UI configuration actions
    setIsAutoAdvanceEnabled: (enabled: boolean) => void;
    setIsReviewToolsMaximized: (isMaximized: boolean) => void;

    // Paging actions
    setActivelyPagingClusterId: (id: string | null) => void;
    setLargeClusterConnectionsPage: (page: number) => void;
    setIsLoadingConnectionPageData: (loading: boolean) => void;

    // Utility actions
    triggerRefresh: (target?: "all" | "clusters" | "current_visualization" | "current_connection") => void;
    clearSelections: () => void;
    resetUIState: () => void;
  };

  // Queries for derived state
  queries: {
    hasValidSelection: () => boolean;
    isLargeClusterPaging: () => boolean;
    getCurrentPageInfo: () => { clusterId: string | null; page: number };
  };
}

// ============================================================================
// Context Creation
// ============================================================================

const EntityStateContext = createContext<EntityStateContextType | undefined>(
  undefined
);

// ============================================================================
// Default Values
// ============================================================================

const DEFAULT_STATE = {
  selectedClusterId: null as string | null,
  selectedEdgeId: null as string | null,
  resolutionMode: "entity" as ResolutionMode,
  reviewerId: "default-reviewer",
  lastReviewedEdgeId: null as string | null,
  workflowFilter: "all" as WorkflowFilter,
  clusterFilterStatus: "unreviewed" as ClusterFilterStatus,
  disconnectDependentServicesEnabled: false,
  isAutoAdvanceEnabled: true,
  isReviewToolsMaximized: false,
  activelyPagingClusterId: null as string | null,
  largeClusterConnectionsPage: 0,
  isLoadingConnectionPageData: false,
  refreshTrigger: 0,
};

// ============================================================================
// Provider Component
// ============================================================================

export function EntityStateProvider({ children }: { children: ReactNode }) {
  const { user } = useAuth();

  // ========================================================================
  // Core State
  // ========================================================================

  const [selectedClusterId, setSelectedClusterIdState] = useState<string | null>(
    DEFAULT_STATE.selectedClusterId
  );
  const [selectedEdgeId, setSelectedEdgeIdState] = useState<string | null>(
    DEFAULT_STATE.selectedEdgeId
  );
  const [resolutionMode, setResolutionModeState] = useState<ResolutionMode>(
    DEFAULT_STATE.resolutionMode
  );
  const [reviewerId, setReviewerIdState] = useState<string>(
    DEFAULT_STATE.reviewerId
  );
  const [lastReviewedEdgeId, setLastReviewedEdgeIdState] = useState<string | null>(
    DEFAULT_STATE.lastReviewedEdgeId
  );

  // ========================================================================
  // Filter and Preference State
  // ========================================================================

  const [workflowFilter, setWorkflowFilterState] = useState<WorkflowFilter>(
    DEFAULT_STATE.workflowFilter
  );
  const [clusterFilterStatus, setClusterFilterStatusState] = useState<ClusterFilterStatus>(
    DEFAULT_STATE.clusterFilterStatus
  );
  const [disconnectDependentServicesEnabled, setDisconnectDependentServicesEnabledState] = useState<boolean>(
    DEFAULT_STATE.disconnectDependentServicesEnabled
  );

  // ========================================================================
  // UI Configuration State
  // ========================================================================

  const [isAutoAdvanceEnabled, setIsAutoAdvanceEnabledState] = useState<boolean>(
    DEFAULT_STATE.isAutoAdvanceEnabled
  );
  const [isReviewToolsMaximized, setIsReviewToolsMaximizedState] = useState<boolean>(
    DEFAULT_STATE.isReviewToolsMaximized
  );

  // ========================================================================
  // Paging State
  // ========================================================================

  const [activelyPagingClusterId, setActivelyPagingClusterIdState] = useState<string | null>(
    DEFAULT_STATE.activelyPagingClusterId
  );
  const [largeClusterConnectionsPage, setLargeClusterConnectionsPageState] = useState<number>(
    DEFAULT_STATE.largeClusterConnectionsPage
  );
  const [isLoadingConnectionPageData, setIsLoadingConnectionPageDataState] = useState<boolean>(
    DEFAULT_STATE.isLoadingConnectionPageData
  );

  // ========================================================================
  // Utility State
  // ========================================================================

  const [refreshTrigger, setRefreshTriggerState] = useState<number>(
    DEFAULT_STATE.refreshTrigger
  );

  // ========================================================================
  // Smart Actions with Business Logic
  // ========================================================================

  const setResolutionMode = useCallback((mode: ResolutionMode) => {
    if (mode === resolutionMode) return;
    
    console.log(`🔄 [EntityState] Switching resolution mode from ${resolutionMode} to ${mode}`);
    
    // Clear all selections when mode changes
    setSelectedClusterIdState(null);
    setSelectedEdgeIdState(null);
    setLastReviewedEdgeIdState(null);
    setActivelyPagingClusterIdState(null);
    setLargeClusterConnectionsPageState(0);
    setIsLoadingConnectionPageDataState(false);
    
    // Reset UI state to defaults
    setIsAutoAdvanceEnabledState(true);
    setIsReviewToolsMaximizedState(false);
    
    // Update mode
    setResolutionModeState(mode);
    
    // Trigger refresh to reload data for new mode
    setRefreshTriggerState(prev => prev + 1);
  }, [resolutionMode]);

  const setSelectedClusterId = useCallback((id: string | null) => {
    if (id === selectedClusterId) return;
    
    console.log(`🎯 [EntityState] Setting selected cluster: ${id}`);
    
    // Clear edge selection when cluster changes
    setSelectedEdgeIdState(null);
    setLastReviewedEdgeIdState(null);
    
    // Clear paging state if not selecting the actively paging cluster
    if (id !== activelyPagingClusterId) {
      setActivelyPagingClusterIdState(null);
      setLargeClusterConnectionsPageState(0);
      setIsLoadingConnectionPageDataState(false);
    }
    
    setSelectedClusterIdState(id);
  }, [selectedClusterId, activelyPagingClusterId]);

  const setSelectedEdgeId = useCallback((id: string | null) => {
    if (id === selectedEdgeId) return;
    
    console.log(`🔗 [EntityState] Setting selected edge: ${id}`);
    setSelectedEdgeIdState(id);
  }, [selectedEdgeId]);

  const setWorkflowFilter = useCallback((filter: WorkflowFilter) => {
    if (filter === workflowFilter) return;
    
    console.log(`🔍 [EntityState] Setting workflow filter: ${filter} - will validate current selection`);
    setWorkflowFilterState(filter);
    
    // 🔧 NEW: Don't trigger full refresh, but do increment trigger for edge validation
    // This allows EntityWorkflowContext to detect the filter change and validate current edge
    setRefreshTriggerState(prev => prev + 1);
  }, [workflowFilter]);

  const setClusterFilterStatus = useCallback((status: ClusterFilterStatus) => {
    if (status === clusterFilterStatus) return;
    
    console.log(`📊 [EntityState] Setting cluster filter status: ${status}`);
    setClusterFilterStatusState(status);
    
    // Trigger refresh to reload clusters with new filter
    setRefreshTriggerState(prev => prev + 1);
  }, [clusterFilterStatus]);

  const triggerRefresh = useCallback((
    target: "all" | "clusters" | "current_visualization" | "current_connection" = "all"
  ) => {
    console.log(`🔄 [EntityState] Triggering refresh for target: ${target}`);
    
    if (target === "all" || target === "clusters") {
      // Reset paging state on cluster refresh
      setActivelyPagingClusterIdState(null);
      setLargeClusterConnectionsPageState(0);
      setIsLoadingConnectionPageDataState(false);
    }
    
    setRefreshTriggerState(prev => prev + 1);
  }, []);

  const clearSelections = useCallback(() => {
    console.log("🧹 [EntityState] Clearing all selections");
    setSelectedClusterIdState(null);
    setSelectedEdgeIdState(null);
    setLastReviewedEdgeIdState(null);
    setActivelyPagingClusterIdState(null);
    setLargeClusterConnectionsPageState(0);
    setIsLoadingConnectionPageDataState(false);
  }, []);

  const resetUIState = useCallback(() => {
    console.log("🔄 [EntityState] Resetting UI state to defaults");
    setIsAutoAdvanceEnabledState(true);
    setIsReviewToolsMaximizedState(false);
    setDisconnectDependentServicesEnabledState(false);
  }, []);

  // ========================================================================
  // Effects
  // ========================================================================

  // Set reviewer ID from authenticated user
  useEffect(() => {
    if (user?.id && user.id !== reviewerId) {
      console.log(`👤 [EntityState] Setting reviewer ID from user: ${user.id}`);
      setReviewerIdState(user.id);
    }
  }, [user?.id, reviewerId]);

  // ========================================================================
  // Memoized Actions Object
  // ========================================================================

  const actions = useMemo(() => ({
    // Selection actions
    setSelectedClusterId,
    setSelectedEdgeId,
    setResolutionMode,
    setReviewerId: setReviewerIdState,
    setLastReviewedEdgeId: setLastReviewedEdgeIdState,

    // Filter and preference actions
    setWorkflowFilter,
    setClusterFilterStatus,
    setDisconnectDependentServicesEnabled: setDisconnectDependentServicesEnabledState,

    // UI configuration actions
    setIsAutoAdvanceEnabled: setIsAutoAdvanceEnabledState,
    setIsReviewToolsMaximized: setIsReviewToolsMaximizedState,

    // Paging actions
    setActivelyPagingClusterId: setActivelyPagingClusterIdState,
    setLargeClusterConnectionsPage: setLargeClusterConnectionsPageState,
    setIsLoadingConnectionPageData: setIsLoadingConnectionPageDataState,

    // Utility actions
    triggerRefresh,
    clearSelections,
    resetUIState,
  }), [
    setSelectedClusterId,
    setSelectedEdgeId,
    setResolutionMode,
    setWorkflowFilter,
    setClusterFilterStatus,
    triggerRefresh,
    clearSelections,
    resetUIState,
  ]);

  // ========================================================================
  // Memoized Queries Object
  // ========================================================================

  const queries = useMemo(() => ({
    hasValidSelection: () => selectedClusterId !== null,
    
    isLargeClusterPaging: () => 
      activelyPagingClusterId !== null && 
      activelyPagingClusterId === selectedClusterId,
    
    getCurrentPageInfo: () => ({
      clusterId: activelyPagingClusterId,
      page: largeClusterConnectionsPage,
    }),
  }), [selectedClusterId, activelyPagingClusterId, largeClusterConnectionsPage]);

  // ========================================================================
  // Context Value
  // ========================================================================

  const contextValue: EntityStateContextType = {
    // Core selection state
    selectedClusterId,
    selectedEdgeId,
    resolutionMode,
    reviewerId,
    lastReviewedEdgeId,

    // Filter and preference state
    workflowFilter,
    clusterFilterStatus,
    disconnectDependentServicesEnabled,

    // UI configuration state
    isAutoAdvanceEnabled,
    isReviewToolsMaximized,

    // Paging state
    activelyPagingClusterId,
    largeClusterConnectionsPage,
    isLoadingConnectionPageData,

    // Utility state
    refreshTrigger,

    // Actions and queries
    actions,
    queries,
  };

  return (
    <EntityStateContext.Provider value={contextValue}>
      {children}
    </EntityStateContext.Provider>
  );
}

// ============================================================================
// Hook for Consuming Context
// ============================================================================

export function useEntityState(): EntityStateContextType {
  const context = useContext(EntityStateContext);
  
  if (context === undefined) {
    throw new Error(
      "useEntityState must be used within an EntityStateProvider"
    );
  }
  
  return context;
}

// ============================================================================
// Utility Hooks for Common Patterns
// ============================================================================

/**
 * Hook for components that only need selection state
 */
export function useEntitySelection() {
  const { 
    selectedClusterId, 
    selectedEdgeId, 
    actions: { setSelectedClusterId, setSelectedEdgeId }
  } = useEntityState();
  
  return {
    selectedClusterId,
    selectedEdgeId,
    setSelectedClusterId,
    setSelectedEdgeId,
  };
}

/**
 * Hook for components that only need filter state
 */
export function useEntityFilters() {
  const {
    workflowFilter,
    clusterFilterStatus,
    resolutionMode,
    actions: { setWorkflowFilter, setClusterFilterStatus, setResolutionMode }
  } = useEntityState();
  
  return {
    workflowFilter,
    clusterFilterStatus,
    resolutionMode,
    setWorkflowFilter,
    setClusterFilterStatus,
    setResolutionMode,
  };
}

/**
 * Hook for components that only need UI state
 */
export function useEntityUI() {
  const {
    isAutoAdvanceEnabled,
    isReviewToolsMaximized,
    actions: { setIsAutoAdvanceEnabled, setIsReviewToolsMaximized }
  } = useEntityState();
  
  return {
    isAutoAdvanceEnabled,
    isReviewToolsMaximized,
    setIsAutoAdvanceEnabled,
    setIsReviewToolsMaximized,
  };
}

/**
 * Hook for debugging and development
 */
export function useEntityStateDebug() {
  const state = useEntityState();
  
  const logCurrentState = useCallback(() => {
    console.group("🐛 [EntityState] Current State");
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
    console.log("Paging:", {
      activeCluster: state.activelyPagingClusterId,
      page: state.largeClusterConnectionsPage,
      loading: state.isLoadingConnectionPageData,
    });
    console.groupEnd();
  }, [state]);
  
  return { logCurrentState };
}
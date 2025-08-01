/*
================================================================================
|
|   File: /context/entity-state-context.tsx - ENHANCED: Complete Data Clearing
|
|   Description: Enhanced state management with comprehensive data clearing
|   - Ensures complete data clearing on ALL mode switches
|   - Consistent clearing behavior for workflowFilter, resolutionMode, auditMode
|   - Eliminates stale data across all interface switches
|   - 🔧 ENHANCED: setWorkflowFilter now clears ALL data like other switches
|   - 🔧 ENHANCED: All switches call clearAllData explicitly
|   - 🔧 FIX: Optimized selection setters to prevent unnecessary re-renders
|
================================================================================
*/
"use client";

import {
  ResolutionMode,
  ClusterFilterStatus,
  WorkflowFilter,
} from "@/types/entity-resolution";
import type { AuditMode } from "@/types/post-processing";
import {
  createContext,
  useCallback,
  useContext,
  useState,
  useEffect,
  useMemo,
  useRef,
  type ReactNode,
} from "react";
import { useAuth } from "./auth-context";

// ============================================================================
// Types and Interfaces
// ============================================================================

export interface EntityStateContextType {
  // 🆕 NEW: Audit-specific state
  auditMode: AuditMode;
  postProcessingFilter: string | null;

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
    // 🆕 NEW: Audit actions
    setAuditMode: (mode: AuditMode) => void;
    setPostProcessingFilter: (filter: string | null) => void;

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
    triggerRefresh: (
      target?:
        | "all"
        | "clusters"
        | "current_visualization"
        | "current_connection"
    ) => void;
    clearSelections: () => void;
    resetUIState: () => void;

    // 🆕 NEW: Explicit data clearing action
    triggerCompleteDataClear: () => void;

    // 🆕 STEP 4: Registration functions for EntityDataContext integration
    registerClearAllData: (clearAllDataFn: () => void) => void;
    registerCancelAllRequests: (
      cancelAllRequestsFn: (pattern?: string) => void
    ) => void;
  };

  // Queries for derived state
  queries: {
    hasValidSelection: () => boolean;
    isLargeClusterPaging: () => boolean;
    getCurrentPageInfo: () => { clusterId: string | null; page: number };

    // 🆕 NEW: Audit queries
    isInAuditMode: () => boolean;
    getAuditFilterDisplayName: () => string;
  };

  // 🆕 STEP 4: Internal refs for cross-context communication (read-only access)
  _clearAllDataRef: React.MutableRefObject<(() => void) | null>;
  _cancelAllRequestsRef: React.MutableRefObject<
    ((pattern?: string) => void) | null
  >;
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
  // 🆕 NEW: Audit defaults
  auditMode: "normal" as AuditMode,
  postProcessingFilter: null as string | null,

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
  // Audit State
  // ========================================================================

  const [auditMode, setAuditModeState] = useState<AuditMode>(
    DEFAULT_STATE.auditMode
  );
  const [postProcessingFilter, setPostProcessingFilterState] = useState<
    string | null
  >(DEFAULT_STATE.postProcessingFilter);

  // ========================================================================
  // Core State
  // ========================================================================

  const [selectedClusterId, setSelectedClusterIdState] = useState<
    string | null
  >(DEFAULT_STATE.selectedClusterId);
  const [selectedEdgeId, setSelectedEdgeIdState] = useState<string | null>(
    DEFAULT_STATE.selectedEdgeId
  );
  const [resolutionMode, setResolutionModeState] = useState<ResolutionMode>(
    DEFAULT_STATE.resolutionMode
  );
  const [reviewerId, setReviewerIdState] = useState<string>(
    DEFAULT_STATE.reviewerId
  );
  const [lastReviewedEdgeId, setLastReviewedEdgeIdState] = useState<
    string | null
  >(DEFAULT_STATE.lastReviewedEdgeId);

  // ========================================================================
  // Filter and Preference State
  // ========================================================================

  const [workflowFilter, setWorkflowFilterState] = useState<WorkflowFilter>(
    DEFAULT_STATE.workflowFilter
  );
  const [clusterFilterStatus, setClusterFilterStatusState] =
    useState<ClusterFilterStatus>(DEFAULT_STATE.clusterFilterStatus);
  const [
    disconnectDependentServicesEnabled,
    setDisconnectDependentServicesEnabledState,
  ] = useState<boolean>(DEFAULT_STATE.disconnectDependentServicesEnabled);

  // ========================================================================
  // UI Configuration State
  // ========================================================================

  const [isAutoAdvanceEnabled, setIsAutoAdvanceEnabledState] =
    useState<boolean>(DEFAULT_STATE.isAutoAdvanceEnabled);
  const [isReviewToolsMaximized, setIsReviewToolsMaximizedState] =
    useState<boolean>(DEFAULT_STATE.isReviewToolsMaximized);

  // ========================================================================
  // Paging State
  // ========================================================================

  const [activelyPagingClusterId, setActivelyPagingClusterIdState] = useState<
    string | null
  >(DEFAULT_STATE.activelyPagingClusterId);
  const [largeClusterConnectionsPage, setLargeClusterConnectionsPageState] =
    useState<number>(DEFAULT_STATE.largeClusterConnectionsPage);
  const [isLoadingConnectionPageData, setIsLoadingConnectionPageDataState] =
    useState<boolean>(DEFAULT_STATE.isLoadingConnectionPageData);

  // ========================================================================
  // Utility State
  // ========================================================================

  const [refreshTrigger, setRefreshTriggerState] = useState<number>(
    DEFAULT_STATE.refreshTrigger
  );

  // 🆕 NEW: Special trigger for complete data clearing
  const [completeDataClearTrigger, setCompleteDataClearTrigger] =
    useState<number>(0);

  // ========================================================================
  // Ref for accessing clearAllData from EntityDataContext
  // ========================================================================

  const clearAllDataRef = useRef<(() => void) | null>(null);
  const cancelAllRequestsRef = useRef<((pattern?: string) => void) | null>(
    null
  );

  // ========================================================================
  // 🔧 ENHANCED: Complete State Reset Function
  // ========================================================================

  // Function to register the clearAllData function from EntityDataContext
  const registerClearAllData = useCallback((clearAllDataFn: () => void) => {
    console.log(
      "📝 [EntityState] Registering clearAllData function from EntityDataContext"
    );
    clearAllDataRef.current = clearAllDataFn;
  }, []);

  // Function to register the cancelAllRequests function from EntityDataContext
  const registerCancelAllRequests = useCallback(
    (cancelAllRequestsFn: (pattern?: string) => void) => {
      console.log(
        "📝 [EntityState] Registering cancelAllRequests function from EntityDataContext"
      );
      cancelAllRequestsRef.current = cancelAllRequestsFn;
    },
    []
  );

  const performCompleteStateReset = useCallback(() => {
    console.log("🧹🧹🧹 [EntityState] PERFORMING ENHANCED STATE RESET 🧹🧹🧹");

    // Step 1: Cancel all active requests with error handling
    if (cancelAllRequestsRef.current) {
      try {
        console.log(
          "🛑 [EntityState] Cancelling all active requests before state reset"
        );
        cancelAllRequestsRef.current();
      } catch (error) {
        console.error("❌ [EntityState] Error cancelling requests:", error);
      }
    }

    // Step 2: Clear all selections with validation
    try {
      setSelectedClusterIdState(null);
      setSelectedEdgeIdState(null);
      setLastReviewedEdgeIdState(null);
      setActivelyPagingClusterIdState(null);
      setLargeClusterConnectionsPageState(0);
      setIsLoadingConnectionPageDataState(false);

      // Reset UI state to defaults
      setIsAutoAdvanceEnabledState(true);
      setIsReviewToolsMaximizedState(false);

      // Reset filter status to default for fresh start
      setClusterFilterStatusState("unreviewed");
    } catch (error) {
      console.error("❌ [EntityState] Error during state clearing:", error);
    }

    // Step 3: Call external data clearing with error handling
    if (clearAllDataRef.current) {
      try {
        console.log(
          "🧹 [EntityState] Calling EntityDataContext.clearAllData()"
        );
        clearAllDataRef.current();
      } catch (error) {
        console.error("❌ [EntityState] Error calling clearAllData:", error);
      }
    }

    // Step 4: Trigger refresh
    setRefreshTriggerState((prev) => prev + 1);
    setCompleteDataClearTrigger((prev) => prev + 1);

    console.log("✅ [EntityState] Enhanced state reset completed");
  }, []);
  // ========================================================================
  // 🔧 ENHANCED: Smart Actions with Complete Data Clearing
  // ========================================================================

  const setAuditMode = useCallback(
    (mode: AuditMode) => {
      if (mode === auditMode) {
        console.log(`🔄 [EntityState] Audit mode already ${mode}, skipping`);
        return;
      }

      console.log(
        `🔄 [EntityState] Switching audit mode: ${auditMode} → ${mode}`
      );

      // Cancel audit-specific requests
      if (cancelAllRequestsRef.current) {
        try {
          console.log("🛑 [EntityState] Cancelling audit-related requests");
          cancelAllRequestsRef.current("audit");
          cancelAllRequestsRef.current("clusters-audit");
          cancelAllRequestsRef.current("audit-data");
        } catch (error) {
          console.warn(
            "⚠️ [EntityState] Error cancelling audit requests:",
            error
          );
        }
      }

      setAuditModeState(mode);
      performCompleteStateReset();
    },
    [auditMode, performCompleteStateReset]
  );

  const setPostProcessingFilter = useCallback(
    (filter: string | null) => {
      if (filter === postProcessingFilter) {
        console.log(
          `🔍 [EntityState] Post-processing filter already ${filter}, skipping`
        );
        return;
      }

      console.log(
        `🔍 [EntityState] Setting post-processing filter: ${postProcessingFilter} → ${filter}`
      );

      // Cancel filter-specific requests
      if (cancelAllRequestsRef.current) {
        try {
          console.log(
            "🛑 [EntityState] Cancelling post-processing filter requests"
          );
          cancelAllRequestsRef.current("audit-data");
          cancelAllRequestsRef.current("clusters-audit");
        } catch (error) {
          console.warn(
            "⚠️ [EntityState] Error cancelling filter requests:",
            error
          );
        }
      }

      setPostProcessingFilterState(filter);
      performCompleteStateReset();
    },
    [postProcessingFilter, performCompleteStateReset]
  );
  const setResolutionMode = useCallback(
    (mode: ResolutionMode) => {
      if (mode === resolutionMode) {
        console.log(
          `🔄 [EntityState] Resolution mode already ${mode}, skipping`
        );
        return;
      }

      console.log(
        `🔄 [EntityState] Switching resolution mode: ${resolutionMode} → ${mode}`
      );

      // Cancel specific request types for mode changes
      if (cancelAllRequestsRef.current) {
        try {
          console.log("🛑 [EntityState] Cancelling mode-specific requests");
          cancelAllRequestsRef.current("cluster-progress");
          cancelAllRequestsRef.current("visualization");
          cancelAllRequestsRef.current("node-details");
        } catch (error) {
          console.warn(
            "⚠️ [EntityState] Error cancelling mode-specific requests:",
            error
          );
        }
      }

      setResolutionModeState(mode);
      performCompleteStateReset();
    },
    [resolutionMode, performCompleteStateReset]
  );

  const setWorkflowFilter = useCallback(
    (filter: WorkflowFilter) => {
      if (filter === workflowFilter) {
        console.log(
          `🔍 [EntityState] Workflow filter already ${filter}, skipping`
        );
        return;
      }

      console.log(
        `🔍 [EntityState] Switching workflow filter: ${workflowFilter} → ${filter}`
      );

      // Cancel workflow-specific requests
      if (cancelAllRequestsRef.current) {
        try {
          console.log("🛑 [EntityState] Cancelling workflow filter requests");
          cancelAllRequestsRef.current("cluster-progress");
          cancelAllRequestsRef.current("visualization");
          cancelAllRequestsRef.current("connections");
        } catch (error) {
          console.warn(
            "⚠️ [EntityState] Error cancelling workflow filter requests:",
            error
          );
        }
      }

      setWorkflowFilterState(filter);
      performCompleteStateReset();
    },
    [workflowFilter, performCompleteStateReset]
  );

  const setSelectedClusterId = useCallback(
    (id: string | null) => {
      setSelectedClusterIdState((currentId) => {
        if (id === currentId) {
          return currentId;
        }

        console.log(
          `🎯 [EntityState] Setting selected cluster: ${currentId} → ${id}`
        );

        // Cancel cluster-specific requests when switching clusters
        if (cancelAllRequestsRef.current && currentId) {
          try {
            console.log(
              `🛑 [EntityState] Cancelling requests for previous cluster: ${currentId}`
            );
            cancelAllRequestsRef.current(`viz-${currentId}`);
            cancelAllRequestsRef.current(`audit-cluster-rich-${currentId}`);
          } catch (error) {
            console.warn(
              `⚠️ [EntityState] Error cancelling requests for cluster ${currentId}:`,
              error
            );
          }
        }

        // Clear edge selection when cluster changes
        setSelectedEdgeIdState(null);
        setLastReviewedEdgeIdState(null);

        // Reset paging if switching clusters
        if (id !== activelyPagingClusterId) {
          setActivelyPagingClusterIdState(null);
          setLargeClusterConnectionsPageState(0);
          setIsLoadingConnectionPageDataState(false);
        }

        return id;
      });
    },
    [activelyPagingClusterId]
  );

  const setSelectedEdgeId = useCallback((id: string | null) => {
    setSelectedEdgeIdState((currentId) => {
      if (id === currentId) {
        return currentId;
      }

      console.log(
        `🔗 [EntityState] Setting selected edge: ${currentId} → ${id}`
      );

      // Cancel edge-specific requests when switching edges
      if (cancelAllRequestsRef.current && currentId) {
        try {
          console.log(
            `🛑 [EntityState] Cancelling requests for previous edge: ${currentId}`
          );
          cancelAllRequestsRef.current(`connections-${currentId}`);
        } catch (error) {
          console.warn(
            `⚠️ [EntityState] Error cancelling requests for edge ${currentId}:`,
            error
          );
        }
      }

      return id;
    });
  }, []);

  const setClusterFilterStatus = useCallback(
    (status: ClusterFilterStatus) => {
      if (status === clusterFilterStatus) {
        console.log(
          `📊 [EntityState] Cluster filter status already ${status}, skipping`
        );
        return;
      }

      console.log(
        `📊 [EntityState] Setting cluster filter status: ${clusterFilterStatus} → ${status}`
      );

      // Cancel cluster filter-specific requests
      if (cancelAllRequestsRef.current) {
        try {
          console.log("🛑 [EntityState] Cancelling cluster filter requests");
          cancelAllRequestsRef.current("cluster-progress");
        } catch (error) {
          console.warn(
            "⚠️ [EntityState] Error cancelling cluster filter requests:",
            error
          );
        }
      }

      setClusterFilterStatusState(status);

      // Clear selections when filter changes (but not all data since this is just a view filter)
      try {
        setSelectedClusterIdState(null);
        setSelectedEdgeIdState(null);
        setLastReviewedEdgeIdState(null);
        setActivelyPagingClusterIdState(null);
        setLargeClusterConnectionsPageState(0);
        setIsLoadingConnectionPageDataState(false);
      } catch (error) {
        console.error("❌ [EntityState] Error clearing selections:", error);
      }

      // Trigger refresh to reload clusters with new filter
      setRefreshTriggerState((prev) => prev + 1);
    },
    [clusterFilterStatus]
  );

  const triggerRefresh = useCallback(
    (
      target:
        | "all"
        | "clusters"
        | "current_visualization"
        | "current_connection" = "all"
    ) => {
      console.log(`🔄 [EntityState] Triggering refresh for target: ${target}`);

      // Cancel requests based on refresh target with error handling
      if (cancelAllRequestsRef.current) {
        try {
          switch (target) {
            case "all":
              console.log(
                "🛑 [EntityState] Cancelling all requests for full refresh"
              );
              cancelAllRequestsRef.current();
              break;
            case "clusters":
              console.log(
                "🛑 [EntityState] Cancelling cluster-related requests"
              );
              cancelAllRequestsRef.current("cluster-progress");
              break;
            case "current_visualization":
              if (selectedClusterId) {
                console.log(
                  `🛑 [EntityState] Cancelling visualization requests for cluster: ${selectedClusterId}`
                );
                cancelAllRequestsRef.current(`viz-${selectedClusterId}`);
              }
              break;
            case "current_connection":
              if (selectedEdgeId) {
                console.log(
                  `🛑 [EntityState] Cancelling connection requests for edge: ${selectedEdgeId}`
                );
                cancelAllRequestsRef.current(`connections-${selectedEdgeId}`);
              }
              break;
          }
        } catch (error) {
          console.warn(
            `⚠️ [EntityState] Error cancelling requests for ${target}:`,
            error
          );
        }
      }

      if (target === "all" || target === "clusters") {
        // Reset paging state on cluster refresh
        try {
          setActivelyPagingClusterIdState(null);
          setLargeClusterConnectionsPageState(0);
          setIsLoadingConnectionPageDataState(false);
        } catch (error) {
          console.error(
            "❌ [EntityState] Error resetting paging state:",
            error
          );
        }
      }

      setRefreshTriggerState((prev) => prev + 1);
    },
    [selectedClusterId, selectedEdgeId]
  );

  const triggerCompleteDataClear = useCallback(() => {
    console.log("🧹 [EntityState] Explicit complete data clear requested");

    // Cancel all requests first
    if (cancelAllRequestsRef.current) {
      console.log(
        "🛑 [EntityState] Cancelling all requests for explicit data clear"
      );
      cancelAllRequestsRef.current();
    }

    performCompleteStateReset();
  }, [performCompleteStateReset]);

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

  const actions = useMemo(
    () => ({
      // Audit actions
      setAuditMode,
      setPostProcessingFilter,

      // Selection actions
      setSelectedClusterId,
      setSelectedEdgeId,
      setResolutionMode,
      setReviewerId: setReviewerIdState,
      setLastReviewedEdgeId: setLastReviewedEdgeIdState,

      // Filter and preference actions
      setWorkflowFilter,
      setClusterFilterStatus,
      setDisconnectDependentServicesEnabled:
        setDisconnectDependentServicesEnabledState,

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
      triggerCompleteDataClear,

      // 🆕 STEP 4: Registration functions for EntityDataContext integration
      registerClearAllData,
      registerCancelAllRequests,
    }),
    [
      setAuditMode,
      setPostProcessingFilter,
      setSelectedClusterId,
      setSelectedEdgeId,
      setResolutionMode,
      setWorkflowFilter,
      setClusterFilterStatus,
      triggerRefresh,
      clearSelections,
      resetUIState,
      triggerCompleteDataClear,
      registerClearAllData,
      registerCancelAllRequests,
    ]
  );

  // ========================================================================
  // Memoized Queries Object
  // ========================================================================

  const queries = useMemo(
    () => ({
      hasValidSelection: () => selectedClusterId !== null,

      isLargeClusterPaging: () =>
        activelyPagingClusterId !== null &&
        activelyPagingClusterId === selectedClusterId,

      getCurrentPageInfo: () => ({
        clusterId: activelyPagingClusterId,
        page: largeClusterConnectionsPage,
      }),

      // 🆕 NEW: Audit queries
      isInAuditMode: () => auditMode === "post_processing_audit",

      getAuditFilterDisplayName: () => {
        if (!postProcessingFilter) return "All Automated Decisions";

        switch (postProcessingFilter) {
          case "disconnectDependentServices":
            return "Disconnect Dependent Services";
          default:
            return postProcessingFilter;
        }
      },
    }),
    [
      selectedClusterId,
      activelyPagingClusterId,
      largeClusterConnectionsPage,
      auditMode,
      postProcessingFilter,
    ]
  );

  // ========================================================================
  // Context Value
  // ========================================================================

  const contextValue: EntityStateContextType = {
    // Audit state
    auditMode,
    postProcessingFilter,

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

    // 🆕 STEP 4: Internal refs for cross-context communication
    _clearAllDataRef: clearAllDataRef,
    _cancelAllRequestsRef: cancelAllRequestsRef,
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
    actions: { setSelectedClusterId, setSelectedEdgeId },
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
    actions: { setWorkflowFilter, setClusterFilterStatus, setResolutionMode },
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
 * 🆕 NEW: Hook for components that only need audit state
 */
export function useEntityAudit() {
  const {
    auditMode,
    postProcessingFilter,
    actions: { setAuditMode, setPostProcessingFilter },
    queries: { isInAuditMode, getAuditFilterDisplayName },
  } = useEntityState();

  return {
    auditMode,
    postProcessingFilter,
    setAuditMode,
    setPostProcessingFilter,
    isInAuditMode,
    getAuditFilterDisplayName,
  };
}

/**
 * Hook for components that only need UI state
 */
export function useEntityUI() {
  const {
    isAutoAdvanceEnabled,
    isReviewToolsMaximized,
    actions: { setIsAutoAdvanceEnabled, setIsReviewToolsMaximized },
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
    console.log("Audit:", {
      mode: state.auditMode,
      filter: state.postProcessingFilter,
      isInAuditMode: state.queries.isInAuditMode(),
    });
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

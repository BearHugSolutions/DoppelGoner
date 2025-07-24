/*
================================================================================
|
|   File: /context/entity-workflow-context.tsx - Phase 3: Business Logic & Actions
|
|   Description: Workflow management context for Entity Resolution
|   - Manages business logic and complex workflows
|   - Handles progress calculations (client + server)
|   - Controls auto-navigation and edge selection
|   - Manages review submission and business rules
|   - Provides computed/derived state
|   - Consumes EntityStateContext and EntityDataContext
|   - 🔧 PHASE 1: Enhanced optimistic updates with progress calculation
|   - 🔧 BUG FIX: Corrected optimistic progression when workflow filters are active.
|
================================================================================
*/
"use client";

import {
  EntityCluster,
  EntityVisualizationDataResponse,
  EntityConnectionDataResponse,
  ClusterReviewProgress,
  GroupReviewDecision,
  BaseLink,
  BaseNode,
  EdgeReviewApiPayload,
  WorkflowFilter,
  DisconnectDependentServicesRequest,
  DisconnectDependentServicesResponse,
} from "@/types/entity-resolution";
import {
  postEdgeReview,
  postDisconnectDependentServices,
  getOpinionPreferences,
  updateOpinionPreferences,
} from "@/utils/api-client";
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
import { useEntityState } from "./entity-state-context";
import { useEntityData } from "./entity-data-context";
import { useToast } from "@/hooks/use-toast";
import { produce } from "immer";

// ============================================================================
// Constants
// ============================================================================

const LARGE_CLUSTER_THRESHOLD = 200;
const CONNECTION_PAGE_SIZE = 200;

// ============================================================================
// Types and Interfaces
// ============================================================================

interface EdgeSelectionInfo {
  currentEdgeId: string | null;
  nextUnreviewedEdgeId: string | null;
  hasUnreviewedEdges: boolean;
  currentEdgeIndex: number;
  totalEdgesInView: number;
  totalUnreviewedEdgesInCluster: number;
  currentUnreviewedEdgeIndexInCluster: number;
  totalEdgesInEntireCluster: number;
}

export interface OverallWorkflowProgress {
  totalPendingDecisions: number;
  totalCompletedDecisions: number;
  overallProgressPercentage: number;
  clustersComplete: number;
  totalClusters: number;
}

export interface EntityWorkflowContextType {
  // Computed progress (uses both client + server data)
  clusterProgress: Record<string, ClusterReviewProgress>;
  overallProgress: OverallWorkflowProgress;
  edgeSelectionInfo: EdgeSelectionInfo;

  // Current computed data
  currentVisualizationData: EntityVisualizationDataResponse | null;
  currentConnectionData: EntityConnectionDataResponse | null;
  selectedClusterDetails: EntityCluster | null;

  // Edge submission state
  edgeSubmissionStatus: Record<
    string,
    { isSubmitting: boolean; error: string | null }
  >;

  // Business actions
  selectNextUnreviewedEdge: (afterEdgeId?: string | null) => void;
  selectPreviousUnreviewedInCluster: () => void;
  selectNextUnreviewedInCluster: () => void;
  advanceToNextCluster: () => Promise<void>;
  checkAndAdvanceIfComplete: (clusterIdToCheck?: string) => Promise<void>;

  // Review actions
  submitEdgeReview: (
    edgeId: string,
    decision: GroupReviewDecision,
    notes?: string
  ) => Promise<void>;
  enableDisconnectDependentServices: () => Promise<void>;

  // Large cluster paging
  initializeLargeClusterConnectionPaging: (clusterId: string) => Promise<void>;
  viewNextConnectionPage: (clusterId: string) => Promise<void>;
  loadConnectionDataForLinkPage: (
    clusterId: string,
    pageToLoad: number,
    isPrefetch?: boolean
  ) => Promise<void>;

  // Enhanced selection actions
  handleSetSelectedClusterId: (id: string | null, isManualSelection?: boolean) => Promise<void>;
  setSelectedEdgeIdAction: (id: string | null) => void;

  // Progress queries
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

  // 🔧 NEW: Filter-aware navigation helpers
  isClusterCompleteForCurrentFilter: (clusterId: string) => boolean;
  findNextViableCluster: (
    fromClusterId?: string
  ) => EntityCluster | "next_page" | null;

  // Cross-source detection
  isCrossSourceConnection: (link: BaseLink, nodes: BaseNode[]) => boolean;
}

// ============================================================================
// Context Creation
// ============================================================================

const EntityWorkflowContext = createContext<
  EntityWorkflowContextType | undefined
>(undefined);

// ============================================================================
// Provider Component
// ============================================================================

export function EntityWorkflowProvider({ children }: { children: ReactNode }) {
  const { user, selectedOpinion } = useAuth();
  const { toast } = useToast();
  const state = useEntityState();
  const data = useEntityData();

  // Destructure needed values
  const {
    selectedClusterId,
    selectedEdgeId,
    lastReviewedEdgeId,
    resolutionMode,
    workflowFilter,
    isAutoAdvanceEnabled,
    activelyPagingClusterId,
    largeClusterConnectionsPage,
    disconnectDependentServicesEnabled,
    actions: stateActions,
  } = state;

  const {
    clusters,
    visualizationData,
    connectionData,
    serverProgress,
    overallServerProgress,
    loadClusterProgress,
    loadVisualizationDataForClusters,
    loadBulkConnections,
    loadBulkNodeDetails,
    getClusterById,
    isVisualizationDataLoaded,
    updateEdgeStatusOptimistically, // 🔧 PHASE 1: Enhanced with progress updates
    updateClusterCompletionOptimistically, // 🔧 PHASE 1: Enhanced with progress updates
  } = data;

  // ========================================================================
  // Internal State
  // ========================================================================

  const [edgeSubmissionStatus, setEdgeSubmissionStatus] = useState<
    Record<string, { isSubmitting: boolean; error: string | null }>
  >({});

  const [manualClusterSelection, setManualClusterSelection] = useState<{
    clusterId: string | null;
    timestamp: number;
  }>({ clusterId: null, timestamp: 0 });

  const [lastWorkflowFilter, setLastWorkflowFilter] =
    useState<WorkflowFilter>(workflowFilter);

  // ========================================================================
  // Cross-source Detection Utility
  // ========================================================================

  const isCrossSourceConnection = useCallback(
    (link: BaseLink, nodes: BaseNode[]): boolean => {
      const sourceNodeId =
        typeof link.source === "string" ? link.source : link.source.id;
      const targetNodeId =
        typeof link.target === "string" ? link.target : link.target.id;

      const sourceNode = nodes.find((n) => n.id === sourceNodeId);
      const targetNode = nodes.find((n) => n.id === targetNodeId);

      if (!sourceNode || !targetNode) return false;

      const sourceSystem1 = sourceNode.sourceSystem;
      const sourceSystem2 = targetNode.sourceSystem;

      return !!(
        sourceSystem1 &&
        sourceSystem2 &&
        sourceSystem1 !== sourceSystem2
      );
    },
    []
  );

  // ========================================================================
  // Progress Calculation Functions
  // ========================================================================

  // Client-side progress calculation (fallback)
  const getClusterProgressInternal = useCallback(
    (
      clusterIdToQuery: string,
      filter: WorkflowFilter
    ): ClusterReviewProgress => {
      const clusterDetails = clusters.data.find(
        (c) => c.id === clusterIdToQuery
      );

      // 💡 FIX: Prioritize the definitive 'wasReviewed' flag from the cluster list.
      // This is the source of truth for whether a cluster is fully finalized.
      if (clusterDetails?.wasReviewed) {
        const total = clusterDetails.groupCount ?? 0;
        return {
          totalEdges: total,
          reviewedEdges: total,
          pendingEdges: 0,
          confirmedMatches: 0, // Note: We might not have detailed counts here, but completion is key.
          confirmedNonMatches: 0,
          progressPercentage: 100,
          isComplete: true,
        };
      }

      const vizState = visualizationData[clusterIdToQuery];
      const currentVizData = vizState?.data;

      if (currentVizData?.links && currentVizData?.nodes) {
        let relevantLinks = currentVizData.links;

        if (filter === "cross-source-only") {
          relevantLinks = currentVizData.links.filter((link) =>
            isCrossSourceConnection(link, currentVizData.nodes)
          );
        }

        const totalEdges = relevantLinks.length;
        const reviewedEdges = relevantLinks.filter((l) => l.wasReviewed).length;
        const pendingReviewLinks = relevantLinks.filter((l) => !l.wasReviewed);

        const progressPercentage =
          totalEdges > 0 ? Math.round((reviewedEdges / totalEdges) * 100) : 100;

        return {
          totalEdges,
          reviewedEdges,
          pendingEdges: pendingReviewLinks.length,
          confirmedMatches: relevantLinks.filter(
            (l) => l.status === "CONFIRMED_MATCH"
          ).length,
          confirmedNonMatches: relevantLinks.filter(
            (l) => l.status === "CONFIRMED_NON_MATCH"
          ).length,
          progressPercentage,
          isComplete: pendingReviewLinks.length === 0,
        };
      }

      // Fallback logic for when visualization data isn't loaded
      if (clusterDetails) {
        if (typeof clusterDetails.groupCount === "number") {
          const total = clusterDetails.groupCount;
          return {
            totalEdges: total,
            reviewedEdges: 0,
            pendingEdges: total,
            confirmedMatches: 0,
            confirmedNonMatches: 0,
            progressPercentage: 0,
            isComplete: total === 0,
          };
        }
      }

      return {
        totalEdges: -1,
        reviewedEdges: 0,
        pendingEdges: -1,
        confirmedMatches: 0,
        confirmedNonMatches: 0,
        progressPercentage: -1,
        isComplete: false,
      };
    },
    [visualizationData, clusters.data, isCrossSourceConnection]
  );

  // ========================================================================
  // Progress Query Functions
  // ========================================================================

  // Get cluster progress (current filter view) - prefers server data
  const getClusterProgress = useCallback(
    (clusterIdToQuery: string): ClusterReviewProgress => {
      // Prefer server-side progress if available
      const serverProg = serverProgress[clusterIdToQuery];
      if (serverProg) {
        const currentProgress = serverProg.currentView;
        return {
          totalEdges: currentProgress.totalEdges,
          reviewedEdges: currentProgress.reviewedEdges,
          pendingEdges: currentProgress.pendingEdges,
          confirmedMatches: currentProgress.confirmedMatches,
          confirmedNonMatches: currentProgress.confirmedNonMatches,
          progressPercentage: currentProgress.progressPercentage,
          isComplete: currentProgress.isComplete,
        };
      }

      // Fallback to existing client-side calculation
      return getClusterProgressInternal(clusterIdToQuery, workflowFilter);
    },
    [serverProgress, getClusterProgressInternal, workflowFilter]
  );

  // Get unfiltered cluster progress (all edges)
  const getClusterProgressUnfiltered = useCallback(
    (clusterIdToQuery: string): ClusterReviewProgress => {
      const serverProg = serverProgress[clusterIdToQuery];
      if (serverProg) {
        const allProgress = serverProg.allEdges;
        return {
          totalEdges: allProgress.totalEdges,
          reviewedEdges: allProgress.reviewedEdges,
          pendingEdges: allProgress.pendingEdges,
          confirmedMatches: allProgress.confirmedMatches,
          confirmedNonMatches: allProgress.confirmedNonMatches,
          progressPercentage: allProgress.progressPercentage,
          isComplete: allProgress.isComplete,
        };
      }

      // Fallback to existing client-side calculation without filter
      return getClusterProgressInternal(clusterIdToQuery, "all");
    },
    [serverProgress, getClusterProgressInternal]
  );

  // Get cross-source only cluster progress
  const getClusterProgressCrossSource = useCallback(
    (clusterIdToQuery: string): ClusterReviewProgress => {
      const serverProg = serverProgress[clusterIdToQuery];
      if (serverProg) {
        const crossSourceProgress = serverProg.crossSourceEdges;
        return {
          totalEdges: crossSourceProgress.totalEdges,
          reviewedEdges: crossSourceProgress.reviewedEdges,
          pendingEdges: crossSourceProgress.pendingEdges,
          confirmedMatches: crossSourceProgress.confirmedMatches,
          confirmedNonMatches: crossSourceProgress.confirmedNonMatches,
          progressPercentage: crossSourceProgress.progressPercentage,
          isComplete: crossSourceProgress.isComplete,
        };
      }

      // Fallback to existing client-side calculation with cross-source filter
      return getClusterProgressInternal(clusterIdToQuery, "cross-source-only");
    },
    [serverProgress, getClusterProgressInternal]
  );

  // ========================================================================
  // Computed State
  // ========================================================================

  // Cluster progress map
  const clusterProgress = useMemo(() => {
    const reconstructed: Record<string, ClusterReviewProgress> = {};

    clusters.data.forEach((cluster) => {
      reconstructed[cluster.id] = getClusterProgress(cluster.id);
    });

    if (selectedClusterId && !reconstructed[selectedClusterId]) {
      const hasVisualizationData =
        visualizationData[selectedClusterId]?.data?.links;
      const hasClusterDetails = getClusterById(selectedClusterId);
      const hasServerProgress = serverProgress[selectedClusterId];

      if (hasVisualizationData || hasClusterDetails || hasServerProgress) {
        reconstructed[selectedClusterId] =
          getClusterProgress(selectedClusterId);
      }
    }

    return reconstructed;
  }, [
    clusters.data,
    selectedClusterId,
    selectedClusterId
      ? visualizationData[selectedClusterId]?.data?.links
      : undefined,
    serverProgress,
    getClusterProgress,
    visualizationData,
    getClusterById,
  ]);

  // Overall progress
  const overallProgress = useMemo(() => {
    if (overallServerProgress) {
      // Return the current view progress (respects current workflow filter)
      return overallServerProgress.currentView;
    }

    // Fallback to existing client-side calculation
    let totalPending = 0;
    let totalCompleted = 0;
    let clustersComplete = 0;

    clusters.data.forEach((cluster) => {
      const progress = clusterProgress[cluster.id];
      if (progress) {
        totalPending += progress.pendingEdges;
        totalCompleted += progress.reviewedEdges;
        if (progress.isComplete) {
          clustersComplete++;
        }
      }
    });

    const totalDecisions = totalPending + totalCompleted;
    const overallProgressPercentage =
      totalDecisions > 0
        ? Math.round((totalCompleted / totalDecisions) * 100)
        : 100;

    return {
      totalPendingDecisions: totalPending,
      totalCompletedDecisions: totalCompleted,
      overallProgressPercentage,
      clustersComplete,
      totalClusters: clusters.data.length,
    };
  }, [overallServerProgress, clusters.data, clusterProgress]);

  // Current visualization data with filters
  const currentVisualizationData =
    useMemo((): EntityVisualizationDataResponse | null => {
      if (!selectedClusterId) return null;

      const vizState = visualizationData[selectedClusterId];
      if (!vizState?.data) return null;

      let data = vizState.data;

      // Apply paging if needed
      if (
        selectedClusterId === activelyPagingClusterId &&
        largeClusterConnectionsPage > 0
      ) {
        const allLinks = vizState.data.links;
        const startIndex =
          (largeClusterConnectionsPage - 1) * CONNECTION_PAGE_SIZE;
        const endIndex = startIndex + CONNECTION_PAGE_SIZE;
        const pagedLinks = allLinks.slice(startIndex, endIndex);
        data = { ...vizState.data, links: pagedLinks };
      }

      // Apply workflow filter
      if (workflowFilter === "cross-source-only") {
        const filteredLinks = data.links.filter((link) =>
          isCrossSourceConnection(link, data.nodes)
        );
        data = { ...data, links: filteredLinks };
      }

      return data;
    }, [
      selectedClusterId,
      visualizationData,
      activelyPagingClusterId,
      largeClusterConnectionsPage,
      workflowFilter,
      isCrossSourceConnection,
    ]);

  // Current connection data
  const currentConnectionData =
    useMemo((): EntityConnectionDataResponse | null => {
      if (!selectedEdgeId) return null;
      const selectedEdgeConnectionState = connectionData[selectedEdgeId];
      if (!selectedEdgeConnectionState) return null;
      return selectedEdgeConnectionState.data || null;
    }, [selectedEdgeId, connectionData]);

  // Selected cluster details
  const selectedClusterDetails = useMemo((): EntityCluster | null => {
    if (!selectedClusterId) return null;
    return clusters.data.find((c) => c.id === selectedClusterId) || null;
  }, [selectedClusterId, clusters.data]);

  // Edge selection info
  const edgeSelectionInfo = useMemo((): EdgeSelectionInfo => {
    const defaultEdgeInfo: EdgeSelectionInfo = {
      currentEdgeId: selectedEdgeId,
      nextUnreviewedEdgeId: null,
      hasUnreviewedEdges: false,
      currentEdgeIndex: -1,
      totalEdgesInView: 0,
      totalUnreviewedEdgesInCluster: 0,
      currentUnreviewedEdgeIndexInCluster: -1,
      totalEdgesInEntireCluster: 0,
    };

    const fullViz = selectedClusterId
      ? visualizationData[selectedClusterId]?.data
      : null;
    if (!fullViz?.links) {
      return defaultEdgeInfo;
    }

    const allLinksInCluster = fullViz.links;
    defaultEdgeInfo.totalEdgesInEntireCluster = allLinksInCluster.length;

    const allUnreviewedLinksInCluster = allLinksInCluster.filter(
      (link) => !link.wasReviewed
    );
    defaultEdgeInfo.hasUnreviewedEdges = allUnreviewedLinksInCluster.length > 0;
    defaultEdgeInfo.totalUnreviewedEdgesInCluster =
      allUnreviewedLinksInCluster.length;

    if (selectedEdgeId) {
      defaultEdgeInfo.currentUnreviewedEdgeIndexInCluster =
        allUnreviewedLinksInCluster.findIndex((l) => l.id === selectedEdgeId);
    }

    const currentVizForView = currentVisualizationData;
    if (currentVizForView?.links) {
      defaultEdgeInfo.totalEdgesInView = currentVizForView.links.length;
      if (selectedEdgeId) {
        defaultEdgeInfo.currentEdgeIndex = currentVizForView.links.findIndex(
          (l) => l.id === selectedEdgeId
        );
      }
    }

    if (defaultEdgeInfo.hasUnreviewedEdges) {
      if (selectedEdgeId) {
        const currentIdx = allUnreviewedLinksInCluster.findIndex(
          (l) => l.id === selectedEdgeId
        );
        if (
          currentIdx > -1 &&
          currentIdx < allUnreviewedLinksInCluster.length - 1
        ) {
          defaultEdgeInfo.nextUnreviewedEdgeId =
            allUnreviewedLinksInCluster[currentIdx + 1].id;
        } else {
          defaultEdgeInfo.nextUnreviewedEdgeId =
            allUnreviewedLinksInCluster[0].id;
        }
      } else {
        defaultEdgeInfo.nextUnreviewedEdgeId =
          allUnreviewedLinksInCluster[0].id;
      }
    }

    return defaultEdgeInfo;
  }, [
    currentVisualizationData,
    selectedEdgeId,
    selectedClusterId,
    visualizationData,
  ]);

  // ========================================================================
  // Helper Query Functions
  // ========================================================================

  const canAdvanceToNextCluster = useCallback(() => {
    if (!selectedClusterId) return false;
    const progress = getClusterProgress(selectedClusterId);
    return progress?.isComplete || false;
  }, [selectedClusterId, getClusterProgress]);

  const isEdgeReviewed = useCallback(
    (edgeId: string) => {
      const currentViz = selectedClusterId
        ? visualizationData[selectedClusterId]?.data
        : null;
      if (!currentViz?.links) return false;
      const edge = currentViz.links.find((l) => l.id === edgeId);
      return edge ? edge.wasReviewed === true : false;
    },
    [selectedClusterId, visualizationData]
  );

  const getEdgeStatus = useCallback(
    (edgeId: string) => {
      const currentViz = selectedClusterId
        ? visualizationData[selectedClusterId]?.data
        : null;
      if (!currentViz?.links) return null;
      const edge = currentViz.links.find((l) => l.id === edgeId);
      return edge?.status ?? null;
    },
    [selectedClusterId, visualizationData]
  );

  const getEdgeSubmissionStatus = useCallback(
    (edgeId: string) => {
      return (
        edgeSubmissionStatus[edgeId] || { isSubmitting: false, error: null }
      );
    },
    [edgeSubmissionStatus]
  );

  const isCurrentEdgeValidForFilter = useCallback(
    (filter: WorkflowFilter): boolean => {
      if (!selectedEdgeId || !selectedClusterId) return true;

      const vizState = visualizationData[selectedClusterId];
      if (!vizState?.data?.links || !vizState?.data?.nodes) return true;

      const currentEdge = vizState.data.links.find(
        (link) => link.id === selectedEdgeId
      );
      if (!currentEdge) return false;

      if (filter === "cross-source-only") {
        return isCrossSourceConnection(currentEdge, vizState.data.nodes);
      }

      return true;
    },
    [
      selectedEdgeId,
      selectedClusterId,
      visualizationData,
      isCrossSourceConnection,
    ]
  );

  const isClusterCompleteForCurrentFilter = useCallback(
    (clusterId: string) => {
      const progress = getClusterProgress(clusterId);
      return progress.isComplete;
    },
    [getClusterProgress]
  );

  const findNextViableCluster = useCallback(
    (fromClusterId?: string) => {
      const currentClusterId = fromClusterId || selectedClusterId;
      if (!currentClusterId) return null;

      const currentIndex = clusters.data.findIndex(
        (c) => c.id === currentClusterId
      );
      if (currentIndex === -1) return null;

      // Look for next cluster that has unreviewed connections matching the current filter
      for (let i = currentIndex + 1; i < clusters.data.length; i++) {
        const cluster = clusters.data[i];
        if (cluster.wasReviewed) continue; // Skip finalized clusters

        const progress = getClusterProgress(cluster.id);
        // A cluster is viable if it has pending edges according to current filter
        if (progress.pendingEdges > 0) {
          console.log(
            `📍 [EntityWorkflow] Found next viable cluster: ${cluster.id} (${progress.pendingEdges} pending for filter: ${workflowFilter})`
          );
          return cluster;
        }
      }

      // If no cluster found on current page, check if there are more pages
      if (clusters.page < Math.ceil(clusters.total / clusters.limit)) {
        console.log(
          `📄 [EntityWorkflow] No viable clusters on current page, suggesting next page load`
        );
        return "next_page";
      }

      console.log(
        `🏁 [EntityWorkflow] No more viable clusters found for filter: ${workflowFilter}`
      );
      return null;
    },
    [selectedClusterId, clusters, getClusterProgress, workflowFilter]
  );

  // ========================================================================
  // Enhanced Selection Actions
  // ========================================================================

  const handleSetSelectedClusterId = useCallback(
    async (id: string | null, isManualSelection: boolean = true) => {
      if (id === selectedClusterId) return;
  
      // 🔧 NEW: Track manual cluster selections
      if (isManualSelection && id) {
        setManualClusterSelection({ clusterId: id, timestamp: Date.now() });
        console.log(`📍 [EntityWorkflow] Manual cluster selection: ${id}`);
      }
  
      stateActions.setSelectedClusterId(id);
  
      if (id && selectedOpinion) {
        const clusterDetail = getClusterById(id);
        const connectionCount = clusterDetail
          ? clusterDetail.groupCount
          : undefined;
        const isLarge =
          connectionCount !== undefined &&
          connectionCount !== null &&
          connectionCount > LARGE_CLUSTER_THRESHOLD;
  
        if (
          clusterDetail?.wasReviewed ||
          (!isLarge && getClusterProgress(id).isComplete)
        ) {
          stateActions.setIsAutoAdvanceEnabled(false);
        } else {
          if (!(isLarge && largeClusterConnectionsPage === 0)) {
            stateActions.setIsAutoAdvanceEnabled(true);
          }
        }
  
        if (!isLarge) {
          const vizState = visualizationData[id];
          if ((!vizState?.data || vizState?.error) && !vizState?.loading) {
            await loadVisualizationDataForClusters([
              { clusterId: id, itemType: resolutionMode },
            ]);
          } else if (vizState?.data?.links) {
            const unloadedConnectionItems = vizState.data.links
              .filter((link) => {
                const connState = connectionData[link.id];
                const needsLoad =
                  !connState ||
                  (!connState.data && !connState.loading && !connState.error) ||
                  connState.error;
                return needsLoad;
              })
              .map((link) => ({ edgeId: link.id, itemType: resolutionMode }));
  
            if (unloadedConnectionItems.length > 0) {
              console.log(
                `🔄 [EntityWorkflow] Loading ${unloadedConnectionItems.length} missing connections for cluster ${id}`
              );
              loadBulkConnections(unloadedConnectionItems);
            }
          }
        } else {
          const vizState = visualizationData[id];
          if (vizState?.data?.nodes) {
            const nodeIdsFromViz = vizState.data.nodes.map((node) => ({
              id: node.id,
              nodeType: resolutionMode,
            }));
            await loadBulkNodeDetails(nodeIdsFromViz);
          }
        }
      }
    },
    [
      selectedClusterId,
      selectedOpinion,
      stateActions,
      getClusterById,
      getClusterProgress,
      largeClusterConnectionsPage,
      visualizationData,
      resolutionMode,
      loadVisualizationDataForClusters,
      connectionData,
      loadBulkConnections,
      loadBulkNodeDetails,
    ]
  );

  const setSelectedEdgeIdAction = useCallback(
    (id: string | null) => {
      if (id === selectedEdgeId) return;

      console.log(`🔗 [EntityWorkflow] Setting selected edge ID to: ${id}`);
      if (id && isEdgeReviewed(id)) {
        console.log(`Edge ${id} is already reviewed. Pausing auto-advance.`);
        stateActions.setIsAutoAdvanceEnabled(false);
      } else if (id) {
        const currentClusterId = selectedClusterId;
        if (currentClusterId) {
          const clusterDetail = getClusterById(currentClusterId);
          if (!clusterDetail?.wasReviewed) {
            stateActions.setIsAutoAdvanceEnabled(true);
          }
        }
      }
      stateActions.setSelectedEdgeId(id);
    },
    [
      selectedEdgeId,
      isEdgeReviewed,
      stateActions,
      selectedClusterId,
      getClusterById,
    ]
  );

  // ========================================================================
  // Business Action Functions - 🔧 PHASE 1: Enhanced Optimistic Updates
  // ========================================================================

  const enableDisconnectDependentServices = useCallback(async () => {
    if (!user?.id || !selectedOpinion) {
      toast({
        title: "Auth Error",
        description: "Login required and opinion must be selected.",
        variant: "destructive",
      });
      return;
    }

    try {
      await updateOpinionPreferences(
        {
          disconnectDependentServices: true,
        },
        selectedOpinion
      );

      stateActions.setDisconnectDependentServicesEnabled(true);

      const bulkRequest: DisconnectDependentServicesRequest = {
        reviewerId: user.id,
        notes: "Bulk enable dependent service disconnection",
        asyncProcessing: true,
      };

      const response = await postDisconnectDependentServices(
        bulkRequest,
        selectedOpinion
      );

      toast({
        title: "Dependent Service Disconnection Enabled",
        description: `Processed ${response.serviceEdgesProcessed} service edges. Setting saved.`,
      });
    } catch (error) {
      stateActions.setDisconnectDependentServicesEnabled(false);
      toast({
        title: "Error",
        description: `Failed to enable: ${(error as Error).message}`,
        variant: "destructive",
      });
    }
  }, [user?.id, selectedOpinion, toast, stateActions]);

  const advanceToNextCluster = useCallback(async () => {
    if (!selectedClusterId) {
      console.warn("🚫 [EntityWorkflow] No cluster selected, cannot advance.");
      return;
    }

    console.log(
      `🚀 [EntityWorkflow] Advancing to next cluster from: ${selectedClusterId}`
    );

    const currentIndex = clusters.data.findIndex(
      (c) => c.id === selectedClusterId
    );
    if (currentIndex === -1) {
      console.warn(
        "Current cluster not found in cluster list. Reloading clusters."
      );
      loadClusterProgress(clusters.page, clusters.limit);
      return;
    }

    // Clear paging state when advancing clusters
    stateActions.setActivelyPagingClusterId(null);
    stateActions.setLargeClusterConnectionsPage(0);

    let nextClusterSelected = false;

    // Look for next unreviewed cluster on current page
    for (let i = currentIndex + 1; i < clusters.data.length; i++) {
      const nextClusterOnPage = clusters.data[i];
      if (!nextClusterOnPage.wasReviewed) {
        console.log(
          `Selecting next unreviewed cluster in current page: ${nextClusterOnPage.id}`
        );

        // Set the selected cluster
        handleSetSelectedClusterId(nextClusterOnPage.id);
        nextClusterSelected = true;
        break;
      }
    }

    // If no unreviewed cluster found on current page, try next page
    if (!nextClusterSelected) {
      if (clusters.page < Math.ceil(clusters.total / clusters.limit)) {
        const nextPageToLoad = clusters.page + 1;
        console.log(`Loading next page (${nextPageToLoad}) of clusters.`);

        await loadClusterProgress(nextPageToLoad, clusters.limit);

        // After loading new page, the auto-selection logic will handle
        // selecting the first unreviewed cluster on the new page
      } else {
        // No more pages - all clusters reviewed
        toast({
          title: "Workflow Complete",
          description: "All clusters have been reviewed.",
        });
        console.log("All clusters reviewed.");
        stateActions.setIsAutoAdvanceEnabled(false);

        // Set selected cluster to null if all are reviewed and no more pages
        stateActions.setSelectedClusterId(null);
      }
    }
  }, [
    selectedClusterId,
    clusters,
    loadClusterProgress,
    toast,
    handleSetSelectedClusterId,
    stateActions,
  ]);

  const checkAndAdvanceIfComplete = useCallback(
    async (clusterIdToCheck?: string) => {
      const targetClusterId = clusterIdToCheck || selectedClusterId;
      if (!targetClusterId) {
        return;
      }

      const progress = getClusterProgress(targetClusterId);

      if (progress.isComplete && progress.totalEdges !== -1) {
        // Mark cluster as reviewed - this would need to be handled by EntityDataContext
        console.log(
          `✅ [EntityWorkflow] Cluster ${targetClusterId} is complete`
        );

        if (targetClusterId === selectedClusterId && isAutoAdvanceEnabled) {
          console.log(
            `Cluster ${targetClusterId} is complete. Auto-advance ON. Advancing.`
          );
          await advanceToNextCluster();
        } else {
          console.log(
            `Cluster ${targetClusterId} is complete. Auto-advance OFF or cluster not selected. Not advancing.`
          );
        }
      }
    },
    [
      selectedClusterId,
      getClusterProgress,
      isAutoAdvanceEnabled,
      advanceToNextCluster,
    ]
  );

  // Enhanced selectNextUnreviewedEdge to handle manual selections
  const selectNextUnreviewedEdge = useCallback(
    (afterEdgeId?: string | null) => {
      const currentClusterId = selectedClusterId;
      if (!currentClusterId) {
        console.warn(
          "🚫 [EntityWorkflow] No cluster selected for selecting next edge."
        );
        return;
      }

      const clusterDetail = getClusterById(currentClusterId);
      if (clusterDetail?.wasReviewed) {
        console.log(
          `Cluster ${currentClusterId} was reviewed. No edges to select. Checking advance.`
        );
        checkAndAdvanceIfComplete(currentClusterId);
        return;
      }

      // 🔧 NEW: Check if this is a recent manual selection and should not auto-advance
      const isRecentManualSelection =
        manualClusterSelection.clusterId === currentClusterId &&
        Date.now() - manualClusterSelection.timestamp < 5000; // 5 second grace period

      console.log(
        `🔍 [EntityWorkflow] Selecting next unreviewed edge in cluster ${currentClusterId}, manual selection: ${isRecentManualSelection}`
      );

      const currentVizForSelection = currentVisualizationData;

      if (
        !currentVizForSelection?.links ||
        currentVizForSelection.links.length === 0
      ) {
        if (currentClusterId === activelyPagingClusterId) {
          const fullViz = visualizationData[currentClusterId]?.data;
          if (
            fullViz &&
            fullViz.links.length >
              largeClusterConnectionsPage * CONNECTION_PAGE_SIZE
          ) {
            console.log(
              `No more unreviewed links on current page ${largeClusterConnectionsPage} of large cluster ${currentClusterId}. User may need to advance page.`
            );
            return;
          }
        }

        // 🔧 ENHANCED: Handle manual selections differently
        if (isRecentManualSelection) {
          console.log(
            `🚫 [EntityWorkflow] No links in current view for manually selected cluster ${currentClusterId}. Not auto-advancing due to recent manual selection.`
          );
          return; // Don't auto-advance if this was a recent manual selection
        }

        console.log(
          `No links in current view for cluster ${currentClusterId}. Advancing to next cluster.`
        );
        advanceToNextCluster();
        return;
      }

      const { links } = currentVizForSelection;
      let startIdx = 0;
      const referenceEdgeId =
        afterEdgeId || lastReviewedEdgeId || selectedEdgeId;

      if (referenceEdgeId) {
        const idx = links.findIndex((l) => l.id === referenceEdgeId);
        if (idx !== -1) {
          startIdx = idx + 1;
        } else {
          console.warn(
            `Ref edge ${referenceEdgeId} not found in current view. Starting from beginning of current view.`
          );
        }
      }

      for (let i = 0; i < links.length; i++) {
        const link = links[(startIdx + i) % links.length];
        if (!isEdgeReviewed(link.id)) {
          console.log(
            `Next unreviewed edge found in current view: ${link.id}. Selecting.`
          );
          setSelectedEdgeIdAction(link.id);
          return;
        }
      }

      // 🔧 ENHANCED: Handle completion differently for manual selections
      if (isRecentManualSelection) {
        console.log(
          `🚫 [EntityWorkflow] No unreviewed edges in manually selected cluster ${currentClusterId} for filter '${workflowFilter}'. Not auto-advancing.`
        );
        setSelectedEdgeIdAction(null);
        return;
      }

      // Original logic for auto-navigation
      console.log(
        `No more unreviewed edges in current view of cluster ${currentClusterId} for filter '${workflowFilter}'. Advancing.`
      );
      setSelectedEdgeIdAction(null);

      const nextCluster = findNextViableCluster();

      if (nextCluster === "next_page") {
        const nextPageToLoad = clusters.page + 1;
        if (nextPageToLoad <= Math.ceil(clusters.total / clusters.limit)) {
          console.log(
            `📄 [selectNextUnreviewedEdge] Loading next page (${nextPageToLoad}) of clusters.`
          );
          loadClusterProgress(nextPageToLoad, clusters.limit);
        } else {
          toast({
            title: "Workflow Complete",
            description: `All clusters have been reviewed for the current filter (${workflowFilter}).`,
          });
        }
      } else if (nextCluster) {
        console.log(
          `➡️ [selectNextUnreviewedEdge] Advancing to next unreviewed cluster: ${nextCluster.id}`
        );
        handleSetSelectedClusterId(nextCluster.id, false); // Mark as automatic selection
      } else {
        toast({
          title: "Workflow Complete",
          description: `All clusters have been reviewed for the current filter (${workflowFilter}).`,
        });
        console.log(
          "✅ [selectNextUnreviewedEdge] All clusters reviewed for current filter."
        );
      }
    },
    [
      selectedClusterId,
      selectedEdgeId,
      lastReviewedEdgeId,
      manualClusterSelection, // 🔧 NEW: Add manual selection tracking
      checkAndAdvanceIfComplete,
      getClusterById,
      currentVisualizationData,
      activelyPagingClusterId,
      largeClusterConnectionsPage,
      visualizationData,
      setSelectedEdgeIdAction,
      isEdgeReviewed,
      workflowFilter,
      advanceToNextCluster,
      findNextViableCluster,
      clusters,
      loadClusterProgress,
      handleSetSelectedClusterId,
      toast,
    ]
  );

  // 🔧 PHASE 1: Enhanced submitEdgeReview with optimistic progress updates
  const submitEdgeReview = useCallback(
    async (edgeId: string, decision: GroupReviewDecision, notes?: string) => {
      if (!user?.id || !selectedOpinion) {
        toast({
          title: "Auth Error",
          description: "Login required and opinion must be selected.",
          variant: "destructive",
        });
        return;
      }
      if (!selectedClusterId) {
        toast({
          title: "Selection Error", 
          description: "Cluster must be selected.",
          variant: "destructive",
        });
        return;
      }

      console.log(`🚀 [EntityWorkflow] Submitting edge review: ${edgeId} -> ${decision}`);

      // Set submission status
      setEdgeSubmissionStatus((prev) => ({
        ...prev,
        [edgeId]: { isSubmitting: true, error: null },
      }));

      // 🔧 PHASE 1: Apply enhanced optimistic update with progress calculation
      const newStatus: BaseLink["status"] =
        decision === "ACCEPTED" ? "CONFIRMED_MATCH" : "CONFIRMED_NON_MATCH";
      
      const revertOptimisticUpdate = updateEdgeStatusOptimistically(
        selectedClusterId,
        edgeId,
        newStatus,
        true // wasReviewed = true
      );

      try {
        const payload: EdgeReviewApiPayload = {
          decision: decision as "ACCEPTED" | "REJECTED",
          reviewerId: user.id,
          notes,
          type: resolutionMode,
          disconnectDependentServices: disconnectDependentServicesEnabled,
        };

        const response = await postEdgeReview(edgeId, payload, selectedOpinion);

        console.log(`✅ [EntityWorkflow] Edge review submitted successfully: ${edgeId}`);

        // Clear submission status on success
        setEdgeSubmissionStatus((prev) => ({
          ...prev,
          [edgeId]: { isSubmitting: false, error: null },
        }));

        stateActions.setLastReviewedEdgeId(edgeId);

        // Show additional feedback if dependent services were disconnected
        if (
          response.dependentServicesDisconnected &&
          response.dependentServicesDisconnected > 0
        ) {
          toast({
            title: "Review Submitted",
            description: `Edge reviewed successfully. ${response.dependentServicesDisconnected} dependent service matches were also disconnected.`,
          });
        }

        // 🔧 PHASE 1: Handle cluster finalization with optimistic update
        if (response.clusterFinalized && selectedClusterId) {
          console.log(`🎉 [EntityWorkflow] Cluster ${selectedClusterId} finalized`);
          updateClusterCompletionOptimistically(selectedClusterId, true);

          if (isAutoAdvanceEnabled) {
            console.log(`🚀 [EntityWorkflow] Auto-advancing to next cluster after finalization`);
            setTimeout(() => {
              advanceToNextCluster();
            }, 100);
          }
        } else {
          console.log(`🔍 [EntityWorkflow] Looking for next unreviewed edge`);
          setTimeout(() => {
            selectNextUnreviewedEdge(edgeId);
          }, 100);
        }

        // Note: We don't revert the optimistic update on success since the server confirmed it
      } catch (error) {
        const errorMessage =
          (error as Error).message || "An unknown error occurred.";
        console.error(
          `❌ [EntityWorkflow] Failed to submit review for edge ${edgeId}:`,
          error
        );

        // 🔧 PHASE 1: Revert optimistic update on error (including progress)
        revertOptimisticUpdate();

        setEdgeSubmissionStatus((prev) => ({
          ...prev,
          [edgeId]: { isSubmitting: false, error: errorMessage },
        }));

        toast({
          title: "Submission Failed",
          description: errorMessage,
          variant: "destructive",
        });
      }
    },
    [
      user,
      selectedClusterId,
      selectedOpinion,
      resolutionMode,
      toast,
      disconnectDependentServicesEnabled,
      stateActions,
      updateEdgeStatusOptimistically, // 🔧 PHASE 1: Enhanced function
      updateClusterCompletionOptimistically,
      isAutoAdvanceEnabled,
      advanceToNextCluster,
      selectNextUnreviewedEdge,
    ]
  );

  const selectNextUnreviewedInCluster = useCallback(() => {
    const currentClusterId = selectedClusterId;
    if (!currentClusterId) return;

    const clusterDetail = getClusterById(currentClusterId);
    if (clusterDetail?.wasReviewed) {
      checkAndAdvanceIfComplete(currentClusterId);
      return;
    }

    const currentLinksForNav = currentVisualizationData?.links || [];
    if (currentLinksForNav.length === 0) {
      checkAndAdvanceIfComplete(currentClusterId);
      return;
    }

    const unreviewedLinksInView = currentLinksForNav.filter(
      (l) => !isEdgeReviewed(l.id)
    );

    if (unreviewedLinksInView.length === 0) {
      toast({
        title: "Info",
        description: "No unreviewed connections in the current view.",
      });
      checkAndAdvanceIfComplete(currentClusterId);
      return;
    }

    let nextEdgeToSelectId: string | null = null;
    if (selectedEdgeId) {
      const currentIndexInUnreviewedView = unreviewedLinksInView.findIndex(
        (l) => l.id === selectedEdgeId
      );
      if (currentIndexInUnreviewedView !== -1) {
        nextEdgeToSelectId =
          unreviewedLinksInView[
            (currentIndexInUnreviewedView + 1) % unreviewedLinksInView.length
          ].id;
      } else {
        nextEdgeToSelectId = unreviewedLinksInView[0].id;
      }
    } else {
      nextEdgeToSelectId = unreviewedLinksInView[0].id;
    }

    if (nextEdgeToSelectId) {
      setSelectedEdgeIdAction(nextEdgeToSelectId);
    }
  }, [
    selectedClusterId,
    selectedEdgeId,
    getClusterById,
    checkAndAdvanceIfComplete,
    toast,
    currentVisualizationData,
    setSelectedEdgeIdAction,
    isEdgeReviewed,
  ]);

  const selectPreviousUnreviewedInCluster = useCallback(() => {
    const currentClusterId = selectedClusterId;
    if (!currentClusterId) return;

    const clusterDetail = getClusterById(currentClusterId);
    if (clusterDetail?.wasReviewed) {
      checkAndAdvanceIfComplete(currentClusterId);
      return;
    }

    const currentLinksForNav = currentVisualizationData?.links || [];
    if (currentLinksForNav.length === 0) {
      checkAndAdvanceIfComplete(currentClusterId);
      return;
    }

    const unreviewedLinksInView = currentLinksForNav.filter(
      (l) => !isEdgeReviewed(l.id)
    );

    if (unreviewedLinksInView.length === 0) {
      toast({
        title: "Info",
        description: "No unreviewed connections in the current view.",
      });
      checkAndAdvanceIfComplete(currentClusterId);
      return;
    }

    let prevEdgeToSelectId: string | null = null;
    if (selectedEdgeId) {
      const currentIndexInUnreviewedView = unreviewedLinksInView.findIndex(
        (l) => l.id === selectedEdgeId
      );
      if (currentIndexInUnreviewedView !== -1) {
        prevEdgeToSelectId =
          unreviewedLinksInView[
            (currentIndexInUnreviewedView - 1 + unreviewedLinksInView.length) %
              unreviewedLinksInView.length
          ].id;
      } else {
        prevEdgeToSelectId =
          unreviewedLinksInView[unreviewedLinksInView.length - 1].id;
      }
    } else {
      prevEdgeToSelectId =
        unreviewedLinksInView[unreviewedLinksInView.length - 1].id;
    }

    if (prevEdgeToSelectId) {
      setSelectedEdgeIdAction(prevEdgeToSelectId);
    }
  }, [
    selectedClusterId,
    selectedEdgeId,
    getClusterById,
    checkAndAdvanceIfComplete,
    toast,
    currentVisualizationData,
    setSelectedEdgeIdAction,
    isEdgeReviewed,
  ]);

  // ========================================================================
  // Large Cluster Paging Functions
  // ========================================================================

  const loadConnectionDataForLinkPage = useCallback(
    async (
      clusterId: string,
      pageToLoad: number,
      isPrefetch: boolean = false
    ) => {
      const viz = visualizationData[clusterId]?.data;
      if (!viz || !viz.links) {
        console.warn(
          `🚫 [EntityWorkflow] loadConnectionDataForLinkPage: No visualization data or links for cluster ${clusterId}.`
        );
        if (!isPrefetch) stateActions.setIsLoadingConnectionPageData(false);
        return;
      }

      if (!isPrefetch) {
        stateActions.setIsLoadingConnectionPageData(true);
      }

      const startIndex = (pageToLoad - 1) * CONNECTION_PAGE_SIZE;
      const endIndex = startIndex + CONNECTION_PAGE_SIZE;
      const linksForPage = viz.links.slice(startIndex, endIndex);

      if (linksForPage.length === 0) {
        console.log(
          `🚫 [EntityWorkflow] loadConnectionDataForLinkPage: No links for page ${pageToLoad} in cluster ${clusterId}.`
        );
        if (!isPrefetch) stateActions.setIsLoadingConnectionPageData(false);
        return;
      }

      const connectionItemsToFetch = linksForPage
        .map((link) => ({ edgeId: link.id, itemType: resolutionMode }))
        .filter((item) => {
          const connState = connectionData[item.edgeId];
          return !connState || !connState.data || connState.error;
        });

      console.log(
        `🔄 [EntityWorkflow] loadConnectionDataForLinkPage: Fetching connection data for ${connectionItemsToFetch.length} links on page ${pageToLoad} of cluster ${clusterId}. Prefetch: ${isPrefetch}`
      );

      if (connectionItemsToFetch.length > 0) {
        try {
          await loadBulkConnections(connectionItemsToFetch);
        } catch (error) {
          console.error(
            `❌ [EntityWorkflow] Error loading connection data for page ${pageToLoad}, cluster ${clusterId}:`,
            error
          );
          toast({
            title: "Error Loading Connections",
            description: (error as Error).message,
            variant: "destructive",
          });
        }
      }

      if (!isPrefetch) {
        stateActions.setIsLoadingConnectionPageData(false);
        const totalLinks = viz.links.length;
        if (endIndex < totalLinks) {
          console.log(
            `🔄 [EntityWorkflow] Prefetching connection data for page ${
              pageToLoad + 1
            } of cluster ${clusterId}`
          );
          loadConnectionDataForLinkPage(clusterId, pageToLoad + 1, true).catch(
            (err) => {
              console.warn(
                `❌ [EntityWorkflow] Error prefetching connection data for page ${
                  pageToLoad + 1
                }:`,
                err
              );
            }
          );
        }
      }
    },
    [
      visualizationData,
      resolutionMode,
      connectionData,
      toast,
      loadBulkConnections,
      stateActions,
    ]
  );

  const initializeLargeClusterConnectionPaging = useCallback(
    async (clusterId: string) => {
      console.log(
        `🚀 [EntityWorkflow] Initializing connection paging for large cluster: ${clusterId}`
      );
      stateActions.setActivelyPagingClusterId(clusterId);
      stateActions.setLargeClusterConnectionsPage(1);
      stateActions.setIsLoadingConnectionPageData(true);

      let viz = visualizationData[clusterId]?.data;
      if (
        !viz ||
        visualizationData[clusterId]?.error ||
        !visualizationData[clusterId]?.data?.links
      ) {
        console.log(
          `🔄 [EntityWorkflow] Fetching/Re-fetching visualization data for large cluster ${clusterId} before paging connections.`
        );
        try {
          await loadVisualizationDataForClusters([
            { clusterId, itemType: resolutionMode },
          ]);
          viz = visualizationData[clusterId]?.data;
          if (!viz || !viz.links)
            throw new Error(
              "Visualization data (with links) still not available after fetch."
            );

          const nodeIdsFromViz = viz.nodes.map((node) => ({
            id: node.id,
            nodeType: resolutionMode,
          }));
          if (nodeIdsFromViz.length > 0) {
            await loadBulkNodeDetails(nodeIdsFromViz);
          }
        } catch (error) {
          toast({
            title: "Error Initializing Cluster",
            description: `Failed to load visualization for ${clusterId}: ${
              (error as Error).message
            }`,
            variant: "destructive",
          });
          stateActions.setIsLoadingConnectionPageData(false);
          stateActions.setActivelyPagingClusterId(null);
          stateActions.setLargeClusterConnectionsPage(0);
          return;
        }
      } else {
        const nodeIdsFromViz = viz.nodes.map((node) => ({
          id: node.id,
          nodeType: resolutionMode,
        }));
        if (nodeIdsFromViz.length > 0) {
          await loadBulkNodeDetails(nodeIdsFromViz);
        }
      }

      await loadConnectionDataForLinkPage(clusterId, 1, false);
      setSelectedEdgeIdAction(null);
    },
    [
      resolutionMode,
      visualizationData,
      toast,
      loadConnectionDataForLinkPage,
      loadBulkNodeDetails,
      loadVisualizationDataForClusters,
      setSelectedEdgeIdAction,
      stateActions,
    ]
  );

  const viewNextConnectionPage = useCallback(
    async (clusterId: string) => {
      if (clusterId !== activelyPagingClusterId) {
        console.warn(
          "🚫 [EntityWorkflow] viewNextConnectionPage called for a cluster that is not actively paging."
        );
        return;
      }
      const viz = visualizationData[clusterId]?.data;
      if (!viz || !viz.links) {
        console.warn(
          "🚫 [EntityWorkflow] No visualization data to page for next connections."
        );
        return;
      }
      const totalLinks = viz.links.length;
      const nextPage = largeClusterConnectionsPage + 1;
      const startIndexForNextPage = (nextPage - 1) * CONNECTION_PAGE_SIZE;

      if (startIndexForNextPage < totalLinks) {
        console.log(
          `🔄 [EntityWorkflow] Viewing next connection page ${nextPage} for cluster ${clusterId}`
        );
        stateActions.setLargeClusterConnectionsPage(nextPage);
        setSelectedEdgeIdAction(null);
        await loadConnectionDataForLinkPage(clusterId, nextPage, false);
      } else {
        toast({
          title: "No More Connections",
          description:
            "You have reached the end of the connections for this cluster.",
        });
      }
    },
    [
      activelyPagingClusterId,
      largeClusterConnectionsPage,
      visualizationData,
      toast,
      loadConnectionDataForLinkPage,
      setSelectedEdgeIdAction,
      stateActions,
    ]
  );

  // ========================================================================
  // Effects
  // ========================================================================

  // Load opinion preferences on opinion change
  useEffect(() => {
    if (selectedOpinion && user?.id) {
      console.log(
        `🔄 [EntityWorkflow] Loading preferences for opinion: ${selectedOpinion}`
      );

      getOpinionPreferences(selectedOpinion)
        .then((response) => {
          console.log(
            `✅ [EntityWorkflow] Loaded preferences for ${selectedOpinion}:`,
            response.preferences
          );
          stateActions.setDisconnectDependentServicesEnabled(
            response.preferences.disconnectDependentServices
          );
        })
        .catch((error) => {
          console.error(
            "❌ [EntityWorkflow] Failed to load opinion preferences:",
            error
          );
          // Fallback to default (false) on error
          stateActions.setDisconnectDependentServicesEnabled(false);

          // Only show toast for non-404 errors (404 means no preferences saved yet)
          if (
            !error.message?.includes("404") &&
            !error.message?.includes("Not found")
          ) {
            toast({
              title: "Warning",
              description: "Could not load saved preferences. Using defaults.",
              variant: "destructive",
            });
          }
        });
    } else if (!selectedOpinion) {
      // No opinion selected, reset to default
      stateActions.setDisconnectDependentServicesEnabled(false);
    }
  }, [selectedOpinion, user?.id, toast, stateActions]);

  // Effect to auto-select edge when visualization data loads
  useEffect(() => {
    if (
      selectedClusterId &&
      currentVisualizationData?.links &&
      !selectedEdgeId
    ) {
      console.log(
        `🔄 [EntityWorkflow] Cluster ${selectedClusterId} selected, (paged) viz data loaded, no edge selected. Evaluating next action using selectNextUnreviewedEdge.`
      );
      const justLoadedNewPageForLargeCluster =
        selectedClusterId === activelyPagingClusterId &&
        largeClusterConnectionsPage > 0 &&
        !lastReviewedEdgeId;

      if (
        isAutoAdvanceEnabled ||
        !lastReviewedEdgeId ||
        justLoadedNewPageForLargeCluster
      ) {
        selectNextUnreviewedEdge();
      }
    }
  }, [
    selectedClusterId,
    currentVisualizationData?.links?.length,
    selectedEdgeId,
    selectNextUnreviewedEdge,
    activelyPagingClusterId,
    largeClusterConnectionsPage,
    lastReviewedEdgeId,
    isAutoAdvanceEnabled,
  ]);

  // Effect to auto-load connection data when edge is selected
  useEffect(() => {
    if (!selectedEdgeId) return;

    const currentEdgeState = connectionData[selectedEdgeId];
    const needsLoad =
      !currentEdgeState?.data &&
      !currentEdgeState?.loading &&
      !currentEdgeState?.error;

    if (needsLoad) {
      console.log(
        `🔄 [EntityWorkflow] Auto-loading connection data for edge: ${selectedEdgeId}`
      );
      // This would need to be handled by EntityDataContext
      // For now, we'll use the data context's function
      data.loadSingleConnectionData(selectedEdgeId);
    }
  }, [selectedEdgeId, connectionData, data]);

  // Auto-select cluster when clusters load
  useEffect(() => {
    if (!selectedClusterId && clusters.data.length > 0 && !clusters.loading) {
      const firstNonReviewedCluster = clusters.data.find((c) => !c.wasReviewed);
      if (firstNonReviewedCluster) {
        console.log(
          "🎯 [EntityWorkflow] Auto-selecting first unreviewed cluster:",
          firstNonReviewedCluster.id
        );
        handleSetSelectedClusterId(firstNonReviewedCluster.id);
      } else if (clusters.data.length > 0) {
        const firstCluster = clusters.data[0];
        console.log(
          "🎯 [EntityWorkflow] All clusters on current page are reviewed. Auto-selecting first for viewing:",
          firstCluster.id
        );
        handleSetSelectedClusterId(firstCluster.id);
      } else if (clusters.total === 0) {
        stateActions.setSelectedClusterId(null);
      }
    } else if (
      selectedClusterId &&
      clusters.data.length === 0 &&
      !clusters.loading
    ) {
      console.log(
        "🎯 [EntityWorkflow] Current selected cluster no longer exists in an empty cluster list. Deselecting."
      );
      stateActions.setSelectedClusterId(null);
    }
  }, [
    selectedClusterId,
    clusters.data,
    clusters.loading,
    clusters.total,
    handleSetSelectedClusterId,
    stateActions,
  ]);

  // Effect to handle workflow filter changes (Edge Case 1)
  useEffect(() => {
    // Track filter changes
    if (lastWorkflowFilter !== workflowFilter) {
      setLastWorkflowFilter(workflowFilter);

      // Check if current edge is still valid for the new filter
      if (selectedEdgeId && selectedClusterId) {
        const isValidForNewFilter = isCurrentEdgeValidForFilter(workflowFilter);

        if (!isValidForNewFilter) {
          console.log(
            `🔍 [EntityWorkflow] Current edge ${selectedEdgeId} is not valid for filter ${workflowFilter}. Deselecting and finding next.`
          );

          // Clear the current edge selection
          setSelectedEdgeIdAction(null);

          // Trigger progress update by refreshing cluster progress
          data.loadClusterProgress(clusters.page, clusters.limit);

          // Find next valid edge after a brief delay to allow progress update
          setTimeout(() => {
            selectNextUnreviewedEdge();
          }, 100);
        } else {
          // Edge is still valid, but trigger progress update to refresh UI
          data.loadClusterProgress(clusters.page, clusters.limit);
        }
      } else {
        // No edge selected, just update progress
        data.loadClusterProgress(clusters.page, clusters.limit);
      }
    }
  }, [
    workflowFilter,
    lastWorkflowFilter,
    selectedEdgeId,
    selectedClusterId,
    isCurrentEdgeValidForFilter,
    setSelectedEdgeIdAction,
    selectNextUnreviewedEdge,
    data,
    clusters,
  ]);

  useEffect(() => {
    if (selectedClusterId && currentVisualizationData?.links && !selectedEdgeId) {
      console.log(`🔄 [EntityWorkflow] Cluster ${selectedClusterId} selected, viz data loaded, no edge selected.`);
      
      const justLoadedNewPageForLargeCluster =
        selectedClusterId === activelyPagingClusterId &&
        largeClusterConnectionsPage > 0 &&
        !lastReviewedEdgeId;
  
      const isRecentManualSelection = manualClusterSelection.clusterId === selectedClusterId && 
        (Date.now() - manualClusterSelection.timestamp) < 5000;
  
      // 🔧 ENHANCED: More intelligent auto-advance logic
      if (isAutoAdvanceEnabled || !lastReviewedEdgeId || justLoadedNewPageForLargeCluster) {
        // Check if there are any unreviewed edges in the current filter view
        const hasUnreviewedInCurrentView = currentVisualizationData.links.some(link => !isEdgeReviewed(link.id));
        
        if (hasUnreviewedInCurrentView || isRecentManualSelection) {
          selectNextUnreviewedEdge();
        } else if (!isRecentManualSelection) {
          // No unreviewed edges in current view and not a manual selection - advance
          console.log(`🚀 [EntityWorkflow] No unreviewed edges in current filter view for cluster ${selectedClusterId}. Auto-advancing.`);
          selectNextUnreviewedEdge();
        } else {
          // Recent manual selection with no unreviewed edges - don't advance
          console.log(`🚫 [EntityWorkflow] Recent manual selection of cluster ${selectedClusterId} with no unreviewed edges. Not auto-advancing.`);
        }
      }
    }
  }, [
    selectedClusterId,
    currentVisualizationData?.links?.length,
    selectedEdgeId,
    selectNextUnreviewedEdge,
    activelyPagingClusterId,
    largeClusterConnectionsPage,
    lastReviewedEdgeId,
    isAutoAdvanceEnabled,
    manualClusterSelection, // 🔧 NEW: Track manual selections
    isEdgeReviewed,
  ]);

  // ========================================================================
  // Context Value
  // ========================================================================

  const contextValue: EntityWorkflowContextType = {
    // Computed progress
    clusterProgress,
    overallProgress,
    edgeSelectionInfo,

    // Current computed data
    currentVisualizationData,
    currentConnectionData,
    selectedClusterDetails,

    // Edge submission state
    edgeSubmissionStatus,

    // Business actions
    selectNextUnreviewedEdge,
    selectPreviousUnreviewedInCluster,
    selectNextUnreviewedInCluster,
    advanceToNextCluster,
    checkAndAdvanceIfComplete,

    // Review actions
    submitEdgeReview,
    enableDisconnectDependentServices,

    // Large cluster paging
    initializeLargeClusterConnectionPaging,
    viewNextConnectionPage,
    loadConnectionDataForLinkPage,

    // Enhanced selection actions
    handleSetSelectedClusterId,
    setSelectedEdgeIdAction,

    // Progress queries
    getClusterProgress,
    getClusterProgressUnfiltered,
    getClusterProgressCrossSource,
    canAdvanceToNextCluster,
    isEdgeReviewed,
    getEdgeStatus,
    getEdgeSubmissionStatus,

    // 🔧 NEW: Filter-aware navigation helpers
    isClusterCompleteForCurrentFilter,
    findNextViableCluster,

    // Cross-source detection
    isCrossSourceConnection,
  };

  return (
    <EntityWorkflowContext.Provider value={contextValue}>
      {children}
    </EntityWorkflowContext.Provider>
  );
}

// ============================================================================
// Hook for Consuming Context
// ============================================================================

export function useEntityWorkflow(): EntityWorkflowContextType {
  const context = useContext(EntityWorkflowContext);

  if (context === undefined) {
    throw new Error(
      "useEntityWorkflow must be used within an EntityWorkflowProvider"
    );
  }

  return context;
}

// ============================================================================
// Utility Hooks
// ============================================================================

/**
 * Hook for components that only need progress data
 */
export function useEntityProgress() {
  const {
    clusterProgress,
    overallProgress,
    getClusterProgress,
    getClusterProgressUnfiltered,
    getClusterProgressCrossSource,
  } = useEntityWorkflow();

  return {
    clusterProgress,
    overallProgress,
    getClusterProgress,
    getClusterProgressUnfiltered,
    getClusterProgressCrossSource,
  };
}

/**
 * Hook for components that only need edge selection
 */
export function useEdgeSelection() {
  const {
    edgeSelectionInfo,
    selectNextUnreviewedEdge,
    selectPreviousUnreviewedInCluster,
    selectNextUnreviewedInCluster,
    setSelectedEdgeIdAction,
    isEdgeReviewed,
    getEdgeStatus,
  } = useEntityWorkflow();

  return {
    edgeSelectionInfo,
    selectNextUnreviewedEdge,
    selectPreviousUnreviewedInCluster,
    selectNextUnreviewedInCluster,
    setSelectedEdgeIdAction,
    isEdgeReviewed,
    getEdgeStatus,
  };
}

/**
 * Hook for components that only need review actions
 */
export function useEntityReview() {
  const {
    submitEdgeReview,
    enableDisconnectDependentServices,
    edgeSubmissionStatus,
    getEdgeSubmissionStatus,
  } = useEntityWorkflow();

  return {
    submitEdgeReview,
    enableDisconnectDependentServices,
    edgeSubmissionStatus,
    getEdgeSubmissionStatus,
  };
}

/**
 * Hook for components that need current data
 */
export function useCurrentEntityData() {
  const {
    currentVisualizationData,
    currentConnectionData,
    selectedClusterDetails,
  } = useEntityWorkflow();

  return {
    currentVisualizationData,
    currentConnectionData,
    selectedClusterDetails,
  };
}
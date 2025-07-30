/*
================================================================================
|
|   File: /context/entity-workflow-context.tsx - Enhanced with Pagination & Dual-Action Workflow
|
|   Description: Workflow management with pagination and dual-action manual override for audit mode
|   - Manages business logic and complex workflows
|   - Implements cursor-based pagination for large clusters
|   - Implements dual-action submitEdgeReview for audit mode
|   - Handles auto-marking of audit decisions as reviewed
|   - Provides audit-aware navigation and filtering
|
================================================================================
*/
"use client";

import {
  EntityCluster,
  VisualizationData,
  EntityConnectionDataResponse,
  ClusterReviewProgress,
  GroupReviewDecision,
  BaseLink,
  BaseNode,
  EdgeReviewApiPayload,
  WorkflowFilter,
  DisconnectDependentServicesRequest,
  DisconnectDependentServicesResponse,
  EdgeSelectionInfo,
} from "@/types/entity-resolution";

import type {
  AuditMode,
  PostProcessingAuditParams,
} from "@/types/post-processing";

import {
  postEdgeReview,
  postDisconnectDependentServices,
  getOpinionPreferences,
  updateOpinionPreferences,
  bulkMarkPostProcessingReviewed,
} from "@/utils/api-client";
import {
  createContext,
  useCallback,
  useContext,
  useState,
  useEffect,
  useMemo,
  type ReactNode,
  useRef,
} from "react";
import { useAuth } from "./auth-context";
import { useEntityState } from "./entity-state-context";
import { useEntityData } from "./entity-data-context";
import { useToast } from "@/hooks/use-toast";

// ============================================================================
// Constants
// ============================================================================

const LARGE_CLUSTER_THRESHOLD = 200;
const CONNECTION_PAGE_SIZE = 200;

// ============================================================================
// Types and Interfaces
// ============================================================================

export interface OverallWorkflowProgress {
  totalPendingDecisions: number;
  totalCompletedDecisions: number;
  overallProgressPercentage: number;
  clustersComplete: number;
  totalClusters: number;
}

export interface EntityWorkflowContextType {
  // Computed progress
  clusterProgress: Record<string, ClusterReviewProgress>;
  overallProgress: OverallWorkflowProgress;
  edgeSelectionInfo: EdgeSelectionInfo;

  // Current computed data
  currentVisualizationData: VisualizationData | null;
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

  // Audit-specific workflow functions
  initializeAuditMode: () => Promise<void>;
  findNextAuditCluster: () => Promise<void>;
  isClusterCompleteForAudit: (clusterId: string) => boolean;
  getAuditNavigationState: () => {
    currentClusterIndex: number;
    totalClusters: number;
    hasNextCluster: boolean;
    hasPreviousCluster: boolean;
  };

  // Review actions with dual-action support
  submitEdgeReview: (
    edgeId: string,
    decision: GroupReviewDecision,
    notes?: string
  ) => Promise<void>;
  enableDisconnectDependentServices: () => Promise<void>;

  // ✨ REFACTORED/NEW pagination actions
  initializeLargeClusterConnectionPaging: (clusterId: string) => Promise<void>;
  loadMoreConnectionsForCluster: (clusterId: string) => Promise<void>;

  // Enhanced selection actions
  handleSetSelectedClusterId: (
    id: string | null,
    isManualSelection?: boolean
  ) => Promise<void>;
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

  // Filter-aware navigation helpers
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
// Provider Component - Enhanced with Pagination & Dual-Action Workflow
// ============================================================================

export function EntityWorkflowProvider({ children }: { children: ReactNode }) {
  const { user, selectedOpinion } = useAuth();
  const { toast } = useToast();
  const state = useEntityState();
  const data = useEntityData();

  const {
    selectedClusterId,
    selectedEdgeId,
    lastReviewedEdgeId,
    resolutionMode,
    workflowFilter,
    auditMode,
    postProcessingFilter,
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
    postProcessingAuditData,
    clustersWithAuditData,
    loadClusterProgress,
    loadVisualizationDataForClusters,
    loadMoreConnectionsForCluster: dataLoadMore, // Renamed to avoid conflict
    loadBulkConnections,
    loadBulkNodeDetails,
    loadPostProcessingAuditData,
    loadClustersWithAuditData,
    bulkMarkDecisionsReviewed,
    getClusterById,
    isVisualizationDataLoaded,
    updateEdgeStatusOptimistically,
    updateClusterCompletionOptimistically,
    isClustersWithAuditDataLoading,
    getAuditFilteredVisualizationData,
    loadAuditClusterRichData,
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

  const initialAuditSelectionDone = useRef(false);

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
  // Audit-specific workflow functions
  // ========================================================================

  const initializeAuditMode = useCallback(async () => {
    if (auditMode !== "post_processing_audit" || !selectedOpinion) return;

    console.log("🔄 [EntityWorkflow] Initializing audit mode");

    initialAuditSelectionDone.current = false;

    // ✅ SIMPLE FIX: Don't specify entity type initially
    const auditParams: PostProcessingAuditParams = {
      // ❌ REMOVE: entityType: resolutionMode, // Let API return all available audit data
      postProcFilter: postProcessingFilter || undefined,
      reviewedByHuman: false,
      page: 1,
      limit: 20,
      workflowFilter: workflowFilter || undefined,
    };

    console.log(
      "🔄 [EntityWorkflow] Loading audit data without entity type filter:",
      auditParams
    );

    // Data loading is handled by EntityDataContext
  }, [auditMode, selectedOpinion, postProcessingFilter, workflowFilter]);

  const getCorrectItemType = useCallback(
    (clusterId?: string): "entity" | "service" => {
      const targetClusterId = clusterId || selectedClusterId;

      if (auditMode === "post_processing_audit" && targetClusterId) {
        const auditClusterInfo = clustersWithAuditData?.data?.clusters?.find(
          (c) => c.id === targetClusterId
        );

        if (auditClusterInfo) {
          console.log(
            `🔍 [EntityWorkflow] Using audit cluster entityType: ${auditClusterInfo.entityType} for cluster ${targetClusterId}`
          );
          return auditClusterInfo.entityType;
        } else {
          console.warn(
            `⚠️ [EntityWorkflow] No audit cluster info found for ${targetClusterId}, falling back to resolutionMode: ${resolutionMode}`
          );
        }
      }

      return resolutionMode;
    },
    [auditMode, selectedClusterId, clustersWithAuditData, resolutionMode]
  );

  const findNextAuditCluster = useCallback(async () => {
    if (auditMode !== "post_processing_audit") return;

    const clusters = clustersWithAuditData?.data?.clusters || [];
    const currentIndex = clusters.findIndex((c) => c.id === selectedClusterId);

    for (let i = currentIndex + 1; i < clusters.length; i++) {
      const cluster = clusters[i];
      if (cluster.unreviewedDecisionsCount > 0) {
        console.log(
          `📍 [EntityWorkflow] Found next audit cluster: ${cluster.id}`
        );
        stateActions.setSelectedClusterId(cluster.id);
        return;
      }
    }

    const auditData = clustersWithAuditData?.data;
    if (
      auditData &&
      auditData.page < Math.ceil(auditData.total / auditData.limit)
    ) {
      console.log("📄 [EntityWorkflow] Loading next page of audit clusters");

      const nextPage = auditData.page + 1;
      const auditParams: PostProcessingAuditParams = {
        entityType: resolutionMode,
        postProcFilter: postProcessingFilter || undefined,
        reviewedByHuman: false,
        page: nextPage,
        limit: auditData.limit,
        workflowFilter: workflowFilter || undefined,
      };

      await loadClustersWithAuditData(auditParams);
    } else {
      toast({
        title: "Audit Complete",
        description: "All automated decisions have been reviewed.",
      });
    }
  }, [
    auditMode,
    clustersWithAuditData,
    selectedClusterId,
    resolutionMode,
    postProcessingFilter,
    stateActions,
    loadClustersWithAuditData,
    toast,
    workflowFilter,
  ]);

  const isClusterCompleteForAudit = useCallback(
    (clusterId: string) => {
      if (auditMode !== "post_processing_audit") return false;

      const clusters = clustersWithAuditData?.data?.clusters || [];
      const cluster = clusters.find((c) => c.id === clusterId);

      return cluster ? cluster.unreviewedDecisionsCount === 0 : false;
    },
    [auditMode, clustersWithAuditData]
  );

  const getAuditNavigationState = useCallback(() => {
    const clusters = clustersWithAuditData?.data?.clusters || [];
    const currentIndex = clusters.findIndex((c) => c.id === selectedClusterId);

    return {
      currentClusterIndex: currentIndex,
      totalClusters: clusters.length,
      hasNextCluster: currentIndex < clusters.length - 1,
      hasPreviousCluster: currentIndex > 0,
    };
  }, [clustersWithAuditData, selectedClusterId]);

  // ========================================================================
  // Progress Calculation Functions
  // ========================================================================

  const getClusterProgressInternal = useCallback(
    (
      clusterIdToQuery: string,
      filter: WorkflowFilter
    ): ClusterReviewProgress => {
      const clusterDetails = clusters.data.find(
        (c: EntityCluster) => c.id === clusterIdToQuery
      );
      const vizState = visualizationData[clusterIdToQuery];
      const currentVizData = vizState?.data;

      if (currentVizData?.links && currentVizData?.nodes) {
        let relevantLinks = currentVizData.links;

        if (filter === "cross-source-only") {
          relevantLinks = currentVizData.links.filter((link: BaseLink) =>
            isCrossSourceConnection(link, currentVizData.nodes)
          );
        }

        const totalEdges = relevantLinks.length;
        const reviewedEdges = relevantLinks.filter(
          (l: BaseLink) => l.wasReviewed
        ).length;
        const pendingReviewLinks = relevantLinks.filter(
          (l: BaseLink) => !l.wasReviewed
        );

        const progressPercentage =
          totalEdges > 0 ? Math.round((reviewedEdges / totalEdges) * 100) : 100;

        const confirmedMatches = relevantLinks.filter(
          (l: BaseLink) => l.status === "CONFIRMED_MATCH"
        ).length;
        const confirmedNonMatches = relevantLinks.filter(
          (l: BaseLink) => l.status === "CONFIRMED_NON_MATCH"
        ).length;

        return {
          totalEdges,
          reviewedEdges,
          pendingEdges: pendingReviewLinks.length,
          confirmedMatches,
          confirmedNonMatches,
          progressPercentage,
          isComplete: pendingReviewLinks.length === 0,
        };
      }

      if (clusterDetails) {
        if (typeof clusterDetails.groupCount === "number") {
          const total = clusterDetails.groupCount;

          if (clusterDetails.wasReviewed) {
            return {
              totalEdges: total,
              reviewedEdges: total,
              pendingEdges: 0,
              confirmedMatches: 0,
              confirmedNonMatches: 0,
              progressPercentage: 100,
              isComplete: true,
            };
          } else {
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

  const getClusterProgress = useCallback(
    (clusterIdToQuery: string): ClusterReviewProgress => {
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

      return getClusterProgressInternal(clusterIdToQuery, workflowFilter);
    },
    [serverProgress, getClusterProgressInternal, workflowFilter]
  );

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

      return getClusterProgressInternal(clusterIdToQuery, "all");
    },
    [serverProgress, getClusterProgressInternal]
  );

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

      return getClusterProgressInternal(clusterIdToQuery, "cross-source-only");
    },
    [serverProgress, getClusterProgressInternal]
  );

  // ========================================================================
  // Computed State
  // ========================================================================

  const clusterProgress = useMemo(() => {
    const reconstructed: Record<string, ClusterReviewProgress> = {};

    clusters.data.forEach((cluster: EntityCluster) => {
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

  const overallProgress = useMemo(() => {
    if (overallServerProgress) {
      return overallServerProgress.currentView;
    }

    let totalPending = 0;
    let totalCompleted = 0;
    let clustersComplete = 0;

    clusters.data.forEach((cluster: EntityCluster) => {
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

  // Get visualization data with audit filtering and pagination support
  const currentVisualizationData = useMemo((): VisualizationData | null => {
    if (!selectedClusterId) return null;

    // In audit mode, use filtered visualization data
    if (auditMode === "post_processing_audit") {
      if (postProcessingAuditData.loading) {
        console.log(
          `⏳ [EntityWorkflow] Audit data still loading for cluster ${selectedClusterId}, waiting...`
        );
        return null;
      }

      const vizState = visualizationData[selectedClusterId];
      if (vizState?.loading) {
        console.log(
          `⏳ [EntityWorkflow] Visualization data still loading for cluster ${selectedClusterId}, waiting...`
        );
        return null;
      }

      if (vizState?.data && !postProcessingAuditData.loading) {
        const filteredData =
          getAuditFilteredVisualizationData(selectedClusterId);

        if (!filteredData && vizState.data) {
          console.log(
            `ℹ️ [EntityWorkflow] No audit decisions for cluster ${selectedClusterId}, showing full cluster data`
          );
          return vizState.data;
        }

        return filteredData;
      }

      return null;
    }

    // Normal mode logic with pagination support
    const vizState = visualizationData[selectedClusterId];
    if (!vizState?.data) return null;

    let dataToReturn = vizState.data;

    // Apply workflow filter
    if (workflowFilter === "cross-source-only") {
      const filteredLinks = dataToReturn.links.filter((link: BaseLink) =>
        isCrossSourceConnection(link, dataToReturn.nodes)
      );
      dataToReturn = { ...dataToReturn, links: filteredLinks };
    }

    return dataToReturn;
  }, [
    selectedClusterId,
    auditMode,
    postProcessingAuditData.loading,
    visualizationData,
    workflowFilter,
    isCrossSourceConnection,
    getAuditFilteredVisualizationData,
  ]);

  const currentConnectionData =
    useMemo((): EntityConnectionDataResponse | null => {
      if (!selectedEdgeId) return null;
      const selectedEdgeConnectionState = connectionData[selectedEdgeId];
      if (!selectedEdgeConnectionState) return null;
      return selectedEdgeConnectionState.data || null;
    }, [selectedEdgeId, connectionData]);

  const selectedClusterDetails = useMemo((): EntityCluster | null => {
    if (!selectedClusterId) return null;

    if (auditMode === "post_processing_audit") {
      const auditCluster = clustersWithAuditData?.data?.clusters?.find(
        (c) => c.id === selectedClusterId
      );
      if (auditCluster) {
        return {
          id: auditCluster.id,
          name: `Audit Cluster ${auditCluster.id.substring(0, 8)}`,
          entityCount: auditCluster.totalDecisionsCount,
          groupCount: auditCluster.unreviewedDecisionsCount,
          averageCoherenceScore: null,
          wasReviewed: auditCluster.unreviewedDecisionsCount === 0,
        };
      }
    }

    return (
      clusters.data.find((c: EntityCluster) => c.id === selectedClusterId) ||
      null
    );
  }, [selectedClusterId, clusters.data, auditMode, clustersWithAuditData]);

  // ✨ ENHANCED: Edge selection info with pagination support
  const edgeSelectionInfo = useMemo((): EdgeSelectionInfo => {
    const defaultInfo: EdgeSelectionInfo = {
      currentEdgeId: selectedEdgeId,
      nextUnreviewedEdgeId: null,
      hasUnreviewedEdges: false,
      currentEdgeIndex: -1,
      totalEdges: 0,
      totalUnreviewedEdgesInCluster: 0,
      currentUnreviewedEdgeIndexInCluster: -1,
      totalEdgesInEntireCluster: 0,
      loadedLinksCount: 0,
      totalConnectionsInFilter: null,
    };

    const vizState = selectedClusterId
      ? visualizationData[selectedClusterId]
      : null;
    if (!vizState?.data) return defaultInfo;

    const { data } = vizState;
    const allLinksInCluster = data.links; // This is the accumulated list
    const totalConnections = data.totalConnections;

    defaultInfo.totalEdgesInEntireCluster = allLinksInCluster.length;
    defaultInfo.loadedLinksCount = allLinksInCluster.length;
    defaultInfo.totalConnectionsInFilter = totalConnections;

    const unreviewedLinks = allLinksInCluster.filter(
      (link: BaseLink) => !link.wasReviewed
    );
    defaultInfo.hasUnreviewedEdges = unreviewedLinks.length > 0;
    defaultInfo.totalUnreviewedEdgesInCluster = unreviewedLinks.length;

    if (selectedEdgeId) {
      defaultInfo.currentEdgeIndex = allLinksInCluster.findIndex(
        (l: BaseLink) => l.id === selectedEdgeId
      );
      defaultInfo.currentUnreviewedEdgeIndexInCluster =
        unreviewedLinks.findIndex((l: BaseLink) => l.id === selectedEdgeId);
    }

    if (unreviewedLinks.length > 0) {
      const currentIndex = unreviewedLinks.findIndex(
        (l: BaseLink) => l.id === selectedEdgeId
      );
      if (currentIndex > -1 && currentIndex < unreviewedLinks.length - 1) {
        defaultInfo.nextUnreviewedEdgeId = unreviewedLinks[currentIndex + 1].id;
      } else {
        defaultInfo.nextUnreviewedEdgeId = unreviewedLinks[0].id;
      }
    }

    return defaultInfo;
  }, [selectedEdgeId, selectedClusterId, visualizationData]);

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
      const edge = currentViz.links.find((l: BaseLink) => l.id === edgeId);
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
      const edge = currentViz.links.find((l: BaseLink) => l.id === edgeId);
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
        (link: BaseLink) => link.id === selectedEdgeId
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
        (c: EntityCluster) => c.id === currentClusterId
      );
      if (currentIndex === -1) return null;

      for (let i = currentIndex + 1; i < clusters.data.length; i++) {
        const cluster: EntityCluster = clusters.data[i];
        if (cluster.wasReviewed) continue;

        const progress = getClusterProgress(cluster.id);
        if (progress.pendingEdges > 0) {
          console.log(
            `📍 [EntityWorkflow] Found next viable cluster: ${cluster.id} (${progress.pendingEdges} pending for filter: ${workflowFilter})`
          );
          return cluster;
        }
      }

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

      if (isManualSelection && id) {
        setManualClusterSelection({ clusterId: id, timestamp: Date.now() });
        console.log(
          `📍 [EntityWorkflow] Manual cluster selection: ${id} (audit mode: ${auditMode})`
        );
      }

      stateActions.setSelectedClusterId(id);

      if (id && selectedOpinion) {
        if (auditMode === "post_processing_audit") {
          // Load rich data for audit cluster
          await loadAuditClusterRichData(id);
        } else {
          // Normal mode logic
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
            // ✅ FIXED: Get correct item type for this cluster
            const correctItemType = getCorrectItemType(id);

            const vizState = visualizationData[id];
            if ((!vizState?.data || vizState?.error) && !vizState?.loading) {
              await loadVisualizationDataForClusters([
                {
                  clusterId: id,
                  itemType: correctItemType, // ✅ FIXED: Use correct item type
                  limit: CONNECTION_PAGE_SIZE,
                  crossSystemOnly: workflowFilter === "cross-source-only",
                },
              ]);
            }
          } else {
            // ✅ FIXED: For large clusters, use correct item type for node details
            const correctItemType = getCorrectItemType(id);

            const vizState = visualizationData[id];
            if (vizState?.data?.nodes) {
              const nodeIdsFromViz = vizState.data.nodes.map(
                (node: BaseNode) => ({
                  id: node.id,
                  nodeType: correctItemType, // ✅ FIXED: Use correct item type
                })
              );
              await loadBulkNodeDetails(nodeIdsFromViz);
            }
          }
        }
      }
    },
    [
      selectedClusterId,
      auditMode,
      selectedOpinion,
      stateActions,
      loadAuditClusterRichData,
      getClusterById,
      getClusterProgress,
      largeClusterConnectionsPage,
      getCorrectItemType, // ✅ ADDED: Include the helper function
      visualizationData,
      loadVisualizationDataForClusters,
      workflowFilter,
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
  // Business Action Functions
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
    // Check if in audit mode and handle navigation separately
    if (auditMode === "post_processing_audit") {
      console.log(`🚀 [EntityWorkflow] Advancing to next audit cluster.`);
      await findNextAuditCluster();
      return;
    }

    // --- Original logic for manual review mode ---
    if (!selectedClusterId) {
      console.warn("🚫 [EntityWorkflow] No cluster selected, cannot advance.");
      return;
    }

    console.log(
      `🚀 [EntityWorkflow] Advancing to next cluster from: ${selectedClusterId}`
    );

    const currentIndex = clusters.data.findIndex(
      (c: EntityCluster) => c.id === selectedClusterId
    );
    if (currentIndex === -1) {
      console.warn(
        "Current cluster not found in cluster list. Reloading clusters."
      );
      loadClusterProgress(clusters.page, clusters.limit);
      return;
    }

    stateActions.setActivelyPagingClusterId(null);
    stateActions.setLargeClusterConnectionsPage(0);

    let nextClusterSelected = false;

    for (let i = currentIndex + 1; i < clusters.data.length; i++) {
      const nextClusterOnPage = clusters.data[i];
      if (!nextClusterOnPage.wasReviewed) {
        console.log(
          `Selecting next unreviewed cluster in current page: ${nextClusterOnPage.id}`
        );

        await handleSetSelectedClusterId(nextClusterOnPage.id, false);
        nextClusterSelected = true;
        break;
      }
    }

    if (!nextClusterSelected) {
      if (clusters.page < Math.ceil(clusters.total / clusters.limit)) {
        const nextPageToLoad = clusters.page + 1;
        console.log(`Loading next page (${nextPageToLoad}) of clusters.`);

        await loadClusterProgress(nextPageToLoad, clusters.limit);
      } else {
        toast({
          title: "Workflow Complete",
          description: "All clusters have been reviewed.",
        });
        console.log("All clusters reviewed.");
        stateActions.setIsAutoAdvanceEnabled(false);
        stateActions.setSelectedClusterId(null);
      }
    }
  }, [
    auditMode,
    findNextAuditCluster,
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

      const vizState = visualizationData[targetClusterId];
      // ✨ UPDATED: A cluster is complete if there are no more unreviewed links AND hasMore is false
      const unreviewedCount =
        vizState?.data?.links.filter((l: BaseLink) => !l.wasReviewed).length ??
        -1;

      if (unreviewedCount === 0 && vizState && !vizState.hasMore) {
        console.log(
          `✅ [EntityWorkflow] Cluster ${targetClusterId} is complete.`
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
      visualizationData,
      isAutoAdvanceEnabled,
      advanceToNextCluster,
    ]
  );

  const selectNextUnreviewedEdge = useCallback(
    (afterEdgeId?: string | null) => {
      if (!selectedClusterId) return;
      const viz = visualizationData[selectedClusterId]?.data;
      if (!viz) return;

      const unreviewedLinks = viz.links.filter((l: BaseLink) => !l.wasReviewed);
      if (unreviewedLinks.length === 0) {
        // ✨ UPDATED: If no unreviewed links and more can be loaded, don't advance cluster
        if (visualizationData[selectedClusterId]?.hasMore) {
          toast({
            title: "Load More",
            description:
              "No more unreviewed connections in the loaded set. Click 'Load More' to see more.",
          });
        } else {
          checkAndAdvanceIfComplete(selectedClusterId);
        }
        return;
      }

      const refId = afterEdgeId || lastReviewedEdgeId || selectedEdgeId;
      let nextEdge: BaseLink | undefined;
      if (refId) {
        const currentIdx = unreviewedLinks.findIndex(
          (l: BaseLink) => l.id === refId
        );
        if (currentIdx > -1 && currentIdx < unreviewedLinks.length - 1) {
          nextEdge = unreviewedLinks[currentIdx + 1];
        }
      }

      if (!nextEdge) {
        nextEdge = unreviewedLinks[0];
      }

      if (nextEdge) {
        stateActions.setSelectedEdgeId(nextEdge.id);
      }
    },
    [
      selectedClusterId,
      selectedEdgeId,
      lastReviewedEdgeId,
      visualizationData,
      stateActions,
      toast,
      checkAndAdvanceIfComplete,
    ]
  );

  const initializeLargeClusterConnectionPaging = useCallback(
    async (clusterId: string) => {
      console.log(
        `🚀 [EntityWorkflow] Initializing paginated fetch for large cluster: ${clusterId}`
      );

      // ✅ FIXED: Get correct item type for this cluster
      const correctItemType = getCorrectItemType(clusterId);

      // Set state to indicate paging has started for this cluster.
      stateActions.setActivelyPagingClusterId(clusterId);
      stateActions.setLargeClusterConnectionsPage(1); // Start at page 1
      stateActions.setIsLoadingConnectionPageData(true);

      try {
        await loadVisualizationDataForClusters([
          {
            clusterId,
            itemType: correctItemType, // ✅ FIXED: Use correct item type
            limit: CONNECTION_PAGE_SIZE,
            crossSystemOnly: workflowFilter === "cross-source-only",
          },
        ]);
      } finally {
        // Set loading to false after the data fetch attempt is complete.
        stateActions.setIsLoadingConnectionPageData(false);
      }
    },
    [
      getCorrectItemType, // ✅ ADDED: Include the helper function
      workflowFilter,
      loadVisualizationDataForClusters,
      stateActions,
    ]
  );

  // ✅ NEW: Added function to handle loading more connections.
  const loadMoreConnectionsForCluster = useCallback(
    async (clusterId: string) => {
      const vizState = visualizationData[clusterId];
      if (!vizState || !vizState.hasMore || state.isLoadingConnectionPageData) {
        return;
      }

      console.log(
        `🔄 [EntityWorkflow] Loading more connections for cluster ${clusterId}`
      );

      stateActions.setIsLoadingConnectionPageData(true);
      stateActions.setLargeClusterConnectionsPage(
        largeClusterConnectionsPage + 1
      );

      try {
        await dataLoadMore(clusterId);
      } finally {
        stateActions.setIsLoadingConnectionPageData(false);
      }
    },
    [
      visualizationData,
      state.isLoadingConnectionPageData,
      largeClusterConnectionsPage,
      stateActions,
      dataLoadMore,
    ]
  );

  // ========================================================================
  // DUAL-ACTION submitEdgeReview - Core of the audit integration
  // ========================================================================

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

      // ✅ FIXED: Get correct item type for the current cluster
      const correctItemType = getCorrectItemType(selectedClusterId);

      console.log(
        `🚀 [EntityWorkflow] Submitting edge review: ${edgeId} -> ${decision} (audit mode: ${auditMode}, itemType: ${correctItemType})`
      );

      setEdgeSubmissionStatus((prev) => ({
        ...prev,
        [edgeId]: { isSubmitting: true, error: null },
      }));

      const newStatus: BaseLink["status"] =
        decision === "ACCEPTED" ? "CONFIRMED_MATCH" : "CONFIRMED_NON_MATCH";
      const revertOptimisticUpdate = updateEdgeStatusOptimistically(
        selectedClusterId,
        edgeId,
        newStatus,
        true
      );

      try {
        // 1. UNCHANGED: Submit traditional edge review
        const payload: EdgeReviewApiPayload = {
          decision: decision as "ACCEPTED" | "REJECTED",
          reviewerId: user.id,
          notes,
          type: correctItemType, // ✅ FIXED: Use correct item type
          disconnectDependentServices: disconnectDependentServicesEnabled,
        };

        const response = await postEdgeReview(edgeId, payload, selectedOpinion);
        console.log(
          `✅ [EntityWorkflow] Traditional edge review submitted: ${edgeId}`
        );

        // 2. NEW: If in audit mode, auto-mark related decisions as reviewed
        if (auditMode === "post_processing_audit") {
          // ✅ FIXED: Use existing cluster-specific audit data, don't reload global data
          const clusterAuditDecisions =
            postProcessingAuditData?.data?.decisions?.filter(
              (d) =>
                d.clusterId === selectedClusterId &&
                d.edgeId === edgeId &&
                !d.reviewedByHuman
            ) || [];

          if (clusterAuditDecisions.length > 0) {
            const decisionIds = clusterAuditDecisions.map((d) => d.id);
            console.log(
              `🔄 [EntityWorkflow] Auto-marking ${decisionIds.length} related audit decisions as reviewed`
            );

            await bulkMarkDecisionsReviewed(decisionIds, user.id);

            console.log(
              `✅ [EntityWorkflow] ${decisionIds.length} decisions marked as reviewed in backend`
            );

            // Only reload clusters data, not audit decisions
            await loadClustersWithAuditData({
              entityType: correctItemType, // ✅ FIXED: Use correct item type
              postProcFilter: postProcessingFilter || undefined,
              reviewedByHuman: false,
              page: 1,
              limit: 20,
              workflowFilter: workflowFilter || undefined,
            });

            toast({
              title: "Manual Review Submitted",
              description: `Edge reviewed and ${clusterAuditDecisions.length} automated decisions marked as reviewed.`,
            });
          } else {
            console.log(
              `ℹ️ [EntityWorkflow] No unreviewed audit decisions found for edge ${edgeId} in cluster ${selectedClusterId}`
            );
            toast({
              title: "Manual Review Submitted",
              description: "Edge reviewed successfully.",
            });
          }
        } else {
          // Normal mode toast
          if (
            response.dependentServicesDisconnected &&
            response.dependentServicesDisconnected > 0
          ) {
            toast({
              title: "Review Submitted",
              description: `Edge reviewed successfully. ${response.dependentServicesDisconnected} dependent service matches were also disconnected.`,
            });
          }
        }

        setEdgeSubmissionStatus((prev) => ({
          ...prev,
          [edgeId]: { isSubmitting: false, error: null },
        }));

        stateActions.setLastReviewedEdgeId(edgeId);

        // Handle cluster finalization
        if (response.clusterFinalized && selectedClusterId) {
          console.log(
            `🎉 [EntityWorkflow] Cluster ${selectedClusterId} finalized`
          );
          updateClusterCompletionOptimistically(selectedClusterId, true);

          if (auditMode === "post_processing_audit") {
            // Only reload clusters data for audit mode, not audit decisions
            await loadClustersWithAuditData({
              entityType: correctItemType, // ✅ FIXED: Use correct item type
              postProcFilter: postProcessingFilter || undefined,
              reviewedByHuman: false,
              page: 1,
              limit: 20,
              workflowFilter: workflowFilter || undefined,
            });
          }

          if (isAutoAdvanceEnabled) {
            console.log(
              `🚀 [EntityWorkflow] Auto-advancing to next cluster after finalization`
            );
            setTimeout(() => {
              if (auditMode === "post_processing_audit") {
                findNextAuditCluster();
              } else {
                advanceToNextCluster();
              }
            }, 100);
          }
        } else {
          console.log(`🔍 [EntityWorkflow] Looking for next unreviewed edge`);
          setTimeout(() => {
            selectNextUnreviewedEdge(edgeId);
          }, 100);
        }
      } catch (error) {
        const errorMessage =
          (error as Error).message || "An unknown error occurred.";
        console.error(
          `❌ [EntityWorkflow] Failed to submit review for edge ${edgeId}:`,
          error
        );

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
      auditMode,
      postProcessingFilter,
      postProcessingAuditData,
      toast,
      disconnectDependentServicesEnabled,
      stateActions,
      updateEdgeStatusOptimistically,
      updateClusterCompletionOptimistically,
      bulkMarkDecisionsReviewed,
      loadClustersWithAuditData,
      isAutoAdvanceEnabled,
      findNextAuditCluster,
      advanceToNextCluster,
      selectNextUnreviewedEdge,
      workflowFilter,
      getCorrectItemType, // ✅ ADDED: Include the helper function
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
  // Effects
  // ========================================================================

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
          stateActions.setDisconnectDependentServicesEnabled(false);

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
      stateActions.setDisconnectDependentServicesEnabled(false);
    }
  }, [selectedOpinion, user?.id, toast, stateActions]);

  useEffect(() => {
    if (auditMode === "post_processing_audit") {
      initializeAuditMode();
    }
  }, [auditMode, initializeAuditMode]);

  useEffect(() => {
    if (
      selectedClusterId &&
      currentVisualizationData?.links &&
      !selectedEdgeId
    ) {
      console.log(
        `🔄 [EntityWorkflow] Cluster ${selectedClusterId} selected, viz data loaded, no edge selected.`
      );

      if (auditMode === "post_processing_audit") {
        // ✅ FIX: In audit mode, do NOT auto-advance - let users manually navigate
        console.log(
          `🛑 [EntityWorkflow] Audit mode: Skipping auto-advance, user should manually select connections`
        );
        return;
      } else {
        // Normal mode logic
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
    }
  }, [
    selectedClusterId,
    currentVisualizationData?.links?.length,
    selectedEdgeId,
    auditMode,
    selectNextUnreviewedEdge,
    activelyPagingClusterId,
    largeClusterConnectionsPage,
    lastReviewedEdgeId,
    isAutoAdvanceEnabled,
  ]);

  // This effect is the safety net that ensures data is loaded for a selected edge if it wasn't pre-fetched.
  useEffect(() => {
    if (!selectedEdgeId) return;

    const currentEdgeState = connectionData[selectedEdgeId];
    const needsLoad =
      !currentEdgeState?.data &&
      !currentEdgeState?.loading &&
      !currentEdgeState?.error;

    if (needsLoad) {
      console.log(
        `🔄 [EntityWorkflow] Fallback: Auto-loading connection data for edge: ${selectedEdgeId}`
      );
      data.loadSingleConnectionData(selectedEdgeId);
    }
  }, [selectedEdgeId, connectionData, data.loadSingleConnectionData]);

  // FIX: This new useEffect handles proactive pre-fetching of connection details.
  useEffect(() => {
    if (!selectedClusterId) return;

    const vizState = visualizationData[selectedClusterId];
    // Only run if we have links and are not in the middle of another operation
    if (vizState?.data?.links && !state.isLoadingConnectionPageData) {
      // ✅ FIXED: Get the correct item type for this cluster
      const correctItemType = getCorrectItemType(selectedClusterId);

      const unloadedConnectionItems = vizState.data.links
        .filter((link: BaseLink) => {
          const connState = connectionData[link.id];
          // Check if data is missing and not already loading/in error state
          const needsLoad =
            !connState ||
            (!connState.data && !connState.loading && !connState.error);
          return needsLoad;
        })
        .map((link: BaseLink) => ({
          edgeId: link.id,
          itemType: correctItemType, // ✅ FIXED: Use correct item type
        }));

      if (unloadedConnectionItems.length > 0) {
        console.log(
          `🔄 [EntityWorkflow] Auto-pre-fetching ${unloadedConnectionItems.length} connection details for cluster ${selectedClusterId} with itemType: ${correctItemType}`
        );
        // Use a small timeout to batch requests if multiple updates happen quickly
        const timer = setTimeout(() => {
          loadBulkConnections(
            unloadedConnectionItems,
            workflowFilter === "cross-source-only"
          );
        }, 100);
        return () => clearTimeout(timer);
      }
    }
  }, [
    selectedClusterId,
    visualizationData, // This will trigger the effect when new links are loaded
    connectionData, // To re-evaluate if something failed and needs a retry
    getCorrectItemType, // ✅ ADDED: Include the helper function
    workflowFilter,
    loadBulkConnections,
    state.isLoadingConnectionPageData,
  ]);

  useEffect(() => {
    if (
      auditMode === "post_processing_audit" &&
      selectedClusterId &&
      !postProcessingAuditData.loading &&
      !isClustersWithAuditDataLoading()
    ) {
      const vizState = visualizationData[selectedClusterId];

      if (
        !vizState?.data ||
        (!postProcessingAuditData.data && !postProcessingAuditData.loading)
      ) {
        console.log(
          `🔄 [EntityWorkflow] Audit mode: ensuring rich data is loaded for cluster ${selectedClusterId}`
        );

        loadAuditClusterRichData(selectedClusterId);
      }
    }
  }, [
    auditMode,
    selectedClusterId,
    postProcessingAuditData.loading,
    postProcessingAuditData.data,
    visualizationData,
    isClustersWithAuditDataLoading,
    loadAuditClusterRichData,
  ]);

  useEffect(() => {
    if (
      auditMode === "normal" &&
      !selectedClusterId &&
      clusters.data.length > 0 &&
      !clusters.loading
    ) {
      const firstNonReviewedCluster = clusters.data.find((c) => !c.wasReviewed);
      if (firstNonReviewedCluster) {
        console.log(
          "🎯 [EntityWorkflow] Auto-selecting first unreviewed cluster:",
          firstNonReviewedCluster.id
        );
        handleSetSelectedClusterId(firstNonReviewedCluster.id, false);
      } else if (clusters.data.length > 0) {
        const firstCluster = clusters.data[0];
        console.log(
          "🎯 [EntityWorkflow] All clusters on current page are reviewed. Auto-selecting first for viewing:",
          firstCluster.id
        );
        handleSetSelectedClusterId(firstCluster.id, false);
      }
    }

    if (
      auditMode === "post_processing_audit" &&
      !isClustersWithAuditDataLoading() &&
      !initialAuditSelectionDone.current
    ) {
      const auditClusters = clustersWithAuditData?.data?.clusters || [];
      if (!selectedClusterId && auditClusters.length > 0) {
        const firstUnreviewed =
          auditClusters.find(
            (c: { unreviewedDecisionsCount: number }) =>
              c.unreviewedDecisionsCount > 0
          ) || auditClusters[0];
        if (firstUnreviewed) {
          console.log(
            `🎯 [EntityWorkflow] Auto-selecting first unreviewed audit cluster: ${firstUnreviewed.id}`
          );
          handleSetSelectedClusterId(firstUnreviewed.id, false);
          initialAuditSelectionDone.current = true;
        }
      }
    }

    if (
      auditMode === "normal" &&
      selectedClusterId &&
      clusters.data.length > 0 &&
      !clusters.loading
    ) {
      const selectedExists = clusters.data.some(
        (c: { id: string }) => c.id === selectedClusterId
      );
      if (!selectedExists) {
        console.log(
          "🎯 [EntityWorkflow] Current selected cluster no longer exists in list. Deselecting."
        );
        stateActions.setSelectedClusterId(null);
      }
    }
  }, [
    auditMode,
    selectedClusterId,
    clusters.data,
    clusters.loading,
    clustersWithAuditData,
    isClustersWithAuditDataLoading,
    handleSetSelectedClusterId,
    stateActions,
  ]);

  useEffect(() => {
    if (lastWorkflowFilter !== workflowFilter) {
      setLastWorkflowFilter(workflowFilter);

      if (selectedEdgeId && selectedClusterId) {
        const isValidForNewFilter = isCurrentEdgeValidForFilter(workflowFilter);

        if (!isValidForNewFilter) {
          console.log(
            `🔍 [EntityWorkflow] Current edge ${selectedEdgeId} is not valid for filter ${workflowFilter}. Deselecting and finding next.`
          );

          setSelectedEdgeIdAction(null);

          data.loadClusterProgress(clusters.page, clusters.limit);

          setTimeout(() => {
            selectNextUnreviewedEdge();
          }, 100);
        } else {
          data.loadClusterProgress(clusters.page, clusters.limit);
        }
      } else {
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

  // ========================================================================
  // Context Value
  // ========================================================================

  const contextValue: EntityWorkflowContextType = {
    clusterProgress,
    overallProgress,
    edgeSelectionInfo,

    currentVisualizationData,
    currentConnectionData,
    selectedClusterDetails,

    edgeSubmissionStatus,

    selectNextUnreviewedEdge,
    selectPreviousUnreviewedInCluster,
    selectNextUnreviewedInCluster,
    advanceToNextCluster,
    checkAndAdvanceIfComplete,

    initializeAuditMode,
    findNextAuditCluster,
    isClusterCompleteForAudit,
    getAuditNavigationState,

    submitEdgeReview,
    enableDisconnectDependentServices,

    initializeLargeClusterConnectionPaging,
    loadMoreConnectionsForCluster,

    handleSetSelectedClusterId,
    setSelectedEdgeIdAction,

    getClusterProgress,
    getClusterProgressUnfiltered,
    getClusterProgressCrossSource,
    canAdvanceToNextCluster,
    isEdgeReviewed,
    getEdgeStatus,
    getEdgeSubmissionStatus,

    isClusterCompleteForCurrentFilter,
    findNextViableCluster,

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

export function useEntityAudit() {
  const {
    initializeAuditMode,
    findNextAuditCluster,
    isClusterCompleteForAudit,
    getAuditNavigationState,
  } = useEntityWorkflow();

  return {
    initializeAuditMode,
    findNextAuditCluster,
    isClusterCompleteForAudit,
    getAuditNavigationState,
  };
}

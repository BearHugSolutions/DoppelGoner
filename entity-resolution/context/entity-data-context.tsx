/*
================================================================================
|
|   File: /context/entity-data-context.tsx - COMPLETE: Pagination + Enhanced Clearing
|
|   Description: Complete data management context with pagination and enhanced clearing
|   - Manages fetching and caching of all entity resolution data
|   - Implements cursor-based pagination for connection loading
|   - Handles appending new links to existing visualization data
|   - Enhanced data clearing integration with EntityStateContext
|   - Comprehensive request management and optimization
|
================================================================================
*/
"use client";

import {
  EntityCluster,
  VisualizationDataResponse,
  VisualizationData,
  EntityConnectionDataResponse,
  NodeDetailResponse,
  NodeIdentifier,
  BulkConnectionRequestItem,
  BulkConnectionsRequest,
  BulkVisualizationRequestItem,
  ClusterProgress,
  OverallProgress,
  ClusterProgressResponse,
  ResolutionMode,
  BaseLink,
  WorkflowFilter,
  EdgeSelectionInfo,
} from "@/types/entity-resolution";

import type {
  PostProcessingAuditParams,
  PostProcessingAuditResponse,
  ClustersWithPostProcessingResponse,
  PostProcessingAuditState,
  ClustersWithAuditState,
  BulkMarkReviewedRequest,
  BulkMarkReviewedResponse,
  PostProcessingDecision,
} from "@/types/post-processing";

import {
  getBulkNodeDetails,
  getBulkConnections,
  getBulkVisualizations,
  getClusterProgress,
  getPostProcessingAuditData,
  getClustersWithPostProcessingDecisions,
  bulkMarkPostProcessingReviewed,
  validateAuditClustersHaveData,
} from "@/utils/api-client";

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
import { useEntityState } from "./entity-state-context";
import { useToast } from "@/hooks/use-toast";
import { produce } from "immer";
import _ from "lodash";

// ============================================================================
// Constants
// ============================================================================

const MAX_BULK_FETCH_SIZE = 50;
const LARGE_CLUSTER_THRESHOLD = 200;
const CONNECTION_PAGE_SIZE = 50; // Page size for paginated connection details
const CONNECTION_PAGINATION_THRESHOLD = 100; // Threshold to switch to pagination

// ============================================================================
// State Interfaces
// ============================================================================

interface ClustersState {
  data: EntityCluster[];
  total: number;
  page: number;
  limit: number;
  loading: boolean;
  error: string | null;
}

// Enhanced VisualizationState with pagination support
export interface VisualizationState {
  data: VisualizationData | null;
  loading: boolean;
  error: string | null;
  lastUpdated: number | null;
  // Pagination state
  nextCursor: string | null;
  hasMore: boolean;
  isLoadingMore: boolean;
}

interface ConnectionState {
  data: EntityConnectionDataResponse | null;
  loading: boolean;
  error: string | null;
  lastUpdated: number | null;
}

// ✨ NEW: State slice for paginated bulk connection data
interface BulkConnectionDataState {
  connections: Record<string, EntityConnectionDataResponse>;
  requestedEdgeIds: string[];
  nextCursor: string | null;
  hasMore: boolean;
  isLoading: boolean;
  isLoadingMore: boolean;
  error: string | null;
  crossSystemOnly: boolean;
}

// ============================================================================
// Context Type - Enhanced with Complete Clearing and Pagination
// ============================================================================

export interface EntityDataContextType {
  // Audit data stores
  postProcessingAuditData: PostProcessingAuditState;
  clustersWithAuditData: ClustersWithAuditState;

  // Data stores
  clusters: ClustersState;
  visualizationData: Record<string, VisualizationState>;
  connectionData: Record<string, ConnectionState>;
  nodeDetails: Record<string, NodeDetailResponse | null | "loading" | "error">;

  // ✨ NEW: Paginated connection data store
  bulkConnectionData: BulkConnectionDataState;

  // Server-side progress data
  serverProgress: Record<string, ClusterProgress>;
  overallServerProgress: OverallProgress | null;

  // Loading functions
  loadClusters: (page: number, limit?: number) => Promise<void>;
  loadClusterProgress: (page: number, limit?: number) => Promise<void>;
  loadVisualizationDataForClusters: (
    items: BulkVisualizationRequestItem[]
  ) => Promise<void>;
  loadMoreConnectionsForCluster: (clusterId: string) => Promise<void>; // NEW: Pagination
  loadBulkConnections: (
    items: BulkConnectionRequestItem[],
    crossSystemOnly: boolean
  ) => Promise<void>;
  loadMoreBulkConnections: () => Promise<void>;
  loadBulkNodeDetails: (nodesToFetch: NodeIdentifier[]) => Promise<void>;
  loadSingleConnectionData: (
    edgeId: string
  ) => Promise<EntityConnectionDataResponse | null>;

  // Audit loading functions
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

  // Cache management
  invalidateVisualizationData: (clusterId: string) => Promise<void>;
  invalidateConnectionData: (edgeId: string) => Promise<void>;
  performThreeClusterCleanup: (force?: boolean) => void;
  clearAllData: () => void;

  // Optimistic update functions
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

  updateProgressOptimistically: (
    clusterId: string,
    edgeId: string,
    oldWasReviewed: boolean,
    newWasReviewed: boolean,
    newStatus: BaseLink["status"],
    filterType: WorkflowFilter
  ) => () => void;

  // Audit filtering functions
  getAuditAffectedEdges: (
    clusterId: string,
    auditDecisions?: PostProcessingDecision[]
  ) => string[];
  getAuditFilteredVisualizationData: (
    clusterId: string
  ) => VisualizationData | null;
  loadAuditClusterRichData: (clusterId: string) => Promise<void>;

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

  // NEW: Pagination query
  getClusterPaginationState: (clusterId: string) => {
    hasMore: boolean;
    isLoadingMore: boolean;
    nextCursor: string | null;
  };

  // Audit data queries
  isPostProcessingAuditDataLoaded: () => boolean;
  isPostProcessingAuditDataLoading: () => boolean;
  isClustersWithAuditDataLoaded: () => boolean;
  isClustersWithAuditDataLoading: () => boolean;
  getPostProcessingAuditError: () => string | null;
  getClustersWithAuditError: () => string | null;

  // Cache statistics
  getCacheStats: () => any;
}

// ============================================================================
// Context Creation
// ============================================================================

const EntityDataContext = createContext<EntityDataContextType | undefined>(
  undefined
);

// ============================================================================
// Initial State
// ============================================================================

const initialEdgeSelectionInfo: EdgeSelectionInfo = {
  currentEdgeId: null,
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

const initialClustersState: ClustersState = {
  data: [],
  total: 0,
  page: 1,
  limit: 10,
  loading: false,
  error: null,
};

const initialVisualizationState: Omit<VisualizationState, "data"> = {
  loading: false,
  error: null,
  lastUpdated: null,
  nextCursor: null,
  hasMore: false,
  isLoadingMore: false,
};

// ✨ NEW: Initial state for the bulk connection data slice
const initialBulkConnectionDataState: BulkConnectionDataState = {
  connections: {},
  requestedEdgeIds: [],
  nextCursor: null,
  hasMore: false,
  isLoading: false,
  isLoadingMore: false,
  error: null,
  crossSystemOnly: false,
};

// ============================================================================
// Utility Functions
// ============================================================================

function uniqueBy<T>(items: T[], keySelector: (item: T) => string): T[] {
  return Array.from(
    new Map(items.map((item) => [keySelector(item), item])).values()
  );
}

// ============================================================================
// Provider Component - Enhanced with Complete Clearing and Pagination
// ============================================================================

export function EntityDataProvider({ children }: { children: ReactNode }) {
  const { selectedOpinion } = useAuth();
  const { toast } = useToast();
  const state = useEntityState();

  // Destructure needed state values
  const {
    resolutionMode,
    clusterFilterStatus,
    workflowFilter,
    selectedClusterId,
    activelyPagingClusterId,
    refreshTrigger,
    auditMode,
    postProcessingFilter,
  } = state;

  // ========================================================================
  // Internal State
  // ========================================================================

  const [clusters, setClusters] = useState<ClustersState>(initialClustersState);
  const [visualizationData, setVisualizationData] = useState<
    Record<string, VisualizationState>
  >({});
  const [bulkConnectionData, setBulkConnectionData] =
    useState<BulkConnectionDataState>(initialBulkConnectionDataState);
  const [edgeSelectionInfo, setEdgeSelectionInfo] = useState<EdgeSelectionInfo>(
    initialEdgeSelectionInfo
  );
  const [connectionData, setConnectionData] = useState<
    Record<string, ConnectionState>
  >({});
  const [nodeDetails, setNodeDetails] = useState<
    Record<string, NodeDetailResponse | null | "loading" | "error">
  >({});
  const [serverProgress, setServerProgress] = useState<
    Record<string, ClusterProgress>
  >({});
  const [overallServerProgress, setOverallServerProgress] =
    useState<OverallProgress | null>(null);
  const [pendingNodeFetches, setPendingNodeFetches] = useState<Set<string>>(
    new Set()
  );

  // Audit data state
  const [postProcessingAuditData, setPostProcessingAuditData] =
    useState<PostProcessingAuditState>({
      data: null,
      loading: false,
      error: null,
      lastUpdated: null,
    });

  const [clustersWithAuditData, setClustersWithAuditData] =
    useState<ClustersWithAuditState>({
      data: null,
      loading: false,
      error: null,
      lastUpdated: null,
    });

  // ========================================================================
  // Request Management Refs
  // ========================================================================

  const activeRequestsRef = useRef<Set<string>>(new Set());
  const lastRequestTimeRef = useRef<number>(0);
  const cleanupTimeoutRef = useRef<NodeJS.Timeout | null>(null);
  const lastCleanupStateRef = useRef<string>("");

  // Refs for stable state access
  const clustersRef = useRef(clusters);
  clustersRef.current = clusters;

  const selectedOpinionRef = useRef(selectedOpinion);
  selectedOpinionRef.current = selectedOpinion;

  const resolutionModeRef = useRef(resolutionMode);
  resolutionModeRef.current = resolutionMode;

  const clusterFilterStatusRef = useRef(clusterFilterStatus);
  clusterFilterStatusRef.current = clusterFilterStatus;

  const workflowFilterRef = useRef(workflowFilter);
  workflowFilterRef.current = workflowFilter;

  const initializationRef = useRef<{
    hasInitialized: boolean;
    lastInitParams: string;
  }>({
    hasInitialized: false,
    lastInitParams: "",
  });

  // Track last clear trigger to prevent redundant clearing
  const lastClearTriggerRef = useRef<number>(0);

  // ========================================================================
  // ENHANCED: Complete Data Clearing Function
  // ========================================================================

  const clearAllData = useCallback(() => {
    const currentTime = Date.now();

    // Prevent redundant clearing within short time window
    if (currentTime - lastClearTriggerRef.current < 100) {
      console.log("🚫 [EntityData] Skipping redundant clearAllData call");
      return;
    }

    lastClearTriggerRef.current = currentTime;

    console.log("🧹🧹🧹 [EntityData] PERFORMING COMPLETE DATA CLEARING 🧹🧹🧹");

    // Clear all data states
    setClusters(initialClustersState);
    setVisualizationData({});
    setConnectionData({});
    setNodeDetails({});
    setPendingNodeFetches(new Set());
    setServerProgress({});
    setOverallServerProgress(null);

    // Clear audit data
    setPostProcessingAuditData({
      data: null,
      loading: false,
      error: null,
      lastUpdated: null,
    });
    setClustersWithAuditData({
      data: null,
      loading: false,
      error: null,
      lastUpdated: null,
    });

    // Clear all active requests
    activeRequestsRef.current.clear();
    lastRequestTimeRef.current = 0;

    // Reset initialization tracking
    initializationRef.current = {
      hasInitialized: false,
      lastInitParams: "",
    };

    // Clear any pending cleanup
    if (cleanupTimeoutRef.current) {
      clearTimeout(cleanupTimeoutRef.current);
      cleanupTimeoutRef.current = null;
    }

    // Reset cleanup state tracking
    lastCleanupStateRef.current = "";

    console.log("✅ [EntityData] Complete data clearing finished");
  }, []);

  // ========================================================================
  // NEW: Register clearAllData with EntityStateContext
  // ========================================================================

  useEffect(() => {
    // Register this clearAllData function with EntityStateContext
    if (state.actions && typeof state.actions === "object") {
      (state as any).clearAllDataRef = { current: clearAllData };
    }
  }, [clearAllData, state]);

  // ========================================================================
  // Request Management Functions
  // ========================================================================

  const isRequestInProgress = useCallback((requestKey: string): boolean => {
    return activeRequestsRef.current.has(requestKey);
  }, []);

  const addActiveRequest = useCallback((requestKey: string) => {
    activeRequestsRef.current.add(requestKey);
  }, []);

  const removeActiveRequest = useCallback((requestKey: string) => {
    activeRequestsRef.current.delete(requestKey);
  }, []);

  // ========================================================================
  // Data Loading Functions
  // ========================================================================

  const debugAuditDataMismatch = useCallback(
    (clusterId: string, auditResponse: PostProcessingAuditResponse) => {
      console.group(
        `🐛 [EntityData] Debug audit data for cluster ${clusterId}`
      );
      console.log(`Requested cluster: ${clusterId}`);
      console.log(
        `Total decisions in response: ${auditResponse.decisions.length}`
      );

      const clusterIds = [
        ...new Set(auditResponse.decisions.map((d) => d.clusterId)),
      ];
      console.log(`Unique cluster IDs in response:`, clusterIds);

      const matchingDecisions = auditResponse.decisions.filter(
        (d) => d.clusterId === clusterId
      );
      console.log(
        `Decisions for requested cluster: ${matchingDecisions.length}`
      );

      if (matchingDecisions.length === 0) {
        console.warn(
          `⚠️ No decisions found for requested cluster. This may indicate:`
        );
        console.warn(`   - The cluster has no audit decisions`);
        console.warn(`   - The API is not filtering by cluster_id correctly`);
        console.warn(
          `   - The cluster is not part of the current audit filter`
        );
      }

      console.groupEnd();
    },
    []
  );

  const loadPostProcessingAuditData = useCallback(
    async (
      params: PostProcessingAuditParams
    ): Promise<PostProcessingAuditResponse | null> => {
      if (!selectedOpinionRef.current) {
        console.log(
          "🚫 [EntityData] No opinion selected, skipping audit data fetch"
        );
        return null;
      }

      const requestKey = `audit-${JSON.stringify(params)}-${
        selectedOpinionRef.current
      }`;
      if (isRequestInProgress(requestKey)) {
        console.log(
          "🚫 [EntityData] Duplicate audit data request in progress, skipping"
        );
        return null;
      }

      // ✅ FIX: Add detailed logging for debugging
      console.log(`🔍 [EntityData] Loading post-processing audit data:`, {
        params,
        opinion: selectedOpinionRef.current,
        isClusterSpecific: !!params.clusterId,
      });

      addActiveRequest(requestKey);

      setPostProcessingAuditData((prev) => ({
        ...prev,
        loading: true,
        error: null,
      }));

      try {
        const response = await getPostProcessingAuditData(
          params,
          selectedOpinionRef.current
        );

        const decisionCount = Array.isArray(response?.decisions)
          ? response.decisions.length
          : 0;

        // ✅ FIX: Add validation for cluster-specific requests
        if (params.clusterId && response?.decisions) {
          const relevantDecisions = response.decisions.filter(
            (d) => d.clusterId === params.clusterId
          );
          const uniqueClusterIds = [
            ...new Set(response.decisions.map((d) => d.clusterId)),
          ];

          console.log(`🔍 [EntityData] Audit data validation:`, {
            requestedCluster: params.clusterId,
            totalDecisions: response.decisions.length,
            relevantDecisions: relevantDecisions.length,
            uniqueClusterIds: uniqueClusterIds,
          });

          // ✅ FIX: If no relevant decisions found, log detailed information
          if (relevantDecisions.length === 0 && response.decisions.length > 0) {
            console.warn(`⚠️ [EntityData] CLUSTER MISMATCH DETECTED:
              Requested cluster: ${params.clusterId}
              Available clusters in response: ${uniqueClusterIds.join(", ")}
              This suggests either:
              1. The backend filtering is not working correctly
              2. The requested cluster has no audit decisions
              3. There's a data consistency issue between clusters and audit decisions`);

            // ✅ FIX: Return early with empty response rather than mismatched data
            setPostProcessingAuditData({
              data: {
                decisions: [],
                total: 0,
                page: params.page || 1,
                limit: params.limit || 10,
                _clusterSpecific: params.clusterId,
              } as any,
              loading: false,
              error: null,
              lastUpdated: Date.now(),
            });

            return {
              decisions: [],
              total: 0,
              page: params.page || 1,
              limit: params.limit || 10,
            };
          }
        }

        // ✅ ENHANCED: Preserve cluster-specific data logic
        setPostProcessingAuditData((prev) => {
          // If previous data is marked as cluster-specific and this is a global call, preserve it
          if (
            (prev.data as any)?._clusterSpecific &&
            !params.clusterId &&
            (prev.data as any)?.decisions?.length > 0
          ) {
            console.log(
              `🛡️ [EntityData] Preserving cluster-specific audit data for cluster ${
                (prev.data as any)._clusterSpecific
              }, skipping global overwrite`
            );
            return prev;
          }

          return {
            data: response
              ? ({
                  ...response,
                  _clusterSpecific: params.clusterId || undefined,
                } as any)
              : null,
            loading: false,
            error: null,
            lastUpdated: Date.now(),
          };
        });

        console.log(
          `✅ [EntityData] Post-processing audit data loaded: ${decisionCount} decisions${
            params.clusterId ? ` for cluster ${params.clusterId}` : " (global)"
          }`
        );

        return response;
      } catch (error) {
        console.error(
          "❌ [EntityData] Failed to load post-processing audit data:",
          error
        );

        setPostProcessingAuditData((prev) => ({
          ...prev,
          loading: false,
          error: (error as Error).message,
        }));
        return null;
      } finally {
        removeActiveRequest(requestKey);
      }
    },
    [isRequestInProgress, addActiveRequest, removeActiveRequest]
  );

  const loadBulkNodeDetails = useCallback(
    async (nodesToFetch: NodeIdentifier[]) => {
      if (!selectedOpinionRef.current) {
        console.log(
          "🚫 [EntityData] No opinion selected, skipping node details fetch"
        );
        return;
      }

      const caller =
        new Error().stack?.split("\n")[2]?.match(/at (\w+)/)?.[1] || "unknown";
      console.log(`🔍 [EntityData] loadBulkNodeDetails called by: ${caller}`);
      console.log(
        `🔍 [EntityData] Requested: ${nodesToFetch.length} nodes, First 3 items:`,
        nodesToFetch.slice(0, 3)
      );

      if (nodesToFetch.length === 0) {
        console.log("🚫 [EntityData] No nodes to fetch, early return");
        return;
      }

      const requestKey = `nodes-${nodesToFetch
        .map((n) => `${n.id}-${n.nodeType}`)
        .sort()
        .join(",")}`;
      if (isRequestInProgress(requestKey)) {
        console.log(
          "🚫 [EntityData] Duplicate node request in progress, skipping"
        );
        return;
      }

      const uniqueNodesToFetch = uniqueBy(
        nodesToFetch,
        (node) => `${node.id}-${node.nodeType}`
      );
      const trulyNeedsFetching = uniqueNodesToFetch.filter((node) => {
        const currentState = nodeDetails[node.id];
        const isPending = pendingNodeFetches.has(node.id);
        const shouldSkip =
          (currentState && currentState !== "error") || isPending;

        if (shouldSkip) {
          console.log(
            `🚫 [EntityData] Skipping node ${node.id}: state=${currentState}, pending=${isPending}`
          );
        }
        return !shouldSkip;
      });

      if (trulyNeedsFetching.length === 0) {
        console.log(
          "🚫 [EntityData] All nodes already loaded/loading, early return"
        );
        return;
      }

      console.log(
        `📦 [EntityData] Actually fetching: ${trulyNeedsFetching.length} nodes`
      );

      addActiveRequest(requestKey);

      const nodeIdsToLoad = trulyNeedsFetching.map((n) => n.id);
      setPendingNodeFetches((prev) => {
        const newSet = new Set(prev);
        nodeIdsToLoad.forEach((id) => newSet.add(id));
        console.log(`📝 [EntityData] Added to pending:`, nodeIdsToLoad);
        return newSet;
      });

      setNodeDetails((prev) => {
        const newState = { ...prev };
        trulyNeedsFetching.forEach((node) => {
          newState[node.id] = "loading";
        });
        return newState;
      });

      const cleanupPendingState = () => {
        setPendingNodeFetches((prev) => {
          const newSet = new Set(prev);
          nodeIdsToLoad.forEach((id) => newSet.delete(id));
          console.log(`🧹 [EntityData] Removed from pending:`, nodeIdsToLoad);
          return newSet;
        });
        removeActiveRequest(requestKey);
      };

      try {
        const NODE_FETCH_SIZE = 200;
        for (let i = 0; i < trulyNeedsFetching.length; i += NODE_FETCH_SIZE) {
          const chunk = trulyNeedsFetching.slice(i, i + NODE_FETCH_SIZE);

          try {
            const response = await getBulkNodeDetails(
              { items: chunk },
              selectedOpinionRef.current
            );

            setNodeDetails((prev) => {
              const newState = { ...prev };
              response.forEach((detail) => {
                newState[detail.id] = detail;
              });

              chunk.forEach((requestedNode) => {
                if (!response.find((r) => r.id === requestedNode.id)) {
                  newState[requestedNode.id] = "error";
                }
              });

              return newState;
            });
          } catch (error) {
            console.error(`❌ [EntityData] Error fetching node chunk:`, error);

            setNodeDetails((prev) => {
              const newState = { ...prev };
              chunk.forEach((node) => {
                newState[node.id] = "error";
              });
              return newState;
            });
          }
        }
      } finally {
        cleanupPendingState();
      }
    },
    [
      nodeDetails,
      pendingNodeFetches,
      isRequestInProgress,
      addActiveRequest,
      removeActiveRequest,
    ]
  );

  // ========================================================================
  // REFACTORED: Pagination-Aware Visualization Data Loading
  // ========================================================================

  const loadVisualizationDataForClusters = useCallback(
    async (items: BulkVisualizationRequestItem[]) => {
      if (!selectedOpinionRef.current || items.length === 0) return;

      const requestKey = `viz-${items
        .map((i) => `${i.clusterId}-${i.cursor || "initial"}`)
        .sort()
        .join(",")}`;
      if (isRequestInProgress(requestKey)) return;
      addActiveRequest(requestKey);

      setVisualizationData((prev) =>
        produce(prev, (draft) => {
          items.forEach((item) => {
            const isLoadMore = !!item.cursor;
            if (!draft[item.clusterId]) {
              draft[item.clusterId] = {
                ...initialVisualizationState,
                data: null,
              };
            }
            draft[item.clusterId]!.loading = !isLoadMore;
            draft[item.clusterId]!.isLoadingMore = isLoadMore;
            draft[item.clusterId]!.error = null;
          });
        })
      );

      try {
        const response: VisualizationDataResponse[] =
          await getBulkVisualizations({ items }, selectedOpinionRef.current);

        setVisualizationData((prev) =>
          produce(prev, (draft) => {
            response.forEach((vizData) => {
              const isLoadMore = !!items.find(
                (i) => i.clusterId === vizData.clusterId
              )?.cursor;
              const existingState = draft[vizData.clusterId];

              // FIX: Handle both paginated and non-paginated API responses
              const isPaginatedResponse =
                vizData.links &&
                typeof vizData.links === "object" &&
                "links" in vizData.links &&
                Array.isArray((vizData.links as any).links);

              const newLinks: BaseLink[] = isPaginatedResponse
                ? (vizData.links as any).links
                : (vizData.links as any);
              const nextCursor = isPaginatedResponse
                ? (vizData.links as any).next_cursor
                : null;
              const hasMore = isPaginatedResponse
                ? (vizData.links as any).has_more
                : false;

              if (!Array.isArray(newLinks)) {
                console.error(
                  `[EntityData] Invalid links format for cluster ${vizData.clusterId}`,
                  vizData.links
                );
                draft[vizData.clusterId] = {
                  ...initialVisualizationState,
                  ...existingState,
                  loading: false,
                  isLoadingMore: false,
                  error: "Invalid data format from server.",
                };
                return;
              }

              const accumulatedLinks =
                isLoadMore && existingState?.data?.links
                  ? uniqueBy(
                      [...existingState.data.links, ...newLinks],
                      (l) => l.id
                    )
                  : uniqueBy(newLinks, (l) => l.id);

              const totalConnections =
                vizData.totalConnections ??
                (isPaginatedResponse ? 0 : newLinks.length);

              // The data we store in our context (type: VisualizationData)
              const storedVizData: VisualizationData = {
                clusterId: vizData.clusterId,
                nodes: uniqueBy(vizData.nodes, (n) => n.id),
                groups: vizData.groups,
                links: accumulatedLinks,
                totalConnections: totalConnections,
                crossSystemConnections: vizData.crossSystemConnections,
              };

              draft[vizData.clusterId] = {
                ...initialVisualizationState,
                ...existingState,
                loading: false,
                isLoadingMore: false,
                lastUpdated: Date.now(),
                data: storedVizData,
                nextCursor: nextCursor,
                hasMore: hasMore,
              };
            });
          })
        );

        const allNodeIdentifiers = response.flatMap((viz) => {
          const itemType =
            items.find((i) => i.clusterId === viz.clusterId)?.itemType ||
            resolutionModeRef.current;
          return viz.nodes.map((node) => ({ id: node.id, nodeType: itemType }));
        });

        if (allNodeIdentifiers.length > 0) {
          loadBulkNodeDetails(allNodeIdentifiers);
        }
      } catch (error) {
        console.error(
          "❌ [EntityData] Failed to load visualization data:",
          error
        );
        setVisualizationData((prev) =>
          produce(prev, (draft) => {
            items.forEach((item) => {
              if (draft[item.clusterId]) {
                draft[item.clusterId]!.loading = false;
                draft[item.clusterId]!.isLoadingMore = false;
                draft[item.clusterId]!.error = (error as Error).message;
              }
            });
          })
        );
      } finally {
        removeActiveRequest(requestKey);
      }
    },
    [
      isRequestInProgress,
      addActiveRequest,
      removeActiveRequest,
      loadBulkNodeDetails,
    ]
  );

  // ========================================================================
  // NEW: Load More Connections for Cluster (Pagination)
  // ========================================================================

  const loadMoreConnectionsForCluster = useCallback(
    async (clusterId: string) => {
      const vizState = visualizationData[clusterId];
      if (!vizState || !vizState.hasMore || vizState.isLoadingMore) {
        return;
      }

      console.log(
        `🔄 [EntityData] Loading more connections for cluster ${clusterId} with cursor ${vizState.nextCursor}`
      );

      await loadVisualizationDataForClusters([
        {
          clusterId,
          itemType: resolutionModeRef.current,
          limit: CONNECTION_PAGE_SIZE,
          cursor: vizState.nextCursor!,
          crossSystemOnly: workflowFilterRef.current === "cross-source-only",
        },
      ]);
    },
    [visualizationData, loadVisualizationDataForClusters]
  );

  const loadClustersWithAuditData = useCallback(
    async (params: PostProcessingAuditParams) => {
      if (!selectedOpinionRef.current) {
        console.log(
          "🚫 [EntityData] No opinion selected, skipping clusters audit data fetch"
        );
        return;
      }

      const requestKey = `clusters-audit-${JSON.stringify(params)}-${
        selectedOpinionRef.current
      }`;
      if (isRequestInProgress(requestKey)) {
        console.log(
          "🚫 [EntityData] Duplicate clusters audit data request in progress, skipping"
        );
        return;
      }

      console.log(`🔍 [EntityData] Loading clusters with audit data:`, params);

      addActiveRequest(requestKey);

      setClustersWithAuditData((prev) => ({
        ...prev,
        loading: true,
        error: null,
      }));

      try {
        // ✅ FIX: Try both entity types if no specific entity type provided
        // or if the provided entity type returns no results
        let response = await getClustersWithPostProcessingDecisions(
          params,
          selectedOpinionRef.current
        );

        // ✅ FIX: If no clusters found for the requested entity type, try the other type
        if (response.clusters.length === 0 && params.entityType) {
          console.log(
            `🔄 [EntityData] No clusters found for entityType: ${params.entityType}, trying opposite type`
          );

          const alternateEntityType: "entity" | "service" =
            params.entityType === "entity" ? "service" : "entity";
          const alternateParams: PostProcessingAuditParams = {
            ...params,
            entityType: alternateEntityType,
          };

          const alternateResponse =
            await getClustersWithPostProcessingDecisions(
              alternateParams,
              selectedOpinionRef.current
            );

          if (alternateResponse.clusters.length > 0) {
            console.log(
              `✅ [EntityData] Found ${alternateResponse.clusters.length} clusters for entityType: ${alternateEntityType}`
            );
            response = alternateResponse;

            // Store the detected entity type for UI updates
            console.log(
              `ℹ️ [EntityData] Audit data found for ${alternateEntityType} type`
            );
          }
        }

        const clustersToValidate = response.clusters.map((c) => ({
          id: c.id,
          itemType: c.entityType,
        }));

        if (clustersToValidate.length > 0) {
          const { valid, invalid } = await validateAuditClustersHaveData(
            clustersToValidate,
            selectedOpinionRef.current
          );

          if (invalid.length > 0) {
            console.warn(
              `[EntityData] ${invalid.length} audit clusters have no visualization data, filtering out`
            );
          }

          const validClusters = response.clusters.filter((c) =>
            valid.includes(c.id)
          );

          const updatedResponse = {
            ...response,
            clusters: validClusters,
            auditCounts: {
              ...response.auditCounts,
              totalUnreviewed: validClusters.reduce(
                (sum, c) => sum + c.unreviewedDecisionsCount,
                0
              ),
            },
          };

          setClustersWithAuditData({
            data: updatedResponse,
            loading: false,
            error: null,
            lastUpdated: Date.now(),
          });

          console.log(
            `✅ [EntityData] Filtered clusters with audit data: ${validClusters.length} valid clusters`
          );

          if (validClusters.length > 0) {
            const clustersToPreload = validClusters.slice(0, 3).map((c) => ({
              clusterId: c.id,
              itemType: c.entityType,
            }));

            setTimeout(() => {
              loadVisualizationDataForClusters(clustersToPreload);
            }, 100);
          }
        } else {
          setClustersWithAuditData({
            data: response,
            loading: false,
            error: null,
            lastUpdated: Date.now(),
          });
          console.log(`✅ [EntityData] No audit clusters to validate.`);
        }
      } catch (error) {
        console.error(
          "❌ [EntityData] Failed to load clusters with audit data:",
          error
        );

        setClustersWithAuditData((prev) => ({
          ...prev,
          loading: false,
          error: (error as Error).message,
        }));
      } finally {
        removeActiveRequest(requestKey);
      }
    },
    [
      isRequestInProgress,
      addActiveRequest,
      removeActiveRequest,
      loadVisualizationDataForClusters,
    ]
  );

  const bulkMarkDecisionsReviewed = useCallback(
    async (decisionIds: string[], reviewerId: string) => {
      if (!selectedOpinionRef.current) {
        toast({
          title: "Error",
          description: "No opinion selected.",
          variant: "destructive",
        });
        return;
      }

      if (decisionIds.length === 0) {
        toast({
          title: "Error",
          description: "No decisions selected to mark as reviewed.",
          variant: "destructive",
        });
        return;
      }

      console.log(
        `🔄 [EntityData] Bulk marking ${decisionIds.length} decisions as reviewed`
      );

      try {
        const request: BulkMarkReviewedRequest = {
          decisionIds,
          reviewerId,
        };

        const response = await bulkMarkPostProcessingReviewed(
          request,
          selectedOpinionRef.current
        );

        console.log(
          `✅ [EntityData] Bulk mark reviewed completed: ${response.markedCount} marked, ${response.failedCount} failed`
        );

        if (state.auditMode === "post_processing_audit") {
          const auditParams: PostProcessingAuditParams = {
            entityType: state.resolutionMode,
            postProcFilter: state.postProcessingFilter || undefined,
            reviewedByHuman: false,
            page: 1,
            limit: 20,
            workflowFilter: state.workflowFilter || undefined,
          };

          await Promise.all([
            loadPostProcessingAuditData(auditParams),
            loadClustersWithAuditData(auditParams),
          ]);
        }

        toast({
          title: "Decisions Marked as Reviewed",
          description: `Successfully marked ${
            response.markedCount
          } decisions as reviewed.${
            response.failedCount > 0 ? ` ${response.failedCount} failed.` : ""
          }`,
        });
      } catch (error) {
        console.error(
          "❌ [EntityData] Failed to bulk mark decisions as reviewed:",
          error
        );

        toast({
          title: "Error",
          description: (error as Error).message,
          variant: "destructive",
        });
      }
    },
    [
      selectedOpinionRef,
      state.auditMode,
      state.resolutionMode,
      state.postProcessingFilter,
      loadPostProcessingAuditData,
      loadClustersWithAuditData,
      toast,
    ]
  );

  const loadVisualizationDataForClustersRef = useRef(
    loadVisualizationDataForClusters
  );
  loadVisualizationDataForClustersRef.current =
    loadVisualizationDataForClusters;

  const loadClusterProgress = useCallback(
    async (page: number, limit: number = 10) => {
      const currentOpinion = selectedOpinionRef.current;
      const currentMode = resolutionModeRef.current;
      const currentFilterStatus = clusterFilterStatusRef.current;
      const currentWorkflowFilter = workflowFilterRef.current;

      if (!currentOpinion) {
        console.log(
          "🚫 [EntityData] No opinion selected, skipping cluster progress fetch"
        );
        return;
      }

      const requestKey = `cluster-progress-${page}-${limit}-${currentFilterStatus}-${currentWorkflowFilter}-${currentMode}-${currentOpinion}`;
      if (isRequestInProgress(requestKey)) {
        console.log(
          "🚫 [EntityData] Duplicate cluster progress request in progress, skipping"
        );
        return;
      }

      console.log(
        `🔄 [EntityData] Loading cluster progress: page=${page}, limit=${limit}, filter=${currentFilterStatus}, workflow=${currentWorkflowFilter}, mode=${currentMode}, opinion=${currentOpinion}`
      );

      addActiveRequest(requestKey);

      setClusters((prev) => ({
        ...prev,
        loading: true,
        error: null,
        page,
        limit,
      }));

      try {
        const response = await getClusterProgress(
          page,
          limit,
          currentFilterStatus,
          currentWorkflowFilter,
          currentMode,
          currentOpinion
        );

        console.log(
          `✅ [EntityData] Cluster progress loaded: ${response.clusters.length} clusters, overall progress: ${response.overallProgress.currentView.overallProgressPercentage}%`
        );

        if (response.clusters.length === 0 && response.page > 1) {
          console.warn(
            `No clusters on page ${response.page} with workflow filter ${currentWorkflowFilter}. This may indicate all clusters have been filtered out.`
          );
          setClusters((prev) => ({
            ...prev,
            loading: false,
            error: null,
            data: [],
            total: 0,
          }));
          removeActiveRequest(requestKey);

          toast({
            title: "No More Clusters",
            description: `No clusters found on page ${response.page} for the current filter. All available clusters may have been reviewed.`,
          });
          return;
        }

        const progressMap: Record<string, ClusterProgress> = {};
        response.clusters.forEach((cluster) => {
          progressMap[cluster.id] = cluster.progress;
        });

        setServerProgress(progressMap);
        setOverallServerProgress(response.overallProgress);

        setClusters({
          data: response.clusters.map((c) => ({
            ...c,
            wasReviewed: c.progress.currentView.isComplete,
          })),
          total: response.total,
          page: response.page,
          limit: response.limit,
          loading: false,
          error: null,
        });

        const vizRequestItemsNormal: BulkVisualizationRequestItem[] = [];
        response.clusters.forEach((c) => {
          const connectionCount = c.groupCount;
          const hasEdgesForFilter = c.progress.currentView.totalEdges > 0;

          if (
            hasEdgesForFilter &&
            connectionCount !== undefined &&
            connectionCount !== null &&
            connectionCount <= LARGE_CLUSTER_THRESHOLD
          ) {
            vizRequestItemsNormal.push({
              clusterId: c.id,
              itemType: currentMode,
            });
          }
        });

        if (vizRequestItemsNormal.length > 0) {
          setTimeout(() => {
            loadVisualizationDataForClustersRef.current(vizRequestItemsNormal);
          }, 100);
        }
      } catch (error) {
        console.error(
          "❌ [EntityData] Failed to load cluster progress:",
          error
        );
        setClusters((prev) => ({
          ...prev,
          loading: false,
          error: (error as Error).message,
          data: [],
          total: 0,
        }));

        setServerProgress({});
        setOverallServerProgress(null);
      } finally {
        removeActiveRequest(requestKey);
      }
    },
    [isRequestInProgress, addActiveRequest, removeActiveRequest, toast]
  );

  const loadClusters = loadClusterProgress;

  const loadBulkConnections = useCallback(
    async (items: BulkConnectionRequestItem[], crossSystemOnly = false) => {
      const opinion = selectedOpinionRef.current;
      if (!opinion) {
        console.warn(
          "[EntityData] No opinion selected, skipping bulk connection fetch."
        );
        return;
      }

      // ✅ FIX: Validate that items array is not empty
      if (!items || items.length === 0) {
        console.warn(
          "[EntityData] No items provided for bulk connection fetch"
        );
        return;
      }

      const requestKey = `bulk-conn-${items
        .map((i) => i.edgeId)
        .sort()
        .join(",")}-${crossSystemOnly}`;
      if (isRequestInProgress(requestKey)) {
        console.log(
          `[EntityData] Duplicate bulk connection request in progress, skipping: ${requestKey}`
        );
        return;
      }
      addActiveRequest(requestKey);

      // ✅ FIX: Log the items being requested for debugging
      console.log(
        `[EntityData] Requesting bulk connections for ${items.length} items:`,
        {
          items: items.slice(0, 3), // Log first 3 items
          crossSystemOnly,
          opinion,
        }
      );

      // Set loading state for the individual connections
      setConnectionData(
        produce((draft) => {
          items.forEach((item) => {
            draft[item.edgeId] = {
              data: null,
              loading: true,
              error: null,
              lastUpdated: null,
            };
          });
        })
      );

      setBulkConnectionData(
        produce((draft) => {
          draft.isLoading = true;
          draft.error = null;
          draft.crossSystemOnly = crossSystemOnly;
          draft.requestedEdgeIds = items.map((i) => i.edgeId);
        })
      );

      try {
        // ✅ FIX: Ensure request payload matches backend expectations
        const request: BulkConnectionsRequest = {
          items: items.map((item) => ({
            edgeId: item.edgeId,
            itemType: item.itemType,
          })),
          limit: CONNECTION_PAGE_SIZE,
          cursor: undefined, // Start without cursor for initial request
          crossSystemOnly,
        };

        console.log(`[EntityData] Sending bulk connections request:`, request);

        const response = await getBulkConnections(request, opinion);

        console.log(`[EntityData] Received bulk connections response:`, {
          connectionsCount: response.connections.length,
          hasMore: response.hasMore,
          nextCursor: response.nextCursor ? "present" : "null",
          requestedItems: items.length,
        });

        // ✅ FIX: Handle case where no connections are returned
        if (response.connections.length === 0) {
          console.warn(`[EntityData] No connections returned for ${items.length} requested items. This could indicate:
            1. The edges don't exist in the database
            2. The edges are filtered out by crossSystemOnly=${crossSystemOnly}
            3. There's a data consistency issue`);
        }

        // Populate the main `connectionData` state
        setConnectionData(
          produce((draft) => {
            const returnedEdgeIds = new Set(
              response.connections.map((c) => c.edge.id)
            );

            // Set data for successful responses
            response.connections.forEach((conn) => {
              draft[conn.edge.id] = {
                data: conn,
                loading: false,
                error: null,
                lastUpdated: Date.now(),
              };
            });

            // Set error for items that were requested but not returned
            items.forEach((item) => {
              if (!returnedEdgeIds.has(item.edgeId)) {
                if (draft[item.edgeId]) {
                  draft[item.edgeId].loading = false;
                  draft[item.edgeId].error =
                    "Connection data not found in API response. Edge may not exist or may be filtered out.";
                }
              }
            });
          })
        );

        setBulkConnectionData(
          produce((draft) => {
            draft.isLoading = false;
            draft.hasMore = response.hasMore;
            draft.nextCursor = response.nextCursor;
            draft.connections = {};
            response.connections.forEach((conn) => {
              draft.connections[conn.edge.id] = conn;
            });
          })
        );
      } catch (error) {
        const errorMessage = (error as Error).message;
        console.error(
          "❌ [EntityData] Failed to load bulk connections:",
          error
        );

        // Set error state for the individual connections
        setConnectionData(
          produce((draft) => {
            items.forEach((item) => {
              if (draft[item.edgeId]) {
                draft[item.edgeId].loading = false;
                draft[item.edgeId].error = errorMessage;
              }
            });
          })
        );

        setBulkConnectionData(
          produce((draft) => {
            draft.isLoading = false;
            draft.error = errorMessage;
          })
        );
      } finally {
        removeActiveRequest(requestKey);
      }
    },
    [isRequestInProgress, addActiveRequest, removeActiveRequest]
  );

  const loadMoreBulkConnections = useCallback(async () => {
    const {
      nextCursor,
      requestedEdgeIds,
      crossSystemOnly,
      isLoading,
      isLoadingMore,
    } = bulkConnectionData;

    if (!nextCursor || isLoading || isLoadingMore) return;

    setBulkConnectionData(
      produce((draft) => {
        draft.isLoadingMore = true;
      })
    );

    const request: BulkConnectionsRequest = {
      items: requestedEdgeIds.map((id) => ({
        edgeId: id,
        itemType: resolutionModeRef.current,
      })),
      limit: CONNECTION_PAGE_SIZE,
      cursor: nextCursor ?? undefined,
      crossSystemOnly,
    };

    const requestKey = `bulk-conn-page-${nextCursor}`;
    if (isRequestInProgress(requestKey)) return;
    addActiveRequest(requestKey);

    try {
      const response = await getBulkConnections(
        request,
        selectedOpinionRef.current || undefined
      );

      setBulkConnectionData(
        produce((draft) => {
          draft.isLoadingMore = false;
          draft.hasMore = response.hasMore;
          draft.nextCursor = response.nextCursor;
          response.connections.forEach((conn) => {
            draft.connections[conn.edge.id] = conn;
          });
        })
      );
    } catch (error) {
      console.error(
        "❌ [EntityData] Failed to load more bulk connections:",
        error
      );
      setBulkConnectionData(
        produce((draft) => {
          draft.isLoadingMore = false;
          draft.error = (error as Error).message;
        })
      );
    } finally {
      removeActiveRequest(requestKey);
    }
  }, [
    bulkConnectionData,
    isRequestInProgress,
    addActiveRequest,
    removeActiveRequest,
  ]);

  const loadSingleConnectionData = useCallback(
    async (edgeId: string): Promise<EntityConnectionDataResponse | null> => {
      let itemType = resolutionModeRef.current;
      if (auditMode === "post_processing_audit" && selectedClusterId) {
        // ... (rest of the code remains the same)
        const auditClusterInfo = clustersWithAuditData.data?.clusters.find(
          (c) => c.id === selectedClusterId
        );
        if (auditClusterInfo) {
          itemType = auditClusterInfo.entityType;
        } else {
          console.warn(
            `[EntityData] loadSingleConnectionData: Could not find audit info for cluster ${selectedClusterId}. Falling back to global mode.`
          );
        }
      }

      await loadBulkConnections([{ edgeId, itemType: itemType }]);
      return connectionData[edgeId]?.data ?? null;
    },
    [
      loadBulkConnections,
      connectionData,
      auditMode,
      selectedClusterId,
      clustersWithAuditData.data,
    ]
  );

  const invalidateVisualizationData = useCallback(async (clusterId: string) => {
    console.log(
      `🔄 [EntityData] Invalidating and reloading visualization data for cluster: ${clusterId}`
    );
    await loadVisualizationDataForClustersRef.current([
      { clusterId, itemType: resolutionModeRef.current },
    ]);
  }, []);

  const invalidateConnectionData = useCallback(
    async (edgeId: string) => {
      console.log(
        `🔄 [EntityData] Invalidating and reloading connection data for edge: ${edgeId}`
      );

      let itemType = resolutionModeRef.current;
      if (auditMode === "post_processing_audit" && selectedClusterId) {
        const auditClusterInfo = clustersWithAuditData.data?.clusters.find(
          (c) => c.id === selectedClusterId
        );
        if (auditClusterInfo) {
          itemType = auditClusterInfo.entityType;
        } else {
          console.warn(
            `[EntityData] invalidateConnectionData: Could not find audit info for cluster ${selectedClusterId}. Falling back to global mode.`
          );
        }
      }

      await loadBulkConnections([{ edgeId, itemType: itemType }]);
    },
    [
      loadBulkConnections,
      auditMode,
      selectedClusterId,
      clustersWithAuditData.data,
    ]
  );

  // ========================================================================
  // Helper Functions for Progress Updates
  // ========================================================================

  const isCrossSourceEdge = useCallback(
    (clusterId: string, edgeId: string): boolean => {
      const vizState = visualizationData[clusterId];
      if (!vizState?.data?.links || !vizState?.data?.nodes) return false;

      const edge = vizState.data.links.find((l) => l.id === edgeId);
      if (!edge) return false;

      const sourceNodeId =
        typeof edge.source === "string" ? edge.source : edge.source.id;
      const targetNodeId =
        typeof edge.target === "string" ? edge.target : edge.target.id;

      const sourceNode = vizState.data.nodes.find((n) => n.id === sourceNodeId);
      const targetNode = vizState.data.nodes.find((n) => n.id === targetNodeId);

      if (!sourceNode || !targetNode) return false;

      return !!(
        sourceNode.sourceSystem &&
        targetNode.sourceSystem &&
        sourceNode.sourceSystem !== targetNode.sourceSystem
      );
    },
    [visualizationData]
  );

  // ========================================================================
  // Audit Filtering Functions
  // ========================================================================

  const getAuditAffectedEdges = useCallback(
    (
      clusterId: string,
      auditDecisions?: PostProcessingDecision[]
    ): string[] => {
      const decisionsToUse =
        auditDecisions || postProcessingAuditData?.data?.decisions || [];

      console.log(
        `🔍 [EntityData] getAuditAffectedEdges called for cluster ${clusterId} with ${decisionsToUse.length} total decisions`
      );

      // ✅ ADD: Debug the unique cluster IDs in the decisions
      const uniqueClusterIds = [
        ...new Set(decisionsToUse.map((d) => d.clusterId)),
      ];
      console.log(
        `🔍 [EntityData] Unique cluster IDs in audit decisions:`,
        uniqueClusterIds
      );

      const clusterDecisions = decisionsToUse.filter(
        (d) => d.clusterId === clusterId
      );

      console.log(
        `📋 [EntityData] Found ${clusterDecisions.length} decisions for cluster ${clusterId}`
      );

      // ✅ ADD: If no decisions found but we have decisions for other clusters, log warning
      if (clusterDecisions.length === 0 && decisionsToUse.length > 0) {
        console.warn(
          `⚠️ [EntityData] MISMATCH: Have ${decisionsToUse.length} total decisions but 0 for cluster ${clusterId}`
        );
        console.warn(`   Requested cluster: ${clusterId}`);
        console.warn(
          `   Available cluster IDs: ${uniqueClusterIds.join(", ")}`
        );
        console.warn(
          `   This suggests global audit data overwrote cluster-specific data`
        );
      }

      const affectedEdgeIds = [
        ...new Set(clusterDecisions.map((d) => d.edgeId)),
      ];

      console.log(
        `🔍 [EntityData] Audit affected edges for cluster ${clusterId}: ${affectedEdgeIds.length} edges`,
        affectedEdgeIds.length > 0 ? affectedEdgeIds : "none"
      );

      return affectedEdgeIds;
    },
    [postProcessingAuditData]
  );

  const getAuditFilteredVisualizationData = useCallback(
    (clusterId: string): VisualizationData | null => {
      const vizState = visualizationData[clusterId];
      if (!vizState?.data) {
        console.log(
          `🚫 [EntityData] No visualization data available for cluster ${clusterId}`
        );
        return null;
      }
  
      if (auditMode === "post_processing_audit") {
        if (postProcessingAuditData.loading) {
          console.log(
            `⏳ [EntityData] Audit data still loading for cluster ${clusterId}, returning null`
          );
          return null;
        }
  
        if (!postProcessingAuditData.data) {
          console.log(
            `🚫 [EntityData] No audit data available for cluster ${clusterId}`
          );
          // If no audit data, return an empty graph structure
          return {
            ...vizState.data,
            nodes: [],
            links: [],
          };
        }
  
        const affectedEdgeIds = new Set(getAuditAffectedEdges(clusterId));
  
        console.log(
          `🔍 [EntityData] Filtering audit visualization for cluster ${clusterId}: ${affectedEdgeIds.size} affected edges out of ${vizState.data.links.length} total links`
        );
  
        // If no affected edges are found for this cluster, return an empty graph.
        if (affectedEdgeIds.size === 0) {
          console.log(
            `⚠️ [EntityData] No audit-affected edges for cluster ${clusterId} - returning an empty graph.`
          );
          return {
            ...vizState.data,
            nodes: [],
            links: [],
          };
        }
  
        const filteredLinks = vizState.data.links.filter((link) =>
          affectedEdgeIds.has(link.id)
        );
  
        const connectedNodeIds = new Set<string>();
        filteredLinks.forEach((link) => {
          const sourceId =
            typeof link.source === "string" ? link.source : link.source.id;
          const targetId =
            typeof link.target === "string" ? link.target : link.target.id;
          connectedNodeIds.add(sourceId);
          connectedNodeIds.add(targetId);
        });
  
        const filteredNodes = vizState.data.nodes.filter((node) =>
          connectedNodeIds.has(node.id)
        );
  
        console.log(
          `✅ [EntityData] Audit filtered visualization: ${filteredLinks.length}/${vizState.data.links.length} links, ${filteredNodes.length}/${vizState.data.nodes.length} nodes`
        );
  
        return {
          ...vizState.data,
          links: filteredLinks,
          nodes: filteredNodes,
        };
      }
  
      // If not in audit mode, return the original data
      return vizState.data;
    },
    [
      visualizationData,
      auditMode,
      postProcessingAuditData,
      getAuditAffectedEdges,
    ]
  );
  

  const loadAuditClusterRichData = useCallback(
    async (clusterId: string) => {
      if (!selectedOpinionRef.current) {
        console.log(
          "🚫 [EntityData] No opinion selected, skipping audit cluster data load"
        );
        return;
      }

      try {
        const auditClusterInfo = clustersWithAuditData.data?.clusters.find(
          (c) => c.id === clusterId
        );
        const itemTypeForCluster =
          auditClusterInfo?.entityType || resolutionModeRef.current;

        console.log(
          `🔄 [EntityData] Loading rich data for audit cluster: ${clusterId} with itemType: ${itemTypeForCluster}`
        );

        // ✅ FIX: Ensure visualization data is loaded first
        const vizState = visualizationData[clusterId];
        if (!vizState?.data || vizState?.error) {
          console.log(
            `🔄 [EntityData] Loading visualization data first for cluster ${clusterId}`
          );
          await loadVisualizationDataForClusters([
            {
              clusterId,
              itemType: itemTypeForCluster,
            },
          ]);
        }

        // ✅ FIX: Load cluster-specific audit data with better error handling
        setPostProcessingAuditData((prev) => ({
          ...prev,
          loading: true,
          error: null,
        }));

        // ✅ FIX: Load audit data with multiple attempts if needed
        let auditDataResponse: PostProcessingAuditResponse | null = null;
        let totalDecisionsLoaded = 0;
        const maxPages = 10; // Prevent infinite loops

        for (let page = 1; page <= maxPages; page++) {
          const pageParams: PostProcessingAuditParams = {
            clusterId, // ✅ CRITICAL: This ensures we only get data for this cluster
            entityType: itemTypeForCluster,
            postProcFilter: postProcessingFilter || undefined,
            reviewedByHuman: false,
            page: page,
            limit: 100,
          };

          console.log(
            `📄 [EntityData] Loading audit data page ${page} for cluster ${clusterId}:`,
            pageParams
          );

          const pageResponse = await getPostProcessingAuditData(
            pageParams,
            selectedOpinionRef.current
          );

          if (!pageResponse || pageResponse.decisions.length === 0) {
            console.log(
              `📄 [EntityData] No more audit data on page ${page}, stopping`
            );
            break;
          }

          // ✅ FIX: Validate that all decisions are for the requested cluster
          const clusterMismatchCount = pageResponse.decisions.filter(
            (d) => d.clusterId !== clusterId
          ).length;

          if (clusterMismatchCount > 0) {
            console.error(
              `❌ [EntityData] CLUSTER MISMATCH: ${clusterMismatchCount}/${pageResponse.decisions.length} decisions are for wrong cluster on page ${page}`
            );
            // Continue with only the correct decisions
            pageResponse.decisions = pageResponse.decisions.filter(
              (d) => d.clusterId === clusterId
            );
          }

          if (page === 1) {
            auditDataResponse = pageResponse;
            totalDecisionsLoaded = pageResponse.decisions.length;
          } else {
            auditDataResponse!.decisions.push(...pageResponse.decisions);
            totalDecisionsLoaded += pageResponse.decisions.length;
          }

          // Check if we've loaded all available data
          if (pageResponse.decisions.length < 100) {
            console.log(
              `📄 [EntityData] Loaded all available audit data (${totalDecisionsLoaded} decisions)`
            );
            break;
          }
        }

        if (!auditDataResponse || auditDataResponse.decisions.length === 0) {
          console.warn(
            `⚠️ [EntityData] No audit decisions found for cluster ${clusterId}`
          );
          setPostProcessingAuditData({
            data: {
              decisions: [],
              total: 0,
              page: 1,
              limit: 0,
              _clusterSpecific: clusterId,
            } as any,
            loading: false,
            error: null,
            lastUpdated: Date.now(),
          });
          return;
        }

        // ✅ FIX: Deduplicate decisions by ID
        const uniqueDecisions = Array.from(
          new Map(auditDataResponse.decisions.map((d) => [d.id, d])).values()
        );

        setPostProcessingAuditData({
          data: {
            decisions: uniqueDecisions,
            total: uniqueDecisions.length,
            page: 1,
            limit: uniqueDecisions.length,
            _clusterSpecific: clusterId,
          } as any,
          loading: false,
          error: null,
          lastUpdated: Date.now(),
        });

        console.log(
          `📊 [EntityData] Loaded ${uniqueDecisions.length} audit decisions for cluster ${clusterId}`
        );

        // ✅ FIX: Get affected edges and validate they exist
        const affectedEdgeIds = getAuditAffectedEdges(
          clusterId,
          uniqueDecisions
        );

        if (affectedEdgeIds.length > 0) {
          console.log(
            `🔗 [EntityData] Loading connection data for ${affectedEdgeIds.length} affected edges`
          );

          // ✅ FIX: Validate edges exist in visualization data first
          const currentVizState = visualizationData[clusterId];
          if (currentVizState?.data?.links) {
            const existingEdgeIds = new Set(
              currentVizState.data.links.map((l) => l.id)
            );
            const validEdgeIds = affectedEdgeIds.filter((edgeId) => {
              const exists = existingEdgeIds.has(edgeId);
              if (!exists) {
                console.warn(
                  `⚠️ [EntityData] Audit decision references non-existent edge: ${edgeId}`
                );
              }
              return exists;
            });

            if (validEdgeIds.length > 0) {
              const connectionItems = validEdgeIds.map((edgeId) => ({
                edgeId,
                itemType: itemTypeForCluster,
              }));

              await loadBulkConnections(
                connectionItems,
                workflowFilter === "cross-source-only"
              );

              console.log(
                `🔗 [EntityData] Loaded connection data for ${validEdgeIds.length} valid edges`
              );
            } else {
              console.warn(
                `⚠️ [EntityData] No valid edges found for audit decisions in cluster ${clusterId}`
              );
            }
          } else {
            console.warn(
              `⚠️ [EntityData] No visualization data available to validate audit edges for cluster ${clusterId}`
            );
          }
        } else {
          console.log(
            `ℹ️ [EntityData] No affected edges found for cluster ${clusterId}`
          );
        }

        // Load node details
        const updatedVizState = visualizationData[clusterId];
        if (updatedVizState?.data?.nodes) {
          const nodeIds = updatedVizState.data.nodes.map((node) => ({
            id: node.id,
            nodeType: itemTypeForCluster,
          }));
          await loadBulkNodeDetails(nodeIds);
          console.log(
            `👥 [EntityData] Loaded node details for ${nodeIds.length} nodes`
          );
        }

        console.log(
          `✅ [EntityData] Audit cluster rich data loaded for ${clusterId}`
        );
      } catch (error) {
        console.error(
          `❌ [EntityData] Failed to load audit cluster rich data:`,
          error
        );
        setPostProcessingAuditData((prev) => ({
          ...prev,
          loading: false,
          error: (error as Error).message,
        }));

        toast({
          title: "Error",
          description: `Failed to load audit cluster data: ${
            (error as Error).message
          }`,
          variant: "destructive",
        });
      }
    },
    [
      clustersWithAuditData.data,
      resolutionModeRef,
      postProcessingFilter,
      workflowFilter,
      visualizationData,
      loadVisualizationDataForClusters,
      loadBulkConnections,
      loadBulkNodeDetails,
      getAuditAffectedEdges,
      toast,
    ]
  );

  // ========================================================================
  // Optimistic Update Functions
  // ========================================================================

  const updateProgressOptimistically = useCallback(
    (
      clusterId: string,
      edgeId: string,
      oldWasReviewed: boolean,
      newWasReviewed: boolean,
      newStatus: BaseLink["status"],
      filterType: WorkflowFilter
    ): (() => void) => {
      console.log(
        `🚀 [EntityData] Optimistic progress update: Cluster ${clusterId}, Edge ${edgeId} -> ${newStatus}`
      );

      const originalServerProgress = serverProgress[clusterId];

      if (!originalServerProgress) {
        console.warn(`No server progress data for cluster ${clusterId}`);
        return () => {};
      }

      setServerProgress((prev) => {
        return produce(prev, (draft) => {
          const clusterProg = draft[clusterId];
          if (!clusterProg) return;

          const shouldUpdateCrossSource = isCrossSourceEdge(clusterId, edgeId);

          const viewsToUpdate = [
            { key: "allEdges", shouldUpdate: true },
            { key: "crossSourceEdges", shouldUpdate: shouldUpdateCrossSource },
            {
              key: "currentView",
              shouldUpdate:
                filterType === "all" ||
                (filterType === "cross-source-only" && shouldUpdateCrossSource),
            },
          ];

          viewsToUpdate.forEach(({ key, shouldUpdate }) => {
            if (!shouldUpdate) return;

            const view = clusterProg[key as keyof ClusterProgress];

            if (!oldWasReviewed && newWasReviewed && view.pendingEdges > 0) {
              view.pendingEdges -= 1;
              view.reviewedEdges += 1;
              view.progressPercentage =
                view.totalEdges > 0
                  ? Math.round((view.reviewedEdges / view.totalEdges) * 100)
                  : 100;
              view.isComplete = view.pendingEdges === 0;

              if (newStatus === "CONFIRMED_MATCH") {
                view.confirmedMatches += 1;
              } else if (newStatus === "CONFIRMED_NON_MATCH") {
                view.confirmedNonMatches += 1;
              }

              console.log(
                `✅ [EntityData] Updated ${key} progress for cluster ${clusterId}: ${view.reviewedEdges}/${view.totalEdges} (${view.progressPercentage}%)`
              );
            }
          });
        });
      });

      return () => {
        console.log(
          `↩️ [EntityData] Reverting optimistic progress update for cluster ${clusterId}`
        );

        if (originalServerProgress) {
          setServerProgress((prev) => ({
            ...prev,
            [clusterId]: originalServerProgress,
          }));
        }
      };
    },
    [serverProgress, isCrossSourceEdge]
  );

  const updateEdgeStatusOptimistically = useCallback(
    (
      clusterId: string,
      edgeId: string,
      newStatus: BaseLink["status"],
      wasReviewed: boolean = true
    ): (() => void) => {
      console.log(
        `🚀 [EntityData] Optimistic edge update: Edge ${edgeId} in cluster ${clusterId} -> ${newStatus}`
      );

      const originalVizState = visualizationData[clusterId];
      const originalConnectionState = connectionData[edgeId];

      const currentEdge = originalVizState?.data?.links?.find(
        (l) => l.id === edgeId
      );
      const oldWasReviewed = currentEdge?.wasReviewed ?? false;

      setVisualizationData((prev) => {
        if (!prev[clusterId]?.data) return prev;

        return produce(prev, (draft) => {
          const vizState = draft[clusterId];
          if (vizState?.data?.links) {
            const link = vizState.data.links.find((l) => l.id === edgeId);
            if (link) {
              link.status = newStatus;
              link.wasReviewed = wasReviewed;
              console.log(
                `✅ [EntityData] Optimistically updated edge ${edgeId} status to ${newStatus}`
              );
            }
          }
        });
      });

      setConnectionData((prev) => {
        if (!prev[edgeId]?.data) return prev;

        return produce(prev, (draft) => {
          const connState = draft[edgeId];
          if (connState?.data?.edge) {
            connState.data.edge.status = newStatus ?? null;
            connState.data.edge.wasReviewed = wasReviewed;
            console.log(
              `✅ [EntityData] Optimistically updated connection ${edgeId} status to ${newStatus}`
            );
          }
        });
      });

      const revertProgress = updateProgressOptimistically(
        clusterId,
        edgeId,
        oldWasReviewed,
        wasReviewed,
        newStatus,
        workflowFilterRef.current
      );

      return () => {
        console.log(
          `↩️ [EntityData] Reverting optimistic update for edge ${edgeId}`
        );

        if (originalVizState) {
          setVisualizationData((prev) => ({
            ...prev,
            [clusterId]: originalVizState,
          }));
        }

        if (originalConnectionState) {
          setConnectionData((prev) => ({
            ...prev,
            [edgeId]: originalConnectionState,
          }));
        }

        revertProgress();
      };
    },
    [visualizationData, connectionData, updateProgressOptimistically]
  );

  const updateClusterCompletionOptimistically = useCallback(
    (clusterId: string, wasReviewed: boolean): (() => void) => {
      console.log(
        `🚀 [EntityData] Optimistic cluster completion update: ${clusterId} -> ${wasReviewed}`
      );

      const originalClusterState = clusters;

      setClusters((prev) => {
        return produce(prev, (draft) => {
          const cluster = draft.data.find((c) => c.id === clusterId);
          if (cluster) {
            cluster.wasReviewed = wasReviewed;
            console.log(
              `✅ [EntityData] Optimistically updated cluster ${clusterId} completion to ${wasReviewed}`
            );
          }
        });
      });

      return () => {
        console.log(
          `↩️ [EntityData] Reverting optimistic cluster completion update for ${clusterId}`
        );
        setClusters(originalClusterState);
      };
    },
    [clusters]
  );

  // ========================================================================
  // Helper Functions
  // ========================================================================

  const getThreeClustersToKeep = useCallback(
    (
      currentClusterId: string | null,
      clustersData: EntityCluster[]
    ): Set<string> => {
      const clustersToKeep = new Set<string>();

      if (!currentClusterId || clustersData.length === 0) {
        return clustersToKeep;
      }

      clustersToKeep.add(currentClusterId);
      const currentIndex = clustersData.findIndex(
        (c) => c.id === currentClusterId
      );

      if (currentIndex !== -1) {
        if (currentIndex > 0) {
          clustersToKeep.add(clustersData[currentIndex - 1].id);
        }
        if (currentIndex < clustersData.length - 1) {
          clustersToKeep.add(clustersData[currentIndex + 1].id);
        }

        if (clustersToKeep.size < 3) {
          if (currentIndex === 0 && clustersData.length > 2) {
            clustersToKeep.add(clustersData[2].id);
          } else if (
            currentIndex === clustersData.length - 1 &&
            clustersData.length > 2
          ) {
            clustersToKeep.add(clustersData[clustersData.length - 3].id);
          }
        }
      } else {
        if (clustersData.length > 0) clustersToKeep.add(clustersData[0].id);
        if (clustersData.length > 1) clustersToKeep.add(clustersData[1].id);
      }

      return clustersToKeep;
    },
    []
  );

  // ========================================================================
  // Audit Data Query Functions
  // ========================================================================

  const isPostProcessingAuditDataLoaded = useCallback(() => {
    return (
      !!postProcessingAuditData.data &&
      !postProcessingAuditData.loading &&
      !postProcessingAuditData.error
    );
  }, [postProcessingAuditData]);

  const isPostProcessingAuditDataLoading = useCallback(() => {
    return postProcessingAuditData.loading;
  }, [postProcessingAuditData.loading]);

  const isClustersWithAuditDataLoaded = useCallback(() => {
    return (
      !!clustersWithAuditData.data &&
      !clustersWithAuditData.loading &&
      !clustersWithAuditData.error
    );
  }, [clustersWithAuditData]);

  const isClustersWithAuditDataLoading = useCallback(() => {
    return clustersWithAuditData.loading;
  }, [clustersWithAuditData.loading]);

  const getPostProcessingAuditError = useCallback(() => {
    return postProcessingAuditData.error;
  }, [postProcessingAuditData.error]);

  const getClustersWithAuditError = useCallback(() => {
    return clustersWithAuditData.error;
  }, [clustersWithAuditData.error]);

  // ========================================================================
  // Cache Management
  // ========================================================================

  const performThreeClusterCleanup = useCallback(
    (force: boolean = false) => {
      const currentState = JSON.stringify({
        selectedClusterId,
        clustersData: clusters.data.map((c) => c.id),
        visualizationKeys: Object.keys(visualizationData),
        activelyPaging: activelyPagingClusterId,
      });

      if (!force && currentState === lastCleanupStateRef.current) {
        console.log("🚫 [EntityData] Skipping redundant cleanup - same state");
        return;
      }

      if (cleanupTimeoutRef.current) {
        clearTimeout(cleanupTimeoutRef.current);
        cleanupTimeoutRef.current = null;
      }

      lastCleanupStateRef.current = currentState;

      if (!selectedClusterId) {
        const keepSet = activelyPagingClusterId
          ? new Set([activelyPagingClusterId])
          : new Set<string>();

        setVisualizationData((prev) => {
          const cleaned = Object.fromEntries(
            Object.entries(prev).filter(([clusterId]) => keepSet.has(clusterId))
          );
          if (Object.keys(cleaned).length !== Object.keys(prev).length) {
            console.log(
              `🧹 [EntityData] Three-cluster cleanup (no selection): ${
                Object.keys(prev).length
              } -> ${Object.keys(cleaned).length} clusters`
            );
          }
          return cleaned;
        });
        return;
      }

      const clustersToKeep = getThreeClustersToKeep(
        selectedClusterId,
        clusters.data
      );

      if (activelyPagingClusterId) {
        clustersToKeep.add(activelyPagingClusterId);
      }

      const currentClusterIds = new Set(Object.keys(visualizationData));
      const clustersToRemove = [...currentClusterIds].filter(
        (id) => !clustersToKeep.has(id)
      );

      if (clustersToRemove.length === 0 && !force) {
        return;
      }

      console.log(
        `🧹 [EntityData] Three-cluster cleanup: keeping [${Array.from(
          clustersToKeep
        ).join(", ")}], removing [${clustersToRemove.join(", ")}]`
      );

      setVisualizationData((prev) => {
        const cleaned = Object.fromEntries(
          Object.entries(prev).filter(([clusterId]) =>
            clustersToKeep.has(clusterId)
          )
        );
        return cleaned;
      });

      setServerProgress((prev) => {
        const cleaned = Object.fromEntries(
          Object.entries(prev).filter(([clusterId]) =>
            clustersToKeep.has(clusterId)
          )
        );
        return cleaned;
      });

      const edgesToKeep = new Set<string>();
      if (state.selectedEdgeId) {
        edgesToKeep.add(state.selectedEdgeId);
      }

      Object.entries(visualizationData).forEach(([clusterId, vizState]) => {
        if (clustersToKeep.has(clusterId) && vizState.data?.links) {
          vizState.data.links.forEach((link) => edgesToKeep.add(link.id));
        }
      });

      setConnectionData((prev) => {
        const cleaned = Object.fromEntries(
          Object.entries(prev).filter(([edgeId]) => edgesToKeep.has(edgeId))
        );
        if (Object.keys(cleaned).length !== Object.keys(prev).length) {
          console.log(
            `🧹 [EntityData] Connection cleanup: ${
              Object.keys(prev).length
            } -> ${Object.keys(cleaned).length} edges`
          );
        }
        return cleaned;
      });

      const nodeIdsToKeep = new Set<string>();
      Object.entries(visualizationData).forEach(([clusterId, vizState]) => {
        if (clustersToKeep.has(clusterId) && vizState.data?.nodes) {
          vizState.data.nodes.forEach((node) => nodeIdsToKeep.add(node.id));
        }
      });

      Object.entries(connectionData).forEach(([edgeId, connState]) => {
        if (edgesToKeep.has(edgeId) && connState.data) {
          if (connState.data.entity1)
            nodeIdsToKeep.add(connState.data.entity1.id);
          if (connState.data.entity2)
            nodeIdsToKeep.add(connState.data.entity2.id);
        }
      });

      setNodeDetails((prev) => {
        const cleaned = Object.fromEntries(
          Object.entries(prev).filter(([nodeId]) => nodeIdsToKeep.has(nodeId))
        );
        if (Object.keys(cleaned).length !== Object.keys(prev).length) {
          console.log(
            `🧹 [EntityData] Node cleanup: ${Object.keys(prev).length} -> ${
              Object.keys(cleaned).length
            } nodes`
          );
        }
        return cleaned;
      });

      setPendingNodeFetches((prev) => {
        const cleaned = new Set(
          [...prev].filter((nodeId) => nodeIdsToKeep.has(nodeId))
        );
        return cleaned;
      });
    },
    [
      selectedClusterId,
      clusters.data,
      getThreeClustersToKeep,
      activelyPagingClusterId,
      state.selectedEdgeId,
      visualizationData,
      connectionData,
    ]
  );

  // ========================================================================
  // ENHANCED: Effects with Aggressive Refresh Handling
  // ========================================================================

  useEffect(() => {
    const currentOpinion = selectedOpinionRef.current;
    const currentMode = resolutionModeRef.current;
    const currentFilterStatus = clusterFilterStatusRef.current;
    const currentWorkflowFilter = workflowFilterRef.current;
    const currentClusters = clustersRef.current;

    if (!currentOpinion) return;

    const initParams = `${currentMode}-${currentFilterStatus}-${currentWorkflowFilter}-${currentOpinion}`;

    // ENHANCED: More aggressive refresh trigger detection
    const previousParams = initializationRef.current.lastInitParams;
    const isNewParamCombination = initParams !== previousParams;

    if (isNewParamCombination) {
      console.log(
        `🔄 [EntityData] Parameter combination changed from '${previousParams}' to '${initParams}' - clearing all data`
      );

      // Clear all data immediately when parameters change
      clearAllData();

      initializationRef.current = {
        hasInitialized: true,
        lastInitParams: initParams,
      };

      // Load fresh data after clearing
      setTimeout(() => {
        loadClusterProgress(1, currentClusters.limit);
      }, 100);

      return;
    }

    // Normal initialization logic for first load
    if (
      !initializationRef.current.hasInitialized &&
      (currentClusters.data.length === 0 || currentClusters.error) &&
      !currentClusters.loading
    ) {
      console.log(
        `[EntityData] Initial cluster progress load triggered for filter: ${currentFilterStatus}, workflow: ${currentWorkflowFilter}`
      );

      initializationRef.current = {
        hasInitialized: true,
        lastInitParams: initParams,
      };

      loadClusterProgress(1, currentClusters.limit);
    }
  }, [
    resolutionMode,
    clusterFilterStatus,
    workflowFilter,
    selectedOpinion,
    refreshTrigger,
    clearAllData,
  ]);

  // ENHANCED: Refresh trigger effect with complete data clearing
  useEffect(() => {
    if (refreshTrigger > 0) {
      console.log(
        "🔄 [EntityData] Refresh trigger activated - clearing all data and reloading"
      );

      // Clear all data when refresh is triggered
      clearAllData();

      // Small delay to ensure state is cleared before reloading
      setTimeout(() => {
        if (selectedOpinionRef.current) {
          const currentClusters = clustersRef.current;
          loadClusterProgress(1, currentClusters.limit);
        }
      }, 50);
    }
  }, [refreshTrigger, clearAllData]);

  useEffect(() => {
    if (
      state.auditMode === "post_processing_audit" &&
      selectedOpinionRef.current &&
      !postProcessingAuditData.data &&
      !postProcessingAuditData.loading
    ) {
      // ✅ SIMPLE FIX: Don't filter by entity type initially - let the API return what's available
      const auditParams: PostProcessingAuditParams = {
        // ❌ REMOVE: entityType: state.resolutionMode, // Don't filter by entity type
        postProcFilter: state.postProcessingFilter || undefined,
        reviewedByHuman: false,
        page: 1,
        limit: 20,
        workflowFilter: state.workflowFilter || undefined,
      };

      console.log(
        `🔄 [EntityData] Audit mode activated, loading initial audit data (no entity type filter):`,
        auditParams
      );

      Promise.all([
        loadPostProcessingAuditData(auditParams),
        loadClustersWithAuditData(auditParams),
      ]);
    }
  }, [
    state.auditMode,
    state.resolutionMode, // Keep this for re-triggering if mode changes
    state.postProcessingFilter,
    postProcessingAuditData.data,
    postProcessingAuditData.loading,
    loadPostProcessingAuditData,
    loadClustersWithAuditData,
  ]);

  useEffect(() => {
    if (!selectedClusterId || clusters.data.length === 0) return;

    if (cleanupTimeoutRef.current) {
      clearTimeout(cleanupTimeoutRef.current);
    }

    cleanupTimeoutRef.current = setTimeout(() => {
      const currentState = JSON.stringify({
        selectedClusterId,
        clustersCount: clusters.data.length,
        visualizationKeys: Object.keys(visualizationData).length,
      });

      if (currentState !== lastCleanupStateRef.current) {
        console.log("🧹 [EntityData] Debounced cleanup triggered");
        performThreeClusterCleanup();
        lastCleanupStateRef.current = currentState;
      }
      cleanupTimeoutRef.current = null;
    }, 5000);

    return () => {
      if (cleanupTimeoutRef.current) {
        clearTimeout(cleanupTimeoutRef.current);
        cleanupTimeoutRef.current = null;
      }
    };
  }, [
    selectedClusterId,
    clusters.data.length,
    visualizationData,
    performThreeClusterCleanup,
  ]);

  // ========================================================================
  // Context Value Assembly
  // ========================================================================

  const contextValue: EntityDataContextType = {
    // Audit data stores
    postProcessingAuditData,
    clustersWithAuditData,

    // Data stores
    clusters,
    visualizationData,
    connectionData,
    nodeDetails,
    bulkConnectionData,
    serverProgress,
    overallServerProgress,

    // Loading functions
    loadClusters,
    loadClusterProgress,
    loadVisualizationDataForClusters,
    loadMoreConnectionsForCluster, // NEW: Pagination
    loadBulkConnections,
    loadMoreBulkConnections,
    loadBulkNodeDetails,
    loadSingleConnectionData,

    // Audit loading functions
    loadPostProcessingAuditData,
    loadClustersWithAuditData,
    bulkMarkDecisionsReviewed,

    // Cache management
    invalidateVisualizationData,
    invalidateConnectionData,
    performThreeClusterCleanup,
    clearAllData,

    // Optimistic update functions
    updateEdgeStatusOptimistically,
    updateClusterCompletionOptimistically,
    updateProgressOptimistically,

    // Audit filtering functions
    getAuditAffectedEdges,
    getAuditFilteredVisualizationData,
    loadAuditClusterRichData,

    // Data queries
    isVisualizationDataLoaded: useCallback(
      (clusterId: string) =>
        !!visualizationData[clusterId]?.data &&
        !visualizationData[clusterId]?.loading &&
        !visualizationData[clusterId]?.error,
      [visualizationData]
    ),

    isVisualizationDataLoading: useCallback(
      (clusterId: string) => !!visualizationData[clusterId]?.loading,
      [visualizationData]
    ),

    isConnectionDataLoaded: useCallback(
      (edgeId: string) =>
        !!connectionData[edgeId]?.data &&
        !connectionData[edgeId]?.loading &&
        !connectionData[edgeId]?.error,
      [connectionData]
    ),

    isConnectionDataLoading: useCallback(
      (edgeId: string) => !!connectionData[edgeId]?.loading,
      [connectionData]
    ),

    getVisualizationError: useCallback(
      (clusterId: string) => visualizationData[clusterId]?.error || null,
      [visualizationData]
    ),

    getConnectionError: useCallback(
      (edgeId: string) => connectionData[edgeId]?.error || null,
      [connectionData]
    ),

    getClusterById: useCallback(
      (clusterId: string) => clusters.data.find((c) => c.id === clusterId),
      [clusters.data]
    ),

    getNodeDetail: useCallback(
      (nodeId: string) => {
        const detail = nodeDetails[nodeId];
        if (detail) {
          return detail;
        }

        for (const key in connectionData) {
          const conn = connectionData[key]?.data;
          if (conn?.entity1?.id === nodeId)
            return {
              id: nodeId,
              nodeType: "entity",
              baseData: conn.entity1,
              attributes: {},
            };
          if (conn?.entity2?.id === nodeId)
            return {
              id: nodeId,
              nodeType: "entity",
              baseData: conn.entity2,
              attributes: {},
            };
        }

        return null;
      },
      [nodeDetails, connectionData]
    ),

    // NEW: Pagination query
    getClusterPaginationState: useCallback(
      (clusterId: string) => {
        const state = visualizationData[clusterId];
        return {
          hasMore: state?.hasMore ?? false,
          isLoadingMore: state?.isLoadingMore ?? false,
          nextCursor: state?.nextCursor ?? null,
        };
      },
      [visualizationData]
    ),

    // Audit data queries
    isPostProcessingAuditDataLoaded,
    isPostProcessingAuditDataLoading,
    isClustersWithAuditDataLoaded,
    isClustersWithAuditDataLoading,
    getPostProcessingAuditError,
    getClustersWithAuditError,

    getCacheStats: useCallback(() => {
      const stats = {
        clusters: Object.keys(visualizationData).length,
        connections: Object.keys(connectionData).length,
        nodes: Object.keys(nodeDetails).length,
        pendingFetches: pendingNodeFetches.size,
        activeRequests: activeRequestsRef.current.size,
        auditData: {
          postProcessingLoaded: !!postProcessingAuditData.data,
          clustersAuditLoaded: !!clustersWithAuditData.data,
        },
      };
      console.table(stats);
      return stats;
    }, [
      visualizationData,
      connectionData,
      nodeDetails,
      pendingNodeFetches,
      postProcessingAuditData,
      clustersWithAuditData,
    ]),
  };

  return (
    <EntityDataContext.Provider value={contextValue}>
      {children}
    </EntityDataContext.Provider>
  );
}

// ============================================================================
// Hook for Consuming Context
// ============================================================================

export function useEntityData(): EntityDataContextType {
  const context = useContext(EntityDataContext);

  if (context === undefined) {
    throw new Error("useEntityData must be used within an EntityDataProvider");
  }

  return context;
}

// ============================================================================
// Utility Hooks
// ============================================================================

export function useClusterData() {
  const { clusters, loadClusters, loadClusterProgress, getClusterById } =
    useEntityData();

  return {
    clusters,
    loadClusters,
    loadClusterProgress,
    getClusterById,
  };
}

export function useVisualizationData() {
  const {
    visualizationData,
    loadVisualizationDataForClusters,
    loadMoreConnectionsForCluster, // NEW
    isVisualizationDataLoaded,
    isVisualizationDataLoading,
    getVisualizationError,
    getClusterPaginationState, // NEW
    invalidateVisualizationData,
    updateEdgeStatusOptimistically,
  } = useEntityData();

  return {
    visualizationData,
    loadVisualizationDataForClusters,
    loadMoreConnectionsForCluster, // NEW
    isVisualizationDataLoaded,
    isVisualizationDataLoading,
    getVisualizationError,
    getClusterPaginationState, // NEW
    invalidateVisualizationData,
    updateEdgeStatusOptimistically,
  };
}

export function useConnectionData() {
  const {
    connectionData,
    loadBulkConnections,
    loadSingleConnectionData,
    isConnectionDataLoaded,
    isConnectionDataLoading,
    getConnectionError,
    invalidateConnectionData,
  } = useEntityData();

  return {
    connectionData,
    loadBulkConnections,
    loadSingleConnectionData,
    isConnectionDataLoaded,
    isConnectionDataLoading,
    getConnectionError,
    invalidateConnectionData,
  };
}

export function useAuditData() {
  const {
    postProcessingAuditData,
    clustersWithAuditData,
    loadPostProcessingAuditData,
    loadClustersWithAuditData,
    bulkMarkDecisionsReviewed,
    isPostProcessingAuditDataLoaded,
    isPostProcessingAuditDataLoading,
    isClustersWithAuditDataLoaded,
    isClustersWithAuditDataLoading,
    getPostProcessingAuditError,
    getClustersWithAuditError,
    getAuditAffectedEdges,
    getAuditFilteredVisualizationData,
    loadAuditClusterRichData,
  } = useEntityData();

  return {
    postProcessingAuditData,
    clustersWithAuditData,
    loadPostProcessingAuditData,
    loadClustersWithAuditData,
    bulkMarkDecisionsReviewed,
    isPostProcessingAuditDataLoaded,
    isPostProcessingAuditDataLoading,
    isClustersWithAuditDataLoaded,
    isClustersWithAuditDataLoading,
    getPostProcessingAuditError,
    getClustersWithAuditError,
    getAuditAffectedEdges,
    getAuditFilteredVisualizationData,
    loadAuditClusterRichData,
  };
}

// context/entity-resolution-context.tsx
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
  BulkConnectionRequestItem,
  BulkVisualizationRequestItem,
  ClusterReviewProgress,
  EdgeReviewApiPayload,
  ClusterFilterStatus,
} from "@/types/entity-resolution";
import {
  getServiceClusters,
  getBulkNodeDetails,
  getBulkConnections,
  getBulkVisualizations,
  getOrganizationClusters,
  getOrganizationConnectionData,
  getServiceConnectionData,
  postEdgeReview,
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
import { useToast } from "@/hooks/use-toast";
import { produce } from "immer";
import _ from "lodash";

// Helper to get unique items from an array based on a key selector
function uniqueBy<T>(items: T[], keySelector: (item: T) => string): T[] {
  return Array.from(
    new Map(items.map((item) => [keySelector(item), item])).values()
  );
}

// Constants
const MAX_BULK_FETCH_SIZE = 50;
const LARGE_CLUSTER_THRESHOLD = 200;
const CONNECTION_PAGE_SIZE = 200;

// State interfaces
interface ClustersState {
  data: EntityCluster[];
  total: number;
  page: number;
  limit: number;
  loading: boolean;
  error: string | null;
}

interface VisualizationState {
  data: EntityVisualizationDataResponse | null;
  loading: boolean;
  error: string | null;
  lastUpdated: number | null;
}

interface ConnectionState {
  data: EntityConnectionDataResponse | null;
  loading: boolean;
  error: string | null;
  lastUpdated: number | null;
}

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

export interface EntityResolutionContextType {
  resolutionMode: ResolutionMode;
  selectedClusterId: string | null;
  selectedEdgeId: string | null;
  reviewerId: string;
  lastReviewedEdgeId: string | null;
  refreshTrigger: number;
  isAutoAdvanceEnabled: boolean;
  isReviewToolsMaximized: boolean;
  clusterFilterStatus: ClusterFilterStatus;

  clusters: ClustersState;
  visualizationData: Record<string, VisualizationState>;
  connectionData: Record<string, ConnectionState>;
  nodeDetails: Record<string, NodeDetailResponse | null | "loading" | "error">;

  clusterProgress: Record<string, ClusterReviewProgress>;
  edgeSelectionInfo: EdgeSelectionInfo;

  currentVisualizationData: EntityVisualizationDataResponse | null;
  currentConnectionData: EntityConnectionDataResponse | null;
  selectedClusterDetails: EntityCluster | null;

  activelyPagingClusterId: string | null;
  largeClusterConnectionsPage: number;
  isLoadingConnectionPageData: boolean;

  actions: {
    setResolutionMode: (mode: ResolutionMode) => void;
    setSelectedClusterId: (id: string | null) => void;
    setSelectedEdgeId: (id: string | null) => void;
    setReviewerId: (id: string) => void;
    setLastReviewedEdgeId: (id: string | null) => void;
    setIsReviewToolsMaximized: (isMaximized: boolean) => void;
    setClusterFilterStatus: (status: ClusterFilterStatus) => void;
    triggerRefresh: (
      target?:
        | "all"
        | "clusters"
        | "current_visualization"
        | "current_connection"
    ) => void;
    loadClusters: (page: number, limit?: number) => Promise<void>;
    loadBulkNodeDetails: (nodesToFetch: NodeIdentifier[]) => Promise<void>;
    loadSingleConnectionData: (
      edgeId: string
    ) => Promise<EntityConnectionDataResponse | null>;
    invalidateVisualizationData: (clusterId: string) => Promise<void>;
    invalidateConnectionData: (edgeId: string) => Promise<void>;
    clearAllData: () => void;
    selectNextUnreviewedEdge: (afterEdgeId?: string | null) => void;
    advanceToNextCluster: () => Promise<void>;
    checkAndAdvanceIfComplete: (clusterIdToCheck?: string) => Promise<void>;
    submitEdgeReview: (
      edgeId: string,
      decision: GroupReviewDecision,
      notes?: string
    ) => Promise<void>;
    setIsAutoAdvanceEnabled: (enabled: boolean) => void;
    selectPreviousUnreviewedInCluster: () => void;
    selectNextUnreviewedInCluster: () => void;
    initializeLargeClusterConnectionPaging: (
      clusterId: string
    ) => Promise<void>;
    viewNextConnectionPage: (clusterId: string) => Promise<void>;
    getActivelyPagingClusterId: () => string | null;
    getLargeClusterConnectionsPage: () => number;
    performThreeClusterCleanup: () => void;
    getCacheStats: () => any;
  };

  queries: {
    isVisualizationDataLoaded: (clusterId: string) => boolean;
    isVisualizationDataLoading: (clusterId: string) => boolean;
    isConnectionDataLoaded: (edgeId: string) => boolean;
    isConnectionDataLoading: (edgeId: string) => boolean;
    getVisualizationError: (clusterId: string) => string | null;
    getConnectionError: (edgeId: string) => string | null;
    getClusterProgress: (clusterId: string) => ClusterReviewProgress;
    canAdvanceToNextCluster: () => boolean;
    isEdgeReviewed: (edgeId: string) => boolean;
    getEdgeStatus: (edgeId: string) => BaseLink["status"] | null;
    getEdgeSubmissionStatus: (edgeId: string) => {
      isSubmitting: boolean;
      error: string | null;
    };
    getClusterById: (clusterId: string) => EntityCluster | undefined;
    getNodeDetail: (
      nodeId: string
    ) => NodeDetailResponse | null | "loading" | "error";
  };
}

const EntityResolutionContext = createContext<
  EntityResolutionContextType | undefined
>(undefined);

const initialClustersState: ClustersState = {
  data: [],
  total: 0,
  page: 1,
  limit: 10,
  loading: false,
  error: null,
};

export function EntityResolutionProvider({
  children,
}: {
  children: ReactNode;
}) {
  const { user } = useAuth();
  const { toast } = useToast();

  // ==========================================
  // 1. ADD DEBOUNCING AND GUARDS TO CLEANUP
  // ==========================================
  const cleanupTimeoutRef = useRef<NodeJS.Timeout | null>(null);
  const lastCleanupStateRef = useRef<string>("");

  // Core state
  const [resolutionMode, setResolutionModeState] =
    useState<ResolutionMode>("entity");
  const [selectedClusterId, setSelectedClusterIdState] = useState<
    string | null
  >(null);
  const [selectedEdgeId, setSelectedEdgeIdState] = useState<string | null>(
    null
  );
  const [reviewerId, setReviewerId] = useState<string>("default-reviewer");
  const [refreshTrigger, setRefreshTrigger] = useState<number>(0);
  const [lastReviewedEdgeId, setLastReviewedEdgeId] = useState<string | null>(
    null
  );
  const [isAutoAdvanceEnabled, setIsAutoAdvanceEnabledState] =
    useState<boolean>(true);
  const [isReviewToolsMaximized, setIsReviewToolsMaximized] =
    useState<boolean>(false);
  const [clusterFilterStatus, setClusterFilterStatus] =
    useState<ClusterFilterStatus>("unreviewed");

  // Data state
  const [clusters, setClusters] = useState<ClustersState>(initialClustersState);
  const [visualizationData, setVisualizationData] = useState<
    Record<string, VisualizationState>
  >({});
  const [connectionData, setConnectionData] = useState<
    Record<string, ConnectionState>
  >({});
  const [nodeDetails, setNodeDetails] = useState<
    Record<string, NodeDetailResponse | null | "loading" | "error">
  >({});
  const [edgeSubmissionStatus, setEdgeSubmissionStatus] = useState<
    Record<string, { isSubmitting: boolean; error: string | null }>
  >({});

  // Paging state
  const [activelyPagingClusterId, setActivelyPagingClusterId] = useState<
    string | null
  >(null);
  const [largeClusterConnectionsPage, setLargeClusterConnectionsPage] =
    useState<number>(0);
  const [isLoadingPage, setIsLoadingPage] = useState<boolean>(false);
  const [pendingNodeFetches, setPendingNodeFetches] = useState<Set<string>>(
    new Set()
  );

  // Helper function to determine the three clusters to keep in memory
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

  // REPLACE the performThreeClusterCleanup function with this version:
  const performThreeClusterCleanup = useCallback(
    (
      targetClusterId: string | null = selectedClusterId,
      force: boolean = false
    ) => {
      // Generate state signature to prevent redundant cleanups
      const currentState = JSON.stringify({
        targetClusterId,
        clustersData: clusters.data.map((c) => c.id),
        visualizationKeys: Object.keys(visualizationData),
        activelyPaging: activelyPagingClusterId,
      });

      // Skip if we just ran cleanup with the same state (unless forced)
      if (!force && currentState === lastCleanupStateRef.current) {
        console.log("🚫 Skipping redundant cleanup - same state");
        return;
      }

      // Clear any pending cleanup
      if (cleanupTimeoutRef.current) {
        clearTimeout(cleanupTimeoutRef.current);
        cleanupTimeoutRef.current = null;
      }

      lastCleanupStateRef.current = currentState;

      if (!targetClusterId) {
        const keepSet = activelyPagingClusterId
          ? new Set([activelyPagingClusterId])
          : new Set<string>();

        setVisualizationData((prev) => {
          const cleaned = Object.fromEntries(
            Object.entries(prev).filter(([clusterId]) => keepSet.has(clusterId))
          );
          if (Object.keys(cleaned).length !== Object.keys(prev).length) {
            console.log(
              `🧹 Three-cluster cleanup (no selection): ${
                Object.keys(prev).length
              } -> ${Object.keys(cleaned).length} clusters`
            );
          }
          return cleaned;
        });
        return;
      }

      const clustersToKeep = getThreeClustersToKeep(
        targetClusterId,
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
        `🧹 Three-cluster cleanup: keeping [${Array.from(clustersToKeep).join(
          ", "
        )}], removing [${clustersToRemove.join(", ")}]`
      );

      // Clean visualization data
      setVisualizationData((prev) => {
        const cleaned = Object.fromEntries(
          Object.entries(prev).filter(([clusterId]) =>
            clustersToKeep.has(clusterId)
          )
        );
        return cleaned;
      });

      // Determine edges to keep
      const edgesToKeep = new Set<string>();
      if (selectedEdgeId) {
        edgesToKeep.add(selectedEdgeId);
      }

      Object.entries(visualizationData).forEach(([clusterId, state]) => {
        if (clustersToKeep.has(clusterId) && state.data?.links) {
          state.data.links.forEach((link) => edgesToKeep.add(link.id));
        }
      });

      // Clean connection data
      setConnectionData((prev) => {
        const cleaned = Object.fromEntries(
          Object.entries(prev).filter(([edgeId]) => edgesToKeep.has(edgeId))
        );
        if (Object.keys(cleaned).length !== Object.keys(prev).length) {
          console.log(
            `🧹 Connection cleanup: ${Object.keys(prev).length} -> ${
              Object.keys(cleaned).length
            } edges`
          );
        }
        return cleaned;
      });

      // Determine nodes to keep
      const nodeIdsToKeep = new Set<string>();
      Object.entries(visualizationData).forEach(([clusterId, state]) => {
        if (clustersToKeep.has(clusterId) && state.data?.nodes) {
          state.data.nodes.forEach((node) => nodeIdsToKeep.add(node.id));
        }
      });

      Object.entries(connectionData).forEach(([edgeId, state]) => {
        if (edgesToKeep.has(edgeId) && state.data) {
          if (state.data.entity1) nodeIdsToKeep.add(state.data.entity1.id);
          if (state.data.entity2) nodeIdsToKeep.add(state.data.entity2.id);
        }
      });

      // Clean node details
      setNodeDetails((prev) => {
        const cleaned = Object.fromEntries(
          Object.entries(prev).filter(([nodeId]) => nodeIdsToKeep.has(nodeId))
        );
        if (Object.keys(cleaned).length !== Object.keys(prev).length) {
          console.log(
            `🧹 Node cleanup: ${Object.keys(prev).length} -> ${
              Object.keys(cleaned).length
            } nodes`
          );
        }
        return cleaned;
      });

      // Clean edge submission status and pending fetches
      setEdgeSubmissionStatus((prev) => {
        const cleaned = Object.fromEntries(
          Object.entries(prev).filter(([edgeId]) => edgesToKeep.has(edgeId))
        );
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
      selectedEdgeId,
      visualizationData,
      connectionData,
    ]
  );

  // Queries object
  const queries = useMemo(() => {
    const getClusterById = (clusterId: string): EntityCluster | undefined => {
      return clusters.data.find((c) => c.id === clusterId);
    };

    const getClusterProgress = (
      clusterIdToQuery: string
    ): ClusterReviewProgress => {
      const vizState = visualizationData[clusterIdToQuery];
      const clusterDetails = getClusterById(clusterIdToQuery);

      if (vizState?.data?.links) {
        const totalEdges = vizState.data.links.length;
        const reviewedEdges = vizState.data.links.filter(
          (l) => l.wasReviewed
        ).length;
        const progressPercentage =
          totalEdges > 0 ? Math.round((reviewedEdges / totalEdges) * 100) : 100;
        return {
          totalEdges,
          reviewedEdges,
          pendingEdges: totalEdges - reviewedEdges,
          confirmedMatches: vizState.data.links.filter(
            (l) => l.status === "CONFIRMED_MATCH"
          ).length,
          confirmedNonMatches: vizState.data.links.filter(
            (l) => l.status === "CONFIRMED_NON_MATCH"
          ).length,
          progressPercentage,
          isComplete: reviewedEdges === totalEdges,
        };
      }

      if (clusterDetails) {
        if (clusterDetails.wasReviewed) {
          const total = clusterDetails.groupCount ?? 0;
          return {
            totalEdges: total,
            reviewedEdges: total,
            pendingEdges: 0,
            confirmedMatches: 0,
            confirmedNonMatches: 0,
            progressPercentage: 100,
            isComplete: true,
          };
        }
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
    };

    return {
      isVisualizationDataLoaded: (clusterId: string) =>
        !!visualizationData[clusterId]?.data &&
        !visualizationData[clusterId]?.loading &&
        !visualizationData[clusterId]?.error,
      isVisualizationDataLoading: (clusterId: string) =>
        !!visualizationData[clusterId]?.loading,
      isConnectionDataLoaded: (edgeId: string) =>
        !!connectionData[edgeId]?.data &&
        !connectionData[edgeId]?.loading &&
        !connectionData[edgeId]?.error,
      isConnectionDataLoading: (edgeId: string) =>
        !!connectionData[edgeId]?.loading,
      getVisualizationError: (clusterId: string) =>
        visualizationData[clusterId]?.error || null,
      getConnectionError: (edgeId: string) =>
        connectionData[edgeId]?.error || null,
      getClusterProgress,
      canAdvanceToNextCluster: () => {
        if (!selectedClusterId) return false;
        const progress = getClusterProgress(selectedClusterId);
        return progress?.isComplete || false;
      },
      isEdgeReviewed: (edgeId: string) => {
        const currentViz = selectedClusterId
          ? visualizationData[selectedClusterId]?.data
          : null;
        if (!currentViz?.links) return false;
        const edge = currentViz.links.find((l) => l.id === edgeId);
        return edge ? edge.wasReviewed === true : false;
      },
      getEdgeStatus: (edgeId: string) => {
        const currentViz = selectedClusterId
          ? visualizationData[selectedClusterId]?.data
          : null;
        if (!currentViz?.links) return null;
        const edge = currentViz.links.find((l) => l.id === edgeId);
        return edge?.status ?? null;
      },
      getEdgeSubmissionStatus: (edgeId: string) => {
        return (
          edgeSubmissionStatus[edgeId] || { isSubmitting: false, error: null }
        );
      },
      getClusterById,
      getNodeDetail: (nodeId: string) => nodeDetails[nodeId] || null,
    };
  }, [
    visualizationData,
    connectionData,
    selectedClusterId,
    edgeSubmissionStatus,
    clusters.data,
    nodeDetails,
  ]);

  const clearAllData = useCallback(() => {
    console.log("Clearing all data.");
    setClusters(initialClustersState);
    setVisualizationData({});
    setConnectionData({});
    setNodeDetails({});
    setPendingNodeFetches(new Set());
    setSelectedClusterIdState(null);
    setSelectedEdgeIdState(null);
    setLastReviewedEdgeId(null);
    setIsAutoAdvanceEnabledState(true);
    setActivelyPagingClusterId(null);
    setLargeClusterConnectionsPage(0);
    setIsLoadingPage(false);
    setEdgeSubmissionStatus({});
  }, []);

  const setResolutionMode = useCallback(
    (mode: ResolutionMode) => {
      if (mode === resolutionMode) return;
      console.log(
        `Switching resolution mode from ${resolutionMode} to ${mode}`
      );
      setResolutionModeState(mode);
      clearAllData();
    },
    [resolutionMode, clearAllData]
  );

  const loadBulkNodeDetails = useCallback(
    async (nodesToFetch: NodeIdentifier[]) => {
      const caller =
        new Error().stack?.split("\n")[2]?.match(/at (\w+)/)?.[1] || "unknown";
      console.log(`🔍 loadBulkNodeDetails called by: ${caller}`);
      console.log(
        `🔍 Requested: ${nodesToFetch.length} nodes, First 3 IDs:`,
        nodesToFetch.slice(0, 3).map((n) => n.id)
      );

      if (nodesToFetch.length === 0) {
        console.log("🚫 No nodes to fetch, early return");
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
            `🚫 Skipping node ${node.id}: state=${currentState}, pending=${isPending}`
          );
        }
        return !shouldSkip;
      });

      if (trulyNeedsFetching.length === 0) {
        console.log("🚫 All nodes already loaded/loading, early return");
        return;
      }

      console.log(`📦 Actually fetching: ${trulyNeedsFetching.length} nodes`);

      const nodeIdsToLoad = trulyNeedsFetching.map((n) => n.id);
      setPendingNodeFetches((prev) => {
        const newSet = new Set(prev);
        nodeIdsToLoad.forEach((id) => newSet.add(id));
        console.log(`📝 Added to pending:`, nodeIdsToLoad);
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
          console.log(`🧹 Removed from pending:`, nodeIdsToLoad);
          return newSet;
        });
      };

      try {
        const NODE_FETCH_SIZE = 200;
        for (let i = 0; i < trulyNeedsFetching.length; i += NODE_FETCH_SIZE) {
          const chunk = trulyNeedsFetching.slice(i, i + NODE_FETCH_SIZE);

          try {
            const response = await getBulkNodeDetails({ items: chunk });

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
            console.error(`❌ Error fetching node chunk:`, error);

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
    [nodeDetails, pendingNodeFetches]
  );

  // FIXED: loadBulkConnections with Immer spread pattern
  const loadBulkConnections = useCallback(
    async (items: BulkConnectionRequestItem[]) => {
      if (items.length === 0) return;

      const uniqueItems = uniqueBy(
        items,
        (item) => `${item.edgeId}-${item.itemType}`
      );
      const trulyNeedsFetching = uniqueItems.filter((item) => {
        const existing = connectionData[item.edgeId];
        return !existing || !existing.data || existing.error;
      });

      if (trulyNeedsFetching.length === 0) return;

      // FIXED: Use spread operator instead of direct property assignment
      setConnectionData(
        produce((draft) => {
          trulyNeedsFetching.forEach((item) => {
            if (!draft[item.edgeId] || draft[item.edgeId]?.error) {
              draft[item.edgeId] = {
                data: null,
                loading: true,
                error: null,
                lastUpdated: null,
              };
            } else if (draft[item.edgeId] && !draft[item.edgeId]?.loading) {
              // ✅ FIXED: Replace entire object instead of mutating properties
              draft[item.edgeId] = {
                ...draft[item.edgeId]!,
                loading: true,
                error: null,
              };
            }
          });
        })
      );

      const chunks: BulkConnectionRequestItem[][] = [];
      for (let i = 0; i < trulyNeedsFetching.length; i += MAX_BULK_FETCH_SIZE) {
        chunks.push(trulyNeedsFetching.slice(i, i + MAX_BULK_FETCH_SIZE));
      }

      if (chunks.length === 0) return;

      const firstChunk = chunks.shift();
      if (firstChunk) {
        try {
          const response = await getBulkConnections({ items: firstChunk });
          setConnectionData(
            produce((draft) => {
              response.forEach((connData) => {
                const edgeId = connData.edge.id;
                draft[edgeId] = {
                  data: connData,
                  loading: false,
                  error: null,
                  lastUpdated: Date.now(),
                };
              });
              firstChunk.forEach((reqItem) => {
                if (!response.some((r) => r.edge.id === reqItem.edgeId)) {
                  draft[reqItem.edgeId] = {
                    data: null,
                    loading: false,
                    error: "Not found in response.",
                    lastUpdated: null,
                  };
                }
              });
            })
          );
        } catch (error) {
          setConnectionData(
            produce((draft) => {
              firstChunk.forEach((item) => {
                draft[item.edgeId] = {
                  data: null,
                  loading: false,
                  error: (error as Error).message,
                  lastUpdated: null,
                };
              });
            })
          );
        }
      }

      // Background chunks
      if (chunks.length > 0) {
        chunks.forEach((chunk) => {
          getBulkConnections({ items: chunk })
            .then((response) => {
              setConnectionData(
                produce((draft) => {
                  response.forEach((connData) => {
                    const edgeId = connData.edge.id;
                    draft[edgeId] = {
                      data: connData,
                      loading: false,
                      error: null,
                      lastUpdated: Date.now(),
                    };
                  });
                  chunk.forEach((reqItem) => {
                    if (!response.some((r) => r.edge.id === reqItem.edgeId)) {
                      if (draft[reqItem.edgeId]?.loading) {
                        draft[reqItem.edgeId] = {
                          data: null,
                          loading: false,
                          error: "Not found in response.",
                          lastUpdated: null,
                        };
                      }
                    }
                  });
                })
              );
            })
            .catch((error) => {
              setConnectionData(
                produce((draft) => {
                  chunk.forEach((item) => {
                    if (draft[item.edgeId]?.loading) {
                      // ✅ FIXED: Replace entire object
                      draft[item.edgeId] = {
                        ...draft[item.edgeId]!,
                        loading: false,
                        error: (error as Error).message,
                      };
                    }
                  });
                })
              );
            });
        });
      }
    },
    [connectionData]
  );

  // FIXED: loadVisualizationDataForClusters with Immer spread pattern
  const loadVisualizationDataForClusters = useCallback(
    async (items: BulkVisualizationRequestItem[]) => {
      if (items.length === 0) return;

      const uniqueItems = uniqueBy(
        items,
        (item) => `${item.clusterId}-${item.itemType}`
      );

      // FIXED: Use spread operator pattern
      setVisualizationData((prev) => {
        const newState = { ...prev };
        uniqueItems.forEach((item) => {
          if (!newState[item.clusterId] || newState[item.clusterId]?.error) {
            newState[item.clusterId] = {
              data: null,
              loading: true,
              error: null,
              lastUpdated: null,
            };
          } else if (
            newState[item.clusterId] &&
            !newState[item.clusterId]?.loading
          ) {
            // ✅ FIXED: Replace entire object instead of mutating properties
            newState[item.clusterId] = {
              ...newState[item.clusterId]!,
              loading: true,
              error: null,
            };
          }
        });
        return newState;
      });

      const allFetchedVisualizations: EntityVisualizationDataResponse[] = [];
      const VIZ_FETCH_SIZE = 200;

      for (let i = 0; i < uniqueItems.length; i += VIZ_FETCH_SIZE) {
        const chunk = uniqueItems.slice(i, i + VIZ_FETCH_SIZE);
        try {
          const response = await getBulkVisualizations({ items: chunk });
          allFetchedVisualizations.push(...response);

          setVisualizationData((prev) => {
            const newState = { ...prev };
            response.forEach((vizData) => {
              newState[vizData.clusterId] = {
                data: vizData,
                loading: false,
                error: null,
                lastUpdated: Date.now(),
              };
            });
            chunk.forEach((requestedItem) => {
              if (
                !response.find((r) => r.clusterId === requestedItem.clusterId)
              ) {
                newState[requestedItem.clusterId] = {
                  data: null,
                  loading: false,
                  error: "Not found in bulk response chunk",
                  lastUpdated: null,
                };
              }
            });
            return newState;
          });
        } catch (error) {
          setVisualizationData((prev) => {
            const newState = { ...prev };
            chunk.forEach((item) => {
              newState[item.clusterId] = {
                data: null,
                loading: false,
                error: (error as Error).message,
                lastUpdated: null,
              };
            });
            return newState;
          });
        }
      }

      // Handle final state and cleanup loading states
      setVisualizationData((prev) => {
        const newState = { ...prev };
        uniqueItems.forEach((requestedItem) => {
          if (
            !allFetchedVisualizations.find(
              (r) => r.clusterId === requestedItem.clusterId
            ) &&
            newState[requestedItem.clusterId]?.loading
          ) {
            newState[requestedItem.clusterId] = {
              data: null,
              loading: false,
              error: "Not found in any bulk response chunk",
              lastUpdated: null,
            };
          } else if (newState[requestedItem.clusterId]?.loading) {
            newState[requestedItem.clusterId] = {
              ...newState[requestedItem.clusterId]!,
              loading: false,
            };
          }
        });
        return newState;
      });

      // Load related data
      const allNodeIdentifiers: NodeIdentifier[] = [];
      allFetchedVisualizations.forEach((vizData) => {
        vizData.nodes.forEach((node) =>
          allNodeIdentifiers.push({ id: node.id, nodeType: resolutionMode })
        );
      });

      if (allNodeIdentifiers.length > 0) {
        await loadBulkNodeDetails(allNodeIdentifiers);
      }

      // Load connections with priority
      const priorityLinks: BulkConnectionRequestItem[] = [];
      const backgroundLinks: BulkConnectionRequestItem[] = [];

      let priorityClusterId: string | null = null;
      for (const vizData of allFetchedVisualizations) {
        const clusterDetail = queries.getClusterById(vizData.clusterId);
        if (clusterDetail && !clusterDetail.wasReviewed) {
          priorityClusterId = vizData.clusterId;
          break;
        }
      }

      if (!priorityClusterId && allFetchedVisualizations.length > 0) {
        priorityClusterId = allFetchedVisualizations[0].clusterId;
      }

      allFetchedVisualizations.forEach((vizData) => {
        const clusterDetail = queries.getClusterById(vizData.clusterId);
        const connectionCount = clusterDetail?.groupCount;

        if (
          connectionCount !== undefined &&
          connectionCount !== null &&
          connectionCount <= LARGE_CLUSTER_THRESHOLD
        ) {
          const items = vizData.links.map((link) => ({
            edgeId: link.id,
            itemType: resolutionMode,
          }));

          if (vizData.clusterId === priorityClusterId) {
            priorityLinks.push(...items);
          } else {
            backgroundLinks.push(...items);
          }
        }
      });

      const allLinksToLoad = [...priorityLinks, ...backgroundLinks];
      if (allLinksToLoad.length > 0) {
        await loadBulkConnections(allLinksToLoad);
      }
    },
    [resolutionMode, loadBulkNodeDetails, queries, loadBulkConnections]
  );

  const loadVisualizationDataForClustersRef = useRef(
    loadVisualizationDataForClusters
  );
  useEffect(() => {
    loadVisualizationDataForClustersRef.current =
      loadVisualizationDataForClusters;
  }, [loadVisualizationDataForClusters]);

  // ==========================================
  // 2. REMOVE CLEANUP FROM loadClusters
  // ==========================================

  // REPLACE the loadClusters function - REMOVE the setTimeout cleanup at the end:
  const loadClusters = useCallback(
    async (page: number, limit: number = 10) => {
      setClusters((prev) => ({
        ...prev,
        loading: true,
        error: null,
        page,
        limit,
      }));

      const fetcher =
        resolutionMode === "entity"
          ? getOrganizationClusters
          : getServiceClusters;

      try {
        const response = await fetcher(page, limit, clusterFilterStatus);

        if (response.clusters.length === 0 && response.page > 1) {
          console.warn(
            `No clusters on page ${response.page}. Attempting to load page 1.`
          );
          setClusters((prev) => ({ ...prev, loading: false, error: null }));
          loadClusters(1, limit);
          return;
        }

        setClusters({
          data: response.clusters,
          total: response.total,
          page: response.page,
          limit: response.limit,
          loading: false,
          error: null,
        });

        if (response.clusters.length === 0) {
          setSelectedClusterIdState(null);
        }

        const vizRequestItemsNormal: BulkVisualizationRequestItem[] = [];
        response.clusters.forEach((c) => {
          const connectionCount = c.groupCount;
          if (
            connectionCount !== undefined &&
            connectionCount !== null &&
            connectionCount <= LARGE_CLUSTER_THRESHOLD
          ) {
            vizRequestItemsNormal.push({
              clusterId: c.id,
              itemType: resolutionMode,
            });
          }
        });

        if (vizRequestItemsNormal.length > 0) {
          await loadVisualizationDataForClustersRef.current(
            vizRequestItemsNormal
          );
        }

        // ❌ REMOVED: The setTimeout cleanup that was causing infinite loop
      } catch (error) {
        setClusters((prev) => ({
          ...prev,
          loading: false,
          error: (error as Error).message,
          data: [],
          total: 0,
        }));
        setSelectedClusterIdState(null);
      }
    },
    [
      resolutionMode,
      clusterFilterStatus,
      // ❌ REMOVED: selectedClusterId and performThreeClusterCleanup from dependencies
    ]
  );

  // ==========================================
  // 3. SIMPLIFY handleSetSelectedClusterId
  // ==========================================

  // REPLACE handleSetSelectedClusterId - remove redundant cleanup calls:
  const handleSetSelectedClusterId = useCallback(
    async (id: string | null) => {
      if (id === selectedClusterId) return;

      setSelectedClusterIdState(id);
      setSelectedEdgeIdState(null);
      setLastReviewedEdgeId(null);

      if (id !== activelyPagingClusterId) {
        setActivelyPagingClusterId(null);
        setLargeClusterConnectionsPage(0);
      }

      if (id) {
        const clusterDetail = queries.getClusterById(id);
        const connectionCount = clusterDetail
          ? clusterDetail.groupCount
          : undefined;
        const isLarge =
          connectionCount !== undefined &&
          connectionCount !== null &&
          connectionCount > LARGE_CLUSTER_THRESHOLD;

        if (
          clusterDetail?.wasReviewed ||
          (!isLarge && queries.getClusterProgress(id).isComplete)
        ) {
          setIsAutoAdvanceEnabledState(false);
        } else {
          if (!(isLarge && largeClusterConnectionsPage === 0)) {
            setIsAutoAdvanceEnabledState(true);
          }
        }

        if (!isLarge) {
          const vizState = visualizationData[id];
          if ((!vizState?.data || vizState?.error) && !vizState?.loading) {
            await loadVisualizationDataForClustersRef.current([
              { clusterId: id, itemType: resolutionMode },
            ]);
          } else if (vizState?.data?.links) {
            const unloadedConnectionItems = vizState.data.links
              .map((link) => ({ edgeId: link.id, itemType: resolutionMode }))
              .filter((item) => {
                const connState = connectionData[item.edgeId];
                return !connState || (!connState.data && !connState.loading);
              });
            if (unloadedConnectionItems.length > 0) {
              loadBulkConnections(unloadedConnectionItems);
            }
          }
        } else {
          const vizState = visualizationData[id];
          if (vizState?.data?.nodes) {
            const nodeIdsFromViz: NodeIdentifier[] = vizState.data.nodes.map(
              (node) => ({ id: node.id, nodeType: resolutionMode })
            );
            await loadBulkNodeDetails(nodeIdsFromViz);
          }
        }
      }

      // ❌ REMOVED: All cleanup calls from this function
      // The cleanup will be handled by the debounced useEffect instead
    },
    [
      selectedClusterId,
      activelyPagingClusterId,
      largeClusterConnectionsPage,
      queries,
      visualizationData,
      resolutionMode,
      loadBulkNodeDetails,
      connectionData,
      loadBulkConnections,
      // ❌ REMOVED: performThreeClusterCleanup from dependencies
    ]
  );

  // ==========================================
  // ENHANCED EDGE ID SETTER
  // ==========================================
  const setSelectedEdgeIdAction = useCallback(
    (id: string | null) => {
      if (id === selectedEdgeId) return;

      console.log(`Setting selected edge ID to: ${id}`);
      if (id && queries.isEdgeReviewed(id)) {
        console.log(`Edge ${id} is already reviewed. Pausing auto-advance.`);
        setIsAutoAdvanceEnabledState(false);
      } else if (id) {
        const currentClusterId = selectedClusterId;
        if (currentClusterId) {
          const clusterDetail = queries.getClusterById(currentClusterId);
          if (!clusterDetail?.wasReviewed) {
            setIsAutoAdvanceEnabledState(true);
          }
        }
      }
      setSelectedEdgeIdState(id);
    },
    [queries, selectedClusterId, selectedEdgeId]
  );

  // Other action implementations (simplified for brevity)
  const loadSingleConnectionData = useCallback(
    async (edgeId: string): Promise<EntityConnectionDataResponse | null> => {
      const cached = connectionData[edgeId];
      if (
        cached?.data &&
        !cached.loading &&
        !cached.error &&
        cached.lastUpdated &&
        Date.now() - cached.lastUpdated < 300000
      ) {
        console.log(`Using cached connection data for edge ${edgeId}`);

        const nodesToLoad: NodeIdentifier[] = [];
        const connItem = cached.data;
        if (connItem.entity1)
          nodesToLoad.push({
            id: connItem.entity1.id,
            nodeType: resolutionMode,
          });
        if (connItem.entity2)
          nodesToLoad.push({
            id: connItem.entity2.id,
            nodeType: resolutionMode,
          });

        if (nodesToLoad.length > 0) {
          await loadBulkNodeDetails(nodesToLoad);
        }
        return cached.data;
      }

      console.log(`Fetching single connection data for edge ${edgeId}.`);
      setConnectionData((prev) => ({
        ...prev,
        [edgeId]: {
          data: prev[edgeId]?.data || null,
          loading: true,
          error: null,
          lastUpdated: prev[edgeId]?.lastUpdated || null,
        },
      }));

      try {
        const fetcher =
          resolutionMode === "entity"
            ? getOrganizationConnectionData
            : getServiceConnectionData;
        const response = await fetcher(edgeId);

        setConnectionData((prev) => ({
          ...prev,
          [edgeId]: {
            data: response,
            loading: false,
            error: null,
            lastUpdated: Date.now(),
          },
        }));

        if (response) {
          const nodesToLoad: NodeIdentifier[] = [];
          if (response.entity1)
            nodesToLoad.push({
              id: response.entity1.id,
              nodeType: resolutionMode,
            });
          if (response.entity2)
            nodesToLoad.push({
              id: response.entity2.id,
              nodeType: resolutionMode,
            });

          if (nodesToLoad.length > 0) {
            await loadBulkNodeDetails(nodesToLoad);
          }
        }
        return response;
      } catch (error) {
        console.error(
          `Error in loadSingleConnectionData for edge ${edgeId}:`,
          error
        );
        setConnectionData((prev) => ({
          ...prev,
          [edgeId]: {
            data: null,
            loading: false,
            error: (error as Error).message,
            lastUpdated: null,
          },
        }));
        return null;
      }
    },
    [connectionData, resolutionMode, loadBulkNodeDetails]
  );

  // Simple implementations for other functions
  const triggerRefresh = useCallback(
    (
      target:
        | "all"
        | "clusters"
        | "current_visualization"
        | "current_connection" = "all"
    ) => {
      console.log(`Triggering refresh for target: ${target}`);
      if (target === "all" || target === "clusters") {
        setActivelyPagingClusterId(null);
        setLargeClusterConnectionsPage(0);
        loadClusters(clusters.page, clusters.limit);
      }
      setRefreshTrigger((prev) => prev + 1);
    },
    [clusters.page, clusters.limit, loadClusters]
  );

  const submitEdgeReview = useCallback(
    async (edgeId: string, decision: GroupReviewDecision, notes?: string) => {
      if (!user?.id) {
        toast({
          title: "Auth Error",
          description: "Login required.",
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

      const originalVizState = _.cloneDeep(
        visualizationData[selectedClusterId]
      );

      setEdgeSubmissionStatus((prev) => ({
        ...prev,
        [edgeId]: { isSubmitting: true, error: null },
      }));

      setVisualizationData(
        produce((draft) => {
          const viz = draft[selectedClusterId!]?.data;
          if (viz) {
            const link = viz.links.find((l) => l.id === edgeId);
            if (link) {
              link.wasReviewed = true;
              link.status =
                decision === "ACCEPTED"
                  ? "CONFIRMED_MATCH"
                  : "CONFIRMED_NON_MATCH";
            }
          }
        })
      );

      try {
        const payload: EdgeReviewApiPayload = {
          decision: decision as "ACCEPTED" | "REJECTED",
          reviewerId: user.id,
          notes,
          type: resolutionMode,
        };
        const response = await postEdgeReview(edgeId, payload);

        setEdgeSubmissionStatus((prev) => ({
          ...prev,
          [edgeId]: { isSubmitting: false, error: null },
        }));

        setLastReviewedEdgeId(edgeId);

        if (response.clusterFinalized) {
          setClusters(
            produce((draft) => {
              const cluster = draft.data.find(
                (c) => c.id === response.clusterId
              );
              if (cluster) {
                cluster.wasReviewed = true;
              }
            })
          );
        }
      } catch (error) {
        const errorMessage =
          (error as Error).message || "An unknown error occurred.";
        console.error(`Failed to submit review for edge ${edgeId}:`, error);

        setEdgeSubmissionStatus((prev) => ({
          ...prev,
          [edgeId]: { isSubmitting: false, error: errorMessage },
        }));

        toast({
          title: "Submission Failed",
          description: errorMessage,
          variant: "destructive",
        });

        if (originalVizState) {
          setVisualizationData((prev) => ({
            ...prev,
            [selectedClusterId!]: originalVizState,
          }));
        }
      }
    },
    [user, selectedClusterId, visualizationData, resolutionMode, toast]
  );

  // Computed values
  const currentVisualizationDataForSelection =
    useMemo((): EntityVisualizationDataResponse | null => {
      if (!selectedClusterId) return null;

      const vizState = visualizationData[selectedClusterId];
      if (!vizState?.data) return null;

      if (
        selectedClusterId === activelyPagingClusterId &&
        largeClusterConnectionsPage > 0
      ) {
        const allLinks = vizState.data.links;
        const startIndex =
          (largeClusterConnectionsPage - 1) * CONNECTION_PAGE_SIZE;
        const endIndex = startIndex + CONNECTION_PAGE_SIZE;
        const pagedLinks = allLinks.slice(startIndex, endIndex);
        return { ...vizState.data, links: pagedLinks };
      }
      return vizState.data;
    }, [
      selectedClusterId,
      visualizationData,
      activelyPagingClusterId,
      largeClusterConnectionsPage,
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
    return clusters.data.find((c) => c.id === selectedClusterId) || null;
  }, [selectedClusterId, clusters.data]);

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

    const currentVizForView = currentVisualizationDataForSelection;
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
    currentVisualizationDataForSelection,
    selectedEdgeId,
    selectedClusterId,
    visualizationData,
  ]);

  const clusterProgress = useMemo(() => {
    const reconstructed: Record<string, ClusterReviewProgress> = {};

    clusters.data.forEach((cluster) => {
      reconstructed[cluster.id] = queries.getClusterProgress(cluster.id);
    });

    if (selectedClusterId && !reconstructed[selectedClusterId]) {
      const hasVisualizationData =
        visualizationData[selectedClusterId]?.data?.links;
      const hasClusterDetails = queries.getClusterById(selectedClusterId);

      if (hasVisualizationData || hasClusterDetails) {
        reconstructed[selectedClusterId] =
          queries.getClusterProgress(selectedClusterId);
      }
    }

    return reconstructed;
  }, [
    clusters.data,
    selectedClusterId,
    selectedClusterId
      ? visualizationData[selectedClusterId]?.data?.links
      : undefined,
    queries,
  ]);

  const isLoadingConnectionPageData = useMemo(() => {
    if (!isLoadingPage) return false;
    if (selectedEdgeId) return !queries.isConnectionDataLoaded(selectedEdgeId);
    return true;
  }, [isLoadingPage, selectedEdgeId, queries]);

  const advanceToNextCluster = useCallback(async () => {
    if (!selectedClusterId) {
      console.warn("No cluster selected, cannot advance.");
      return;
    }

    console.log(`🚀 Advancing to next cluster from: ${selectedClusterId}`);

    const currentIndex = clusters.data.findIndex(
      (c) => c.id === selectedClusterId
    );
    if (currentIndex === -1) {
      console.warn(
        "Current cluster not found in cluster list. Reloading clusters."
      );
      loadClusters(clusters.page, clusters.limit);
      return;
    }

    // Clear paging state when advancing clusters
    setActivelyPagingClusterId(null);
    setLargeClusterConnectionsPage(0);

    let nextClusterSelected = false;
    let nextClusterId: string | null = null;

    // Look for next unreviewed cluster on current page
    for (let i = currentIndex + 1; i < clusters.data.length; i++) {
      const nextClusterOnPage = clusters.data[i];
      if (!nextClusterOnPage.wasReviewed) {
        console.log(
          `Selecting next unreviewed cluster in current page: ${nextClusterOnPage.id}`
        );
        nextClusterId = nextClusterOnPage.id;

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

        await loadClusters(nextPageToLoad, clusters.limit);

        // After loading new page, the useEffect for auto-selecting will handle
        // selecting the first unreviewed cluster on the new page
      } else {
        // No more pages - all clusters reviewed
        toast({
          title: "Workflow Complete",
          description: "All clusters have been reviewed.",
        });
        console.log("All clusters reviewed.");
        setIsAutoAdvanceEnabledState(false);

        // Set selected cluster to null if all are reviewed and no more pages
        setSelectedClusterIdState(null);
      }
    }
  }, [
    selectedClusterId,
    clusters,
    loadClusters,
    toast,
    handleSetSelectedClusterId,
    setActivelyPagingClusterId,
    setLargeClusterConnectionsPage,
    setIsAutoAdvanceEnabledState,
    setSelectedClusterIdState,
  ]);

  const checkAndAdvanceIfComplete = useCallback(
    async (clusterIdToCheck?: string) => {
      const targetClusterId = clusterIdToCheck || selectedClusterId;
      if (!targetClusterId) {
        return;
      }

      const progress = queries.getClusterProgress(targetClusterId);

      if (progress.isComplete && progress.totalEdges !== -1) {
        setClusters(
          produce((draft) => {
            const cluster = draft.data.find((c) => c.id === targetClusterId);
            if (cluster) {
              cluster.wasReviewed = true;
            }
          })
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
    [selectedClusterId, queries, advanceToNextCluster, isAutoAdvanceEnabled]
  );

  const selectNextUnreviewedEdge = useCallback(
    (afterEdgeId?: string | null) => {
      const currentClusterId = selectedClusterId;
      if (!currentClusterId) {
        console.warn("No cluster selected for selecting next edge.");
        return;
      }

      const clusterDetail = queries.getClusterById(currentClusterId);
      if (clusterDetail?.wasReviewed) {
        console.log(
          `Cluster ${currentClusterId} was reviewed. No edges to select. Checking advance.`
        );
        checkAndAdvanceIfComplete(currentClusterId);
        return;
      }

      console.log(
        `Attempting to select next unreviewed edge in cluster ${currentClusterId}, after: ${
          afterEdgeId || lastReviewedEdgeId || selectedEdgeId || "start"
        }`
      );

      const currentVizForSelection = currentVisualizationDataForSelection;

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
        console.log(
          `No links in current view for cluster ${currentClusterId}. Checking advance.`
        );
        checkAndAdvanceIfComplete(currentClusterId);
        return;
      }

      const { links } = currentVizForSelection;
      let startIdx = 0;
      const referenceEdgeId =
        afterEdgeId || lastReviewedEdgeId || selectedEdgeId;

      if (referenceEdgeId) {
        const idx = links.findIndex((l) => l.id === referenceEdgeId);
        if (idx !== -1) startIdx = idx + 1;
        else
          console.warn(
            `Ref edge ${referenceEdgeId} not found in current view. Starting from beginning of current view.`
          );
      }

      for (let i = 0; i < links.length; i++) {
        const link = links[(startIdx + i) % links.length];
        if (!queries.isEdgeReviewed(link.id)) {
          console.log(
            `Next unreviewed edge found in current view: ${link.id}. Selecting.`
          );
          setSelectedEdgeIdAction(link.id);
          return;
        }
      }

      console.log(
        `No more unreviewed edges in current view of cluster ${currentClusterId}. Checking advance.`
      );
      setSelectedEdgeIdAction(null);
      checkAndAdvanceIfComplete(currentClusterId);
    },
    [
      selectedClusterId,
      selectedEdgeId,
      lastReviewedEdgeId,
      checkAndAdvanceIfComplete,
      queries,
      currentVisualizationDataForSelection,
      activelyPagingClusterId,
      largeClusterConnectionsPage,
      visualizationData,
      setSelectedEdgeIdAction,
    ]
  );

  const selectNextUnreviewedInCluster = useCallback(() => {
    const currentClusterId = selectedClusterId;
    if (!currentClusterId) return;

    const clusterDetail = queries.getClusterById(currentClusterId);
    if (clusterDetail?.wasReviewed) {
      checkAndAdvanceIfComplete(currentClusterId);
      return;
    }

    const currentLinksForNav =
      currentVisualizationDataForSelection?.links || [];
    if (currentLinksForNav.length === 0) {
      checkAndAdvanceIfComplete(currentClusterId);
      return;
    }

    const unreviewedLinksInView = currentLinksForNav.filter(
      (l) => !queries.isEdgeReviewed(l.id)
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
    queries,
    checkAndAdvanceIfComplete,
    toast,
    currentVisualizationDataForSelection,
    setSelectedEdgeIdAction,
  ]);

  const selectPreviousUnreviewedInCluster = useCallback(() => {
    const currentClusterId = selectedClusterId;
    if (!currentClusterId) return;

    const clusterDetail = queries.getClusterById(currentClusterId);
    if (clusterDetail?.wasReviewed) {
      checkAndAdvanceIfComplete(currentClusterId);
      return;
    }

    const currentLinksForNav =
      currentVisualizationDataForSelection?.links || [];
    if (currentLinksForNav.length === 0) {
      checkAndAdvanceIfComplete(currentClusterId);
      return;
    }

    const unreviewedLinksInView = currentLinksForNav.filter(
      (l) => !queries.isEdgeReviewed(l.id)
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
    queries,
    checkAndAdvanceIfComplete,
    toast,
    currentVisualizationDataForSelection,
    setSelectedEdgeIdAction,
  ]);

  const loadConnectionDataForLinkPage = useCallback(
    async (
      clusterId: string,
      pageToLoad: number,
      isPrefetch: boolean = false
    ) => {
      const viz = visualizationData[clusterId]?.data;
      if (!viz || !viz.links) {
        console.warn(
          `loadConnectionDataForLinkPage: No visualization data or links for cluster ${clusterId}.`
        );
        if (!isPrefetch) setIsLoadingPage(false);
        return;
      }

      if (!isPrefetch) {
        setIsLoadingPage(true);
      }

      const startIndex = (pageToLoad - 1) * CONNECTION_PAGE_SIZE;
      const endIndex = startIndex + CONNECTION_PAGE_SIZE;
      const linksForPage = viz.links.slice(startIndex, endIndex);

      if (linksForPage.length === 0) {
        console.log(
          `loadConnectionDataForLinkPage: No links for page ${pageToLoad} in cluster ${clusterId}.`
        );
        if (!isPrefetch) setIsLoadingPage(false);
        return;
      }

      const connectionItemsToFetch: BulkConnectionRequestItem[] = linksForPage
        .map((link) => ({ edgeId: link.id, itemType: resolutionMode }))
        .filter((item: BulkConnectionRequestItem) => {
          const connState = connectionData[item.edgeId];
          return !connState || !connState.data || connState.error;
        });

      console.log(
        `loadConnectionDataForLinkPage: Fetching connection data for ${connectionItemsToFetch.length} links on page ${pageToLoad} of cluster ${clusterId}. Prefetch: ${isPrefetch}`
      );

      if (connectionItemsToFetch.length > 0) {
        try {
          await loadBulkConnections(connectionItemsToFetch);
        } catch (error) {
          console.error(
            `Error loading connection data for page ${pageToLoad}, cluster ${clusterId}:`,
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
        setIsLoadingPage(false);
        const totalLinks = viz.links.length;
        if (endIndex < totalLinks) {
          console.log(
            `Prefetching connection data for page ${
              pageToLoad + 1
            } of cluster ${clusterId}`
          );
          loadConnectionDataForLinkPage(clusterId, pageToLoad + 1, true).catch(
            (err) => {
              console.warn(
                `Error prefetching connection data for page ${pageToLoad + 1}:`,
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
    ]
  );

  const initializeLargeClusterConnectionPaging = useCallback(
    async (clusterId: string) => {
      console.log(
        `Initializing connection paging for large cluster: ${clusterId}`
      );
      setActivelyPagingClusterId(clusterId);
      setLargeClusterConnectionsPage(1);
      setIsLoadingPage(true);

      let viz = visualizationData[clusterId]?.data;
      if (
        !viz ||
        visualizationData[clusterId]?.error ||
        !visualizationData[clusterId]?.data?.links
      ) {
        console.log(
          `Fetching/Re-fetching visualization data for large cluster ${clusterId} before paging connections.`
        );
        try {
          await loadVisualizationDataForClustersRef.current([
            { clusterId, itemType: resolutionMode },
          ]);
          viz = visualizationData[clusterId]?.data;
          if (!viz || !viz.links)
            throw new Error(
              "Visualization data (with links) still not available after fetch."
            );
          const nodeIdsFromViz: NodeIdentifier[] = viz.nodes.map((node) => ({
            id: node.id,
            nodeType: resolutionMode,
          }));
          if (nodeIdsFromViz.length > 0) {
            const nodesTrulyNeedingFetch = nodeIdsFromViz.filter(
              (n) => !nodeDetails[n.id] || nodeDetails[n.id] === "error"
            );
            if (nodesTrulyNeedingFetch.length > 0) {
              await loadBulkNodeDetails(nodesTrulyNeedingFetch);
            }
          }
        } catch (error) {
          toast({
            title: "Error Initializing Cluster",
            description: `Failed to load visualization for ${clusterId}: ${
              (error as Error).message
            }`,
            variant: "destructive",
          });
          setIsLoadingPage(false);
          setActivelyPagingClusterId(null);
          setLargeClusterConnectionsPage(0);
          return;
        }
      } else {
        const nodeIdsFromViz: NodeIdentifier[] = viz.nodes.map((node) => ({
          id: node.id,
          nodeType: resolutionMode,
        }));
        if (nodeIdsFromViz.length > 0) {
          const nodesTrulyNeedingFetch = nodeIdsFromViz.filter(
            (n) => !nodeDetails[n.id] || nodeDetails[n.id] === "error"
          );
          if (nodesTrulyNeedingFetch.length > 0) {
            await loadBulkNodeDetails(nodesTrulyNeedingFetch);
          }
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
      nodeDetails,
      loadBulkNodeDetails,
      loadVisualizationDataForClustersRef,
      setSelectedEdgeIdAction,
    ]
  );

  const viewNextConnectionPage = useCallback(
    async (clusterId: string) => {
      if (clusterId !== activelyPagingClusterId) {
        console.warn(
          "viewNextConnectionPage called for a cluster that is not actively paging."
        );
        return;
      }
      const viz = visualizationData[clusterId]?.data;
      if (!viz || !viz.links) {
        console.warn("No visualization data to page for next connections.");
        return;
      }
      const totalLinks = viz.links.length;
      const nextPage = largeClusterConnectionsPage + 1;
      const startIndexForNextPage = (nextPage - 1) * CONNECTION_PAGE_SIZE;

      if (startIndexForNextPage < totalLinks) {
        console.log(
          `Viewing next connection page ${nextPage} for cluster ${clusterId}`
        );
        setLargeClusterConnectionsPage(nextPage);
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
    ]
  );
  const invalidateVisualizationData = useCallback(
    async (clusterId: string) => {
      console.log(
        `Invalidating and reloading visualization data for cluster: ${clusterId}`
      );

      // Clear the visualization data and set loading state
      setVisualizationData((prev) => ({
        ...prev,
        [clusterId]: {
          data: null,
          loading: true,
          error: null,
          lastUpdated: null,
        },
      }));

      // Handle large cluster paging case
      if (clusterId === activelyPagingClusterId) {
        await initializeLargeClusterConnectionPaging(clusterId);
      } else {
        // Regular cluster - reload visualization data
        await loadVisualizationDataForClustersRef.current([
          { clusterId, itemType: resolutionMode },
        ]);
      }
    },
    [
      resolutionMode,
      activelyPagingClusterId,
      initializeLargeClusterConnectionPaging,
      loadVisualizationDataForClustersRef,
    ]
  );

  const invalidateConnectionData = useCallback(
    async (edgeId: string) => {
      console.log(
        `Invalidating and reloading connection data for edge: ${edgeId}`
      );

      // Clear the connection data and set loading state
      setConnectionData((prev) => ({
        ...prev,
        [edgeId]: {
          data: null,
          loading: true,
          error: null,
          lastUpdated: null,
        },
      }));

      // Reload the connection data
      await loadBulkConnections([{ edgeId, itemType: resolutionMode }]);
    },
    [resolutionMode, loadBulkConnections]
  );

  // ==========================================
  // 4. REPLACE THE PROBLEMATIC useEFFECTS
  // ==========================================

  // Effect to auto-select edge when visualization data loads
  useEffect(() => {
    if (
      selectedClusterId &&
      currentVisualizationDataForSelection?.links &&
      !selectedEdgeId
    ) {
      console.log(
        `Cluster ${selectedClusterId} selected, (paged) viz data loaded, no edge selected. Evaluating next action using selectNextUnreviewedEdge.`
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
    currentVisualizationDataForSelection,
    selectedEdgeId,
    selectNextUnreviewedEdge,
    isAutoAdvanceEnabled,
    lastReviewedEdgeId,
    activelyPagingClusterId,
    largeClusterConnectionsPage,
  ]);

  // Effect to auto-load connection data when edge is selected
  useEffect(() => {
    if (selectedEdgeId) {
      const currentEdgeState = connectionData[selectedEdgeId];
      if (
        (!currentEdgeState?.data ||
          currentEdgeState?.error ||
          (currentEdgeState?.lastUpdated &&
            Date.now() - currentEdgeState.lastUpdated > 300000)) &&
        !currentEdgeState?.loading
      ) {
        console.log(
          `Fetching/Re-fetching connection data for selected edge: ${selectedEdgeId}`
        );
        loadSingleConnectionData(selectedEdgeId);
      }
    }
  }, [selectedEdgeId, connectionData, loadSingleConnectionData]);

  // REPLACE memory monitoring useEffect - make it less aggressive:
  useEffect(() => {
    const monitorMemoryUsage = () => {
      const clusterCount = Object.keys(visualizationData).length;
      const connectionCount = Object.keys(connectionData).length;
      const nodeCount = Object.keys(nodeDetails).length;

      // More aggressive thresholds to reduce cleanup frequency
      if (clusterCount > 6) {
        // Increased from 4
        console.warn(
          `🚨 Too many clusters in memory (${clusterCount}), forcing three-cluster cleanup`
        );
        performThreeClusterCleanup(selectedClusterId, true);
      }

      const totalObjects = clusterCount + connectionCount + nodeCount;
      if (totalObjects > 800) {
        // Increased from 500
        console.warn(
          `🚨 High memory usage detected: ${totalObjects} total objects`
        );
        performThreeClusterCleanup(selectedClusterId, true);
      }
    };

    const interval = setInterval(monitorMemoryUsage, 30000); // Increased from 15000ms
    return () => clearInterval(interval);
  }, [selectedClusterId, performThreeClusterCleanup]); // Keep minimal dependencies

  // REPLACE the auto-cleanup useEffect with a debounced version:
  useEffect(() => {
    if (selectedClusterId && clusters.data.length > 0) {
      // Clear any existing timeout
      if (cleanupTimeoutRef.current) {
        clearTimeout(cleanupTimeoutRef.current);
      }

      // Debounce cleanup to prevent rapid-fire execution
      cleanupTimeoutRef.current = setTimeout(() => {
        console.log("🧹 Debounced cleanup triggered");
        performThreeClusterCleanup(selectedClusterId);
        cleanupTimeoutRef.current = null;
      }, 1000); // Increased from 300ms to 1000ms for better debouncing

      return () => {
        if (cleanupTimeoutRef.current) {
          clearTimeout(cleanupTimeoutRef.current);
          cleanupTimeoutRef.current = null;
        }
      };
    }
  }, [selectedClusterId, clusters.data.length]); // ❌ REMOVED performThreeClusterCleanup from deps

  // Initial cluster loading
  useEffect(() => {
    if (
      (clusters.data.length === 0 || clusters.error) &&
      !clusters.loading &&
      (clusters.total > 0 || clusterFilterStatus === "unreviewed")
    ) {
      console.log(
        `[useEffect] Initial cluster load/reload triggered for filter: ${clusterFilterStatus}`
      );
      loadClusters(1, clusters.limit);
    } else if (
      clusters.total === 0 &&
      !clusters.loading &&
      selectedClusterId !== null
    ) {
      console.log(
        `[useEffect] Clusters total is 0 for filter ${clusterFilterStatus}. Deselecting current cluster.`
      );
      setSelectedClusterIdState(null);
    }
  }, [
    resolutionMode,
    clusterFilterStatus,
    clusters.data.length,
    clusters.loading,
    loadClusters,
    clusters.limit,
    clusters.error,
    clusters.total,
    selectedClusterId,
  ]);

  // Auto-select cluster when clusters load
  useEffect(() => {
    if (!selectedClusterId && clusters.data.length > 0 && !clusters.loading) {
      const firstNonReviewedCluster = clusters.data.find((c) => !c.wasReviewed);
      if (firstNonReviewedCluster) {
        console.log(
          "Auto-selecting first unreviewed cluster:",
          firstNonReviewedCluster.id
        );
        handleSetSelectedClusterId(firstNonReviewedCluster.id);
      } else if (clusters.data.length > 0) {
        const firstCluster = clusters.data[0];
        console.log(
          "All clusters on current page are reviewed. Auto-selecting first for viewing:",
          firstCluster.id
        );
        handleSetSelectedClusterId(firstCluster.id);
      } else if (clusters.total === 0) {
        setSelectedClusterIdState(null);
      }
    } else if (
      selectedClusterId &&
      clusters.data.length === 0 &&
      !clusters.loading
    ) {
      console.log(
        "Current selected cluster no longer exists in an empty cluster list. Deselecting."
      );
      setSelectedClusterIdState(null);
    }
  }, [
    selectedClusterId,
    clusters.data,
    clusters.loading,
    clusters.total,
    handleSetSelectedClusterId,
  ]);

  // Set reviewer ID from user
  useEffect(() => {
    if (user?.id) setReviewerId(user.id);
  }, [user]);

  // Consolidated actions
  const actions = useMemo(
    () => ({
      setResolutionMode,
      setSelectedClusterId: handleSetSelectedClusterId,
      setSelectedEdgeId: setSelectedEdgeIdAction, // ENHANCED
      setReviewerId,
      setLastReviewedEdgeId,
      setClusterFilterStatus,
      setIsReviewToolsMaximized,
      setIsAutoAdvanceEnabled: setIsAutoAdvanceEnabledState,
      triggerRefresh,
      loadClusters,
      loadBulkNodeDetails,
      loadSingleConnectionData,
      invalidateVisualizationData,
      invalidateConnectionData,
      clearAllData,
      selectNextUnreviewedEdge,
      selectNextUnreviewedInCluster,
      selectPreviousUnreviewedInCluster,
      advanceToNextCluster,
      checkAndAdvanceIfComplete,
      submitEdgeReview,
      initializeLargeClusterConnectionPaging,
      viewNextConnectionPage,
      getActivelyPagingClusterId: () => activelyPagingClusterId,
      getLargeClusterConnectionsPage: () => largeClusterConnectionsPage,
      performThreeClusterCleanup: () =>
        performThreeClusterCleanup(selectedClusterId, true),
      getCacheStats: () => {
        const stats = {
          clusters: Object.keys(visualizationData).length,
          connections: Object.keys(connectionData).length,
          nodes: Object.keys(nodeDetails).length,
          pendingFetches: pendingNodeFetches.size,
          currentClusterSet: getThreeClustersToKeep(
            selectedClusterId,
            clusters.data
          ),
        };
        console.table(stats);
        return stats;
      },
    }),
    [
      setResolutionMode,
      handleSetSelectedClusterId,
      setSelectedEdgeIdAction, // ENHANCED
      triggerRefresh,
      loadClusters,
      loadBulkNodeDetails,
      loadSingleConnectionData,
      invalidateVisualizationData,
      invalidateConnectionData,
      clearAllData,
      selectNextUnreviewedEdge,
      selectNextUnreviewedInCluster,
      selectPreviousUnreviewedInCluster,
      advanceToNextCluster,
      checkAndAdvanceIfComplete,
      submitEdgeReview,
      initializeLargeClusterConnectionPaging,
      viewNextConnectionPage,
      activelyPagingClusterId,
      largeClusterConnectionsPage,
      performThreeClusterCleanup,
      selectedClusterId,
      visualizationData,
      connectionData,
      nodeDetails,
      pendingNodeFetches,
      clusters.data,
      getThreeClustersToKeep,
    ]
  );

  const contextValue: EntityResolutionContextType = {
    resolutionMode,
    selectedClusterId,
    selectedEdgeId,
    reviewerId,
    lastReviewedEdgeId,
    refreshTrigger,
    isAutoAdvanceEnabled,
    isReviewToolsMaximized,
    clusterFilterStatus,
    clusters,
    visualizationData,
    connectionData,
    nodeDetails,
    activelyPagingClusterId,
    largeClusterConnectionsPage,
    isLoadingConnectionPageData,
    clusterProgress,
    edgeSelectionInfo,
    currentVisualizationData: currentVisualizationDataForSelection,
    currentConnectionData,
    selectedClusterDetails,
    actions,
    queries,
  };

  return (
    <EntityResolutionContext.Provider value={contextValue}>
      {children}
    </EntityResolutionContext.Provider>
  );
}

export function useEntityResolution(): EntityResolutionContextType {
  const context = useContext(EntityResolutionContext);
  if (context === undefined) {
    throw new Error(
      "useEntityResolution must be used within an EntityResolutionProvider"
    );
  }
  return context;
}

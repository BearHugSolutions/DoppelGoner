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

// Constants for bulk fetching and large cluster handling
const MAX_BULK_FETCH_SIZE = 50; // MODIFIED: Reduced size for faster initial batches
const LARGE_CLUSTER_THRESHOLD = 200;
const CONNECTION_PAGE_SIZE = 200; // This is for manual paging on large clusters, can remain large

// Defines the shape of the cluster state.
interface ClustersState {
  data: EntityCluster[];
  total: number;
  page: number;
  limit: number;
  loading: boolean;
  error: string | null;
}

// Defines the shape of the visualization data state
interface VisualizationState {
  data: EntityVisualizationDataResponse | null;
  loading: boolean;
  error: string | null;
  lastUpdated: number | null;
}

// Defines the shape of the connection data state
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
  totalEdgesInView: number; // Renamed for clarity
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
  clusterFilterStatus: ClusterFilterStatus; // ADDED

  // State for clusters, visualization, and connection data
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
    setClusterFilterStatus: (status: ClusterFilterStatus) => void; // ADDED
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
  const { currentUser } = useAuth();
  const { toast } = useToast();

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
    useState<ClusterFilterStatus>("unreviewed"); // ADDED STATE

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

  const [activelyPagingClusterId, setActivelyPagingClusterId] = useState<
    string | null
  >(null);
  const [largeClusterConnectionsPage, setLargeClusterConnectionsPage] =
    useState<number>(0);
  const [isLoadingPage, setIsLoadingPage] = useState<boolean>(false);
  const [pendingNodeFetches, setPendingNodeFetches] = useState<Set<string>>(
    new Set()
  );

  const getClusterById = useCallback(
    (clusterId: string): EntityCluster | undefined => {
      return clusters.data.find((c) => c.id === clusterId);
    },
    [clusters.data]
  );

  const queries = useMemo(
    () => ({
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
      // REFACTORED: Simplified progress calculation as per Task 3.6.1
      getClusterProgress: (clusterIdToQuery: string): ClusterReviewProgress => {
        // console.log("getClusterProgress Called");
        const vizState = visualizationData[clusterIdToQuery];
        const clusterDetails = getClusterById(clusterIdToQuery);

        // Case 1: Full visualization data is loaded. This is the primary source of truth.
        if (vizState?.data?.links) {
          // console.log("Case 1: Full visualization data is loaded");
          const totalEdges = vizState.data.links.length;
          const reviewedEdges = vizState.data.links.filter(
            (l) => l.wasReviewed
          ).length;
          // console.log("totalEdges", totalEdges);
          // console.log("reviewedEdges", reviewedEdges);
          const progressPercentage =
            totalEdges > 0
              ? Math.round((reviewedEdges / totalEdges) * 100)
              : 100;
          // console.log("progressPercentage", progressPercentage);
          return {
            totalEdges,
            reviewedEdges,
            pendingEdges: totalEdges - reviewedEdges,
            // These are no longer primary metrics for progress, but can be useful for display
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

        // Case 2: No visualization data, but we have cluster details.
        if (clusterDetails) {
          // console.log(
          //   "Case 2: No visualization data, but we have cluster details"
          // );
          // If the cluster is marked as reviewed from the backend, it's complete.
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
          // If not reviewed, but we have a group count, we can show initial progress.
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

        // Case 3: No data available at all. Return indeterminate state.
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
      canAdvanceToNextCluster: () => {
        if (!selectedClusterId) return false;
        const progress = queries.getClusterProgress(selectedClusterId);
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
    }),
    [
      visualizationData,
      connectionData,
      selectedClusterId,
      edgeSubmissionStatus,
      getClusterById,
      nodeDetails,
      clusters.data, // Added clusters.data as a dependency for getClusterById
    ]
  );

  const optimizedQueries = useMemo(
    () => ({
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

      getClusterProgress: queries.getClusterProgress,

      canAdvanceToNextCluster: () => {
        if (!selectedClusterId) return false;
        const progress = queries.getClusterProgress(selectedClusterId);
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

      // NEW: Debug query for pending node fetches
      getPendingNodeFetches: () => Array.from(pendingNodeFetches),

      // NEW: Utility queries for better debugging
      getNodeLoadingState: (nodeId: string) => ({
        details: nodeDetails[nodeId],
        isPending: pendingNodeFetches.has(nodeId),
        needsLoading:
          !nodeDetails[nodeId] &&
          nodeDetails[nodeId] !== "loading" &&
          nodeDetails[nodeId] !== "error" &&
          !pendingNodeFetches.has(nodeId),
      }),
    }),
    [
      // More specific dependencies to prevent unnecessary recalculations
      visualizationData,
      connectionData,
      selectedClusterId,
      edgeSubmissionStatus,
      getClusterById,
      nodeDetails,
      pendingNodeFetches,
      queries.getClusterProgress,
    ]
  );

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
      // Debug logging to track calls
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

      // Step 1: Remove duplicates from input
      const uniqueNodesToFetch = uniqueBy(
        nodesToFetch,
        (node) => `${node.id}-${node.nodeType}`
      );

      // Step 2: ENHANCED filtering - check nodeDetails AND pendingNodeFetches
      const trulyNeedsFetching = uniqueNodesToFetch.filter((node) => {
        const currentState = nodeDetails[node.id];
        const isPending = pendingNodeFetches.has(node.id);

        // Skip if:
        // - Already has data (any truthy value except "error")
        // - Currently loading
        // - Currently being fetched by another call
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

      // Step 3: IMMEDIATELY mark as pending (before any async operations)
      const nodeIdsToLoad = trulyNeedsFetching.map((n) => n.id);
      setPendingNodeFetches((prev) => {
        const newSet = new Set(prev);
        nodeIdsToLoad.forEach((id) => newSet.add(id));
        console.log(`📝 Added to pending:`, nodeIdsToLoad);
        return newSet;
      });

      // Step 4: IMMEDIATELY mark as loading in nodeDetails
      setNodeDetails((prev) => {
        const newState = { ...prev };
        trulyNeedsFetching.forEach((node) => {
          newState[node.id] = "loading";
        });
        return newState;
      });

      // Step 5: Cleanup function to remove from pending state
      const cleanupPendingState = () => {
        setPendingNodeFetches((prev) => {
          const newSet = new Set(prev);
          nodeIdsToLoad.forEach((id) => newSet.delete(id));
          console.log(`🧹 Removed from pending:`, nodeIdsToLoad);
          return newSet;
        });
      };

      try {
        // Step 6: Batch fetch with proper error handling
        const NODE_FETCH_SIZE = 200;
        for (let i = 0; i < trulyNeedsFetching.length; i += NODE_FETCH_SIZE) {
          const chunk = trulyNeedsFetching.slice(i, i + NODE_FETCH_SIZE);

          try {
            const response = await getBulkNodeDetails({ items: chunk });

            // Update successful fetches
            setNodeDetails((prev) => {
              const newState = { ...prev };
              response.forEach((detail) => {
                newState[detail.id] = detail;
              });

              // Mark any missing nodes as error
              chunk.forEach((requestedNode) => {
                if (!response.find((r) => r.id === requestedNode.id)) {
                  newState[requestedNode.id] = "error";
                }
              });

              return newState;
            });
          } catch (error) {
            console.error(`❌ Error fetching node chunk:`, error);

            // Mark chunk as error
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
        // Always cleanup pending state, even on error
        cleanupPendingState();
      }
    },
    [nodeDetails, pendingNodeFetches] // Include pendingNodeFetches in dependencies
  );

  // REFACTORED: This function now progressively fetches connections.
  // It fetches the first small batch, updates the UI, then fetches the rest in the background.
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
              draft[item.edgeId]!.loading = true;
              draft[item.edgeId]!.error = null;
            }
          });
        })
      );

      const chunks: BulkConnectionRequestItem[][] = [];
      for (let i = 0; i < trulyNeedsFetching.length; i += MAX_BULK_FETCH_SIZE) {
        chunks.push(trulyNeedsFetching.slice(i, i + MAX_BULK_FETCH_SIZE));
      }

      if (chunks.length === 0) return;

      // Immediately fetch and await the first chunk (high priority)
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
              // Mark any requested items not in the response as errored
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

      // Fetch the rest of the chunks in the background
      if (chunks.length > 0) {
        chunks.forEach((chunk, index) => {
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
                        // Only update if still loading
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
                      draft[item.edgeId] = {
                        data: null,
                        loading: false,
                        error: (error as Error).message,
                        lastUpdated: null,
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

  const loadVisualizationDataForClusters = useCallback(
    async (items: BulkVisualizationRequestItem[]) => {
      if (items.length === 0) {
        return;
      }
      const uniqueItems = uniqueBy(
        items,
        (item) => `${item.clusterId}-${item.itemType}`
      );

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
            newState[item.clusterId]!.loading = true;
            newState[item.clusterId]!.error = null;
          }
        });
        return newState;
      });

      const allFetchedVisualizations: EntityVisualizationDataResponse[] = [];

      // A larger fetch size for visualizations is okay as they are lighter weight.
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
            newState[requestedItem.clusterId]!.loading = false;
          }
        });
        return newState;
      });

      const allNodeIdentifiers: NodeIdentifier[] = [];
      allFetchedVisualizations.forEach((vizData) => {
        vizData.nodes.forEach((node) =>
          allNodeIdentifiers.push({ id: node.id, nodeType: resolutionMode })
        );
      });

      if (allNodeIdentifiers.length > 0) {
        await loadBulkNodeDetails(allNodeIdentifiers);
      }

      // REFACTORED: Prioritize connections from the first unreviewed cluster.
      const priorityLinks: BulkConnectionRequestItem[] = [];
      const backgroundLinks: BulkConnectionRequestItem[] = [];

      let priorityClusterId: string | null = null;
      // Find the first cluster in the view that has visualization data and is unreviewed.
      for (const vizData of allFetchedVisualizations) {
        const clusterDetail = getClusterById(vizData.clusterId);
        if (clusterDetail && !clusterDetail.wasReviewed) {
          priorityClusterId = vizData.clusterId;
          break;
        }
      }

      // If no unreviewed clusters are found, just use the first one as priority.
      if (!priorityClusterId && allFetchedVisualizations.length > 0) {
        priorityClusterId = allFetchedVisualizations[0].clusterId;
      }

      allFetchedVisualizations.forEach((vizData) => {
        const clusterDetail = getClusterById(vizData.clusterId);
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

      // Combine the lists with priority items first.
      const allLinksToLoad = [...priorityLinks, ...backgroundLinks];

      if (allLinksToLoad.length > 0) {
        // The `await` is okay because the work inside loadBulkConnections is now progressive.
        // It will return after the first small batch is complete.
        await loadBulkConnections(allLinksToLoad);
      }
    },
    [resolutionMode, loadBulkNodeDetails, getClusterById, loadBulkConnections]
  );

  const loadVisualizationDataForClustersRef = useRef(
    loadVisualizationDataForClusters
  );
  // REMOVED: loadBulkConnectionsRef is no longer needed as the function is self-contained.
  useEffect(() => {
    loadVisualizationDataForClustersRef.current =
      loadVisualizationDataForClusters;
  }, [loadVisualizationDataForClusters]);

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
        const response = await fetcher(page, limit, clusterFilterStatus); // MODIFIED

        // If no clusters are returned, and it's not the first page, we might have over-paged.
        // Or, if it's the first page and no clusters, it means there are genuinely no clusters.
        if (response.clusters.length === 0 && response.page > 1) {
          console.warn(
            `No clusters on page ${response.page}. Attempting to load page 1.`
          );
          // Recursively call loadClusters to reset to page 1
          setClusters((prev) => ({
            ...prev,
            loading: false, // Stop loading state for the failed page
            error: null, // Clear any previous error
          }));
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

        // If no clusters are fetched, clear selected cluster
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
          // This will now handle the prioritized loading of connections internally.
          await loadVisualizationDataForClustersRef.current(
            vizRequestItemsNormal
          );
        }
      } catch (error) {
        setClusters((prev) => ({
          ...prev,
          loading: false,
          error: (error as Error).message,
          data: [],
          total: 0,
        }));
        // If an error occurs, clear selected cluster to prevent issues
        setSelectedClusterIdState(null);
      }
    },
    [resolutionMode, clusterFilterStatus] // ADDED DEPENDENCY
  );

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
            // Load viz data - this will handle node loading internally
            await loadVisualizationDataForClustersRef.current([
              { clusterId: id, itemType: resolutionMode },
            ]);
          }
          // REMOVED: Redundant node loading block that was causing duplicate calls
          // The loadVisualizationDataForClusters function already handles node loading
          // so we don't need to duplicate it here
          else if (vizState?.data?.links) {
            // Only handle connection loading if viz data already exists
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
          // For large clusters, only load nodes if viz data already exists
          const vizState = visualizationData[id];
          if (vizState?.data?.nodes) {
            const nodeIdsFromViz: NodeIdentifier[] = vizState.data.nodes.map(
              (node) => ({ id: node.id, nodeType: resolutionMode })
            );
            // Use the enhanced loadBulkNodeDetails which has proper deduplication
            await loadBulkNodeDetails(nodeIdsFromViz);
          }
        }
      }
    },
    [
      selectedClusterId,
      activelyPagingClusterId,
      largeClusterConnectionsPage,
      queries,
      visualizationData,
      resolutionMode,
      loadBulkNodeDetails, // Updated dependency
      getClusterById,
      connectionData,
      loadBulkConnections,
    ]
  );

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
          // Use the progressive loader for paged data as well.
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
      setSelectedEdgeIdState(null);
    },
    [
      resolutionMode,
      visualizationData,
      toast,
      loadConnectionDataForLinkPage,
      nodeDetails,
      loadBulkNodeDetails,
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
        setSelectedEdgeIdState(null);
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
    ]
  );

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

        // Ensure related nodes are loaded (but don't duplicate if already loading)
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
          // Use enhanced loadBulkNodeDetails which handles deduplication
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
            // Use enhanced loadBulkNodeDetails which handles deduplication
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
    [connectionData, resolutionMode, loadBulkNodeDetails] // Updated dependency
  );

  const invalidateVisualizationData = useCallback(
    async (clusterId: string) => {
      console.log(
        `Invalidating and reloading visualization data for cluster: ${clusterId}`
      );
      setVisualizationData((prev) => ({
        ...prev,
        [clusterId]: {
          data: null,
          loading: true,
          error: null,
          lastUpdated: null,
        },
      }));
      if (clusterId === activelyPagingClusterId) {
        await initializeLargeClusterConnectionPaging(clusterId);
      } else {
        await loadVisualizationDataForClustersRef.current([
          { clusterId, itemType: resolutionMode },
        ]);
      }
    },
    [
      resolutionMode,
      activelyPagingClusterId,
      initializeLargeClusterConnectionPaging,
    ]
  );

  const invalidateConnectionData = useCallback(
    async (edgeId: string) => {
      console.log(
        `Invalidating and reloading connection data for edge: ${edgeId}`
      );
      setConnectionData((prev) => ({
        ...prev,
        [edgeId]: {
          data: null,
          loading: true,
          error: null,
          lastUpdated: null,
        },
      }));
      await loadBulkConnections([{ edgeId, itemType: resolutionMode }]);
    },
    [resolutionMode, loadBulkConnections]
  );

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
      if (
        selectedClusterId &&
        (target === "all" || target === "current_visualization")
      ) {
        invalidateVisualizationData(selectedClusterId);
      }
      if (
        selectedEdgeId &&
        (target === "all" || target === "current_connection")
      ) {
        invalidateConnectionData(selectedEdgeId);
      }
      setRefreshTrigger((prev) => prev + 1);
    },
    [
      clusters.page,
      clusters.limit,
      selectedClusterId,
      selectedEdgeId,
      loadClusters,
      invalidateVisualizationData,
      invalidateConnectionData,
    ]
  );

  const advanceToNextCluster = useCallback(async () => {
    if (!selectedClusterId) {
      console.warn("No cluster selected, cannot advance.");
      return;
    }
    console.log(`Advancing to next cluster from: ${selectedClusterId}`);
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

    setActivelyPagingClusterId(null);
    setLargeClusterConnectionsPage(0);

    let nextClusterSelected = false;
    for (let i = currentIndex + 1; i < clusters.data.length; i++) {
      const nextClusterOnPage = clusters.data[i];
      if (!nextClusterOnPage.wasReviewed) {
        console.log(
          `Selecting next unreviewed cluster in current page: ${nextClusterOnPage.id}`
        );
        handleSetSelectedClusterId(nextClusterOnPage.id);
        nextClusterSelected = true;
        break;
      }
    }

    if (!nextClusterSelected) {
      if (clusters.page < Math.ceil(clusters.total / clusters.limit)) {
        const nextPageToLoad = clusters.page + 1;
        console.log(`Loading next page (${nextPageToLoad}) of clusters.`);
        await loadClusters(nextPageToLoad, clusters.limit);
      } else {
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
          advanceToNextCluster();
        } else {
          console.log(
            `Cluster ${targetClusterId} is complete. Auto-advance OFF or cluster not selected. Not advancing.`
          );
        }
      }
    },
    [selectedClusterId, queries, advanceToNextCluster, isAutoAdvanceEnabled]
  );

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
          setSelectedEdgeIdState(link.id);
          return;
        }
      }

      console.log(
        `No more unreviewed edges in current view of cluster ${currentClusterId}. Checking advance.`
      );
      setSelectedEdgeIdState(null);
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
      setSelectedEdgeIdState(nextEdgeToSelectId);
    }
  }, [
    selectedClusterId,
    selectedEdgeId,
    queries,
    checkAndAdvanceIfComplete,
    toast,
    currentVisualizationDataForSelection,
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
      setSelectedEdgeIdState(prevEdgeToSelectId);
    }
  }, [
    selectedClusterId,
    selectedEdgeId,
    queries,
    checkAndAdvanceIfComplete,
    toast,
    currentVisualizationDataForSelection,
  ]);

  const submitEdgeReview = useCallback(
    async (edgeId: string, decision: GroupReviewDecision, notes?: string) => {
      if (!currentUser?.id) {
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
          reviewerId: currentUser.id,
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
          if (isAutoAdvanceEnabled) {
            await advanceToNextCluster();
          }
        } else if (isAutoAdvanceEnabled) {
          selectNextUnreviewedEdge(edgeId);
        } else {
          await checkAndAdvanceIfComplete(selectedClusterId);
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
    [
      currentUser,
      selectedClusterId,
      visualizationData,
      resolutionMode,
      isAutoAdvanceEnabled,
      toast,
      advanceToNextCluster,
      selectNextUnreviewedEdge,
      checkAndAdvanceIfComplete,
    ]
  );

  const isLoadingConnectionPageData = useMemo(() => {
    if (!isLoadingPage) {
      return false;
    }

    if (selectedEdgeId) {
      return !queries.isConnectionDataLoaded(selectedEdgeId);
    }

    return true;
  }, [isLoadingPage, selectedEdgeId, queries]);

  const selectedEdgeConnectionState = selectedEdgeId
    ? connectionData[selectedEdgeId]
    : null;

  const currentConnectionData =
    useMemo((): EntityConnectionDataResponse | null => {
      if (!selectedEdgeId || !selectedEdgeConnectionState) return null;
      return selectedEdgeConnectionState.data || null;
    }, [
      selectedEdgeId,
      selectedEdgeConnectionState?.data,
      selectedEdgeConnectionState?.loading,
      selectedEdgeConnectionState?.error,
    ]);

  const selectedClusterDetails = useMemo((): EntityCluster | null => {
    if (!selectedClusterId) return null;
    return clusters.data.find((c) => c.id === selectedClusterId) || null;
  }, [selectedClusterId, clusters.data]);

  // REFACTORED: Simplified edge selection info as per Task 3.6.1
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

    // Use the full, non-paginated visualization data for all statistics
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

    // For "next" edge, we still need the currently visible links
    const currentVizForView = currentVisualizationDataForSelection;
    if (currentVizForView?.links) {
      defaultEdgeInfo.totalEdgesInView = currentVizForView.links.length;
      if (selectedEdgeId) {
        defaultEdgeInfo.currentEdgeIndex = currentVizForView.links.findIndex(
          (l) => l.id === selectedEdgeId
        );
      }
    }

    // Find next unreviewed edge in the whole cluster
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
          // It's the last one, or not in the unreviewed list, so just grab the first unreviewed one
          defaultEdgeInfo.nextUnreviewedEdgeId =
            allUnreviewedLinksInCluster[0].id;
        }
      } else {
        // No edge selected, so the next is the first in the unreviewed list
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

  useEffect(() => {
    // Only load clusters if there are no clusters loaded OR if there was an error in previous load
    // AND if the total is not 0 (meaning we haven't already confirmed no clusters exist for the filter).
    // This prevents infinite fetches if the backend truly returns 0 total clusters for a filter.
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
      selectedClusterId !== null // If total is 0, ensure selectedClusterId is null
    ) {
      // If total is 0 and we're not loading, and a cluster is still selected, deselect it.
      // This handles cases where all clusters might have been reviewed and moved to another tab.
      console.log(
        `[useEffect] Clusters total is 0 for filter ${clusterFilterStatus}. Deselecting current cluster.`
      );
      setSelectedClusterIdState(null);
    }
  }, [
    resolutionMode,
    clusterFilterStatus, // ADDED DEPENDENCY
    clusters.data.length,
    clusters.loading,
    loadClusters,
    clusters.limit,
    clusters.error,
    clusters.total, // Added clusters.total to dependency array
    selectedClusterId, // Added selectedClusterId to dependency array
  ]);

  useEffect(() => {
    // This effect handles auto-selecting a cluster when clusters load or filter changes.
    // It should only run if:
    // 1. No cluster is currently selected.
    // 2. Clusters data is available and not empty.
    // 3. Clusters are not currently loading.
    if (!selectedClusterId && clusters.data.length > 0 && !clusters.loading) {
      const firstNonReviewedCluster = clusters.data.find((c) => !c.wasReviewed);
      if (firstNonReviewedCluster) {
        console.log(
          "Auto-selecting first unreviewed cluster:",
          firstNonReviewedCluster.id
        );
        handleSetSelectedClusterId(firstNonReviewedCluster.id);
      } else if (clusters.data.length > 0) {
        // If all clusters on the current page are reviewed, select the first one for viewing.
        // This is important for the "reviewed" tab, where all clusters are expected to be reviewed.
        const firstCluster = clusters.data[0];
        console.log(
          "All clusters on current page are reviewed. Auto-selecting first for viewing:",
          firstCluster.id
        );
        handleSetSelectedClusterId(firstCluster.id);
      } else if (clusters.total === 0) {
        // Explicitly clear selected cluster if there are no clusters at all
        setSelectedClusterIdState(null);
      }
    } else if (
      selectedClusterId &&
      clusters.data.length === 0 &&
      !clusters.loading
    ) {
      // If a cluster is selected, but the cluster list is now empty (e.g., due to filter change resulting in no data)
      // then deselect the current cluster.
      console.log(
        "Current selected cluster no longer exists in an empty cluster list. Deselecting."
      );
      setSelectedClusterIdState(null);
    }
  }, [
    selectedClusterId,
    clusters.data,
    clusters.loading,
    clusters.total, // Add clusters.total to dependencies
    handleSetSelectedClusterId,
  ]);

  const actions = useMemo(
    () => ({
      setResolutionMode,
      setSelectedClusterId: handleSetSelectedClusterId,
      setSelectedEdgeId: setSelectedEdgeIdAction,
      setReviewerId,
      setLastReviewedEdgeId,
      setClusterFilterStatus, // ADDED
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
      setIsAutoAdvanceEnabled: setIsAutoAdvanceEnabledState,
      initializeLargeClusterConnectionPaging,
      viewNextConnectionPage,
      getActivelyPagingClusterId: () => activelyPagingClusterId,
      getLargeClusterConnectionsPage: () => largeClusterConnectionsPage,
      setIsReviewToolsMaximized,
    }),
    [
      setResolutionMode,
      handleSetSelectedClusterId,
      setSelectedEdgeIdAction,
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
      setClusterFilterStatus, // ADDED ACTION
    ]
  );

  const optimizedActions = useMemo(
    () => ({
      // Core state setters
      setResolutionMode,
      setSelectedClusterId: handleSetSelectedClusterId,
      setSelectedEdgeId: setSelectedEdgeIdAction,
      setReviewerId,
      setLastReviewedEdgeId,
      setClusterFilterStatus,
      setIsReviewToolsMaximized,
      setIsAutoAdvanceEnabled: setIsAutoAdvanceEnabledState,

      // Data loading actions
      triggerRefresh,
      loadClusters,
      loadBulkNodeDetails,
      loadSingleConnectionData,
      invalidateVisualizationData,
      invalidateConnectionData,
      clearAllData,

      // Navigation actions
      selectNextUnreviewedEdge,
      selectNextUnreviewedInCluster,
      selectPreviousUnreviewedInCluster,
      advanceToNextCluster,
      checkAndAdvanceIfComplete,

      // Review actions
      submitEdgeReview,

      // Large cluster paging actions
      initializeLargeClusterConnectionPaging,
      viewNextConnectionPage,
      getActivelyPagingClusterId: () => activelyPagingClusterId,
      getLargeClusterConnectionsPage: () => largeClusterConnectionsPage,
    }),
    [
      setResolutionMode,
      handleSetSelectedClusterId,
      setSelectedEdgeIdAction,
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
      setClusterFilterStatus,
    ]
  );

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
        actions.selectNextUnreviewedEdge();
      }
    }
  }, [
    selectedClusterId,
    currentVisualizationDataForSelection,
    selectedEdgeId,
    actions,
    isAutoAdvanceEnabled,
    lastReviewedEdgeId,
    activelyPagingClusterId,
    largeClusterConnectionsPage,
  ]);

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
        actions.loadSingleConnectionData(selectedEdgeId);
      }
    }
  }, [selectedEdgeId, connectionData, actions]);

  useEffect(() => {
    if (currentUser?.id) setReviewerId(currentUser.id);
  }, [currentUser]);

  const clusterProgress = useMemo(() => {
    const reconstructed: Record<string, ClusterReviewProgress> = {};

    // Process all clusters in the current data set
    clusters.data.forEach((cluster) => {
      reconstructed[cluster.id] = queries.getClusterProgress(cluster.id);
    });

    // Ensure selected cluster is included even if not in current page
    if (selectedClusterId && !reconstructed[selectedClusterId]) {
      const hasVisualizationData =
        visualizationData[selectedClusterId]?.data?.links;
      const hasClusterDetails = getClusterById(selectedClusterId);

      if (hasVisualizationData || hasClusterDetails) {
        reconstructed[selectedClusterId] =
          queries.getClusterProgress(selectedClusterId);
      }
    }

    return reconstructed;
  }, [
    clusters.data,
    selectedClusterId,
    // More specific dependencies instead of entire objects
    selectedClusterId
      ? visualizationData[selectedClusterId]?.data?.links
      : undefined,
    queries.getClusterProgress, // Only depend on the specific query function
    getClusterById,
  ]);

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
    actions: optimizedActions,
    queries: optimizedQueries,
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

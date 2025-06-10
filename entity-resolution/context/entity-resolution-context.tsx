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
  ClusterFilterStatus, // IMPORT NEW TYPE
} from "@/types/entity-resolution";
import {
  getServiceClusters,
  getBulkNodeDetails,
  getBulkConnections,
  getBulkVisualizations,
  getOrganizationClusters,
  getOrganizationConnectionData,
  getServiceConnectionData,
  postEdgeReview, // Import the new atomic API function
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
const MAX_BULK_FETCH_SIZE = 200;
const LARGE_CLUSTER_THRESHOLD = 200;
const CONNECTION_PAGE_SIZE = MAX_BULK_FETCH_SIZE;

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
        console.log("getClusterProgress Called");
        const vizState = visualizationData[clusterIdToQuery];
        const clusterDetails = getClusterById(clusterIdToQuery);

        // Case 1: Full visualization data is loaded. This is the primary source of truth.
        if (vizState?.data?.links) {
          console.log("Case 1: Full visualization data is loaded");
          const totalEdges = vizState.data.links.length;
          const reviewedEdges = vizState.data.links.filter(
            (l) => l.wasReviewed
          ).length;
          console.log("totalEdges", totalEdges);
          console.log("reviewedEdges", reviewedEdges);
          const progressPercentage =
            totalEdges > 0
              ? Math.round((reviewedEdges / totalEdges) * 100)
              : 100;
          console.log("progressPercentage", progressPercentage);
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
          console.log(
            "Case 2: No visualization data, but we have cluster details"
          );
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

  const clearAllData = useCallback(() => {
    console.log("Clearing all data.");
    setClusters(initialClustersState);
    setVisualizationData({});
    setConnectionData({});
    setNodeDetails({});
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
      const uniqueNodesToFetch = uniqueBy(
        nodesToFetch,
        (node) => `${node.id}-${node.nodeType}`
      );
      const trulyNeedsFetching = uniqueNodesToFetch.filter(
        (node) => !nodeDetails[node.id] || nodeDetails[node.id] === "error"
      );

      if (trulyNeedsFetching.length === 0) {
        return;
      }

      setNodeDetails((prev) => {
        const newState = { ...prev };
        trulyNeedsFetching.forEach((node) => {
          newState[node.id] = "loading";
        });
        return newState;
      });

      for (let i = 0; i < trulyNeedsFetching.length; i += MAX_BULK_FETCH_SIZE) {
        const chunk = trulyNeedsFetching.slice(i, i + MAX_BULK_FETCH_SIZE);
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
          setNodeDetails((prev) => {
            const newState = { ...prev };
            chunk.forEach((node) => {
              newState[node.id] = "error";
            });
            return newState;
          });
        }
      }
    },
    [nodeDetails]
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

      for (let i = 0; i < uniqueItems.length; i += MAX_BULK_FETCH_SIZE) {
        const chunk = uniqueItems.slice(i, i + MAX_BULK_FETCH_SIZE);
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
      const connectionRequestItemsForNonLarge: BulkConnectionRequestItem[] = [];
      allFetchedVisualizations.forEach((vizData) => {
        const clusterDetail = getClusterById(vizData.clusterId);
        const connectionCount = clusterDetail
          ? clusterDetail.groupCount
          : undefined;
        if (
          connectionCount !== undefined &&
          connectionCount !== null &&
          connectionCount <= LARGE_CLUSTER_THRESHOLD
        ) {
          vizData.links.forEach((link) =>
            connectionRequestItemsForNonLarge.push({
              edgeId: link.id,
              itemType: resolutionMode,
            })
          );
        }
      });
      if (connectionRequestItemsForNonLarge.length > 0) {
        await loadBulkConnections(connectionRequestItemsForNonLarge);
      }
    },
    [resolutionMode, loadBulkNodeDetails, getClusterById]
  );

  const loadBulkConnections = useCallback(
    async (items: BulkConnectionRequestItem[]) => {
      if (items.length === 0) {
        return;
      }
      const uniqueItems = uniqueBy(
        items,
        (item) => `${item.edgeId}-${item.itemType}`
      );
      const trulyNeedsFetching = uniqueItems.filter((item) => {
        const existing = connectionData[item.edgeId];
        return !existing || !existing.data || existing.error;
      });

      if (trulyNeedsFetching.length === 0) {
        return;
      }

      setConnectionData((prev) => {
        const newState = { ...prev };
        trulyNeedsFetching.forEach((item) => {
          if (!newState[item.edgeId] || newState[item.edgeId]?.error) {
            newState[item.edgeId] = {
              data: null,
              loading: true,
              error: null,
              lastUpdated: null,
            };
          } else if (newState[item.edgeId] && !newState[item.edgeId]?.loading) {
            newState[item.edgeId]!.loading = true;
            newState[item.edgeId]!.error = null;
          }
        });
        return newState;
      });

      for (let i = 0; i < trulyNeedsFetching.length; i += MAX_BULK_FETCH_SIZE) {
        const chunk = trulyNeedsFetching.slice(i, i + MAX_BULK_FETCH_SIZE);
        try {
          const response = await getBulkConnections({ items: chunk });
          setConnectionData((prev) => {
            const newState = { ...prev };
            response.forEach((connData) => {
              const edgeId = connData.edge.id;
              newState[edgeId] = {
                data: connData,
                loading: false,
                error: null,
                lastUpdated: Date.now(),
              };
            });
            chunk.forEach((requestedItem) => {
              const wasReturned = response.some(
                (r) => r.edge.id === requestedItem.edgeId
              );
              if (!wasReturned) {
                newState[requestedItem.edgeId] = {
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
          setConnectionData((prev) => {
            const newState = { ...prev };
            chunk.forEach((item) => {
              newState[item.edgeId] = {
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
    },
    [connectionData]
  );

  const loadVisualizationDataForClustersRef = useRef(
    loadVisualizationDataForClusters
  );
  const loadBulkConnectionsRef = useRef(loadBulkConnections);
  useEffect(() => {
    loadVisualizationDataForClustersRef.current =
      loadVisualizationDataForClusters;
  }, [loadVisualizationDataForClusters]);
  useEffect(() => {
    loadBulkConnectionsRef.current = loadBulkConnections;
  }, [loadBulkConnections]);

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
        setClusters({
          data: response.clusters,
          total: response.total,
          page: response.page,
          limit: response.limit,
          loading: false,
          error: null,
        });

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
      } catch (error) {
        setClusters((prev) => ({
          ...prev,
          loading: false,
          error: (error as Error).message,
          data: [],
          total: 0,
        }));
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
            await loadVisualizationDataForClustersRef.current([
              { clusterId: id, itemType: resolutionMode },
            ]);
          } else if (vizState?.data?.nodes) {
            const nodeIdsFromViz: NodeIdentifier[] = vizState.data.nodes.map(
              (node) => ({ id: node.id, nodeType: resolutionMode })
            );
            if (nodeIdsFromViz.length > 0) {
              const nodesTrulyNeedingFetch = nodeIdsFromViz.filter(
                (n) => !nodeDetails[n.id] || nodeDetails[n.id] === "error"
              );
              if (nodesTrulyNeedingFetch.length > 0) {
                await loadBulkNodeDetails(nodesTrulyNeedingFetch);
              }
            }
          }
        } else {
          const vizState = visualizationData[id];
          if (vizState?.data?.nodes) {
            const nodeIdsFromViz: NodeIdentifier[] = vizState.data.nodes.map(
              (node) => ({ id: node.id, nodeType: resolutionMode })
            );
            if (nodeIdsFromViz.length > 0) {
              const nodesTrulyNeedingFetch = nodeIdsFromViz.filter(
                (n) => !nodeDetails[n.id] || nodeDetails[n.id] === "error"
              );
              if (nodesTrulyNeedingFetch.length > 0) {
                await loadBulkNodeDetails(nodesTrulyNeedingFetch);
              }
            }
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
      loadBulkNodeDetails,
      nodeDetails,
      getClusterById,
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
          await loadBulkConnectionsRef.current(connectionItemsToFetch);
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
    [visualizationData, resolutionMode, connectionData, toast]
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
          const nodesTrulyNeedingFetch = nodesToLoad.filter(
            (n) => !nodeDetails[n.id] || nodeDetails[n.id] === "error"
          );
          if (nodesTrulyNeedingFetch.length > 0)
            await loadBulkNodeDetails(nodesTrulyNeedingFetch);
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
            const nodesTrulyNeedingFetch = nodesToLoad.filter(
              (n) => !nodeDetails[n.id] || nodeDetails[n.id] === "error"
            );
            if (nodesTrulyNeedingFetch.length > 0)
              await loadBulkNodeDetails(nodesTrulyNeedingFetch);
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
    [connectionData, resolutionMode, loadBulkNodeDetails, nodeDetails]
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
      await loadBulkConnectionsRef.current([
        { edgeId, itemType: resolutionMode },
      ]);
    },
    [resolutionMode]
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

  const currentConnectionData =
    useMemo((): EntityConnectionDataResponse | null => {
      if (!selectedEdgeId) return null;
      return connectionData[selectedEdgeId]?.data || null;
    }, [selectedEdgeId, connectionData]);

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
    if (clusters.data.length === 0 && !clusters.loading && !clusters.error) {
      loadClusters(1, clusters.limit);
    }
  }, [
    resolutionMode,
    clusterFilterStatus, // ADDED DEPENDENCY
    clusters.data.length,
    clusters.loading,
    loadClusters,
    clusters.limit,
    clusters.error,
  ]);

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
      }
    }
  }, [
    selectedClusterId,
    clusters.data,
    clusters.loading,
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

  const contextValue: EntityResolutionContextType = {
    resolutionMode,
    selectedClusterId,
    selectedEdgeId,
    reviewerId,
    lastReviewedEdgeId,
    refreshTrigger,
    isAutoAdvanceEnabled,
    isReviewToolsMaximized,
    clusterFilterStatus, // ADDED
    clusters,
    visualizationData,
    connectionData,
    nodeDetails,
    activelyPagingClusterId,
    largeClusterConnectionsPage,
    isLoadingConnectionPageData,
    clusterProgress: useMemo(() => {
      const reconstructed: Record<string, ClusterReviewProgress> = {};
      clusters.data.forEach((c) => {
        reconstructed[c.id] = queries.getClusterProgress(c.id);
      });
      if (
        selectedClusterId &&
        !reconstructed[selectedClusterId] &&
        (visualizationData[selectedClusterId]?.data?.links ||
          getClusterById(selectedClusterId))
      ) {
        reconstructed[selectedClusterId] =
          queries.getClusterProgress(selectedClusterId);
      }
      return reconstructed;
    }, [
      clusters.data,
      selectedClusterId,
      visualizationData,
      queries,
      getClusterById,
    ]),
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

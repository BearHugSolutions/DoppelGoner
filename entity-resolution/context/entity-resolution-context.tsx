// context/entity-resolution-context.tsx
"use client";

import {
  ResolutionMode,
  EntityCluster,
  EntityVisualizationDataResponse,
  EntityConnectionDataResponse,
  EntityGroup,
  GroupReviewDecision,
  ClusterFinalizationStatusResponse,
  QueuedReviewBatch,
  BaseLink,
  NodeDetailResponse,
  NodeIdentifier,
  BulkConnectionRequestItem,
  BulkVisualizationRequestItem,
  isEntityConnectionData,
  ClusterReviewProgress,
  Service,
  VisualizationEntityEdge,
} from "@/types/entity-resolution";
import {
  postEntityGroupFeedback,
  triggerEntityClusterFinalization,
  getServiceClusters,
  postServiceGroupFeedback,
  triggerServiceClusterFinalization,
  getBulkNodeDetails,
  getBulkConnections,
  getBulkVisualizations,
  getOrganizationClusters,
  getOrganizationConnectionData, // Added Import
  getServiceConnectionData, // Added Import
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
import { Button } from "@/components/ui/button";
import { v4 as uuidv4 } from "uuid";

// Helper to get unique items from an array based on a key selector
function uniqueBy<T>(items: T[], keySelector: (item: T) => string): T[] {
  return Array.from(
    new Map(items.map((item) => [keySelector(item), item])).values()
  );
}

// Constants for bulk fetching and large cluster handling
const MAX_BULK_FETCH_SIZE = 200;
const BULK_FETCH_DELAY_MS = 200;
const LARGE_CLUSTER_THRESHOLD = 200;
const CONNECTION_PAGE_SIZE = MAX_BULK_FETCH_SIZE;

// Sleep utility function
function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

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
  totalEdges: number;
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

  // State for clusters, visualization, and connection data
  clusters: ClustersState;
  visualizationData: Record<string, VisualizationState>;
  connectionData: Record<string, ConnectionState>;
  nodeDetails: Record<string, NodeDetailResponse | null | "loading" | "error">;

  reviewQueue: QueuedReviewBatch[];
  isProcessingQueue: boolean;

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
      decision: GroupReviewDecision
    ) => Promise<void>;
    retryFailedBatch: (batchId: string) => void;
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
    getQueueItemStatus: (
      edgeId: string
    ) => "pending" | "processing" | "failed" | null;
    getClusterById: (clusterId: string) => EntityCluster | undefined;
    getNodeDetail: (
      nodeId: string
    ) => NodeDetailResponse | null | "loading" | "error";
  };
}

const EntityResolutionContext = createContext<
  EntityResolutionContextType | undefined
>(undefined);

const MAX_REVIEW_ATTEMPTS = 3;

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

  // State definitions are now simplified to use the unified types
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

  const [reviewQueue, setReviewQueue] = useState<QueuedReviewBatch[]>([]);
  const [isProcessingQueue, setIsProcessingQueue] = useState<boolean>(false);
  const processingBatchIdRef = useRef<string | null>(null);

  const [activelyPagingClusterId, setActivelyPagingClusterId] = useState<
    string | null
  >(null);
  const [largeClusterConnectionsPage, setLargeClusterConnectionsPage] =
    useState<number>(0);
  const [isLoadingConnectionPageData, setIsLoadingConnectionPageData] =
    useState<boolean>(false);
  const [finalizingClusters, setFinalizingClusters] = useState<Set<string>>(
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
      getClusterProgress: (clusterIdToQuery: string): ClusterReviewProgress => {
        const vizState = visualizationData[clusterIdToQuery];
        const clusterDetails = getClusterById(clusterIdToQuery);
        const linksForProgress = vizState?.data?.links || [];

        if (clusterDetails?.wasSplit) {
          const reviewedCount = linksForProgress.filter(
            (l) => l.status !== "PENDING_REVIEW"
          ).length;
          return {
            totalEdges: linksForProgress.length,
            reviewedEdges: reviewedCount,
            pendingEdges: 0,
            confirmedMatches: linksForProgress.filter(
              (l) => l.status === "CONFIRMED_MATCH"
            ).length,
            confirmedNonMatches: linksForProgress.filter(
              (l) => l.status === "CONFIRMED_NON_MATCH"
            ).length,
            progressPercentage: 100,
            isComplete: true,
          };
        }

        if (linksForProgress.length === 0) {
          if (clusterDetails) {
            const count = clusterDetails.groupCount;

            if (count === 0) {
              return {
                totalEdges: 0,
                reviewedEdges: 0,
                pendingEdges: 0,
                confirmedMatches: 0,
                confirmedNonMatches: 0,
                progressPercentage: 100,
                isComplete: true,
              };
            } else if (count && count > 0) {
              return {
                totalEdges: -1,
                reviewedEdges: 0,
                pendingEdges: -1,
                confirmedMatches: 0,
                confirmedNonMatches: 0,
                progressPercentage: -1,
                isComplete: false,
              };
            }
          }
          return {
            totalEdges: 0,
            reviewedEdges: 0,
            pendingEdges: 0,
            confirmedMatches: 0,
            confirmedNonMatches: 0,
            progressPercentage: 0,
            isComplete: false,
          };
        }

        const totalEdges = linksForProgress.length;
        const confirmedMatches = linksForProgress.filter(
          (link) => link.status === "CONFIRMED_MATCH"
        ).length;
        const confirmedNonMatches = linksForProgress.filter(
          (link) => link.status === "CONFIRMED_NON_MATCH"
        ).length;
        const reviewedEdges = confirmedMatches + confirmedNonMatches;
        const pendingEdges = totalEdges - reviewedEdges;
        const progressPercentage =
          totalEdges > 0
            ? Math.round((reviewedEdges / totalEdges) * 100)
            : totalEdges === 0
            ? 100
            : 0;
        return {
          totalEdges,
          reviewedEdges,
          pendingEdges,
          confirmedMatches,
          confirmedNonMatches,
          progressPercentage,
          isComplete: pendingEdges === 0 && totalEdges >= 0,
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
        return edge ? edge.status !== "PENDING_REVIEW" : false;
      },
      getEdgeStatus: (edgeId: string) => {
        const currentViz = selectedClusterId
          ? visualizationData[selectedClusterId]?.data
          : null;
        if (!currentViz?.links) return null;
        const edge = currentViz.links.find((l) => l.id === edgeId);
        return edge?.status ?? null;
      },
      getQueueItemStatus: (edgeId: string) => {
        const item = reviewQueue.find((b) => b.edgeId === edgeId);
        if (!item) return null;
        if (item.isTerminalFailure) return "failed";
        if (processingBatchIdRef.current === item.batchId) return "processing";
        return "pending";
      },
      getClusterById,
      getNodeDetail: (nodeId: string) => nodeDetails[nodeId] || null,
    }),
    [
      visualizationData,
      connectionData,
      selectedClusterId,
      reviewQueue,
      processingBatchIdRef,
      getClusterById,
      nodeDetails,
    ]
  );

  const setResolutionMode = useCallback(
    (mode: ResolutionMode) => {
      if (mode === resolutionMode) return;
      console.log(
        `Switching resolution mode from ${resolutionMode} to ${mode}`
      );
      setResolutionModeState(mode);
      setSelectedClusterIdState(null);
      setSelectedEdgeIdState(null);
      setLastReviewedEdgeId(null);
      setClusters(initialClustersState);
      setVisualizationData({});
      setConnectionData({});
      setNodeDetails({});
      setIsAutoAdvanceEnabledState(true);
      setActivelyPagingClusterId(null);
      setLargeClusterConnectionsPage(0);
      setIsLoadingConnectionPageData(false);
    },
    [resolutionMode]
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
      // The resolutionMode still determines which API endpoint to call.
      const fetcher =
        resolutionMode === "entity"
          ? getOrganizationClusters
          : getServiceClusters;

      try {
        // Both fetchers now return PaginatedClustersResponse<EntityCluster>
        const response = await fetcher(page, limit);
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
    [resolutionMode]
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
          clusterDetail?.wasSplit ||
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
        if (!isPrefetch) setIsLoadingConnectionPageData(false);
        return;
      }

      if (!isPrefetch) {
        setIsLoadingConnectionPageData(true);
      }

      const startIndex = (pageToLoad - 1) * CONNECTION_PAGE_SIZE;
      const endIndex = startIndex + CONNECTION_PAGE_SIZE;
      const linksForPage = viz.links.slice(startIndex, endIndex);

      if (linksForPage.length === 0) {
        console.log(
          `loadConnectionDataForLinkPage: No links for page ${pageToLoad} in cluster ${clusterId}.`
        );
        if (!isPrefetch) setIsLoadingConnectionPageData(false);
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
        setIsLoadingConnectionPageData(false);
        const totalLinks = viz.links.length;
        if (endIndex < totalLinks) {
          // Check if there's a next page to prefetch
          console.log(
            `Prefetching connection data for page ${
              pageToLoad + 1
            } of cluster ${clusterId}`
          );
          // Fire and forget prefetch
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
      setLargeClusterConnectionsPage(1); // Start at page 1
      setIsLoadingConnectionPageData(true);

      let viz = visualizationData[clusterId]?.data;
      // Ensure full visualization data (nodes and all links) is loaded for large clusters before paging connections
      if (
        !viz ||
        visualizationData[clusterId]?.error ||
        !visualizationData[clusterId]?.data?.links
      ) {
        // Check if links are present
        console.log(
          `Fetching/Re-fetching visualization data (nodes and all links) for large cluster ${clusterId} before paging connections.`
        );
        try {
          await loadVisualizationDataForClustersRef.current([
            { clusterId, itemType: resolutionMode },
          ]);
          viz = visualizationData[clusterId]?.data; // Re-check after fetch
          if (!viz || !viz.links)
            throw new Error(
              "Visualization data (with links) still not available after fetch."
            );

          // Load node details for the newly fetched visualization
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
          setIsLoadingConnectionPageData(false);
          setActivelyPagingClusterId(null); // Reset paging state on error
          setLargeClusterConnectionsPage(0);
          return;
        }
      } else {
        // If visualization data (with links) is already present, ensure node details are loaded
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

      await loadConnectionDataForLinkPage(clusterId, 1, false); // Load first page of connections
      setSelectedEdgeIdState(null); // Clear selected edge when initializing/changing pages
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
        setSelectedEdgeIdState(null); // Clear selected edge
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

  // UPDATED: This function no longer calls the bulk endpoint for a single item.
  const loadSingleConnectionData = useCallback(
    async (edgeId: string): Promise<EntityConnectionDataResponse | null> => {
      const cached = connectionData[edgeId];
      // Check for valid cache entry first.
      if (
        cached?.data &&
        !cached.loading &&
        !cached.error &&
        cached.lastUpdated &&
        Date.now() - cached.lastUpdated < 300000 // 5-minute cache
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
        // Determine which specific fetcher to use based on the resolution mode.
        const fetcher =
          resolutionMode === "entity"
            ? getOrganizationConnectionData
            : getServiceConnectionData;

        const response = await fetcher(edgeId);

        // Update state with the fetched data.
        setConnectionData((prev) => ({
          ...prev,
          [edgeId]: {
            data: response,
            loading: false,
            error: null,
            lastUpdated: Date.now(),
          },
        }));

        // After getting connection data, fetch details for the nodes involved.
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
            // This checks for nodes that genuinely need fetching.
            const nodesTrulyNeedingFetch = nodesToLoad.filter(
              (n) => !nodeDetails[n.id] || nodeDetails[n.id] === "error"
            );
            if (nodesTrulyNeedingFetch.length > 0)
              await loadBulkNodeDetails(nodesTrulyNeedingFetch);
          }
        }
        return response; // Return the data as per the function's signature.
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
      console.log(`Setting selected edge ID to: ${id}`);
      if (id && queries.isEdgeReviewed(id)) {
        console.log(`Edge ${id} is already reviewed. Pausing auto-advance.`);
        setIsAutoAdvanceEnabledState(false);
      } else if (id) {
        const currentClusterId = selectedClusterId;
        if (currentClusterId) {
          const clusterProgress = queries.getClusterProgress(currentClusterId);
          const clusterDetail = queries.getClusterById(currentClusterId);
          if (!clusterProgress.isComplete && !clusterDetail?.wasSplit) {
            // Re-enable auto-advance if it was off, handled by selectNextUnreviewedEdge etc.
          }
        }
      }
      setSelectedEdgeIdState(id);
    },
    [queries, setIsAutoAdvanceEnabledState, selectedClusterId, getClusterById]
  );

  const clearAllData = useCallback(() => {
    console.log("Clearing all data.");
    setClusters(initialClustersState);
    setVisualizationData({});
    setConnectionData({});
    setNodeDetails({});
    setReviewQueue([]);
    setSelectedClusterIdState(null);
    setSelectedEdgeIdState(null);
    setLastReviewedEdgeId(null);
    setIsAutoAdvanceEnabledState(true);
    setActivelyPagingClusterId(null);
    setLargeClusterConnectionsPage(0);
    setIsLoadingConnectionPageData(false);
  }, []);

  const retryFailedBatch = useCallback((batchId: string) => {
    console.log(`Retrying failed batch: ${batchId}`);
    setReviewQueue((prev) =>
      prev.map((b) => {
        if (b.batchId === batchId && b.isTerminalFailure) {
          return {
            ...b,
            attempt: 0,
            isTerminalFailure: false,
            error: undefined,
            failedOperations: new Set(),
            processedOperations: new Set(),
          };
        }
        return b;
      })
    );
  }, []);

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
      if (
        !queries.getClusterProgress(nextClusterOnPage.id).isComplete &&
        !nextClusterOnPage.wasSplit
      ) {
        console.log(
          `Selecting next uncompleted, non-split cluster in current page: ${nextClusterOnPage.id}`
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
          description:
            "All clusters have been processed or are completed/split.",
        });
        console.log("All clusters processed or completed/split.");
        setIsAutoAdvanceEnabledState(false);
      }
    }
  }, [
    selectedClusterId,
    clusters,
    loadClusters,
    toast,
    queries,
    handleSetSelectedClusterId,
  ]);

  const checkAndAdvanceIfComplete = useCallback(
    async (clusterIdToCheck?: string) => {
      const targetClusterId = clusterIdToCheck || selectedClusterId;
      if (!targetClusterId) {
        console.log("No target cluster ID to check for completion.");
        return;
      }

      // 1. Check if finalization is already in progress for this cluster.
      if (finalizingClusters.has(targetClusterId)) {
        console.log(
          `Finalization for cluster ${targetClusterId} is already in progress. Skipping.`
        );
        return;
      }

      const clusterDetail = queries.getClusterById(targetClusterId);
      const progress = queries.getClusterProgress(targetClusterId);

      console.log(
        `Cluster ${targetClusterId}: Total=${progress.totalEdges}, Reviewed=${progress.reviewedEdges}, Pending=${progress.pendingEdges}, Complete=${progress.isComplete}, WasSplit=${clusterDetail?.wasSplit}`
      );

      if (
        progress.isComplete &&
        progress.totalEdges > 0 &&
        !clusterDetail?.wasSplit
      ) {
        toast({
          title: "Cluster Review Complete",
          description: `Review of cluster ${targetClusterId.substring(
            0,
            8
          )}... is complete. Finalizing...`,
        });
        const finalizer =
          resolutionMode === "entity"
            ? triggerEntityClusterFinalization
            : triggerServiceClusterFinalization;
        try {
          // 2. Set the lock before making the API call.
          setFinalizingClusters((prev) => new Set(prev).add(targetClusterId));

          console.log(
            `Finalizing cluster ${targetClusterId} for mode ${resolutionMode}.`
          );
          const finalizationResponse = await finalizer(targetClusterId);
          toast({
            title: "Cluster Finalization",
            description: `${finalizationResponse.message}. Status: ${finalizationResponse.status}`,
          });
          console.log(
            `Finalization response for ${targetClusterId}:`,
            finalizationResponse
          );

          if (
            finalizationResponse.status !== "ERROR" &&
            finalizationResponse.status !== "CLUSTER_NOT_FOUND" &&
            finalizationResponse.status !== "PENDING_FULL_REVIEW"
          ) {
            setClusters((prevClusters) => {
              const newClusterData = prevClusters.data.map((c) => {
                if (c.id === targetClusterId) {
                  let updatedCluster = { ...c };
                  if (
                    finalizationResponse.status ===
                      "COMPLETED_MARKED_AS_SPLIT" ||
                    finalizationResponse.status === "COMPLETED_SPLIT_DETECTED"
                  ) {
                    updatedCluster.wasSplit = true;
                  } else if (
                    finalizationResponse.status ===
                      "COMPLETED_NO_SPLIT_NEEDED" ||
                    finalizationResponse.status ===
                      "COMPLETED_NO_CONFIRMED_MATCHES"
                  ) {
                    updatedCluster.wasSplit = false;
                  }
                  return updatedCluster;
                }
                return c;
              });
              return { ...prevClusters, data: newClusterData };
            });
            if (targetClusterId === activelyPagingClusterId) {
              setActivelyPagingClusterId(null);
              setLargeClusterConnectionsPage(0);
            }
          }

          if (targetClusterId === selectedClusterId) {
            if (isAutoAdvanceEnabled) {
              console.log(
                `Auto-advance ON. Advancing after finalizing ${targetClusterId}.`
              );
              advanceToNextCluster();
            } else {
              console.log(
                `Cluster ${targetClusterId} finalized, auto-advance OFF. Not advancing.`
              );
            }
          }
        } catch (finalizationError) {
          toast({
            title: "Finalization Error",
            description: `Could not finalize cluster ${targetClusterId.substring(
              0,
              8
            )}...: ${(finalizationError as Error).message}`,
            variant: "destructive",
          });
          console.error(
            `Error finalizing cluster ${targetClusterId}:`,
            finalizationError
          );
        } finally {
          // 3. Always release the lock after the operation is complete.
          setFinalizingClusters((prev) => {
            const newSet = new Set(prev);
            newSet.delete(targetClusterId);
            return newSet;
          });
        }
      } else if (
        (progress.isComplete && progress.totalEdges >= 0) ||
        clusterDetail?.wasSplit
      ) {
        if (targetClusterId === selectedClusterId && isAutoAdvanceEnabled) {
          console.log(
            `Cluster ${targetClusterId} is already split or is an empty/completed cluster. Auto-advance ON. Advancing.`
          );
          advanceToNextCluster();
        } else if (
          (progress.isComplete && progress.totalEdges >= 0) ||
          clusterDetail?.wasSplit
        ) {
          console.log(
            `Cluster ${targetClusterId} is already split or empty/completed. Auto-advance OFF or not selected. Not advancing.`
          );
        }
      }
    },
    // Remember to add the new state to the dependency array
    [
      selectedClusterId,
      queries,
      advanceToNextCluster,
      toast,
      resolutionMode,
      isAutoAdvanceEnabled,
      activelyPagingClusterId,
      finalizingClusters,
    ]
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
      if (clusterDetail?.wasSplit) {
        console.log(
          `Cluster ${currentClusterId} was split. No edges to select. Checking advance.`
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
    if (clusterDetail?.wasSplit) {
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
    if (clusterDetail?.wasSplit) {
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
    async (edgeId: string, decision: GroupReviewDecision) => {
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
      const clusterDetail = queries.getClusterById(selectedClusterId);
      if (clusterDetail?.wasSplit) {
        toast({
          title: "Info",
          description:
            "This cluster has been split and cannot be further reviewed.",
        });
        return;
      }

      console.log(
        `Submitting edge review: ${edgeId}, decision: ${decision}, mode: ${resolutionMode}`
      );
      const connDataState = connectionData[edgeId];
      const vizDataState = visualizationData[selectedClusterId];

      if (!connDataState?.data || !vizDataState?.data) {
        toast({
          title: "Data Error",
          description: "Data not loaded for review. Please wait or refresh.",
          variant: "destructive",
        });
        if (!connDataState?.data && !connDataState?.loading)
          loadSingleConnectionData(edgeId);
        if (
          !vizDataState?.data &&
          selectedClusterId &&
          !vizDataState?.loading
        ) {
          const currentCluster = queries.getClusterById(selectedClusterId);
          // Simplified: Use unified `groupCount`
          const connectionCount = currentCluster?.groupCount;
          if (
            connectionCount === undefined ||
            connectionCount === null ||
            connectionCount <= LARGE_CLUSTER_THRESHOLD
          ) {
            invalidateVisualizationData(selectedClusterId);
          } else {
            console.error(
              "Missing full visualization data for a large cluster during review submission. Attempting to initialize."
            );
            if (selectedClusterId === activelyPagingClusterId) {
              initializeLargeClusterConnectionPaging(selectedClusterId);
            } else {
              invalidateVisualizationData(selectedClusterId);
            }
          }
        }
        return;
      }

      // Data types are now unified
      const connData = connDataState.data;
      const fullVizData = vizDataState.data;

      // Logic for getting groups is now unified, always using entityGroups.
      const relevantGroups: EntityGroup[] = connData.entityGroups.filter(
        (group) =>
          (group.entityId1 ===
            (connData.edge as VisualizationEntityEdge).entityId1 &&
            group.entityId2 ===
              (connData.edge as VisualizationEntityEdge).entityId2) ||
          (group.entityId1 ===
            (connData.edge as VisualizationEntityEdge).entityId2 &&
            group.entityId2 ===
              (connData.edge as VisualizationEntityEdge).entityId1)
      );
      const currentEdgeLink = fullVizData.links.find((l) => l.id === edgeId);

      if (relevantGroups.length === 0) {
        toast({
          title: "Info",
          description: "No underlying groups. Marking edge as reviewed.",
        });
        setVisualizationData((prev) => {
          const newVizData = { ...prev };
          const targetClusterVizState = newVizData[selectedClusterId!]?.data;
          if (targetClusterVizState?.links) {
            const newTargetClusterViz = { ...targetClusterVizState };
            const linkIndex = newTargetClusterViz.links.findIndex(
              (l) => l.id === edgeId
            );
            if (linkIndex !== -1) {
              const newLinks = [...newTargetClusterViz.links];
              newLinks[linkIndex] = {
                ...newLinks[linkIndex],
                status:
                  decision === "ACCEPTED"
                    ? "CONFIRMED_MATCH"
                    : "CONFIRMED_NON_MATCH",
              };
              newTargetClusterViz.links = newLinks;
            }
            newVizData[selectedClusterId!] = {
              ...newVizData[selectedClusterId!],
              data: newTargetClusterViz,
              lastUpdated: Date.now(),
            };
          }
          return newVizData;
        });
        setLastReviewedEdgeId(edgeId);
        if (isAutoAdvanceEnabled) selectNextUnreviewedEdge(edgeId);
        else checkAndAdvanceIfComplete(selectedClusterId);
        return;
      }

      if (!currentEdgeLink) {
        console.error(
          "Critical: Current edge link not found in full visualization data for optimistic update."
        );
        toast({
          title: "Internal Error",
          description: "Edge not found for update. Refresh.",
          variant: "destructive",
        });
        return;
      }

      const operations = relevantGroups.map((group) => ({
        groupId: group.id,
        originalGroupStatus: group.confirmedStatus,
      }));
      const optimisticEdgeStatus =
        decision === "ACCEPTED" ? "CONFIRMED_MATCH" : "CONFIRMED_NON_MATCH";
      const optimisticGroupStatus =
        decision === "ACCEPTED" ? "CONFIRMED_MATCH" : "CONFIRMED_NON_MATCH";

      const batch: QueuedReviewBatch = {
        batchId: uuidv4(),
        edgeId,
        clusterId: selectedClusterId,
        decision,
        reviewerId: currentUser.id,
        operations,
        originalEdgeStatus: currentEdgeLink.status || "PENDING_REVIEW",
        optimisticEdgeStatus,
        optimisticGroupStatus,
        attempt: 0,
        processedOperations: new Set(),
        failedOperations: new Set(),
        mode: resolutionMode,
      };

      // Optimistic update for visualizationData
      setVisualizationData((prevVizData) => {
        const newVizDataState = { ...prevVizData };
        const targetClusterVizState = newVizDataState[batch.clusterId];
        if (targetClusterVizState?.data) {
          const targetClusterViz = JSON.parse(
            JSON.stringify(targetClusterVizState.data)
          ) as EntityVisualizationDataResponse;

          const linkIndex = targetClusterViz.links.findIndex(
            (l) => l.id === batch.edgeId
          );
          if (linkIndex !== -1) {
            targetClusterViz.links[linkIndex].status =
              batch.optimisticEdgeStatus;
          }

          if (
            targetClusterViz.groups &&
            Array.isArray(targetClusterViz.groups)
          ) {
            (targetClusterViz.groups as EntityGroup[]).forEach((group) => {
              if (batch.operations.some((op) => op.groupId === group.id)) {
                group.confirmedStatus = batch.optimisticGroupStatus;
              }
            });
          }
          newVizDataState[batch.clusterId] = {
            ...targetClusterVizState,
            data: targetClusterViz,
            lastUpdated: Date.now(),
          };
        }
        return newVizDataState;
      });

      // Optimistic update for connectionData
      setConnectionData((prevConnData) => {
        const newConnDataState = { ...prevConnData };
        const targetConnState = newConnDataState[batch.edgeId];
        if (targetConnState?.data) {
          let targetConn = JSON.parse(
            JSON.stringify(targetConnState.data)
          ) as EntityConnectionDataResponse;

          // Logic is now unified.
          targetConn.edge.status = batch.optimisticEdgeStatus;
          targetConn.edge.confirmedStatus = batch.optimisticGroupStatus;
          targetConn.entityGroups.forEach((group) => {
            if (batch.operations.some((op) => op.groupId === group.id)) {
              group.confirmedStatus = batch.optimisticGroupStatus;
            }
          });

          newConnDataState[batch.edgeId] = {
            ...targetConnState,
            data: targetConn,
            lastUpdated: Date.now(),
          };
        }
        return newConnDataState;
      });

      setReviewQueue((prevQueue) => [...prevQueue, batch]);
      setLastReviewedEdgeId(edgeId);
      if (isAutoAdvanceEnabled) selectNextUnreviewedEdge(edgeId);
      else checkAndAdvanceIfComplete(selectedClusterId);
    },
    [
      currentUser,
      selectedClusterId,
      connectionData,
      visualizationData,
      toast,
      resolutionMode,
      loadSingleConnectionData,
      invalidateVisualizationData,
      isAutoAdvanceEnabled,
      selectNextUnreviewedEdge,
      checkAndAdvanceIfComplete,
      queries,
      activelyPagingClusterId,
      initializeLargeClusterConnectionPaging,
    ]
  );

  const processReviewQueue = useCallback(async () => {
    if (isProcessingQueue || reviewQueue.length === 0) return;
    const batchToProcess = reviewQueue.find(
      (b) =>
        !b.isTerminalFailure &&
        (b.failedOperations.size === 0 || b.attempt < MAX_REVIEW_ATTEMPTS)
    );
    if (!batchToProcess) {
      setIsProcessingQueue(false);
      return;
    }
    if (
      processingBatchIdRef.current &&
      processingBatchIdRef.current !== batchToProcess.batchId
    ) {
      console.log(
        `Queue busy with ${processingBatchIdRef.current}, skipping ${batchToProcess.batchId}`
      );
      return;
    }

    setIsProcessingQueue(true);
    processingBatchIdRef.current = batchToProcess.batchId;
    console.log(
      `Processing batch: ${batchToProcess.batchId}, attempt: ${
        batchToProcess.attempt + 1
      }`
    );

    let currentBatch = {
      ...batchToProcess,
      attempt: batchToProcess.attempt + 1,
      failedOperations: new Set<string>(batchToProcess.failedOperations),
    };
    let batchOverallSuccess = true;
    const stillPendingOperations = currentBatch.operations.filter(
      (op) => !currentBatch.processedOperations.has(op.groupId)
    );

    // The mode still determines the API endpoint.
    const feedbackPoster =
      currentBatch.mode === "entity"
        ? postEntityGroupFeedback
        : postServiceGroupFeedback;

    for (const op of stillPendingOperations) {
      try {
        const payload = {
          decision: currentBatch.decision,
          reviewerId: currentBatch.reviewerId,
        };
        console.log(
          `Submitting feedback for group ${op.groupId} in batch ${currentBatch.batchId}`
        );
        await feedbackPoster(op.groupId, payload);
        currentBatch.processedOperations.add(op.groupId);
        currentBatch.failedOperations.delete(op.groupId);
        console.log(`Success for group ${op.groupId}`);
      } catch (error) {
        console.error(
          `Failed for group ${op.groupId} (mode: ${currentBatch.mode}) in batch ${currentBatch.batchId}:`,
          error
        );
        currentBatch.failedOperations.add(op.groupId);
        currentBatch.error = `Group ${op.groupId.substring(0, 8)}...: ${
          (error as Error).message
        }`;
        batchOverallSuccess = false;
      }
    }

    setReviewQueue((prevQ) =>
      prevQ.map((b) => (b.batchId === currentBatch.batchId ? currentBatch : b))
    );

    if (!batchOverallSuccess) {
      console.warn(
        `Batch ${currentBatch.batchId} errors. Attempt ${currentBatch.attempt}/${MAX_REVIEW_ATTEMPTS}.`
      );
      if (currentBatch.attempt >= MAX_REVIEW_ATTEMPTS) {
        currentBatch.isTerminalFailure = true;
        setReviewQueue((prevQ) =>
          prevQ.map((b) =>
            b.batchId === currentBatch.batchId
              ? { ...currentBatch, isTerminalFailure: true }
              : b
          )
        );
        toast({
          title: "Review Submission Failed Permanently",
          description: (
            <>
              Failed: connection {currentBatch.edgeId.substring(0, 8)}... (
              {currentBatch.mode}). Error: {currentBatch.error}
              <Button
                variant="link"
                className="p-0 h-auto ml-2 text-destructive-foreground underline"
                onClick={() => setSelectedEdgeIdState(currentBatch.edgeId)}
              >
                View
              </Button>
              <Button
                variant="link"
                className="p-0 h-auto ml-2 text-destructive-foreground underline"
                onClick={() => retryFailedBatch(currentBatch.batchId)}
              >
                Retry
              </Button>
            </>
          ),
          variant: "destructive",
          duration: 10000,
        });
        console.error(`Batch ${currentBatch.batchId} failed permanently.`);

        // Revert optimistic updates
        setVisualizationData((prevVizData) => {
          const newVizDataState = { ...prevVizData };
          const targetClusterVizState = newVizDataState[currentBatch.clusterId];
          if (targetClusterVizState?.data) {
            const targetClusterViz = JSON.parse(
              JSON.stringify(targetClusterVizState.data)
            ) as EntityVisualizationDataResponse;
            const linkIndex = targetClusterViz.links.findIndex(
              (l) => l.id === currentBatch.edgeId
            );
            if (linkIndex !== -1) {
              targetClusterViz.links[linkIndex].status =
                currentBatch.originalEdgeStatus;
            }
            if (
              targetClusterViz.groups &&
              Array.isArray(targetClusterViz.groups)
            ) {
              (targetClusterViz.groups as EntityGroup[]).forEach((group) => {
                const op = currentBatch.operations.find(
                  (o) => o.groupId === group.id
                );
                if (op) group.confirmedStatus = op.originalGroupStatus;
              });
            }
            newVizDataState[currentBatch.clusterId] = {
              ...targetClusterVizState,
              data: targetClusterViz,
              lastUpdated: Date.now(),
            };
          }
          return newVizDataState;
        });

        setConnectionData((prevConnData) => {
          const newConnDataState = { ...prevConnData };
          const targetConnState = newConnDataState[currentBatch.edgeId];
          if (targetConnState?.data) {
            let targetConn = JSON.parse(
              JSON.stringify(targetConnState.data)
            ) as EntityConnectionDataResponse;
            // Revert logic is now unified
            targetConn.edge.status = currentBatch.originalEdgeStatus;
            targetConn.edge.confirmedStatus =
              currentBatch.originalEdgeStatus as string | null;
            targetConn.entityGroups.forEach((group) => {
              const op = currentBatch.operations.find(
                (o) => o.groupId === group.id
              );
              if (op) group.confirmedStatus = op.originalGroupStatus;
            });
            newConnDataState[currentBatch.edgeId] = {
              ...targetConnState,
              data: targetConn,
              lastUpdated: Date.now(),
            };
          }
          return newConnDataState;
        });
      } else {
        toast({
          title: "Review Submission Issue",
          description: `Attempt ${
            currentBatch.attempt
          }/${MAX_REVIEW_ATTEMPTS} failed for ${currentBatch.edgeId.substring(
            0,
            8
          )}... Will retry. Error: ${currentBatch.error}`,
          variant: "default",
          duration: 5000,
        });
      }
    } else {
      console.log(`Batch ${currentBatch.batchId} processed successfully.`);
      setReviewQueue((prevQ) =>
        prevQ.filter((b) => b.batchId !== currentBatch.batchId)
      );
      if (currentBatch.clusterId) {
        checkAndAdvanceIfComplete(currentBatch.clusterId);
      }
    }
    processingBatchIdRef.current = null;
    setIsProcessingQueue(false);
  }, [
    reviewQueue,
    isProcessingQueue,
    toast,
    checkAndAdvanceIfComplete,
    retryFailedBatch,
  ]);

  const currentConnectionData =
    useMemo((): EntityConnectionDataResponse | null => {
      if (!selectedEdgeId) return null;
      return connectionData[selectedEdgeId]?.data || null;
    }, [selectedEdgeId, connectionData]);

  const selectedClusterDetails = useMemo((): EntityCluster | null => {
    if (!selectedClusterId) return null;
    return clusters.data.find((c) => c.id === selectedClusterId) || null;
  }, [selectedClusterId, clusters.data]);

  const edgeSelectionInfo = useMemo((): EdgeSelectionInfo => {
    const currentVizForView = currentVisualizationDataForSelection;
    const fullVizForStats = selectedClusterId
      ? visualizationData[selectedClusterId]?.data
      : null;

    const defaultEdgeInfo: EdgeSelectionInfo = {
      currentEdgeId: selectedEdgeId,
      nextUnreviewedEdgeId: null,
      hasUnreviewedEdges:
        fullVizForStats?.links?.some(
          (link) => !queries.isEdgeReviewed(link.id)
        ) || false,
      currentEdgeIndex: -1,
      totalEdges: 0,
      totalUnreviewedEdgesInCluster:
        fullVizForStats?.links?.filter(
          (link) => !queries.isEdgeReviewed(link.id)
        ).length || 0,
      currentUnreviewedEdgeIndexInCluster: -1,
      totalEdgesInEntireCluster: fullVizForStats?.links?.length || 0,
    };

    if (!currentVizForView?.links || currentVizForView.links.length === 0) {
      if (selectedEdgeId && fullVizForStats?.links) {
        const allUnreviewedLinksInCluster = fullVizForStats.links.filter(
          (link) => !queries.isEdgeReviewed(link.id)
        );
        defaultEdgeInfo.currentUnreviewedEdgeIndexInCluster =
          allUnreviewedLinksInCluster.findIndex((l) => l.id === selectedEdgeId);
      }
      return defaultEdgeInfo;
    }

    const { links: linksInView } = currentVizForView;
    defaultEdgeInfo.totalEdges = linksInView.length;
    if (selectedEdgeId) {
      defaultEdgeInfo.currentEdgeIndex = linksInView.findIndex(
        (link) => link.id === selectedEdgeId
      );
    }

    const allLinksInCluster = fullVizForStats?.links || [];
    const allUnreviewedLinksInCluster = allLinksInCluster.filter(
      (link) => !queries.isEdgeReviewed(link.id)
    );

    if (selectedEdgeId) {
      defaultEdgeInfo.currentUnreviewedEdgeIndexInCluster =
        allUnreviewedLinksInCluster.findIndex((l) => l.id === selectedEdgeId);
    }

    let nextUnreviewedEdgeIdInCluster: string | null = null;
    if (allUnreviewedLinksInCluster.length > 0) {
      if (
        defaultEdgeInfo.currentUnreviewedEdgeIndexInCluster !== -1 &&
        defaultEdgeInfo.currentUnreviewedEdgeIndexInCluster <
          allUnreviewedLinksInCluster.length - 1
      ) {
        nextUnreviewedEdgeIdInCluster =
          allUnreviewedLinksInCluster[
            defaultEdgeInfo.currentUnreviewedEdgeIndexInCluster + 1
          ].id;
      } else if (
        allUnreviewedLinksInCluster.length > 0 &&
        (defaultEdgeInfo.currentUnreviewedEdgeIndexInCluster === -1 ||
          defaultEdgeInfo.currentUnreviewedEdgeIndexInCluster ===
            allUnreviewedLinksInCluster.length - 1)
      ) {
        let searchStartIndexFull = 0;
        if (selectedEdgeId) {
          const currentSelectedFullIndex = allLinksInCluster.findIndex(
            (l) => l.id === selectedEdgeId
          );
          if (currentSelectedFullIndex !== -1)
            searchStartIndexFull = currentSelectedFullIndex + 1;
        }
        for (let i = 0; i < allLinksInCluster.length; i++) {
          const linkToTest =
            allLinksInCluster[
              (searchStartIndexFull + i) % allLinksInCluster.length
            ];
          if (!queries.isEdgeReviewed(linkToTest.id)) {
            nextUnreviewedEdgeIdInCluster = linkToTest.id;
            break;
          }
        }
      }
    }
    defaultEdgeInfo.nextUnreviewedEdgeId = nextUnreviewedEdgeIdInCluster;

    return defaultEdgeInfo;
  }, [
    currentVisualizationDataForSelection,
    selectedEdgeId,
    lastReviewedEdgeId,
    selectedClusterId,
    visualizationData,
    queries,
  ]);

  useEffect(() => {
    if (clusters.data.length === 0 && !clusters.loading && !clusters.error) {
      loadClusters(1, clusters.limit);
    }
  }, [
    resolutionMode,
    clusters.data.length,
    clusters.loading,
    loadClusters,
    clusters.limit,
    clusters.error,
  ]);

  useEffect(() => {
    if (!selectedClusterId && clusters.data.length > 0 && !clusters.loading) {
      const firstNonSplitUncompletedCluster = clusters.data.find(
        (c) => !c.wasSplit && !queries.getClusterProgress(c.id).isComplete
      );
      if (firstNonSplitUncompletedCluster) {
        console.log(
          "Auto-selecting first non-split, uncompleted cluster:",
          firstNonSplitUncompletedCluster.id
        );
        handleSetSelectedClusterId(firstNonSplitUncompletedCluster.id);
      } else if (clusters.data.length > 0) {
        const firstCluster = clusters.data[0];
        console.log(
          "All clusters on current page are split or complete. Auto-selecting first for viewing:",
          firstCluster.id
        );
        handleSetSelectedClusterId(firstCluster.id);
      }
    }
  }, [
    selectedClusterId,
    clusters.data,
    clusters.loading,
    queries,
    handleSetSelectedClusterId,
  ]);

  const actions = useMemo(
    () => ({
      setResolutionMode,
      setSelectedClusterId: handleSetSelectedClusterId,
      setSelectedEdgeId: setSelectedEdgeIdAction,
      setReviewerId,
      setLastReviewedEdgeId,
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
      retryFailedBatch,
      setIsAutoAdvanceEnabled: setIsAutoAdvanceEnabledState,
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
      retryFailedBatch,
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

  useEffect(() => {
    if (reviewQueue.length > 0 && !isProcessingQueue && processReviewQueue) {
      const needsProcessing = reviewQueue.some(
        (b) =>
          !b.isTerminalFailure &&
          (b.failedOperations.size === 0 || b.attempt < MAX_REVIEW_ATTEMPTS)
      );
      if (needsProcessing) {
        const timeoutId = setTimeout(processReviewQueue, 500);
        return () => clearTimeout(timeoutId);
      }
    }
  }, [reviewQueue, isProcessingQueue, processReviewQueue]);

  const contextValue: EntityResolutionContextType = {
    resolutionMode,
    selectedClusterId,
    selectedEdgeId,
    reviewerId,
    lastReviewedEdgeId,
    refreshTrigger,
    isAutoAdvanceEnabled,
    clusters,
    visualizationData,
    connectionData,
    nodeDetails,
    reviewQueue,
    isProcessingQueue,
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

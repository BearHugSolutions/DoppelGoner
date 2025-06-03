// context/entity-resolution-context.tsx
"use client";

import {
  ResolutionMode,
  EntityClustersResponse,
  EntityCluster,
  EntityVisualizationDataResponse,
  EntityConnectionDataResponse,
  EntityGroup,
  EntityLink,
  VisualizationEntityEdge,
  EntityGroupReviewApiPayload,
  ServiceClustersResponse,
  ServiceCluster,
  ServiceVisualizationDataResponse,
  ServiceConnectionDataResponse,
  ServiceGroup,
  ServiceLink,
  VisualizationServiceEdge,
  ServiceGroupReviewApiPayload,
  GroupReviewDecision,
  ClusterFinalizationStatusResponse,
  QueuedReviewBatch,
  BaseCluster,
  BaseLink,
  BaseNode,
  NodeDetailResponse,
  NodeIdentifier, // New import
} from "@/types/entity-resolution";
import {
  getEntityClusters,
  getEntityVisualizationData,
  getEntityConnectionData,
  postEntityGroupFeedback,
  triggerEntityClusterFinalization,
  getServiceClusters,
  getServiceVisualizationData,
  getServiceConnectionData,
  postServiceGroupFeedback,
  triggerServiceClusterFinalization,
  getNodeDetails, // Kept for potential single refresh, but primary load is bulk
  getBulkNodeDetails, // New import
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

interface ClustersState<TCluster extends BaseCluster> {
  data: TCluster[];
  total: number;
  page: number;
  limit: number;
  loading: boolean;
  error: string | null;
}

interface VisualizationState<TVisData> {
  data: TVisData | null;
  loading: boolean;
  error: string | null;
  lastUpdated: number | null;
}

interface ConnectionState<TConnData> {
  data: TConnData | null;
  loading: boolean;
  error: string | null;
  lastUpdated: number | null;
}

interface ClusterProgress {
  clusterId: string;
  totalEdges: number;
  reviewedEdges: number;
  pendingEdges: number;
  confirmedMatches: number;
  confirmedNonMatches: number;
  progressPercentage: number;
  isComplete: boolean;
}

interface EdgeSelectionInfo {
  currentEdgeId: string | null;
  nextUnreviewedEdgeId: string | null;
  hasUnreviewedEdges: boolean;
  currentEdgeIndex: number;
  totalEdges: number;
  totalUnreviewedEdgesInCluster: number;
  currentUnreviewedEdgeIndexInCluster: number;
}

export interface EntityResolutionContextType {
  resolutionMode: ResolutionMode;
  selectedClusterId: string | null;
  selectedEdgeId: string | null;
  reviewerId: string;
  lastReviewedEdgeId: string | null;
  refreshTrigger: number;
  isAutoAdvanceEnabled: boolean;

  clusters: ClustersState<EntityCluster | ServiceCluster>;
  visualizationData: Record<
    string,
    VisualizationState<
      EntityVisualizationDataResponse | ServiceVisualizationDataResponse
    >
  >;
  connectionData: Record<
    string,
    ConnectionState<
      EntityConnectionDataResponse | ServiceConnectionDataResponse
    >
  >;
  nodeDetails: Record<string, NodeDetailResponse | null | "loading" | "error">; // Added 'error' state

  reviewQueue: QueuedReviewBatch[];
  isProcessingQueue: boolean;

  clusterProgress: Record<string, ClusterProgress>;
  edgeSelectionInfo: EdgeSelectionInfo;

  currentVisualizationData:
    | EntityVisualizationDataResponse
    | ServiceVisualizationDataResponse
    | null;
  currentConnectionData:
    | EntityConnectionDataResponse
    | ServiceConnectionDataResponse
    | null;
  selectedClusterDetails: EntityCluster | ServiceCluster | null;

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
    // loadNodeDetails: (nodeId: string, nodeType: ResolutionMode) => Promise<void>; // Deprecated in favor of bulk
    loadBulkNodeDetails: (nodesToFetch: NodeIdentifier[]) => Promise<void>; // New action
    loadConnectionData: (
      edgeId: string
    ) => Promise<
      EntityConnectionDataResponse | ServiceConnectionDataResponse | null
    >;
    invalidateVisualizationData: (clusterId: string) => void;
    invalidateConnectionData: (edgeId: string) => void;
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
  };

  queries: {
    isVisualizationDataLoaded: (clusterId: string) => boolean;
    isVisualizationDataLoading: (clusterId: string) => boolean;
    isConnectionDataLoaded: (edgeId: string) => boolean;
    isConnectionDataLoading: (edgeId: string) => boolean;
    getVisualizationError: (clusterId: string) => string | null;
    getConnectionError: (edgeId: string) => string | null;
    getClusterProgress: (clusterId: string) => ClusterProgress;
    canAdvanceToNextCluster: () => boolean;
    isEdgeReviewed: (edgeId: string) => boolean;
    getEdgeStatus: (edgeId: string) => BaseLink["status"] | null;
    getQueueItemStatus: (
      edgeId: string
    ) => "pending" | "processing" | "failed" | null;
    getClusterById: (
      clusterId: string
    ) => EntityCluster | ServiceCluster | undefined;
    getNodeDetail: (
      nodeId: string
    ) => NodeDetailResponse | null | "loading" | "error";
  };
}

const EntityResolutionContext = createContext<
  EntityResolutionContextType | undefined
>(undefined);

const MAX_REVIEW_ATTEMPTS = 3;

const initialClustersState: ClustersState<EntityCluster | ServiceCluster> = {
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

  const [clusters, setClusters] =
    useState<ClustersState<EntityCluster | ServiceCluster>>(
      initialClustersState
    );
  const [visualizationData, setVisualizationData] = useState<
    Record<
      string,
      VisualizationState<
        EntityVisualizationDataResponse | ServiceVisualizationDataResponse
      >
    >
  >({});
  const [connectionData, setConnectionData] = useState<
    Record<
      string,
      ConnectionState<
        EntityConnectionDataResponse | ServiceConnectionDataResponse
      >
    >
  >({});
  const [nodeDetails, setNodeDetails] = useState<
    Record<string, NodeDetailResponse | null | "loading" | "error">
  >({});

  const [reviewQueue, setReviewQueue] = useState<QueuedReviewBatch[]>([]);
  const [isProcessingQueue, setIsProcessingQueue] = useState<boolean>(false);
  const processingBatchIdRef = useRef<string | null>(null);

  const getClusterById = useCallback(
    (clusterId: string): EntityCluster | ServiceCluster | undefined => {
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
      getClusterProgress: (clusterIdToQuery: string): ClusterProgress => {
        const vizState = visualizationData[clusterIdToQuery];
        const clusterDetails = getClusterById(clusterIdToQuery);

        // If wasSplit is true, treat as 100% complete
        if (clusterDetails?.wasSplit) {
          return {
            clusterId: clusterIdToQuery,
            totalEdges: vizState?.data?.links?.length || 0, // Use actual link count if available
            reviewedEdges: vizState?.data?.links?.length || 0,
            pendingEdges: 0,
            confirmedMatches:
              vizState?.data?.links?.filter(
                (l) => l.status === "CONFIRMED_MATCH"
              ).length || 0,
            confirmedNonMatches:
              vizState?.data?.links?.filter(
                (l) => l.status === "CONFIRMED_NON_MATCH"
              ).length || 0,
            progressPercentage: 100,
            isComplete: true,
          };
        }

        if (!vizState?.data?.links)
          return {
            clusterId: clusterIdToQuery,
            totalEdges: 0,
            reviewedEdges: 0,
            pendingEdges: 0,
            confirmedMatches: 0,
            confirmedNonMatches: 0,
            progressPercentage: 0,
            isComplete: false,
          };
        const { links } = vizState.data;
        const totalEdges = links.length;
        if (totalEdges === 0)
          return {
            clusterId: clusterIdToQuery,
            totalEdges: 0,
            reviewedEdges: 0,
            pendingEdges: 0,
            confirmedMatches: 0,
            confirmedNonMatches: 0,
            progressPercentage: 100,
            isComplete: true,
          };

        const confirmedMatches = links.filter(
          (link) => link.status === "CONFIRMED_MATCH"
        ).length;
        const confirmedNonMatches = links.filter(
          (link) => link.status === "CONFIRMED_NON_MATCH"
        ).length;
        const reviewedEdges = confirmedMatches + confirmedNonMatches;
        const pendingEdges = totalEdges - reviewedEdges;
        const progressPercentage =
          totalEdges > 0 ? Math.round((reviewedEdges / totalEdges) * 100) : 0;
        return {
          clusterId: clusterIdToQuery,
          totalEdges,
          reviewedEdges,
          pendingEdges,
          confirmedMatches,
          confirmedNonMatches,
          progressPercentage,
          isComplete: pendingEdges === 0 && totalEdges > 0,
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
    },
    [resolutionMode]
  );

  const loadBulkNodeDetails = useCallback(
    async (nodesToFetch: NodeIdentifier[]) => {
      const trulyNeedsFetching = nodesToFetch.filter(
        (node) => !nodeDetails[node.id] || nodeDetails[node.id] === "error" // Fetch if not present, or if errored previously
      );

      if (trulyNeedsFetching.length === 0) {
        console.log(
          "BulkNodeDetails: All requested node details already loaded or loading."
        );
        return;
      }

      console.log(
        `BulkNodeDetails: Fetching details for ${trulyNeedsFetching.length} nodes.`
      );
      setNodeDetails((prev) => {
        const newState = { ...prev };
        trulyNeedsFetching.forEach((node) => {
          newState[node.id] = "loading";
        });
        return newState;
      });

      try {
        const response = await getBulkNodeDetails({
          items: trulyNeedsFetching,
        });
        console.log(`loadBulkNodeDetails: Successfully loaded details for ${response.length} nodes.`)
        setNodeDetails((prev) => {
          const newState = { ...prev };
          response.forEach((detail) => {
            newState[detail.id] = detail;
          });
          // For any nodes that were in trulyNeedsFetching but not in response (e.g., API error for specific node not caught by overall catch)
          // we might want to mark them as 'error' here, but getBulkNodeDetails should ideally return all or throw.
          // Assuming API returns successfully fetched items. If an item in request isn't in response, it implies an issue.
          trulyNeedsFetching.forEach((requestedNode) => {
            if (!response.find((r) => r.id === requestedNode.id)) {
              newState[requestedNode.id] = "error"; // Mark as error if not returned
              console.warn(
                `BulkNodeDetails: Node ${requestedNode.id} was requested but not found in response.`
              );
            }
          });
          return newState;
        });
        console.log(
          `BulkNodeDetails: Successfully loaded details for ${response.length} nodes.`
        );
      } catch (error) {
        console.error(
          "BulkNodeDetails: Error loading bulk node details:",
          error
        );
        setNodeDetails((prev) => {
          const newState = { ...prev };
          trulyNeedsFetching.forEach((node) => {
            newState[node.id] = "error"; // Mark all requested as error on bulk failure
          });
          return newState;
        });
        toast({
          title: "Bulk Node Details Error",
          description: `Could not load details for some nodes: ${
            (error as Error).message
          }`,
          variant: "destructive",
        });
      }
    },
    [nodeDetails, toast]
  );

  const loadClusters = useCallback(
    async (page: number, limit: number = 10) => {
      console.log(
        `Loading clusters for mode: ${resolutionMode}, page: ${page}, limit: ${limit}`
      );
      setClusters((prev) => ({
        ...prev,
        loading: true,
        error: null,
        page,
        limit,
      }));
      const fetcher =
        resolutionMode === "entity" ? getEntityClusters : getServiceClusters;

      try {
        const response = await fetcher(page, limit);
        console.log(
          `Successfully loaded clusters for mode ${resolutionMode}`,
          response
        );
        setClusters({
          data: response.clusters as Array<EntityCluster | ServiceCluster>,
          total: response.total,
          page,
          limit,
          loading: false,
          error: null,
        });

        const allNodeIdentifiersToFetch: NodeIdentifier[] = [];
        const visualizationPromises = response.clusters.map(async (cluster) => {
          try {
            // Skip fetching viz data for already split clusters if not needed for node ID collection
            // However, even split clusters might have nodes whose details are useful.
            // For now, always fetch viz to get node IDs.
            // if (cluster.wasSplit) {
            //   console.log(`Cluster ${cluster.id} is split, skipping viz pre-fetch for node collection.`);
            //   setVisualizationData((prev) => ({
            //     ...prev,
            //     [cluster.id]: { data: { clusterId: cluster.id, nodes: [], links: [], groups: [] } as any, loading: false, error: null, lastUpdated: Date.now() },
            //   }));
            //   return;
            // }

            const getVizData =
              resolutionMode === "entity"
                ? getEntityVisualizationData
                : getServiceVisualizationData;
            // Check if viz data is already loading or loaded to prevent re-fetch
            if (
              visualizationData[cluster.id]?.data ||
              visualizationData[cluster.id]?.loading
            ) {
              if (visualizationData[cluster.id]?.data?.nodes) {
                visualizationData[cluster.id]?.data?.nodes.forEach((node) => {
                  allNodeIdentifiersToFetch.push({
                    id: node.id,
                    nodeType: resolutionMode,
                  });
                });
              }
              return;
            }

            setVisualizationData((prev) => ({
              ...prev,
              [cluster.id]: {
                data: null,
                loading: true,
                error: null,
                lastUpdated: null,
              },
            }));
            const vizData = await getVizData(cluster.id);
            setVisualizationData((prev) => ({
              ...prev,
              [cluster.id]: {
                data: vizData,
                loading: false,
                error: null,
                lastUpdated: Date.now(),
              },
            }));
            vizData.nodes.forEach((node) => {
              allNodeIdentifiersToFetch.push({
                id: node.id,
                nodeType: resolutionMode,
              });
            });
          } catch (error) {
            console.error(
              `Error preloading viz data for cluster ${cluster.id}:`,
              error
            );
            setVisualizationData((prev) => ({
              ...prev,
              [cluster.id]: {
                data: null,
                loading: false,
                error: (error as Error).message,
                lastUpdated: null,
              },
            }));
          }
        });

        await Promise.all(visualizationPromises);

        const uniqueNodeIdentifiers = Array.from(
          new Map(
            allNodeIdentifiersToFetch.map((item) => [
              `${item.id}-${item.nodeType}`,
              item,
            ])
          ).values()
        );
        if (uniqueNodeIdentifiers.length > 0) {
          loadBulkNodeDetails(uniqueNodeIdentifiers);
        }
      } catch (error) {
        console.error(
          `Error loading clusters for mode ${resolutionMode}:`,
          error
        );
        setClusters((prev) => ({
          ...prev,
          loading: false,
          error: (error as Error).message,
          data: [],
          total: 0,
          page: 1,
          limit: 10,
        }));
      }
    },
    [resolutionMode, loadBulkNodeDetails, visualizationData] // Added visualizationData
  );

  const handleSetSelectedClusterId = useCallback(
    async (id: string | null) => {
      // Made async
      const previousSelectedClusterId = selectedClusterId;
      console.log(
        `Setting selected cluster ID to: ${id}. Previous: ${previousSelectedClusterId}`
      );
      setSelectedClusterIdState(id);
      setSelectedEdgeIdState(null); // Reset edge on cluster change
      setLastReviewedEdgeId(null);

      if (id && id !== previousSelectedClusterId) {
        const clusterDetail = queries.getClusterById(id); // Use queries.getClusterById
        if (
          clusterDetail?.wasSplit ||
          queries.getClusterProgress(id).isComplete
        ) {
          console.log(
            `Cluster ${id} is already split or complete. Pausing auto-advance.`
          );
          setIsAutoAdvanceEnabledState(false);
        } else {
          console.log(
            `New, uncompleted, non-split cluster ${id} selected. Ensuring auto-advance is ON.`
          );
          setIsAutoAdvanceEnabledState(true);
        }
      }

      if (id) {
        const vizState = visualizationData[id];
        if ((!vizState?.data || vizState?.error) && !vizState?.loading) {
          console.log(
            `Visualization data for selected cluster ${id} not loaded or errored. Fetching...`
          );
          setVisualizationData((prev) => ({
            ...prev,
            [id]: { data: null, loading: true, error: null, lastUpdated: null },
          }));
          try {
            const getVizData =
              resolutionMode === "entity"
                ? getEntityVisualizationData
                : getServiceVisualizationData;
            const vizDataResponse = await getVizData(id);
            setVisualizationData((prev) => ({
              ...prev,
              [id]: {
                data: vizDataResponse,
                loading: false,
                error: null,
                lastUpdated: Date.now(),
              },
            }));
            const nodeIdsFromViz: NodeIdentifier[] = vizDataResponse.nodes.map(
              (node) => ({ id: node.id, nodeType: resolutionMode })
            );
            if (nodeIdsFromViz.length > 0) {
              loadBulkNodeDetails(nodeIdsFromViz);
            }
          } catch (error) {
            console.error(
              `Error loading viz data for selected cluster ${id}:`,
              error
            );
            setVisualizationData((prev) => ({
              ...prev,
              [id]: {
                data: null,
                loading: false,
                error: (error as Error).message,
                lastUpdated: null,
              },
            }));
          }
        } else if (vizState?.data?.nodes) {
          // Viz data already exists, ensure its nodes details are loaded/loading
          const nodeIdsFromViz: NodeIdentifier[] = vizState.data.nodes.map(
            (node) => ({ id: node.id, nodeType: resolutionMode })
          );
          if (nodeIdsFromViz.length > 0) {
            loadBulkNodeDetails(nodeIdsFromViz);
          }
        }
      }
    },
    [
      selectedClusterId,
      queries,
      visualizationData,
      resolutionMode,
      loadBulkNodeDetails,
      setIsAutoAdvanceEnabledState,
    ]
  );

  const loadConnectionData = useCallback(
    async (
      edgeId: string
    ): Promise<
      EntityConnectionDataResponse | ServiceConnectionDataResponse | null
    > => {
      const getConnData =
        resolutionMode === "entity"
          ? getEntityConnectionData
          : getServiceConnectionData;
      const cached = connectionData[edgeId];

      console.log(
        `Loading connection data for edge: ${edgeId}, mode: ${resolutionMode}`
      );
      if (
        cached?.data &&
        !cached.loading &&
        !cached.error &&
        cached.lastUpdated &&
        Date.now() - cached.lastUpdated < 300000
      ) {
        console.log(`Using cached connection data for edge ${edgeId}`);
        // Ensure nodes from cached connection data are processed for bulk loading if needed
        const nodesToLoad: NodeIdentifier[] = [];
        if (cached.data.entity1)
          nodesToLoad.push({
            id: cached.data.entity1.id,
            nodeType: resolutionMode,
          });
        if (cached.data.entity2)
          nodesToLoad.push({
            id: cached.data.entity2.id,
            nodeType: resolutionMode,
          });
        if (nodesToLoad.length > 0) loadBulkNodeDetails(nodesToLoad);
        return cached.data;
      }

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
        const data = await getConnData(edgeId);
        console.log(`Successfully loaded connection data for edge ${edgeId}`);
        setConnectionData((prev) => ({
          ...prev,
          [edgeId]: {
            data,
            loading: false,
            error: null,
            lastUpdated: Date.now(),
          },
        }));

        const nodesToLoad: NodeIdentifier[] = [];
        if (data.entity1)
          nodesToLoad.push({ id: data.entity1.id, nodeType: resolutionMode });
        if (data.entity2)
          nodesToLoad.push({ id: data.entity2.id, nodeType: resolutionMode });
        if (nodesToLoad.length > 0) loadBulkNodeDetails(nodesToLoad);

        return data;
      } catch (error) {
        console.error(
          `Error loading connection data for edge ${edgeId}:`,
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

  const invalidateVisualizationData = useCallback(
    // Now also triggers bulk node detail load
    async (clusterId: string) => {
      console.log(`Invalidating visualization data for cluster: ${clusterId}`);
      setVisualizationData((prev) => ({
        ...prev,
        [clusterId]: {
          data: null,
          loading: true,
          error: null,
          lastUpdated: null,
        },
      }));
      try {
        const getVizData =
          resolutionMode === "entity"
            ? getEntityVisualizationData
            : getServiceVisualizationData;
        const vizDataResponse = await getVizData(clusterId);
        setVisualizationData((prev) => ({
          ...prev,
          [clusterId]: {
            data: vizDataResponse,
            loading: false,
            error: null,
            lastUpdated: Date.now(),
          },
        }));
        const nodeIdsFromViz: NodeIdentifier[] = vizDataResponse.nodes.map(
          (node) => ({ id: node.id, nodeType: resolutionMode })
        );
        if (nodeIdsFromViz.length > 0) {
          loadBulkNodeDetails(nodeIdsFromViz);
        }
      } catch (error) {
        console.error(
          `Error reloading viz data for cluster ${clusterId}:`,
          error
        );
        setVisualizationData((prev) => ({
          ...prev,
          [clusterId]: {
            data: null,
            loading: false,
            error: (error as Error).message,
            lastUpdated: null,
          },
        }));
      }
    },
    [resolutionMode, loadBulkNodeDetails]
  );

  const invalidateConnectionData = useCallback(
    (edgeId: string) => {
      console.log(`Invalidating connection data for edge: ${edgeId}`);
      setConnectionData((prev) => ({
        ...prev,
        [edgeId]: {
          data: null,
          loading: false,
          error: `Invalidated by user action`,
          lastUpdated: null,
        },
      }));
      // Re-load if it's the currently selected edge
      if (selectedEdgeId === edgeId) {
        loadConnectionData(edgeId); // This will also trigger bulk node load for its entities
      }
    },
    [selectedEdgeId, loadConnectionData] // loadConnectionData will handle node details
  );

  const setSelectedEdgeIdAction = useCallback(
    (id: string | null) => {
      console.log(`Setting selected edge ID to: ${id}`);
      if (id && queries.isEdgeReviewed(id)) {
        console.log(`Edge ${id} is already reviewed. Pausing auto-advance.`);
        setIsAutoAdvanceEnabledState(false);
      }
      setSelectedEdgeIdState(id);
      // If a new edge is selected, its connection data (and thus node details)
      // will be loaded by the useEffect hook watching selectedEdgeId
    },
    [queries, setIsAutoAdvanceEnabledState]
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
        loadClusters(clusters.page, clusters.limit); // This will re-trigger viz and bulk node loads
      }
      if (
        selectedClusterId &&
        (target === "all" || target === "current_visualization")
      ) {
        invalidateVisualizationData(selectedClusterId); // This reloads viz and its nodes' details
      }
      if (
        selectedEdgeId &&
        (target === "all" || target === "current_connection")
      ) {
        invalidateConnectionData(selectedEdgeId); // This reloads connection and its nodes' details
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

    let nextClusterSelected = false;
    for (let i = currentIndex + 1; i < clusters.data.length; i++) {
      const nextClusterOnPage = clusters.data[i];
      // Check progress. If viz data for progress check isn't loaded, it might be fetched by setSelectedClusterId
      // This relies on setSelectedClusterId to load viz if needed.
      if (!queries.getClusterProgress(nextClusterOnPage.id).isComplete) {
        console.log(
          `Selecting next uncompleted cluster in current page: ${nextClusterOnPage.id}`
        );
        handleSetSelectedClusterId(nextClusterOnPage.id); // This will load viz & node details
        nextClusterSelected = true;
        break;
      }
    }

    if (!nextClusterSelected) {
      if (clusters.page < Math.ceil(clusters.total / clusters.limit)) {
        const nextPageToLoad = clusters.page + 1;
        console.log(`Loading next page (${nextPageToLoad}) of clusters.`);
        await loadClusters(nextPageToLoad, clusters.limit); // This will load new clusters, their viz, and then bulk node details
        // After loadClusters, a useEffect should pick the first uncompleted cluster from the new page.
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
    setIsAutoAdvanceEnabledState,
  ]);

  const checkAndAdvanceIfComplete = useCallback(
    async (clusterIdToCheck?: string) => {
      const targetClusterId = clusterIdToCheck || selectedClusterId;
      if (!targetClusterId) {
        console.log("No target cluster ID to check for completion.");
        return;
      }
      console.log(
        `Checking if cluster ${targetClusterId} is complete or split.`
      );

      const clusterDetail = queries.getClusterById(targetClusterId);
      const progress = queries.getClusterProgress(targetClusterId);

      console.log(
        `Cluster ${targetClusterId}: Total=${progress.totalEdges}, Reviewed=${progress.reviewedEdges}, Pending=${progress.pendingEdges}, Complete=${progress.isComplete}, WasSplit=${clusterDetail?.wasSplit}`
      );

      if (
        progress.isComplete &&
        !clusterDetail?.wasSplit &&
        progress.totalEdges > 0
      ) {
        toast({
          title: "Cluster Review Complete",
          description: `Review of cluster ${targetClusterId.substring(
            0,
            8
          )}... is complete.`,
        });
        const finalizer =
          resolutionMode === "entity"
            ? triggerEntityClusterFinalization
            : triggerServiceClusterFinalization;
        try {
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

          // Reload current page of clusters to reflect changes (e.g., wasSplit update)
          // This will also re-trigger bulk node detail loading for the page.
          loadClusters(clusters.page, clusters.limit);

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
        }
      } else if (progress.isComplete || clusterDetail?.wasSplit) {
        // Use progress.isComplete which accounts for wasSplit via getClusterProgress
        if (targetClusterId === selectedClusterId && isAutoAdvanceEnabled) {
          console.log(
            `Cluster ${targetClusterId} is already split or is an empty/completed cluster. Auto-advance ON. Advancing.`
          );
          advanceToNextCluster();
        } else if (progress.isComplete || clusterDetail?.wasSplit) {
          console.log(
            `Cluster ${targetClusterId} is already split or empty/completed. Auto-advance OFF or not selected. Not advancing.`
          );
        }
      }
    },
    [
      selectedClusterId,
      queries,
      advanceToNextCluster,
      toast,
      loadClusters,
      clusters.page,
      clusters.limit,
      resolutionMode,
      isAutoAdvanceEnabled,
    ]
  );

  const selectNextUnreviewedEdge = useCallback(
    (afterEdgeId?: string | null) => {
      setIsAutoAdvanceEnabledState(true); // Explicitly enable on manual next edge selection
      const currentClusterId = selectedClusterId;
      if (!currentClusterId) {
        console.warn("No cluster selected for selecting next edge.");
        return;
      }

      const clusterDetail = queries.getClusterById(currentClusterId);
      if (clusterDetail?.wasSplit) {
        // Check wasSplit directly
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
      const currentViz = visualizationData[currentClusterId]?.data;
      if (!currentViz?.links || currentViz.links.length === 0) {
        console.log(
          `No links in viz for cluster ${currentClusterId}. Checking advance.`
        );
        checkAndAdvanceIfComplete(currentClusterId);
        return;
      }

      const { links } = currentViz;
      let startIdx = 0;
      const referenceEdgeId =
        afterEdgeId || lastReviewedEdgeId || selectedEdgeId;

      if (referenceEdgeId) {
        const idx = links.findIndex((l) => l.id === referenceEdgeId);
        if (idx !== -1) startIdx = idx + 1;
        else
          console.warn(
            `Ref edge ${referenceEdgeId} not found. Starting from beginning.`
          );
      }

      for (let i = 0; i < links.length; i++) {
        const link = links[(startIdx + i) % links.length];
        if (link.status === "PENDING_REVIEW") {
          console.log(`Next unreviewed edge found: ${link.id}. Selecting.`);
          setSelectedEdgeIdState(link.id); // This will trigger connection data load via useEffect
          return;
        }
      }

      console.log(
        `No more unreviewed edges in cluster ${currentClusterId}. Checking advance.`
      );
      toast({
        title: "Cluster Review Status",
        description: `All connections in cluster ${currentClusterId.substring(
          0,
          8
        )}... reviewed or processing.`,
      });
      setSelectedEdgeIdState(null);
      checkAndAdvanceIfComplete(currentClusterId);
    },
    [
      selectedClusterId,
      selectedEdgeId,
      lastReviewedEdgeId,
      visualizationData,
      toast,
      checkAndAdvanceIfComplete,
      setIsAutoAdvanceEnabledState,
      queries,
    ]
  );

  const selectNextUnreviewedInCluster = useCallback(() => {
    setIsAutoAdvanceEnabledState(true);
    const currentClusterId = selectedClusterId;
    if (!currentClusterId) return;

    const clusterDetail = queries.getClusterById(currentClusterId);
    if (clusterDetail?.wasSplit) {
      checkAndAdvanceIfComplete(currentClusterId);
      return;
    }

    const currentViz = visualizationData[currentClusterId]?.data;
    if (!currentViz?.links || currentViz.links.length === 0) {
      checkAndAdvanceIfComplete(currentClusterId);
      return;
    }

    const unreviewedLinks = currentViz.links.filter(
      (l) => l.status === "PENDING_REVIEW"
    );
    if (unreviewedLinks.length === 0) {
      toast({
        title: "Info",
        description: "No unreviewed connections in this cluster.",
      });
      checkAndAdvanceIfComplete(currentClusterId);
      return;
    }

    let nextEdgeToSelectId: string | null = null;
    if (selectedEdgeId) {
      const currentIndexInUnreviewed = unreviewedLinks.findIndex(
        (l) => l.id === selectedEdgeId
      );
      if (currentIndexInUnreviewed !== -1) {
        nextEdgeToSelectId =
          unreviewedLinks[
            (currentIndexInUnreviewed + 1) % unreviewedLinks.length
          ].id;
      } else {
        nextEdgeToSelectId = unreviewedLinks[0].id;
      }
    } else {
      nextEdgeToSelectId = unreviewedLinks[0].id;
    }

    if (nextEdgeToSelectId) {
      setSelectedEdgeIdState(nextEdgeToSelectId);
    }
  }, [
    selectedClusterId,
    selectedEdgeId,
    visualizationData,
    queries,
    checkAndAdvanceIfComplete,
    toast,
    setIsAutoAdvanceEnabledState,
  ]);

  const selectPreviousUnreviewedInCluster = useCallback(() => {
    setIsAutoAdvanceEnabledState(true);
    const currentClusterId = selectedClusterId;
    if (!currentClusterId) return;

    const clusterDetail = queries.getClusterById(currentClusterId);
    if (clusterDetail?.wasSplit) {
      checkAndAdvanceIfComplete(currentClusterId);
      return;
    }

    const currentViz = visualizationData[currentClusterId]?.data;
    if (!currentViz?.links || currentViz.links.length === 0) {
      checkAndAdvanceIfComplete(currentClusterId);
      return;
    }

    const unreviewedLinks = currentViz.links.filter(
      (l) => l.status === "PENDING_REVIEW"
    );
    if (unreviewedLinks.length === 0) {
      toast({
        title: "Info",
        description: "No unreviewed connections in this cluster.",
      });
      checkAndAdvanceIfComplete(currentClusterId);
      return;
    }

    let prevEdgeToSelectId: string | null = null;
    if (selectedEdgeId) {
      const currentIndexInUnreviewed = unreviewedLinks.findIndex(
        (l) => l.id === selectedEdgeId
      );
      if (currentIndexInUnreviewed !== -1) {
        prevEdgeToSelectId =
          unreviewedLinks[
            (currentIndexInUnreviewed - 1 + unreviewedLinks.length) %
              unreviewedLinks.length
          ].id;
      } else {
        prevEdgeToSelectId = unreviewedLinks[unreviewedLinks.length - 1].id;
      }
    } else {
      prevEdgeToSelectId = unreviewedLinks[unreviewedLinks.length - 1].id;
    }

    if (prevEdgeToSelectId) {
      setSelectedEdgeIdState(prevEdgeToSelectId);
    }
  }, [
    selectedClusterId,
    selectedEdgeId,
    visualizationData,
    queries,
    checkAndAdvanceIfComplete,
    toast,
    setIsAutoAdvanceEnabledState,
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
      const connData = connectionData[edgeId]?.data;
      const vizData = visualizationData[selectedClusterId]?.data;

      if (!connData || !vizData) {
        toast({
          title: "Data Error",
          description: "Data not loaded for review.",
          variant: "destructive",
        });
        if (!connData) loadConnectionData(edgeId); // This will also trigger node detail load
        if (!vizData && selectedClusterId)
          invalidateVisualizationData(selectedClusterId); // This will reload viz and node details
        return;
      }

      let relevantGroups: Array<EntityGroup | ServiceGroup> = [];
      let currentEdgeLink: EntityLink | ServiceLink | undefined;

      if (
        resolutionMode === "entity" &&
        "entityGroups" in connData &&
        "links" in vizData
      ) {
        const entityConnData = connData as EntityConnectionDataResponse;
        const entityVizData = vizData as EntityVisualizationDataResponse; // vizData.links are generic EntityLink
        relevantGroups = entityConnData.entityGroups.filter(
          (group) =>
            (group.entityId1 === entityConnData.edge.entityId1 &&
              group.entityId2 === entityConnData.edge.entityId2) ||
            (group.entityId1 === entityConnData.edge.entityId2 &&
              group.entityId2 === entityConnData.edge.entityId1)
        );
        currentEdgeLink = entityVizData.links.find((l) => l.id === edgeId);
      } else if (
        resolutionMode === "service" &&
        "entityGroups" in connData &&
        "links" in vizData
      ) {
        const serviceConnData = connData as ServiceConnectionDataResponse;
        const serviceVizData = vizData as ServiceVisualizationDataResponse; // vizData.links are generic ServiceLink
        relevantGroups = (
          serviceConnData.entityGroups as ServiceGroup[]
        ).filter(
          (group) =>
            (group.serviceId1 === serviceConnData.edge.serviceId1 &&
              group.serviceId2 === serviceConnData.edge.serviceId2) ||
            (group.serviceId1 === serviceConnData.edge.serviceId2 &&
              group.serviceId2 === serviceConnData.edge.serviceId1)
        );
        currentEdgeLink = serviceVizData.links.find((l) => l.id === edgeId);
      }

      if (relevantGroups.length === 0) {
        toast({
          title: "Info",
          description: "No underlying groups. Marking edge as reviewed.",
        });
        setVisualizationData((prev) => {
          /* ... optimistic update for edge status ... */
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
          "Critical: Current edge link not found for optimistic update."
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

      // Optimistic UI updates (visualization and connection data)
      setVisualizationData((prevVizData) => {
        const newVizDataState = { ...prevVizData };
        const targetClusterVizState = newVizDataState[batch.clusterId];
        if (targetClusterVizState?.data) {
          const targetClusterViz = { ...targetClusterVizState.data }; // shallow copy
          // Update link status
          const linkIndex = targetClusterViz.links.findIndex(
            (l) => l.id === batch.edgeId
          );
          if (linkIndex !== -1) {
            targetClusterViz.links = [...targetClusterViz.links]; // deep copy links array
            targetClusterViz.links[linkIndex] = {
              ...targetClusterViz.links[linkIndex],
              status: batch.optimisticEdgeStatus,
            };
          }
          // Update group status if groups are part of viz data (they are in VisualizationResponse)
          if (
            targetClusterViz.groups &&
            Array.isArray(targetClusterViz.groups)
          ) {
            targetClusterViz.groups = (
              targetClusterViz.groups as Array<EntityGroup | ServiceGroup>
            ).map((group) =>
              batch.operations.some((op) => op.groupId === group.id)
                ? { ...group, confirmedStatus: batch.optimisticGroupStatus }
                : group
            );
          }
          newVizDataState[batch.clusterId] = {
            ...targetClusterVizState,
            data: targetClusterViz,
            lastUpdated: Date.now(),
          };
        }
        return newVizDataState;
      });

      setConnectionData((prevConnData) => {
        const newConnDataState = { ...prevConnData };
        const targetConnState = newConnDataState[batch.edgeId];
        if (targetConnState?.data) {
          const targetConn = { ...targetConnState.data }; // shallow copy
          // Update edge status in connection data
          if ("edge" in targetConn && targetConn.edge) {
            (
              targetConn.edge as
                | VisualizationEntityEdge
                | VisualizationServiceEdge
            ).status = batch.optimisticEdgeStatus;
            // If your edge model in connection data also has confirmedStatus (like VisualizationEntityEdge does)
            if ("confirmedStatus" in targetConn.edge) {
              (targetConn.edge as VisualizationEntityEdge).confirmedStatus =
                batch.optimisticGroupStatus;
            }
          }
          // Update group status in connection data
          if (
            "entityGroups" in targetConn &&
            Array.isArray(targetConn.entityGroups)
          ) {
            targetConn.entityGroups = (
              targetConn.entityGroups as EntityGroup[]
            ).map((group) =>
              batch.operations.some((op) => op.groupId === group.id)
                ? { ...group, confirmedStatus: batch.optimisticGroupStatus }
                : group
            ) as unknown as typeof targetConn.entityGroups;
          }
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
      loadConnectionData,
      invalidateVisualizationData,
      isAutoAdvanceEnabled,
      selectNextUnreviewedEdge,
      checkAndAdvanceIfComplete,
      queries,
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
      failedOperations: new Set(batchToProcess.failedOperations),
    };
    let batchOverallSuccess = true;
    const stillPendingOperations = currentBatch.operations.filter(
      (op) => !currentBatch.processedOperations.has(op.groupId)
    );
    const feedbackPoster =
      currentBatch.mode === "entity"
        ? postEntityGroupFeedback
        : postServiceGroupFeedback;

    for (const op of stillPendingOperations) {
      try {
        const payload:
          | EntityGroupReviewApiPayload
          | ServiceGroupReviewApiPayload = {
          decision: currentBatch.decision,
          reviewerId: currentBatch.reviewerId,
        };
        console.log(
          `Submitting feedback for group ${op.groupId} in batch ${currentBatch.batchId}`
        );
        await feedbackPoster(op.groupId, payload);
        currentBatch.processedOperations.add(op.groupId);
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

    if (!batchOverallSuccess) {
      console.warn(
        `Batch ${currentBatch.batchId} errors. Attempt ${currentBatch.attempt}/${MAX_REVIEW_ATTEMPTS}.`
      );
      if (currentBatch.attempt >= MAX_REVIEW_ATTEMPTS) {
        currentBatch.isTerminalFailure = true;
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
          /* ... revert logic ... */
          const newVizDataState = { ...prevVizData };
          const targetClusterVizState = newVizDataState[currentBatch.clusterId];
          if (targetClusterVizState?.data) {
            const targetClusterViz = { ...targetClusterVizState.data };
            const linkIndex = targetClusterViz.links.findIndex(
              (l) => l.id === currentBatch.edgeId
            );
            if (linkIndex !== -1) {
              targetClusterViz.links = [...targetClusterViz.links];
              targetClusterViz.links[linkIndex] = {
                ...targetClusterViz.links[linkIndex],
                status: currentBatch.originalEdgeStatus,
              };
            }
            if (
              targetClusterViz.groups &&
              Array.isArray(targetClusterViz.groups)
            ) {
              targetClusterViz.groups = (
                targetClusterViz.groups as Array<EntityGroup | ServiceGroup>
              ).map((group) => {
                const op = currentBatch.operations.find(
                  (o) => o.groupId === group.id
                );
                if (op)
                  return { ...group, confirmedStatus: op.originalGroupStatus };
                return group;
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
          /* ... revert logic ... */
          const newConnDataState = { ...prevConnData };
          const targetConnState = newConnDataState[currentBatch.edgeId];
          if (targetConnState?.data) {
            const targetConn = { ...targetConnState.data };
            if ("edge" in targetConn && targetConn.edge) {
              (
                targetConn.edge as
                  | VisualizationEntityEdge
                  | VisualizationServiceEdge
              ).status = currentBatch.originalEdgeStatus;
              if ("confirmedStatus" in targetConn.edge) {
                (targetConn.edge as VisualizationEntityEdge).confirmedStatus =
                  currentBatch.originalEdgeStatus as any; // Cast if needed
              }
            }
            if (
              "entityGroups" in targetConn &&
              Array.isArray(targetConn.entityGroups)
            ) {
              // Type guard to check if it's an array of EntityGroup
              const isEntityGroups = targetConn.entityGroups.every(
                (group) => "entityId1" in group && "entityId2" in group
              );

              if (isEntityGroups) {
                // If it's EntityGroup[]
                targetConn.entityGroups = (
                  targetConn.entityGroups as EntityGroup[]
                ).map((group) => {
                  const op = currentBatch.operations.find(
                    (o) => o.groupId === group.id
                  );
                  return op
                    ? { ...group, confirmedStatus: op.originalGroupStatus }
                    : group;
                });
              } else {
                // If it's ServiceGroup[]
                targetConn.entityGroups = (
                  targetConn.entityGroups as ServiceGroup[]
                ).map((group) => {
                  const op = currentBatch.operations.find(
                    (o) => o.groupId === group.id
                  );
                  return op
                    ? { ...group, confirmedStatus: op.originalGroupStatus }
                    : group;
                });
              }
            }
            newConnDataState[currentBatch.edgeId] = {
              ...targetConnState,
              data: targetConn,
              lastUpdated: Date.now(),
            };
          }
          return newConnDataState;
        });
        setReviewQueue((prevQ) =>
          prevQ.map((b) =>
            b.batchId === currentBatch.batchId
              ? { ...currentBatch, isTerminalFailure: true }
              : b
          )
        );
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
        setReviewQueue((prevQ) =>
          prevQ.map((b) =>
            b.batchId === currentBatch.batchId ? currentBatch : b
          )
        );
      }
    } else {
      console.log(`Batch ${currentBatch.batchId} processed successfully.`);
      setReviewQueue((prevQ) =>
        prevQ.filter((b) => b.batchId !== currentBatch.batchId)
      );
      if (currentBatch.clusterId) {
        checkAndAdvanceIfComplete(currentBatch.clusterId); // Check completion after successful batch
      }
    }
    processingBatchIdRef.current = null;
    setIsProcessingQueue(false);
  }, [
    reviewQueue,
    isProcessingQueue,
    toast,
    currentUser,
    checkAndAdvanceIfComplete,
    retryFailedBatch,
  ]);

  const currentVisualizationData = useMemo(():
    | EntityVisualizationDataResponse
    | ServiceVisualizationDataResponse
    | null => {
    if (!selectedClusterId) return null;
    return visualizationData[selectedClusterId]?.data || null;
  }, [selectedClusterId, visualizationData]);

  const currentConnectionData = useMemo(():
    | EntityConnectionDataResponse
    | ServiceConnectionDataResponse
    | null => {
    if (!selectedEdgeId) return null;
    return connectionData[selectedEdgeId]?.data || null;
  }, [selectedEdgeId, connectionData]);

  const selectedClusterDetails = useMemo(():
    | EntityCluster
    | ServiceCluster
    | null => {
    if (!selectedClusterId) return null;
    return clusters.data.find((c) => c.id === selectedClusterId) || null;
  }, [selectedClusterId, clusters.data]);

  const edgeSelectionInfo = useMemo((): EdgeSelectionInfo => {
    if (
      !currentVisualizationData?.links ||
      currentVisualizationData.links.length === 0
    ) {
      return {
        currentEdgeId: selectedEdgeId,
        nextUnreviewedEdgeId: null,
        hasUnreviewedEdges: false,
        currentEdgeIndex: -1,
        totalEdges: 0,
        totalUnreviewedEdgesInCluster: 0,
        currentUnreviewedEdgeIndexInCluster: -1,
      };
    }
    const { links } = currentVisualizationData;
    const totalEdges = links.length;
    const unreviewedLinks = links.filter(
      (link) => link.status === "PENDING_REVIEW"
    );
    const totalUnreviewedEdgesInCluster = unreviewedLinks.length;
    let currentUnreviewedEdgeIndexInCluster = -1;
    if (selectedEdgeId) {
      const selectedEdgeIsUnreviewed = unreviewedLinks.some(
        (l) => l.id === selectedEdgeId
      );
      if (selectedEdgeIsUnreviewed) {
        currentUnreviewedEdgeIndexInCluster = unreviewedLinks.findIndex(
          (l) => l.id === selectedEdgeId
        );
      }
    }
    let nextUnreviewedEdgeId: string | null = null;
    if (totalUnreviewedEdgesInCluster > 0) {
      const referenceEdgeId = lastReviewedEdgeId || selectedEdgeId;
      let startIndexInAllLinks = 0;
      if (referenceEdgeId) {
        const idx = links.findIndex((l) => l.id === referenceEdgeId);
        if (idx !== -1) startIndexInAllLinks = idx + 1;
      }
      for (let i = 0; i < links.length; i++) {
        const currentLinkToCheck =
          links[(startIndexInAllLinks + i) % links.length];
        if (currentLinkToCheck.status === "PENDING_REVIEW") {
          if (
            currentLinkToCheck.id !== selectedEdgeId ||
            totalUnreviewedEdgesInCluster > 1
          ) {
            nextUnreviewedEdgeId = currentLinkToCheck.id;
            break;
          } else if (
            currentLinkToCheck.id === selectedEdgeId &&
            totalUnreviewedEdgesInCluster === 1
          ) {
            nextUnreviewedEdgeId = null;
            break;
          }
        }
      }
      if (!nextUnreviewedEdgeId && unreviewedLinks.length > 0) {
        if (
          selectedEdgeId &&
          unreviewedLinks.some((l) => l.id === selectedEdgeId) &&
          unreviewedLinks.length === 1
        ) {
          nextUnreviewedEdgeId = null;
        } else {
          const firstUnreviewed = unreviewedLinks.find(
            (l) => l.id !== selectedEdgeId
          );
          if (firstUnreviewed) {
            nextUnreviewedEdgeId = firstUnreviewed.id;
          } else if (
            unreviewedLinks.length > 0 &&
            selectedEdgeId &&
            !unreviewedLinks.some((l) => l.id === selectedEdgeId)
          ) {
            nextUnreviewedEdgeId = unreviewedLinks[0].id;
          } else if (unreviewedLinks.length > 0 && !selectedEdgeId) {
            nextUnreviewedEdgeId = unreviewedLinks[0].id;
          }
        }
      }
    }
    return {
      currentEdgeId: selectedEdgeId,
      nextUnreviewedEdgeId,
      hasUnreviewedEdges: totalUnreviewedEdgesInCluster > 0,
      currentEdgeIndex: selectedEdgeId
        ? links.findIndex((link) => link.id === selectedEdgeId)
        : -1,
      totalEdges,
      totalUnreviewedEdgesInCluster,
      currentUnreviewedEdgeIndexInCluster,
    };
  }, [currentVisualizationData, selectedEdgeId, lastReviewedEdgeId]);

  useEffect(() => {
    loadClusters(1, clusters.limit);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [resolutionMode]); // Initial load on mode change

  useEffect(() => {
    // Auto-select first cluster if none selected after clusters load
    if (!selectedClusterId && clusters.data.length > 0 && !clusters.loading) {
      const firstNonSplitUncompletedCluster = clusters.data.find(
        (c) => !queries.getClusterProgress(c.id).isComplete
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
          "All clusters on current page are split or complete. Auto-selecting first cluster for viewing:",
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
      loadBulkNodeDetails, // New action added
      loadConnectionData,
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
    }),
    [
      setResolutionMode,
      handleSetSelectedClusterId,
      setSelectedEdgeIdAction,
      setReviewerId,
      setLastReviewedEdgeId,
      triggerRefresh,
      loadClusters,
      loadBulkNodeDetails,
      loadConnectionData,
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
      setIsAutoAdvanceEnabledState,
    ]
  );

  useEffect(() => {
    // Auto-select first edge when cluster is selected and viz data is ready
    if (
      selectedClusterId &&
      currentVisualizationData?.links &&
      !selectedEdgeId
    ) {
      console.log(
        `Cluster ${selectedClusterId} selected, viz data loaded, no edge selected. Evaluating next action using selectNextUnreviewedEdge.`
      );
      actions.selectNextUnreviewedEdge();
    }
  }, [selectedClusterId, currentVisualizationData, selectedEdgeId, actions]);

  useEffect(() => {
    // Load connection data for selected edge
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
          `Fetching connection data for selected edge: ${selectedEdgeId}`
        );
        actions.loadConnectionData(selectedEdgeId); // This will also trigger node detail load
      }
    }
  }, [selectedEdgeId, connectionData, actions]);

  useEffect(() => {
    // Set reviewer ID from current user
    if (currentUser?.id) setReviewerId(currentUser.id);
  }, [currentUser]);

  useEffect(() => {
    // Process review queue
    if (reviewQueue.length > 0 && !isProcessingQueue && processReviewQueue) {
      const timeoutId = setTimeout(processReviewQueue, 500);
      return () => clearTimeout(timeoutId);
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
    clusterProgress: useMemo(() => {
      const reconstructed: Record<string, ClusterProgress> = {};
      clusters.data.forEach((c) => {
        reconstructed[c.id] = queries.getClusterProgress(c.id);
      });
      if (
        selectedClusterId &&
        !reconstructed[selectedClusterId] &&
        visualizationData[selectedClusterId]?.data?.links
      ) {
        reconstructed[selectedClusterId] =
          queries.getClusterProgress(selectedClusterId);
      }
      return reconstructed;
    }, [clusters.data, selectedClusterId, visualizationData, queries]),
    edgeSelectionInfo,
    currentVisualizationData,
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

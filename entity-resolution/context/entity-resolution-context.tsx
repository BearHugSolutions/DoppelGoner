// context/entity-resolution-context.tsx
"use client";

import {
  ResolutionMode,
  EntityClustersResponse,
  EntityCluster, // Ensure this type (via BaseCluster) includes was_split: boolean
  EntityVisualizationDataResponse,
  EntityConnectionDataResponse,
  EntityGroup,
  EntityLink,
  VisualizationEntityEdge,
  EntityGroupReviewApiPayload,
  ServiceClustersResponse,
  ServiceCluster, // Ensure this type (via BaseCluster) includes was_split: boolean
  ServiceVisualizationDataResponse,
  ServiceConnectionDataResponse,
  ServiceGroup,
  ServiceLink,
  VisualizationServiceEdge,
  ServiceGroupReviewApiPayload,
  GroupReviewDecision,
  ClusterFinalizationStatusResponse,
  QueuedReviewBatch,
  BaseCluster, // Ensure this type includes was_split: boolean
  BaseLink,
  BaseNode,
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

// Ensure BaseCluster and thus EntityCluster/ServiceCluster include was_split

interface ClustersState<TCluster extends BaseCluster> { // Use ExtendedCluster
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
}

export interface EntityResolutionContextType {
  resolutionMode: ResolutionMode;
  selectedClusterId: string | null;
  selectedEdgeId: string | null;
  reviewerId: string;
  lastReviewedEdgeId: string | null;
  refreshTrigger: number;
  isAutoAdvanceEnabled: boolean;

  clusters: ClustersState<EntityCluster | ServiceCluster>; // Types will include was_split
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
  selectedClusterDetails: EntityCluster | ServiceCluster | null; // Added to easily access full cluster details

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
    preloadVisualizationData: (clusterIds: string[]) => Promise<void>;
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
    getClusterById: (clusterId: string) => EntityCluster | ServiceCluster | undefined; // Added helper
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
    useState<ClustersState<EntityCluster | ServiceCluster>>( // Types will include was_split
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

  const [reviewQueue, setReviewQueue] = useState<QueuedReviewBatch[]>([]);
  const [isProcessingQueue, setIsProcessingQueue] = useState<boolean>(false);
  const processingBatchIdRef = useRef<string | null>(null);

  const getClusterById = useCallback(
    (clusterId: string): EntityCluster | ServiceCluster | undefined => {
      return clusters.data.find(c => c.id === clusterId);
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
        // A cluster marked as was_split might have 0 links if its viz data is reloaded post-split.
        // Its "completeness" comes from the split status rather than edge review count.
        // However, this progress is about *reviewable edges in the current view*.
        const clusterDetails = getClusterById(clusterIdToQuery);
        if (clusterDetails?.wasSplit) { // If cluster was split, it's considered "complete" for review purposes
             return {
                clusterId: clusterIdToQuery,
                totalEdges: 0, // Or original total if you want to show that, but for review progress, 0 makes sense
                reviewedEdges: 0,
                pendingEdges: 0,
                confirmedMatches: 0,
                confirmedNonMatches: 0,
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
            isComplete: false, // Not complete if no links unless it's an empty cluster
          };
        const { links } = vizState.data;
        const totalEdges = links.length;
        if (totalEdges === 0) // Empty cluster (not split, just no links)
          return {
            clusterId: clusterIdToQuery,
            totalEdges: 0,
            reviewedEdges: 0,
            pendingEdges: 0,
            confirmedMatches: 0,
            confirmedNonMatches: 0,
            progressPercentage: 100, // An empty cluster is "complete" in terms of review
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
    }),
    [
      visualizationData,
      connectionData,
      selectedClusterId,
      reviewQueue,
      processingBatchIdRef,
      getClusterById // Added dependency
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
      setIsAutoAdvanceEnabledState(true);
    },
    [resolutionMode]
  );

  const preloadVisualizationData = useCallback(
    async (clusterIds: string[]) => {
      const getVizData =
        resolutionMode === "entity"
          ? getEntityVisualizationData
          : getServiceVisualizationData;
      console.log(
        `Preloading visualization data for mode: ${resolutionMode}, clusters:`,
        clusterIds
      );

      clusterIds.forEach((clusterId) => {
        const clusterDetail = queries.getClusterById(clusterId);
        if (clusterDetail?.wasSplit) {
          console.log(`Skipping preload for already split cluster: ${clusterId}`);
          // Optionally, set a specific state for split cluster visualization if needed
           setVisualizationData((prev) => ({
            ...prev,
            [clusterId]: {
              data: { nodes: [], links: [], entityGroups: [] } as any, // Empty viz data
              loading: false,
              error: null,
              lastUpdated: Date.now(),
            },
          }));
          return;
        }

        if (
          !visualizationData[clusterId]?.data ||
          visualizationData[clusterId]?.error
        ) {
          setVisualizationData((prev) => ({
            ...prev,
            [clusterId]: {
              data: prev[clusterId]?.data || null,
              loading: true,
              error: null,
              lastUpdated: prev[clusterId]?.lastUpdated || null,
            },
          }));
          getVizData(clusterId)
            .then((data) => {
              console.log(
                `Successfully preloaded viz data for cluster ${clusterId}`
              );
              setVisualizationData((prev) => ({
                ...prev,
                [clusterId]: {
                  data,
                  loading: false,
                  error: null,
                  lastUpdated: Date.now(),
                },
              }));
            })
            .catch((error) => {
              console.error(
                `Error preloading viz data for cluster ${clusterId}:`,
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
            });
        }
      });
    },
    [visualizationData, resolutionMode, queries] // Added queries
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
        Date.now() - cached.lastUpdated < 300000 // 5 min cache
      ) {
        console.log(`Using cached connection data for edge ${edgeId}`);
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
        console.log("data:", JSON.stringify(data));
        setConnectionData((prev) => ({
          ...prev,
          [edgeId]: {
            data,
            loading: false,
            error: null,
            lastUpdated: Date.now(),
          },
        }));
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
    [connectionData, resolutionMode]
  );

  const invalidateVisualizationData = useCallback(
    (clusterId: string) => {
      console.log(`Invalidating visualization data for cluster: ${clusterId}`);
      setVisualizationData((prev) => ({
        ...prev,
        [clusterId]: {
          data: null,
          loading: false,
          error: `Invalidated by user action`,
          lastUpdated: null,
        },
      }));
      if (selectedClusterId === clusterId) {
         const clusterDetail = queries.getClusterById(clusterId);
         if (!clusterDetail?.wasSplit) { // Don't preload for already split clusters
            preloadVisualizationData([clusterId]);
         } else {
             // For a split cluster, ensure its visualization is cleared or set to an empty state
            setVisualizationData((prevViz) => ({
                ...prevViz,
                [clusterId]: { data: { nodes: [], links: [], entityGroups: [] } as any, loading: false, error: null, lastUpdated: Date.now() },
            }));
         }
      }
    },
    [selectedClusterId, preloadVisualizationData, queries] // Added queries
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
      if (selectedEdgeId === edgeId) {
        loadConnectionData(edgeId);
      }
    },
    [selectedEdgeId, loadConnectionData]
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
        const response = await fetcher(page, limit); // response.clusters will have `was_split`
        console.log(
          `Successfully loaded clusters for mode ${resolutionMode}`,
          response
        );
        setClusters({
          data: response.clusters as Array<EntityCluster | ServiceCluster>, // Cast ensures was_split is expected
          total: response.total,
          page,
          limit,
          loading: false,
          error: null,
        });
        const clusterIdsToPreload = response.clusters
            .filter(c => !(c as BaseCluster).wasSplit) // Don't preload viz for already split clusters
            .map((c) => c.id);

        if (clusterIdsToPreload.length > 0) {
          preloadVisualizationData(clusterIdsToPreload);
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
    [preloadVisualizationData, resolutionMode]
  );

  const handleSetSelectedClusterId = useCallback(
    (id: string | null) => {
      const previousSelectedClusterId = selectedClusterId;
      console.log(
        `Setting selected cluster ID to: ${id}. Previous: ${previousSelectedClusterId}`
      );
      
      const clusterDetail = id ? queries.getClusterById(id) : null;

      if (id && id !== previousSelectedClusterId) {
        if (clusterDetail?.wasSplit || queries.getClusterProgress(id).isComplete) {
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

      setSelectedClusterIdState(id);
      setSelectedEdgeIdState(null);
      setLastReviewedEdgeId(null);

      if (
        id &&
        !clusterDetail?.wasSplit && // Don't preload for split clusters
        !visualizationData[id]?.data &&
        !visualizationData[id]?.loading
      ) {
        console.log(
          `Preloading visualization data for newly selected cluster: ${id}`
        );
        preloadVisualizationData([id]);
      } else if (id && clusterDetail?.wasSplit) {
         // For a split cluster, ensure its visualization is cleared or set to an empty state
        setVisualizationData((prevViz) => ({
            ...prevViz,
            [id]: { data: { nodes: [], links: [], entityGroups: [] } as any, loading: false, error: null, lastUpdated: Date.now() },
        }));
      }
    },
    [
      selectedClusterId,
      queries,
      visualizationData,
      preloadVisualizationData,
      setIsAutoAdvanceEnabledState,
    ]
  );

  const setSelectedEdgeIdAction = useCallback(
    (id: string | null) => {
      console.log(`Setting selected edge ID to: ${id}`);
      if (id && queries.isEdgeReviewed(id)) {
        console.log(`Edge ${id} is already reviewed. Pausing auto-advance.`);
        setIsAutoAdvanceEnabledState(false);
      }
      setSelectedEdgeIdState(id);
    },
    [queries, setIsAutoAdvanceEnabledState]
  );

  const clearAllData = useCallback(() => {
    console.log("Clearing all data.");
    setClusters(initialClustersState);
    setVisualizationData({});
    setConnectionData({});
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
        loadClusters(clusters.page, clusters.limit);
      }
      if (
        selectedClusterId &&
        (target === "all" || target === "current_visualization")
      ) {
        invalidateVisualizationData(selectedClusterId); // This will respect was_split
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

    let nextClusterSelected = false;
    for (let i = currentIndex + 1; i < clusters.data.length; i++) {
      const nextClusterOnPage = clusters.data[i];
      // Check progress AND was_split. A split cluster is also "complete" for advancement.
      if (!queries.getClusterProgress(nextClusterOnPage.id).isComplete || (nextClusterOnPage as BaseCluster).wasSplit) {
         if(!(nextClusterOnPage as BaseCluster).wasSplit && !queries.getClusterProgress(nextClusterOnPage.id).isComplete) {
            console.log(
              `Selecting next uncompleted, non-split cluster in current page: ${nextClusterOnPage.id}`
            );
            handleSetSelectedClusterId(nextClusterOnPage.id);
            nextClusterSelected = true;
            break;
         }
      }
    }

    if (!nextClusterSelected) {
      if (clusters.page < Math.ceil(clusters.total / clusters.limit)) {
        const nextPageToLoad = clusters.page + 1;
        console.log(
          `Loading next page (${nextPageToLoad}) of clusters.`
        );
        await loadClusters(nextPageToLoad, clusters.limit);
        setSelectedClusterIdState(null); 
      } else {
        toast({
          title: "Workflow Complete",
          description: "All clusters have been processed or are completed/split.",
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
      console.log(`Checking if cluster ${targetClusterId} is complete or split.`);

      const clusterDetail = queries.getClusterById(targetClusterId);
      const progress = queries.getClusterProgress(targetClusterId); // getClusterProgress now considers was_split
      const isCompleteOrSplit = progress.isComplete || clusterDetail?.wasSplit;

      console.log(
        `Cluster ${targetClusterId}: Total=${progress.totalEdges}, Reviewed=${progress.reviewedEdges}, Pending=${progress.pendingEdges}, Complete=${progress.isComplete}, WasSplit=${clusterDetail?.wasSplit}`
      );

      // Only finalize if it's complete AND NOT already split.
      // A split cluster has already been "finalized" in a sense.
      if (progress.isComplete && !clusterDetail?.wasSplit && progress.totalEdges > 0) {
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
          const finalizationResponse = await finalizer(targetClusterId); // API now handles was_split
          toast({
            title: "Cluster Finalization",
            description: `${finalizationResponse.message}. Status: ${finalizationResponse.status}`,
          });
          console.log(
            `Finalization response for ${targetClusterId}:`,
            finalizationResponse
          );

          // Reload clusters. The reloaded original cluster will have was_split=true if split.
          loadClusters(clusters.page, clusters.limit);
          invalidateVisualizationData(targetClusterId); // Will correctly handle if it became split
          if (
            finalizationResponse.newClusterIds &&
            finalizationResponse.newClusterIds.length > 0
          ) {
            // Preload viz for new clusters (these are not split themselves)
            preloadVisualizationData(finalizationResponse.newClusterIds);
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
        }
      } else if (isCompleteOrSplit) { // If it's already split, or complete with 0 edges
         if (targetClusterId === selectedClusterId && isAutoAdvanceEnabled) {
            console.log(
                `Cluster ${targetClusterId} is already split or is an empty cluster. Auto-advance ON. Advancing.`
            );
            advanceToNextCluster();
         } else if (isCompleteOrSplit) {
             console.log(`Cluster ${targetClusterId} is already split or empty. Auto-advance OFF or not selected. Not advancing.`);
         }
      }
    },
    [
      selectedClusterId,
      queries,
      advanceToNextCluster,
      toast,
      invalidateVisualizationData,
      loadClusters,
      clusters.page,
      clusters.limit,
      resolutionMode,
      preloadVisualizationData,
      isAutoAdvanceEnabled,
    ]
  );
  
  const selectNextUnreviewedEdge = useCallback(
    (afterEdgeId?: string | null) => {
      setIsAutoAdvanceEnabledState(true);
      console.log(
        "Auto-advance explicitly enabled by selectNextUnreviewedEdge call."
      );

      const currentClusterId = selectedClusterId;
      if (!currentClusterId) {
        console.warn("No cluster selected for selecting next edge.");
        return;
      }
      const clusterDetail = queries.getClusterById(currentClusterId);
      if (clusterDetail?.wasSplit) {
        console.log(`Cluster ${currentClusterId} was split. No edges to select. Checking advance.`);
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
        console.log(`No links in viz for cluster ${currentClusterId}. Checking advance.`);
        checkAndAdvanceIfComplete(currentClusterId);
        return;
      }

      const { links } = currentViz;
      let startIdx = 0;
      const referenceEdgeId = afterEdgeId || lastReviewedEdgeId || selectedEdgeId;

      if (referenceEdgeId) {
        const idx = links.findIndex((l) => l.id === referenceEdgeId);
        if (idx !== -1) startIdx = idx + 1;
        else console.warn(`Ref edge ${referenceEdgeId} not found. Starting from beginning.`);
      }

      for (let i = 0; i < links.length; i++) {
        const link = links[(startIdx + i) % links.length];
        if (link.status === "PENDING_REVIEW") {
          console.log(`Next unreviewed edge found: ${link.id}. Selecting.`);
          setSelectedEdgeIdState(link.id);
          return;
        }
      }

      console.log(`No more unreviewed edges in cluster ${currentClusterId}. Checking advance.`);
      toast({
        title: "Cluster Review Status",
        description: `All connections in cluster ${currentClusterId.substring(0,8)}... reviewed or processing.`,
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
      queries, // Added queries
    ]
  );

  const submitEdgeReview = useCallback(
    async (edgeId: string, decision: GroupReviewDecision) => {
      if (!currentUser?.id) {
        toast({ title: "Auth Error", description: "Login required.", variant: "destructive" });
        return;
      }
      if (!selectedClusterId) {
        toast({ title: "Selection Error", description: "Cluster must be selected.", variant: "destructive" });
        return;
      }
      const clusterDetail = queries.getClusterById(selectedClusterId);
      if (clusterDetail?.wasSplit) {
          toast({ title: "Info", description: "This cluster has been split and cannot be further reviewed."});
          return;
      }

      console.log(
        `Submitting edge review: ${edgeId}, decision: ${decision}, mode: ${resolutionMode}`
      );

      const connData = connectionData[edgeId]?.data;
      const vizData = visualizationData[selectedClusterId]?.data;

      if (!connData || !vizData) {
        toast({ title: "Data Error", description: "Data not loaded for review.", variant: "destructive" });
        if (!connData) loadConnectionData(edgeId);
        if (!vizData && selectedClusterId) preloadVisualizationData([selectedClusterId]); // Respects was_split
        return;
      }

      let relevantGroups: Array<EntityGroup | ServiceGroup> = [];
      let currentEdgeLink: EntityLink | ServiceLink | undefined;

      if (resolutionMode === "entity" && "entityGroups" in connData && "links" in vizData) {
        const entityConnData = connData as EntityConnectionDataResponse;
        const entityVizData = vizData as EntityVisualizationDataResponse;
        relevantGroups = entityConnData.entityGroups.filter(
          (group) =>
            (group.entityId1 === entityConnData.edge.entityId1 &&
              group.entityId2 === entityConnData.edge.entityId2) ||
            (group.entityId1 === entityConnData.edge.entityId2 &&
              group.entityId2 === entityConnData.edge.entityId1)
        );
        currentEdgeLink = entityVizData.links.find((l) => l.id === edgeId);
      } else if (resolutionMode === "service" && "entityGroups" in connData && "links" in vizData ) {
         // Assuming ServiceConnectionDataResponse has `entityGroups` as ServiceGroup[] and `edge` as VisualizationServiceEdge
        const serviceConnData = connData as ServiceConnectionDataResponse;
        const serviceVizData = vizData as ServiceVisualizationDataResponse;
        relevantGroups = (serviceConnData.entityGroups as ServiceGroup[]).filter(
          (group) =>
            (group.serviceId1 === serviceConnData.edge.serviceId1 &&
              group.serviceId2 === serviceConnData.edge.serviceId2) ||
            (group.serviceId1 === serviceConnData.edge.serviceId2 &&
              group.serviceId2 === serviceConnData.edge.serviceId1)
        );
        currentEdgeLink = serviceVizData.links.find((l) => l.id === edgeId);
      }


      if (relevantGroups.length === 0) {
        toast({ title: "Info", description: "No underlying groups. Marking edge as reviewed." });
        setVisualizationData((prev) => {
          const newVizData = { ...prev };
          const targetClusterVizState = newVizData[selectedClusterId!]?.data;
          if (targetClusterVizState?.links) {
            const newTargetClusterViz = { ...targetClusterVizState };
            const linkIndex = newTargetClusterViz.links.findIndex((l) => l.id === edgeId);
            if (linkIndex !== -1) {
              const newLinks = [...newTargetClusterViz.links];
              newLinks[linkIndex] = {
                ...newLinks[linkIndex],
                status: decision === "ACCEPTED" ? "CONFIRMED_MATCH" : "CONFIRMED_NON_MATCH",
              };
              newTargetClusterViz.links = newLinks;
            }
            newVizData[selectedClusterId!] = { ...newVizData[selectedClusterId!], data: newTargetClusterViz, lastUpdated: Date.now() };
          }
          return newVizData;
        });
        setLastReviewedEdgeId(edgeId);
        if (isAutoAdvanceEnabled) selectNextUnreviewedEdge(edgeId);
        else checkAndAdvanceIfComplete(selectedClusterId);
        return;
      }

      if (!currentEdgeLink) {
        console.error("Critical: Current edge link not found for optimistic update.");
        toast({ title: "Internal Error", description: "Edge not found for update. Refresh.", variant: "destructive" });
        return;
      }

      const operations = relevantGroups.map((group) => ({
        groupId: group.id,
        originalGroupStatus: group.confirmedStatus,
      }));
      const optimisticEdgeStatus = decision === "ACCEPTED" ? "CONFIRMED_MATCH" : "CONFIRMED_NON_MATCH";
      const optimisticGroupStatus = decision === "ACCEPTED" ? "CONFIRMED_MATCH" : "CONFIRMED_NON_MATCH";

      const batch: QueuedReviewBatch = {
        batchId: uuidv4(), edgeId, clusterId: selectedClusterId, decision, reviewerId: currentUser.id,
        operations, originalEdgeStatus: currentEdgeLink.status || "PENDING_REVIEW",
        optimisticEdgeStatus, optimisticGroupStatus, attempt: 0,
        processedOperations: new Set(), failedOperations: new Set(), mode: resolutionMode,
      };

      // Optimistic UI updates (visualization and connection data)
      // ... (similar to original, ensure types are handled for Entity/Service)
       setVisualizationData((prevVizData) => {
        const newVizDataState = { ...prevVizData };
        const targetClusterVizState = newVizDataState[batch.clusterId];
        if (targetClusterVizState?.data) {
          const targetClusterViz = { ...targetClusterVizState.data };
          const linkIndex = targetClusterViz.links.findIndex((l) => l.id === batch.edgeId);
          if (linkIndex !== -1) {
            targetClusterViz.links = [...targetClusterViz.links];
            targetClusterViz.links[linkIndex] = { ...targetClusterViz.links[linkIndex], status: batch.optimisticEdgeStatus };
          }
          if ("entityGroups" in targetClusterViz && Array.isArray(targetClusterViz.entityGroups)) {
            const groupsToUpdate = targetClusterViz.entityGroups as Array<EntityGroup | ServiceGroup>;
            (targetClusterViz.entityGroups as Array<EntityGroup | ServiceGroup>) = groupsToUpdate.map((group) =>
              batch.operations.some((op) => op.groupId === group.id)
                ? { ...group, confirmed_status: batch.optimisticGroupStatus }
                : group
            );
          }
          newVizDataState[batch.clusterId] = { ...targetClusterVizState, data: targetClusterViz, lastUpdated: Date.now() };
        }
        return newVizDataState;
      });
      setConnectionData((prevConnData) => {
        const newConnDataState = { ...prevConnData };
        const targetConnState = newConnDataState[batch.edgeId];
        if (targetConnState?.data) {
          const targetConn = { ...targetConnState.data };
          if ("edge" in targetConn) {
            (targetConn.edge as VisualizationEntityEdge | VisualizationServiceEdge).status = batch.optimisticEdgeStatus;
            if ("confirmed_status" in targetConn.edge) {
                (targetConn.edge as any).confirmed_status = batch.optimisticGroupStatus;
            }
          }
          if ("entityGroups" in targetConn && Array.isArray(targetConn.entityGroups)) {
             const groupsToUpdate = targetConn.entityGroups as Array<EntityGroup | ServiceGroup>;
            (targetConn.entityGroups as Array<EntityGroup | ServiceGroup>) = groupsToUpdate.map((group) =>
              batch.operations.some((op) => op.groupId === group.id)
                ? { ...group, confirmed_status: batch.optimisticGroupStatus }
                : group
            );
          }
          newConnDataState[batch.edgeId] = { ...targetConnState, data: targetConn, lastUpdated: Date.now() };
        }
        return newConnDataState;
      });


      setReviewQueue((prevQueue) => [...prevQueue, batch]);
      setLastReviewedEdgeId(edgeId);

      if (isAutoAdvanceEnabled) selectNextUnreviewedEdge(edgeId);
      else checkAndAdvanceIfComplete(selectedClusterId);
    },
    [
      currentUser, selectedClusterId, connectionData, visualizationData, toast,
      resolutionMode, loadConnectionData, preloadVisualizationData,
      isAutoAdvanceEnabled, selectNextUnreviewedEdge, checkAndAdvanceIfComplete, queries // Added queries
    ]
  );

  const processReviewQueue = useCallback(async () => {
    if (isProcessingQueue || reviewQueue.length === 0) return;

    const batchToProcess = reviewQueue.find(b => !b.isTerminalFailure && (b.failedOperations.size === 0 || b.attempt < MAX_REVIEW_ATTEMPTS));
    if (!batchToProcess) {
      setIsProcessingQueue(false);
      return;
    }
    if (processingBatchIdRef.current && processingBatchIdRef.current !== batchToProcess.batchId) {
      console.log(`Queue busy with ${processingBatchIdRef.current}, skipping ${batchToProcess.batchId}`);
      return;
    }

    setIsProcessingQueue(true);
    processingBatchIdRef.current = batchToProcess.batchId;
    console.log(`Processing batch: ${batchToProcess.batchId}, attempt: ${batchToProcess.attempt + 1}`);

    let currentBatch = { ...batchToProcess, attempt: batchToProcess.attempt + 1, failedOperations: new Set(batchToProcess.failedOperations) };
    let batchOverallSuccess = true;
    const stillPendingOperations = currentBatch.operations.filter(op => !currentBatch.processedOperations.has(op.groupId));
    const feedbackPoster = currentBatch.mode === "entity" ? postEntityGroupFeedback : postServiceGroupFeedback;

    for (const op of stillPendingOperations) {
      try {
        const payload: EntityGroupReviewApiPayload | ServiceGroupReviewApiPayload = { decision: currentBatch.decision, reviewerId: currentBatch.reviewerId };
        console.log(`Submitting feedback for group ${op.groupId} in batch ${currentBatch.batchId}`);
        await feedbackPoster(op.groupId, payload);
        currentBatch.processedOperations.add(op.groupId);
        console.log(`Success for group ${op.groupId}`);
      } catch (error) {
        console.error(`Failed for group ${op.groupId} (mode: ${currentBatch.mode}) in batch ${currentBatch.batchId}:`, error);
        currentBatch.failedOperations.add(op.groupId);
        currentBatch.error = `Group ${op.groupId.substring(0,8)}...: ${(error as Error).message}`;
        batchOverallSuccess = false;
      }
    }

    if (!batchOverallSuccess) {
      console.warn(`Batch ${currentBatch.batchId} errors. Attempt ${currentBatch.attempt}/${MAX_REVIEW_ATTEMPTS}.`);
      if (currentBatch.attempt >= MAX_REVIEW_ATTEMPTS) {
        currentBatch.isTerminalFailure = true;
        toast({
          title: "Review Submission Failed Permanently",
          description: (<>Failed: connection {currentBatch.edgeId.substring(0,8)}... ({currentBatch.mode}). Error: {currentBatch.error}
            <Button variant="link" className="p-0 h-auto ml-2 text-destructive-foreground underline" onClick={() => setSelectedEdgeIdState(currentBatch.edgeId)}>View</Button>
            <Button variant="link" className="p-0 h-auto ml-2 text-destructive-foreground underline" onClick={() => retryFailedBatch(currentBatch.batchId)}>Retry</Button></>),
          variant: "destructive", duration: 10000,
        });
        console.error(`Batch ${currentBatch.batchId} failed permanently.`);
        // Revert optimistic updates
        // ... (similar to original, ensure types are handled for Entity/Service)
        setVisualizationData((prevVizData) => {
            const newVizDataState = { ...prevVizData };
            const targetClusterVizState = newVizDataState[currentBatch.clusterId];
            if (targetClusterVizState?.data) {
                const targetClusterViz = { ...targetClusterVizState.data };
                const linkIndex = targetClusterViz.links.findIndex(l => l.id === currentBatch.edgeId);
                if (linkIndex !== -1) {
                    targetClusterViz.links = [...targetClusterViz.links];
                    targetClusterViz.links[linkIndex] = { ...targetClusterViz.links[linkIndex], status: currentBatch.originalEdgeStatus };
                }
                if ("entityGroups" in targetClusterViz && Array.isArray(targetClusterViz.entityGroups)) {
                    const groupsToRevert = targetClusterViz.entityGroups as Array<EntityGroup | ServiceGroup>;
                    (targetClusterViz.entityGroups as Array<EntityGroup | ServiceGroup>) = groupsToRevert.map(group => {
                        const op = currentBatch.operations.find(o => o.groupId === group.id);
                        if (op) return { ...group, confirmed_status: op.originalGroupStatus };
                        return group;
                    });
                }
                newVizDataState[currentBatch.clusterId] = { ...targetClusterVizState, data: targetClusterViz, lastUpdated: Date.now() };
            }
            return newVizDataState;
        });
        setConnectionData((prevConnData) => {
            const newConnDataState = { ...prevConnData };
            const targetConnState = newConnDataState[currentBatch.edgeId];
            if (targetConnState?.data) {
                const targetConn = { ...targetConnState.data };
                 if ("edge" in targetConn) {
                    (targetConn.edge as VisualizationEntityEdge | VisualizationServiceEdge).status = currentBatch.originalEdgeStatus;
                     if ("confirmed_status" in targetConn.edge) { (targetConn.edge as any).confirmed_status = currentBatch.originalEdgeStatus; }
                }
                if ("entityGroups" in targetConn && Array.isArray(targetConn.entityGroups)) {
                    const groupsToRevert = targetConn.entityGroups as Array<EntityGroup | ServiceGroup>;
                    (targetConn.entityGroups as Array<EntityGroup | ServiceGroup>) = groupsToRevert.map(group => {
                        const op = currentBatch.operations.find(o => o.groupId === group.id);
                        if (op) return { ...group, confirmed_status: op.originalGroupStatus };
                        return group;
                    });
                }
                newConnDataState[currentBatch.edgeId] = { ...targetConnState, data: targetConn, lastUpdated: Date.now() };
            }
            return newConnDataState;
        });
        setReviewQueue(prevQ => prevQ.map(b => b.batchId === currentBatch.batchId ? { ...currentBatch, isTerminalFailure: true } : b));

      } else {
        toast({ title: "Review Submission Issue", description: `Attempt ${currentBatch.attempt}/${MAX_REVIEW_ATTEMPTS} failed for ${currentBatch.edgeId.substring(0,8)}... Will retry. Error: ${currentBatch.error}`, variant: "default", duration: 5000 });
        setReviewQueue(prevQ => prevQ.map(b => b.batchId === currentBatch.batchId ? currentBatch : b));
      }
    } else {
      console.log(`Batch ${currentBatch.batchId} processed successfully.`);
      setReviewQueue(prevQ => prevQ.filter(b => b.batchId !== currentBatch.batchId));
      if (currentBatch.clusterId) {
        // Check and advance, this will also handle finalization if the cluster is now complete
        // and not split.
        checkAndAdvanceIfComplete(currentBatch.clusterId);
      }
    }
    processingBatchIdRef.current = null;
    setIsProcessingQueue(false);
  }, [reviewQueue, isProcessingQueue, toast, currentUser, checkAndAdvanceIfComplete, retryFailedBatch]);


  const currentVisualizationData = useMemo((): EntityVisualizationDataResponse | ServiceVisualizationDataResponse | null => {
    if (!selectedClusterId) return null;
    return visualizationData[selectedClusterId]?.data || null;
  }, [selectedClusterId, visualizationData]);

  const currentConnectionData = useMemo((): EntityConnectionDataResponse | ServiceConnectionDataResponse | null => {
    if (!selectedEdgeId) return null;
    return connectionData[selectedEdgeId]?.data || null;
  }, [selectedEdgeId, connectionData]);

  const selectedClusterDetails = useMemo((): EntityCluster | ServiceCluster | null => {
    if (!selectedClusterId) return null;
    return clusters.data.find(c => c.id === selectedClusterId) || null;
  }, [selectedClusterId, clusters.data]);


  const edgeSelectionInfo = useMemo((): EdgeSelectionInfo => {
    if (!currentVisualizationData?.links)
      return { currentEdgeId: selectedEdgeId, nextUnreviewedEdgeId: null, hasUnreviewedEdges: false, currentEdgeIndex: -1, totalEdges: 0 };
    const { links } = currentVisualizationData;
    const totalEdges = links.length;

    const findNextAfter = (afterId: string | null): string | null => {
      let startIndex = 0;
      if (afterId) {
        const idx = links.findIndex((l) => l.id === afterId);
        if (idx !== -1) startIndex = idx + 1;
      }
      for (let i = 0; i < links.length; i++) {
        const currentIndex = (startIndex + i) % links.length;
        if (links[currentIndex].status === "PENDING_REVIEW") return links[currentIndex].id;
      }
      return null;
    };
    const nextUnreviewedEdgeId = findNextAfter(lastReviewedEdgeId || selectedEdgeId);
    const unreviewedLinksCount = links.filter(link => link.status === "PENDING_REVIEW").length;

    return {
      currentEdgeId: selectedEdgeId, nextUnreviewedEdgeId,
      hasUnreviewedEdges: unreviewedLinksCount > 0,
      currentEdgeIndex: selectedEdgeId ? links.findIndex((link) => link.id === selectedEdgeId) : -1,
      totalEdges,
    };
  }, [currentVisualizationData, selectedEdgeId, lastReviewedEdgeId]);

  useEffect(() => {
    loadClusters(1, clusters.limit);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [resolutionMode]);

  useEffect(() => {
    if (!selectedClusterId && clusters.data.length > 0 && !clusters.loading) {
      const firstNonSplitUncompletedCluster = clusters.data.find(
        (c) => !(c as BaseCluster).wasSplit && !queries.getClusterProgress(c.id).isComplete
      );
      if (firstNonSplitUncompletedCluster) {
        console.log("Auto-selecting first non-split, uncompleted cluster:", firstNonSplitUncompletedCluster.id);
        handleSetSelectedClusterId(firstNonSplitUncompletedCluster.id);
      } else if (clusters.data.length > 0) {
        const firstCluster = clusters.data[0];
         console.log("All clusters on current page are split or complete. Auto-selecting first cluster for viewing:", firstCluster.id);
        handleSetSelectedClusterId(firstCluster.id); // Will set auto-advance OFF due to completeness/split status
      }
    }
  }, [selectedClusterId, clusters.data, clusters.loading, queries, handleSetSelectedClusterId]);

  const actions = useMemo(
    () => ({
      setResolutionMode, setSelectedClusterId: handleSetSelectedClusterId,
      setSelectedEdgeId: setSelectedEdgeIdAction, setReviewerId, setLastReviewedEdgeId,
      triggerRefresh, loadClusters, preloadVisualizationData, loadConnectionData,
      invalidateVisualizationData, invalidateConnectionData, clearAllData,
      selectNextUnreviewedEdge, advanceToNextCluster, checkAndAdvanceIfComplete,
      submitEdgeReview, retryFailedBatch, setIsAutoAdvanceEnabled: setIsAutoAdvanceEnabledState,
    }),
    [
      setResolutionMode, handleSetSelectedClusterId, setSelectedEdgeIdAction, setReviewerId,
      setLastReviewedEdgeId, triggerRefresh, loadClusters, preloadVisualizationData,
      loadConnectionData, invalidateVisualizationData, invalidateConnectionData,
      clearAllData, selectNextUnreviewedEdge, advanceToNextCluster,
      checkAndAdvanceIfComplete, submitEdgeReview, retryFailedBatch, setIsAutoAdvanceEnabledState
    ]
  );

  useEffect(() => {
    if (selectedClusterId && currentVisualizationData?.links && !selectedEdgeId) {
        const clusterDetail = queries.getClusterById(selectedClusterId);
        if (clusterDetail && !clusterDetail.wasSplit) { // Only select edge if cluster is not split
            if (edgeSelectionInfo.hasUnreviewedEdges) {
                console.log(`Cluster ${selectedClusterId} active, non-split, data loaded, no edge selected, unreviewed edges exist. Triggering edge selection.`);
                actions.selectNextUnreviewedEdge();
            } else {
                console.log(`Cluster ${selectedClusterId} active, non-split, data loaded, no edge selected, NO unreviewed edges. Checking completion/advance.`);
                actions.checkAndAdvanceIfComplete(selectedClusterId);
            }
        } else if (clusterDetail?.wasSplit) {
            console.log(`Cluster ${selectedClusterId} is split. No edge selection. Checking advance.`);
            actions.checkAndAdvanceIfComplete(selectedClusterId); // Check if we should advance from this split cluster
        }
    }
  }, [selectedClusterId, currentVisualizationData, selectedEdgeId, edgeSelectionInfo, actions, queries]); // Added queries

  useEffect(() => {
    if (selectedEdgeId) {
      const currentEdgeState = connectionData[selectedEdgeId];
      if (
        (!currentEdgeState?.data || currentEdgeState?.error ||
         (currentEdgeState?.lastUpdated && Date.now() - currentEdgeState.lastUpdated > 300000)) &&
        !currentEdgeState?.loading
      ) {
        console.log(`Fetching connection data for selected edge: ${selectedEdgeId}`);
        actions.loadConnectionData(selectedEdgeId);
      }
    }
  }, [selectedEdgeId, connectionData, actions]);

  useEffect(() => { if (currentUser?.id) setReviewerId(currentUser.id); }, [currentUser]);

  useEffect(() => {
    if (reviewQueue.length > 0 && !isProcessingQueue && processReviewQueue) {
      const timeoutId = setTimeout(processReviewQueue, 500);
      return () => clearTimeout(timeoutId);
    }
  }, [reviewQueue, isProcessingQueue, processReviewQueue]);

  const contextValue: EntityResolutionContextType = {
    resolutionMode, selectedClusterId, selectedEdgeId, reviewerId, lastReviewedEdgeId,
    refreshTrigger, isAutoAdvanceEnabled, clusters, visualizationData, connectionData,
    reviewQueue, isProcessingQueue,
    clusterProgress: useMemo(() => {
      const reconstructed: Record<string, ClusterProgress> = {};
      clusters.data.forEach((c) => {
        reconstructed[c.id] = queries.getClusterProgress(c.id); // getClusterProgress now considers was_split
      });
      if (selectedClusterId && !reconstructed[selectedClusterId] && visualizationData[selectedClusterId]?.data?.links) {
        reconstructed[selectedClusterId] = queries.getClusterProgress(selectedClusterId);
      }
      return reconstructed;
    }, [clusters.data, selectedClusterId, visualizationData, queries]),
    edgeSelectionInfo, currentVisualizationData, currentConnectionData,
    selectedClusterDetails, // Added
    actions, queries,
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
    throw new Error("useEntityResolution must be used within an EntityResolutionProvider");
  }
  return context;
}

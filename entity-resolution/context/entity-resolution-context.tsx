// context/entity-resolution-context.tsx
"use client";

import {
  ResolutionMode,
  EntityCluster,
  EntityVisualizationDataResponse,
  EntityConnectionDataResponse,
  ServiceConnectionDataResponse,
  EntityGroup,
  EntityLink,
  EntityGroupReviewApiPayload,
  ServiceVisualizationDataResponse,
  ServiceGroup,
  ServiceLink,
  ServiceGroupReviewApiPayload,
  GroupReviewDecision,
  ClusterFinalizationStatusResponse,
  QueuedReviewBatch,
  BaseCluster,
  BaseLink,
  NodeDetailResponse,
  NodeIdentifier,
  BulkConnectionRequestItem,
  BulkVisualizationRequestItem,
  isEntityConnectionData,
  isServiceConnectionData,
  ClusterReviewProgress, 
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
  return Array.from(new Map(items.map(item => [keySelector(item), item])).values());
}

// Constants for bulk fetching and large cluster handling
const MAX_BULK_FETCH_SIZE = 200; 
const BULK_FETCH_DELAY_MS = 200;
const LARGE_CLUSTER_THRESHOLD = 200;
const CONNECTION_PAGE_SIZE = MAX_BULK_FETCH_SIZE;


// Sleep utility function
function sleep(ms: number) {
  return new Promise(resolve => setTimeout(resolve, ms));
}


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

  clusters: ClustersState<EntityCluster>;
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
  nodeDetails: Record<string, NodeDetailResponse | null | "loading" | "error">;

  reviewQueue: QueuedReviewBatch[];
  isProcessingQueue: boolean;

  clusterProgress: Record<string, ClusterReviewProgress>; 
  edgeSelectionInfo: EdgeSelectionInfo;

  currentVisualizationData:
    | EntityVisualizationDataResponse
    | ServiceVisualizationDataResponse
    | null;
  currentConnectionData:
    | EntityConnectionDataResponse
    | ServiceConnectionDataResponse
    | null;
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
    ) => Promise<
      EntityConnectionDataResponse | ServiceConnectionDataResponse | null
    >;
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
    initializeLargeClusterConnectionPaging: (clusterId: string) => Promise<void>;
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
    getClusterById: (
      clusterId: string
    ) => EntityCluster | undefined;
    getNodeDetail: (
      nodeId: string
    ) => NodeDetailResponse | null | "loading" | "error";
  };
}

const EntityResolutionContext = createContext<
  EntityResolutionContextType | undefined
>(undefined);

const MAX_REVIEW_ATTEMPTS = 3;

const initialClustersState: ClustersState<EntityCluster> = {
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
    useState<ClustersState<EntityCluster>>(
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

  const [activelyPagingClusterId, setActivelyPagingClusterId] = useState<string | null>(null);
  const [largeClusterConnectionsPage, setLargeClusterConnectionsPage] = useState<number>(0); 
  const [isLoadingConnectionPageData, setIsLoadingConnectionPageData] = useState<boolean>(false);


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
          const reviewedCount = linksForProgress.filter(l => l.status !== "PENDING_REVIEW").length;
          return {
            // clusterId: clusterIdToQuery, // Removed: Not part of ClusterReviewProgress type
            totalEdges: linksForProgress.length,
            reviewedEdges: reviewedCount, // All edges considered reviewed if split
            pendingEdges: 0,
            confirmedMatches:
              linksForProgress.filter(
                (l) => l.status === "CONFIRMED_MATCH"
              ).length,
            confirmedNonMatches:
              linksForProgress.filter(
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
                totalEdges: 0, reviewedEdges: 0, pendingEdges: 0,
                confirmedMatches: 0, confirmedNonMatches: 0, progressPercentage: 100, isComplete: true,
              };
            } else if (count && count > 0) { 
                 return {
                    totalEdges: -1, reviewedEdges: 0, pendingEdges: -1, 
                    confirmedMatches: 0, confirmedNonMatches: 0, progressPercentage: -1, isComplete: false, 
                 };
            }
          }
          return {
            totalEdges: 0, reviewedEdges: 0, pendingEdges: 0,
            confirmedMatches: 0, confirmedNonMatches: 0, progressPercentage: 0, isComplete: false,
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
          totalEdges > 0 ? Math.round((reviewedEdges / totalEdges) * 100) : (totalEdges === 0 ? 100 : 0);
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
      resolutionMode, 
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
      const uniqueNodesToFetch = uniqueBy(nodesToFetch, node => `${node.id}-${node.nodeType}`);
      const trulyNeedsFetching = uniqueNodesToFetch.filter(
        (node) => !nodeDetails[node.id] || nodeDetails[node.id] === "error"
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

      const allFetchedNodeDetails: NodeDetailResponse[] = [];
      let anyChunkFailed = false;

      for (let i = 0; i < trulyNeedsFetching.length; i += MAX_BULK_FETCH_SIZE) {
        const chunk = trulyNeedsFetching.slice(i, i + MAX_BULK_FETCH_SIZE);
        if (i > 0) {
          await sleep(BULK_FETCH_DELAY_MS);
        }
        console.log(`BulkNodeDetails: Fetching chunk ${i / MAX_BULK_FETCH_SIZE + 1} of ${Math.ceil(trulyNeedsFetching.length / MAX_BULK_FETCH_SIZE)} (size: ${chunk.length})`);
        try {
          const response = await getBulkNodeDetails({ items: chunk });
          allFetchedNodeDetails.push(...response);
          setNodeDetails((prev) => {
            const newState = { ...prev };
            response.forEach((detail) => {
              newState[detail.id] = detail;
            });
            chunk.forEach(requestedNode => {
                if(!response.find(r => r.id === requestedNode.id)) {
                    newState[requestedNode.id] = "error";
                     console.warn(`BulkNodeDetails: Node ${requestedNode.id} (chunk) was requested but not found in response.`);
                }
            });
            return newState;
          });
        } catch (error) {
          console.error(
            `BulkNodeDetails: Error loading chunk ${i / MAX_BULK_FETCH_SIZE + 1}:`,
            error
          );
          anyChunkFailed = true;
          setNodeDetails((prev) => {
            const newState = { ...prev };
            chunk.forEach((node) => {
              newState[node.id] = "error";
            });
            return newState;
          });
        }
      }

      if (anyChunkFailed) {
        toast({
          title: "Bulk Node Details Error",
          description: "Could not load details for some nodes. Some chunks failed.",
          variant: "destructive",
        });
      } else {
        console.log(
          `BulkNodeDetails: Successfully processed all chunks for ${allFetchedNodeDetails.length} nodes.`
        );
      }
        setNodeDetails((prev) => {
            const newState = { ...prev };
            trulyNeedsFetching.forEach(requestedNode => {
                if (!allFetchedNodeDetails.find(r => r.id === requestedNode.id) && newState[requestedNode.id] !== "error") { 
                    newState[requestedNode.id] = "error";
                    console.warn(`BulkNodeDetails: Node ${requestedNode.id} (final check) was requested but not found in any response chunk.`);
                }
            });
            return newState;
        });

    },
    [nodeDetails, toast]
  );
  
  const loadVisualizationDataForClusters = useCallback(async (items: BulkVisualizationRequestItem[]) => {
    if (items.length === 0) {
      console.log("loadVisualizationDataForClusters: No items to fetch.");
      return;
    }
    const uniqueItems = uniqueBy(items, item => `${item.clusterId}-${item.itemType}`);
    console.log(`loadVisualizationDataForClusters: Fetching ${uniqueItems.length} visualizations.`);
    
    setVisualizationData(prev => {
        const newState = {...prev};
        uniqueItems.forEach(item => {
            if (!newState[item.clusterId] || newState[item.clusterId]?.error) {
                 newState[item.clusterId] = { data: null, loading: true, error: null, lastUpdated: null };
            } else if (newState[item.clusterId] && !newState[item.clusterId]?.loading) {
                 newState[item.clusterId]!.loading = true;
                 newState[item.clusterId]!.error = null;
            }
        });
        return newState;
    });

    const allFetchedVisualizations: (EntityVisualizationDataResponse | ServiceVisualizationDataResponse)[] = [];
    let anyChunkFailed = false;

    for (let i = 0; i < uniqueItems.length; i += MAX_BULK_FETCH_SIZE) {
        const chunk = uniqueItems.slice(i, i + MAX_BULK_FETCH_SIZE);
        if (i > 0 && chunk.length > 1) { 
            await sleep(BULK_FETCH_DELAY_MS);
        }
        console.log(`loadVisualizationDataForClusters: Fetching chunk ${i / MAX_BULK_FETCH_SIZE + 1} (size: ${chunk.length})`);
        try {
            const response = await getBulkVisualizations({ items: chunk }); 
            allFetchedVisualizations.push(...response);
            
            setVisualizationData(prev => {
                const newState = {...prev};
                response.forEach(vizData => {
                    newState[vizData.clusterId] = { data: vizData, loading: false, error: null, lastUpdated: Date.now() };
                });
                chunk.forEach(requestedItem => {
                    if (!response.find(r => r.clusterId === requestedItem.clusterId)) {
                        newState[requestedItem.clusterId] = { data: null, loading: false, error: "Not found in bulk response chunk", lastUpdated: null };
                    }
                });
                return newState;
            });

        } catch (error) {
            console.error(`loadVisualizationDataForClusters: Error loading chunk ${i / MAX_BULK_FETCH_SIZE + 1}:`, error);
            anyChunkFailed = true;
            setVisualizationData(prev => {
                const newState = {...prev};
                chunk.forEach(item => {
                     newState[item.clusterId] = { data: null, loading: false, error: (error as Error).message, lastUpdated: null };
                });
                return newState;
            });
        }
    }

    if (anyChunkFailed) {
        toast({
            title: "Bulk Visualization Error",
            description: "Could not load visualization data for some items.",
            variant: "destructive",
        });
    } else {
         console.log(`loadVisualizationDataForClusters: Successfully processed all chunks for ${allFetchedVisualizations.length} visualizations.`);
    }
    
    setVisualizationData(prev => {
        const newState = {...prev};
        uniqueItems.forEach(requestedItem => {
            if (!allFetchedVisualizations.find(r => r.clusterId === requestedItem.clusterId) && newState[requestedItem.clusterId]?.loading) {
                newState[requestedItem.clusterId] = { data: null, loading: false, error: "Not found in any bulk response chunk", lastUpdated: null };
            } else if (newState[requestedItem.clusterId]?.loading) {
                 newState[requestedItem.clusterId]!.loading = false;
            }
        });
        return newState;
    });

    const allNodeIdentifiers: NodeIdentifier[] = [];
    allFetchedVisualizations.forEach(vizData => {
        vizData.nodes.forEach(node => allNodeIdentifiers.push({ id: node.id, nodeType: resolutionMode }));
    });
    
    if (allNodeIdentifiers.length > 0) {
        await loadBulkNodeDetails(allNodeIdentifiers);
    }
    const connectionRequestItemsForNonLarge: BulkConnectionRequestItem[] = [];
    allFetchedVisualizations.forEach(vizData => {
        const clusterDetail = getClusterById(vizData.clusterId); 
        const connectionCount = clusterDetail ? (clusterDetail as EntityCluster)?.groupCount : undefined; 
        if (connectionCount !== undefined && connectionCount !== null && connectionCount <= LARGE_CLUSTER_THRESHOLD) { 
            vizData.links.forEach(link => connectionRequestItemsForNonLarge.push({ edgeId: link.id, itemType: resolutionMode }));
        }
    });
    if (connectionRequestItemsForNonLarge.length > 0) {
        await loadBulkConnections(connectionRequestItemsForNonLarge); 
    }


  }, [resolutionMode, toast, loadBulkNodeDetails, getClusterById]); 


  const loadBulkConnections = useCallback(async (items: BulkConnectionRequestItem[]) => {
    if (items.length === 0) {
        console.log("loadBulkConnections: No items to fetch.");
        return;
    }
    const uniqueItems = uniqueBy(items, item => `${item.edgeId}-${item.itemType}`);
    const trulyNeedsFetching = uniqueItems.filter(item => {
        const existing = connectionData[item.edgeId];
        return !existing || !existing.data || existing.error; 
    });

    if (trulyNeedsFetching.length === 0) {
        console.log("loadBulkConnections: All requested connection details already loaded or loading.");
        return;
    }

    console.log(`loadBulkConnections: Fetching ${trulyNeedsFetching.length} connections.`);
    
    setConnectionData(prev => {
        const newState = {...prev};
        trulyNeedsFetching.forEach(item => {
             if (!newState[item.edgeId] || newState[item.edgeId]?.error) { 
                newState[item.edgeId] = { data: null, loading: true, error: null, lastUpdated: null };
             } else if (newState[item.edgeId] && !newState[item.edgeId]?.loading) { 
                newState[item.edgeId]!.loading = true;
                newState[item.edgeId]!.error = null;
             }
        });
        return newState;
    });

    const allFetchedConnections: (EntityConnectionDataResponse | ServiceConnectionDataResponse)[] = [];
    let anyChunkFailed = false;

    for (let i = 0; i < trulyNeedsFetching.length; i += MAX_BULK_FETCH_SIZE) {
        const chunk = trulyNeedsFetching.slice(i, i + MAX_BULK_FETCH_SIZE);
        if (i > 0) {
            await sleep(BULK_FETCH_DELAY_MS);
        }
        console.log(`loadBulkConnections: Fetching chunk ${i / MAX_BULK_FETCH_SIZE + 1} of ${Math.ceil(trulyNeedsFetching.length / MAX_BULK_FETCH_SIZE)} (size: ${chunk.length})`);
        try {
            const response = await getBulkConnections({ items: chunk });
            allFetchedConnections.push(...response);

            setConnectionData(prev => {
                const newState = {...prev};
                response.forEach(connData => {
                    const edgeId = connData.edge.id;
                    newState[edgeId] = { data: connData, loading: false, error: null, lastUpdated: Date.now() };
                });
                chunk.forEach(requestedItem => {
                    if (!response.find(r => r.edge.id === requestedItem.edgeId)) {
                         newState[requestedItem.edgeId] = { data: null, loading: false, error: "Not found in bulk response chunk", lastUpdated: null };
                    }
                });
                return newState;
            });
        } catch (error) {
            console.error(`loadBulkConnections: Error loading chunk ${i / MAX_BULK_FETCH_SIZE + 1}:`, error);
            anyChunkFailed = true;
            setConnectionData(prev => {
                const newState = {...prev};
                chunk.forEach(item => {
                     newState[item.edgeId] = { data: null, loading: false, error: (error as Error).message, lastUpdated: null };
                });
                return newState;
            });
        }
    }
    
    if (anyChunkFailed) {
        toast({
            title: "Bulk Connection Error",
            description: "Could not load connection data for some items. Some chunks failed.",
            variant: "destructive",
        });
    } else {
        console.log(`loadBulkConnections: Successfully processed all chunks for ${allFetchedConnections.length} connections.`);
    }
    
    setConnectionData(prev => {
        const newState = {...prev};
        trulyNeedsFetching.forEach(requestedItem => {
            if (!allFetchedConnections.find(r => r.edge.id === requestedItem.edgeId) && newState[requestedItem.edgeId]?.loading) {
                 newState[requestedItem.edgeId] = { data: null, loading: false, error: "Not found in any bulk response chunk", lastUpdated: null };
            } else if (newState[requestedItem.edgeId]?.loading) { 
                newState[requestedItem.edgeId]!.loading = false;
            }
        });
        return newState;
    });

  }, [toast, connectionData]); 
  
  const loadVisualizationDataForClustersRef = useRef(loadVisualizationDataForClusters);
  const loadBulkConnectionsRef = useRef(loadBulkConnections);
  useEffect(() => {
    loadVisualizationDataForClustersRef.current = loadVisualizationDataForClusters;
  }, [loadVisualizationDataForClusters]);
  useEffect(() => {
    loadBulkConnectionsRef.current = loadBulkConnections;
  }, [loadBulkConnections]);


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
        resolutionMode === "entity" ? getOrganizationClusters : getServiceClusters;

      try {
        const response = await fetcher(page, limit);
        console.log(
          `Successfully loaded clusters for mode ${resolutionMode}`,
          response
        );
        setClusters({
          data: response.clusters as Array<EntityCluster>,
          total: response.total,
          page,
          limit,
          loading: false,
          error: null,
        });

        const vizRequestItemsNormal: BulkVisualizationRequestItem[] = [];
        response.clusters.forEach(c => {
            const clusterDetail = c as EntityCluster;
            const connectionCount = clusterDetail.groupCount;

            if (connectionCount !== undefined && connectionCount !== null && connectionCount <= LARGE_CLUSTER_THRESHOLD) { 
                vizRequestItemsNormal.push({ clusterId: c.id, itemType: resolutionMode });
            } else {
                console.log(`Cluster ${c.id} is large (${connectionCount} connections). Deferring full data load.`);
            }
        });
        
        if (vizRequestItemsNormal.length > 0) {
            await loadVisualizationDataForClustersRef.current(vizRequestItemsNormal);
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
        }));
      }
    },
    [resolutionMode, loadVisualizationDataForClustersRef] 
  );

  const handleSetSelectedClusterId = useCallback(
    async (id: string | null) => {
      const previousSelectedClusterId = selectedClusterId;
      console.log(
        `Setting selected cluster ID to: ${id}. Previous: ${previousSelectedClusterId}`
      );

      if (id === previousSelectedClusterId) return; 

      setSelectedClusterIdState(id);
      setSelectedEdgeIdState(null); 
      setLastReviewedEdgeId(null);

      if (id !== activelyPagingClusterId) {
        setActivelyPagingClusterId(null);
        setLargeClusterConnectionsPage(0);
      }

      if (id) {
        const clusterDetail = getClusterById(id); 
        const connectionCount = clusterDetail ? clusterDetail.groupCount : undefined;

        const isLarge = connectionCount !== undefined && connectionCount !== null && connectionCount > LARGE_CLUSTER_THRESHOLD;

        if (
          clusterDetail?.wasSplit ||
          (!isLarge && queries.getClusterProgress(id).isComplete) 
        ) {
          console.log(
            `Cluster ${id} is already split or complete. Pausing auto-advance.`
          );
          setIsAutoAdvanceEnabledState(false);
        } else {
          console.log(
            `New cluster ${id} selected. Ensuring auto-advance is ON (unless it's large and uninitialized).`
          );
          if (!(isLarge && largeClusterConnectionsPage === 0)) { 
             setIsAutoAdvanceEnabledState(true);
          }
        }
        
        if (!isLarge) {
            const vizState = visualizationData[id];
            if ((!vizState?.data || vizState?.error) && !vizState?.loading) {
            console.log(
                `Visualization data for selected non-large cluster ${id} not loaded or errored. Fetching...`
            );
            await loadVisualizationDataForClustersRef.current([{ clusterId: id, itemType: resolutionMode }]);
            } else if (vizState?.data?.nodes) { 
                const nodeIdsFromViz: NodeIdentifier[] = vizState.data.nodes.map(
                    (node) => ({ id: node.id, nodeType: resolutionMode })
                );
                if (nodeIdsFromViz.length > 0) {
                    const nodesTrulyNeedingFetch = nodeIdsFromViz.filter(n => !nodeDetails[n.id] || nodeDetails[n.id] === 'error');
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
                    const nodesTrulyNeedingFetch = nodeIdsFromViz.filter(n => !nodeDetails[n.id] || nodeDetails[n.id] === 'error');
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
      loadVisualizationDataForClustersRef,
      loadBulkNodeDetails,
      nodeDetails,
      setIsAutoAdvanceEnabledState,
      getClusterById, 
    ]
  );


  const loadConnectionDataForLinkPage = useCallback(async (clusterId: string, pageToLoad: number, isPrefetch: boolean = false) => {
    const viz = visualizationData[clusterId]?.data;
    if (!viz || !viz.links) {
        console.warn(`loadConnectionDataForLinkPage: No visualization data or links for cluster ${clusterId}.`);
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
        console.log(`loadConnectionDataForLinkPage: No links for page ${pageToLoad} in cluster ${clusterId}.`);
        if (!isPrefetch) setIsLoadingConnectionPageData(false);
        return;
    }

    const connectionItemsToFetch: BulkConnectionRequestItem[] = linksForPage
        .map(link => ({ edgeId: link.id, itemType: resolutionMode }))
        .filter(item => { 
            const connState = connectionData[item.edgeId];
            return !connState || !connState.data || connState.error;
        });
    
    console.log(`loadConnectionDataForLinkPage: Fetching connection data for ${connectionItemsToFetch.length} links on page ${pageToLoad} of cluster ${clusterId}. Prefetch: ${isPrefetch}`);

    if (connectionItemsToFetch.length > 0) {
        try {
            await loadBulkConnectionsRef.current(connectionItemsToFetch);
        } catch (error) {
            console.error(`Error loading connection data for page ${pageToLoad}, cluster ${clusterId}:`, error);
            toast({ title: "Error Loading Connections", description: (error as Error).message, variant: "destructive" });
        }
    }
    
    if (!isPrefetch) {
        setIsLoadingConnectionPageData(false);
        const totalLinks = viz.links.length;
        if (endIndex < totalLinks) { 
            console.log(`Prefetching connection data for page ${pageToLoad + 1} of cluster ${clusterId}`);
            loadConnectionDataForLinkPage(clusterId, pageToLoad + 1, true).catch(err => {
                console.warn(`Error prefetching connection data for page ${pageToLoad + 1}:`, err);
            });
        }
    }
  }, [visualizationData, resolutionMode, connectionData, toast, loadBulkConnectionsRef]);


  const initializeLargeClusterConnectionPaging = useCallback(async (clusterId: string) => {
    console.log(`Initializing connection paging for large cluster: ${clusterId}`);
    setActivelyPagingClusterId(clusterId);
    setLargeClusterConnectionsPage(1); 
    setIsLoadingConnectionPageData(true);

    let viz = visualizationData[clusterId]?.data;
    if (!viz || visualizationData[clusterId]?.error || !visualizationData[clusterId]?.data?.links) { 
        console.log(`Fetching/Re-fetching visualization data (nodes and all links) for large cluster ${clusterId} before paging connections.`);
        try {
            await loadVisualizationDataForClustersRef.current([{ clusterId, itemType: resolutionMode }]);
            viz = visualizationData[clusterId]?.data; 
            if (!viz || !viz.links) throw new Error("Visualization data (with links) still not available after fetch.");

            const nodeIdsFromViz: NodeIdentifier[] = viz.nodes.map(
                (node) => ({ id: node.id, nodeType: resolutionMode })
            );
            if (nodeIdsFromViz.length > 0) {
                const nodesTrulyNeedingFetch = nodeIdsFromViz.filter(n => !nodeDetails[n.id] || nodeDetails[n.id] === 'error');
                if (nodesTrulyNeedingFetch.length > 0) {
                    await loadBulkNodeDetails(nodesTrulyNeedingFetch);
                }
            }

        } catch (error) {
            toast({ title: "Error Initializing Cluster", description: `Failed to load visualization for ${clusterId}: ${(error as Error).message}`, variant: "destructive" });
            setIsLoadingConnectionPageData(false);
            setActivelyPagingClusterId(null); 
            setLargeClusterConnectionsPage(0);
            return;
        }
    } else {
        const nodeIdsFromViz: NodeIdentifier[] = viz.nodes.map(
            (node) => ({ id: node.id, nodeType: resolutionMode })
        );
        if (nodeIdsFromViz.length > 0) {
            const nodesTrulyNeedingFetch = nodeIdsFromViz.filter(n => !nodeDetails[n.id] || nodeDetails[n.id] === 'error');
            if (nodesTrulyNeedingFetch.length > 0) {
                await loadBulkNodeDetails(nodesTrulyNeedingFetch);
            }
        }
    }
    
    await loadConnectionDataForLinkPage(clusterId, 1, false);
    setSelectedEdgeIdState(null); 

  }, [resolutionMode, visualizationData, toast, loadConnectionDataForLinkPage, nodeDetails, loadBulkNodeDetails]);


  const viewNextConnectionPage = useCallback(async (clusterId: string) => {
    if (clusterId !== activelyPagingClusterId) {
        console.warn("viewNextConnectionPage called for a cluster that is not actively paging.");
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
        console.log(`Viewing next connection page ${nextPage} for cluster ${clusterId}`);
        setLargeClusterConnectionsPage(nextPage);
        setSelectedEdgeIdState(null); 
        await loadConnectionDataForLinkPage(clusterId, nextPage, false);
    } else {
        toast({ title: "No More Connections", description: "You have reached the end of the connections for this cluster." });
    }
  }, [activelyPagingClusterId, largeClusterConnectionsPage, visualizationData, toast, loadConnectionDataForLinkPage]);


  const loadSingleConnectionData = useCallback(
    async (
      edgeId: string
    ): Promise<
      EntityConnectionDataResponse | ServiceConnectionDataResponse | null
    > => {
      const cached = connectionData[edgeId];
      console.log(
        `Loading single connection data for edge: ${edgeId}, mode: ${resolutionMode}`
      );
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
        if (isEntityConnectionData(connItem)) {
            if (connItem.entity1) nodesToLoad.push({ id: connItem.entity1.id, nodeType: resolutionMode });
            if (connItem.entity2) nodesToLoad.push({ id: connItem.entity2.id, nodeType: resolutionMode });
        } else if (isServiceConnectionData(connItem)) {
            if (connItem.service1) nodesToLoad.push({ id: connItem.service1.id, nodeType: resolutionMode });
            if (connItem.service2) nodesToLoad.push({ id: connItem.service2.id, nodeType: resolutionMode });
        }
        if (nodesToLoad.length > 0) {
            const nodesTrulyNeedingFetch = nodesToLoad.filter(n => !nodeDetails[n.id] || nodeDetails[n.id] === 'error');
            if (nodesTrulyNeedingFetch.length > 0) await loadBulkNodeDetails(nodesTrulyNeedingFetch);
        }
        return cached.data;
      }

      console.log(`Fetching connection data for edge ${edgeId} via bulk (size 1).`);
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
        await loadBulkConnectionsRef.current([{ edgeId, itemType: resolutionMode }]);
        const updatedConnState = connectionData[edgeId]; 
        if (updatedConnState?.data) {
            const nodesToLoad: NodeIdentifier[] = [];
            if (isEntityConnectionData(updatedConnState.data)) {
                if (updatedConnState.data.entity1) nodesToLoad.push({ id: updatedConnState.data.entity1.id, nodeType: resolutionMode });
                if (updatedConnState.data.entity2) nodesToLoad.push({ id: updatedConnState.data.entity2.id, nodeType: resolutionMode });
            } else if (isServiceConnectionData(updatedConnState.data)) {
                if (updatedConnState.data.service1) nodesToLoad.push({ id: updatedConnState.data.service1.id, nodeType: resolutionMode });
                if (updatedConnState.data.service2) nodesToLoad.push({ id: updatedConnState.data.service2.id, nodeType: resolutionMode });
            }
             if (nodesToLoad.length > 0) {
                const nodesTrulyNeedingFetch = nodesToLoad.filter(n => !nodeDetails[n.id] || nodeDetails[n.id] === 'error');
                if (nodesTrulyNeedingFetch.length > 0) await loadBulkNodeDetails(nodesTrulyNeedingFetch);
            }
        }
        return connectionData[edgeId]?.data || null;
      } catch (error) {
        console.error(`Error in loadSingleConnectionData wrapper for edge ${edgeId}:`, error);
        return null;
      }
    },
    [connectionData, resolutionMode, loadBulkConnectionsRef, loadBulkNodeDetails, nodeDetails]
  );

  const invalidateVisualizationData = useCallback(
    async (clusterId: string) => {
      console.log(`Invalidating and reloading visualization data for cluster: ${clusterId}`);
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
        await loadVisualizationDataForClustersRef.current([{ clusterId, itemType: resolutionMode }]);
      }
    },
    [resolutionMode, loadVisualizationDataForClustersRef, activelyPagingClusterId, initializeLargeClusterConnectionPaging]
  );

  const invalidateConnectionData = useCallback(
    async (edgeId: string) => { 
      console.log(`Invalidating and reloading connection data for edge: ${edgeId}`);
       setConnectionData((prev) => ({
        ...prev,
        [edgeId]: {
          data: null,
          loading: true, 
          error: null,
          lastUpdated: null,
        },
      }));
      await loadBulkConnectionsRef.current([{ edgeId, itemType: resolutionMode }]);
    },
    [resolutionMode, loadBulkConnectionsRef]
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
      if (!queries.getClusterProgress(nextClusterOnPage.id).isComplete && !nextClusterOnPage.wasSplit) {
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
          console.log(
            `Finalizing cluster ${targetClusterId} for mode ${resolutionMode}.`
          );
          const finalizationResponse: ClusterFinalizationStatusResponse = await finalizer(targetClusterId);
          toast({
            title: "Cluster Finalization",
            description: `${finalizationResponse.message}. Status: ${finalizationResponse.status}`,
          });
          console.log(
            `Finalization response for ${targetClusterId}:`,
            finalizationResponse
          );
          
          if (finalizationResponse.status !== 'ERROR' && finalizationResponse.status !== 'CLUSTER_NOT_FOUND' && finalizationResponse.status !== 'PENDING_FULL_REVIEW') {
            setClusters(prevClusters => {
              const newClusterData = prevClusters.data.map(c => {
                if (c.id === targetClusterId) {
                  let updatedCluster = { ...c };
                  if (finalizationResponse.status === 'COMPLETED_MARKED_AS_SPLIT' || finalizationResponse.status === 'COMPLETED_SPLIT_DETECTED') { 
                    updatedCluster.wasSplit = true;
                  } else if (finalizationResponse.status === 'COMPLETED_NO_SPLIT_NEEDED' || finalizationResponse.status === 'COMPLETED_NO_CONFIRMED_MATCHES') {
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
        }
      } else if ((progress.isComplete && progress.totalEdges >= 0) || clusterDetail?.wasSplit) { 
        if (targetClusterId === selectedClusterId && isAutoAdvanceEnabled) {
          console.log(
            `Cluster ${targetClusterId} is already split or is an empty/completed cluster. Auto-advance ON. Advancing.`
          );
          advanceToNextCluster();
        } else if ((progress.isComplete && progress.totalEdges >= 0) || clusterDetail?.wasSplit) {
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
      resolutionMode,
      isAutoAdvanceEnabled,
      getClusterById, 
      activelyPagingClusterId, 
    ]
  );

  const currentVisualizationDataForSelection = useMemo(():
    | EntityVisualizationDataResponse
    | ServiceVisualizationDataResponse
    | null => {
    if (!selectedClusterId) return null;

    const vizState = visualizationData[selectedClusterId];
    if (!vizState?.data) return null;

    if (selectedClusterId === activelyPagingClusterId && largeClusterConnectionsPage > 0) {
        const allLinks = vizState.data.links; 
        const startIndex = (largeClusterConnectionsPage - 1) * CONNECTION_PAGE_SIZE;
        const endIndex = startIndex + CONNECTION_PAGE_SIZE;
        const pagedLinks = allLinks.slice(startIndex, endIndex);
        return { ...vizState.data, links: pagedLinks };
    }
    return vizState.data;
  }, [selectedClusterId, visualizationData, activelyPagingClusterId, largeClusterConnectionsPage]);


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

      if (!currentVizForSelection?.links || currentVizForSelection.links.length === 0) {
        if (currentClusterId === activelyPagingClusterId) {
            const fullViz = visualizationData[currentClusterId]?.data;
            if (fullViz && fullViz.links.length > (largeClusterConnectionsPage * CONNECTION_PAGE_SIZE)) {
                console.log(`No more unreviewed links on current page ${largeClusterConnectionsPage} of large cluster ${currentClusterId}. Checking advance or if more pages exist.`);
                 checkAndAdvanceIfComplete(currentClusterId); 
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
          console.log(`Next unreviewed edge found in current view: ${link.id}. Selecting.`);
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
      getClusterById, 
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

    const currentLinksForNav = currentVisualizationDataForSelection?.links || [];
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
    getClusterById, 
    currentVisualizationDataForSelection 
  ]);

  const selectPreviousUnreviewedInCluster = useCallback(() => {
    const currentClusterId = selectedClusterId;
    if (!currentClusterId) return;

    const clusterDetail = queries.getClusterById(currentClusterId);
    if (clusterDetail?.wasSplit) {
      checkAndAdvanceIfComplete(currentClusterId);
      return;
    }

    const currentLinksForNav = currentVisualizationDataForSelection?.links || [];
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
        prevEdgeToSelectId = unreviewedLinksInView[unreviewedLinksInView.length - 1].id;
      }
    } else { 
      prevEdgeToSelectId = unreviewedLinksInView[unreviewedLinksInView.length - 1].id;
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
    getClusterById, 
    currentVisualizationDataForSelection 
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
        if (!connDataState?.data && !connDataState?.loading) loadSingleConnectionData(edgeId);
        if (!vizDataState?.data && selectedClusterId && !vizDataState?.loading) {
            const currentCluster = queries.getClusterById(selectedClusterId);
            const connectionCount = currentCluster ? currentCluster.groupCount : undefined;
            if (connectionCount === undefined || connectionCount === null || connectionCount <= LARGE_CLUSTER_THRESHOLD) { 
                 invalidateVisualizationData(selectedClusterId);
            } else {
                console.error("Missing full visualization data for a large cluster during review submission.");
                if (selectedClusterId === activelyPagingClusterId) {
                    initializeLargeClusterConnectionPaging(selectedClusterId);
                }
            }
        }
        return;
      }
      
      const connData = connDataState.data;
      const fullVizData = vizDataState.data; 


      let relevantGroups: Array<EntityGroup | ServiceGroup> = [];
      let currentEdgeLink: EntityLink | ServiceLink | undefined;

      if (resolutionMode === "entity" && isEntityConnectionData(connData)) {
        relevantGroups = connData.entityGroups.filter(
          (group) =>
            (group.entityId1 === connData.edge.entityId1 &&
              group.entityId2 === connData.edge.entityId2) ||
            (group.entityId1 === connData.edge.entityId2 &&
              group.entityId2 === connData.edge.entityId1)
        );
        currentEdgeLink = fullVizData.links.find((l) => l.id === edgeId) as EntityLink | undefined;
      } else if (resolutionMode === "service" && isServiceConnectionData(connData)) {
         relevantGroups = connData.serviceGroups.filter(
          (group) =>
            (group.serviceId1 === connData.edge.serviceId1 && 
              group.serviceId2 === connData.edge.serviceId2) ||
            (group.serviceId1 === connData.edge.serviceId2 &&
              group.serviceId2 === connData.edge.serviceId1)
        );
        currentEdgeLink = fullVizData.links.find((l) => l.id === edgeId) as ServiceLink | undefined;
      }


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
              data: newTargetClusterViz as typeof targetClusterVizState, 
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

      setVisualizationData((prevVizData) => {
        const newVizDataState = { ...prevVizData };
        const targetClusterVizState = newVizDataState[batch.clusterId];
        if (targetClusterVizState?.data) {
          const targetClusterViz = { ...targetClusterVizState.data }; 
          const linkIndex = targetClusterViz.links.findIndex(
            (l) => l.id === batch.edgeId
          );
          if (linkIndex !== -1) {
            targetClusterViz.links = [...targetClusterViz.links]; 
            targetClusterViz.links[linkIndex] = {
              ...targetClusterViz.links[linkIndex],
              status: batch.optimisticEdgeStatus,
            };
          }
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
            data: targetClusterViz as typeof targetClusterVizState.data, 
            lastUpdated: Date.now(),
          };
        }
        return newVizDataState;
      });

      setConnectionData((prevConnData) => {
        const newConnDataState = { ...prevConnData };
        const targetConnState = newConnDataState[batch.edgeId];
        if (targetConnState?.data) {
          let targetConn = { ...targetConnState.data }; 
          
          if (isEntityConnectionData(targetConn, resolutionMode)) {
            targetConn = {
                ...targetConn,
                edge: { ...targetConn.edge, status: batch.optimisticEdgeStatus, confirmedStatus: batch.optimisticGroupStatus as any },
                entityGroups: targetConn.entityGroups.map(group => 
                    batch.operations.some(op => op.groupId === group.id) ? 
                    { ...group, confirmedStatus: batch.optimisticGroupStatus } : group
                )
            }
          } else if (isServiceConnectionData(targetConn, resolutionMode)) {
             targetConn = {
                ...targetConn,
                edge: { ...targetConn.edge, status: batch.optimisticEdgeStatus, confirmedStatus: batch.optimisticGroupStatus as any }, 
                serviceGroups: targetConn.serviceGroups.map(group => 
                    batch.operations.some(op => op.groupId === group.id) ? 
                    { ...group, confirmedStatus: batch.optimisticGroupStatus } : group
                )
            }
          }
          newConnDataState[batch.edgeId] = {
            ...targetConnState,
            data: targetConn as typeof targetConnState.data, 
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
      getClusterById, 
      activelyPagingClusterId, initializeLargeClusterConnectionPaging 
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
            prevQ.map((b) => (b.batchId === currentBatch.batchId ? { ...currentBatch, isTerminalFailure: true } : b))
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
        setVisualizationData((prevVizData) => { 
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
              data: targetClusterViz as typeof targetClusterVizState.data,
              lastUpdated: Date.now(),
            };
          }
          return newVizDataState;
        });
        setConnectionData((prevConnData) => {
          const newConnDataState = { ...prevConnData };
          const targetConnState = newConnDataState[currentBatch.edgeId];
          if (targetConnState?.data) {
            let targetConn = { ...targetConnState.data };
             if (isEntityConnectionData(targetConn, currentBatch.mode)) {
                targetConn = {
                    ...targetConn,
                    edge: { ...targetConn.edge, status: currentBatch.originalEdgeStatus, confirmedStatus: currentBatch.originalEdgeStatus as any },
                    entityGroups: targetConn.entityGroups.map(group => {
                        const op = currentBatch.operations.find(o => o.groupId === group.id);
                        return op ? { ...group, confirmedStatus: op.originalGroupStatus } : group;
                    })
                }
            } else if (isServiceConnectionData(targetConn, currentBatch.mode)) {
                targetConn = {
                    ...targetConn,
                    edge: { ...targetConn.edge, status: currentBatch.originalEdgeStatus, confirmedStatus: currentBatch.originalEdgeStatus as any },
                    serviceGroups: targetConn.serviceGroups.map(group => {
                        const op = currentBatch.operations.find(o => o.groupId === group.id);
                        return op ? { ...group, confirmedStatus: op.originalGroupStatus } : group;
                    })
                }
            }
            newConnDataState[currentBatch.edgeId] = {
              ...targetConnState,
              data: targetConn as typeof targetConnState.data,
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


  const currentConnectionData = useMemo(():
    | EntityConnectionDataResponse
    | ServiceConnectionDataResponse
    | null => {
    if (!selectedEdgeId) return null;
    return connectionData[selectedEdgeId]?.data || null;
  }, [selectedEdgeId, connectionData]);

  const selectedClusterDetails = useMemo(():
    | EntityCluster
    | null => {
    if (!selectedClusterId) return null;
    return clusters.data.find((c) => c.id === selectedClusterId) || null;
  }, [selectedClusterId, clusters.data]);

  const edgeSelectionInfo = useMemo((): EdgeSelectionInfo => {
    const currentViz = currentVisualizationDataForSelection; 
    const fullVizForTotalUnreviewed = selectedClusterId ? visualizationData[selectedClusterId]?.data : null;

    const defaultEdgeInfo = {
        currentEdgeId: selectedEdgeId,
        nextUnreviewedEdgeId: null,
        hasUnreviewedEdges: fullVizForTotalUnreviewed?.links?.some(link => !queries.isEdgeReviewed(link.id)) || false,
        currentEdgeIndex: -1,
        totalEdges: 0, 
        totalUnreviewedEdgesInCluster: fullVizForTotalUnreviewed?.links?.filter(link => !queries.isEdgeReviewed(link.id)).length || 0,
        currentUnreviewedEdgeIndexInCluster: -1,
        totalEdgesInEntireCluster: fullVizForTotalUnreviewed?.links?.length || 0,
    };


    if (!currentViz?.links || currentViz.links.length === 0) {
      if (selectedEdgeId && fullVizForTotalUnreviewed?.links) { 
         const allUnreviewedLinksInCluster = fullVizForTotalUnreviewed.links.filter(link => !queries.isEdgeReviewed(link.id));
         defaultEdgeInfo.currentUnreviewedEdgeIndexInCluster = allUnreviewedLinksInCluster.findIndex(l => l.id === selectedEdgeId);
      }
      return defaultEdgeInfo;
    }

    const { links: linksInView } = currentViz; 
    const totalEdgesInView = linksInView.length;
    const allLinksInCluster = fullVizForTotalUnreviewed?.links || [];
    const allUnreviewedLinksInCluster = allLinksInCluster.filter(
      (link) => !queries.isEdgeReviewed(link.id)
    );
    const totalUnreviewedEdgesInEntireCluster = allUnreviewedLinksInCluster.length;

    let currentUnreviewedEdgeIndexInEntireCluster = -1;
    if (selectedEdgeId) {
      currentUnreviewedEdgeIndexInEntireCluster = allUnreviewedLinksInCluster.findIndex(
        (l) => l.id === selectedEdgeId
      );
    }
    
    let nextUnreviewedEdgeId: string | null = null;
    if (totalUnreviewedEdgesInEntireCluster > 0) {
        if (currentUnreviewedEdgeIndexInEntireCluster !== -1 && currentUnreviewedEdgeIndexInEntireCluster < allUnreviewedLinksInCluster.length -1) {
            nextUnreviewedEdgeId = allUnreviewedLinksInCluster[currentUnreviewedEdgeIndexInEntireCluster + 1].id;
        } else if (allUnreviewedLinksInCluster.length > 0 && currentUnreviewedEdgeIndexInEntireCluster === -1 && selectedEdgeId) {
            const currentSelectedFullIndex = allLinksInCluster.findIndex(l => l.id === selectedEdgeId);
            if (currentSelectedFullIndex !== -1) {
                for (let i = currentSelectedFullIndex + 1; i < allLinksInCluster.length; i++) {
                    if (!queries.isEdgeReviewed(allLinksInCluster[i].id)) {
                        nextUnreviewedEdgeId = allLinksInCluster[i].id;
                        break;
                    }
                }
                if (!nextUnreviewedEdgeId) { 
                     for (let i = 0; i < currentSelectedFullIndex; i++) {
                        if (!queries.isEdgeReviewed(allLinksInCluster[i].id)) {
                            nextUnreviewedEdgeId = allLinksInCluster[i].id;
                            break;
                        }
                    }
                }
            } else { 
                 nextUnreviewedEdgeId = allUnreviewedLinksInCluster[0]?.id || null;
            }
        } else if (allUnreviewedLinksInCluster.length > 0 && currentUnreviewedEdgeIndexInEntireCluster === -1 && !selectedEdgeId) {
            nextUnreviewedEdgeId = allUnreviewedLinksInCluster[0]?.id || null;
        }
    }


    return {
      currentEdgeId: selectedEdgeId,
      nextUnreviewedEdgeId, 
      hasUnreviewedEdges: totalUnreviewedEdgesInEntireCluster > 0,
      currentEdgeIndex: selectedEdgeId
        ? linksInView.findIndex((link) => link.id === selectedEdgeId) 
        : -1,
      totalEdges: totalEdgesInView, 
      totalUnreviewedEdgesInCluster: totalUnreviewedEdgesInEntireCluster,
      currentUnreviewedEdgeIndexInCluster: currentUnreviewedEdgeIndexInEntireCluster,
      totalEdgesInEntireCluster: allLinksInCluster.length,
    };
  }, [currentVisualizationDataForSelection, selectedEdgeId, lastReviewedEdgeId, selectedClusterId, visualizationData, queries]);

  useEffect(() => {
    if(clusters.data.length === 0 && !clusters.loading && !clusters.error){ 
        loadClusters(1, clusters.limit);
    }
  }, [resolutionMode, clusters.data.length, clusters.loading, loadClusters, clusters.limit, clusters.error]);

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
      setResolutionMode, handleSetSelectedClusterId, setSelectedEdgeIdAction,
      setReviewerId, setLastReviewedEdgeId, triggerRefresh, loadClusters,
      loadBulkNodeDetails, loadSingleConnectionData, invalidateVisualizationData,
      invalidateConnectionData, clearAllData, selectNextUnreviewedEdge,
      selectNextUnreviewedInCluster, selectPreviousUnreviewedInCluster,
      advanceToNextCluster, checkAndAdvanceIfComplete, submitEdgeReview,
      retryFailedBatch, setIsAutoAdvanceEnabledState,
      initializeLargeClusterConnectionPaging, viewNextConnectionPage,
      activelyPagingClusterId, largeClusterConnectionsPage, 
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
      const justLoadedNewPageForLargeCluster = selectedClusterId === activelyPagingClusterId && largeClusterConnectionsPage > 0 && !lastReviewedEdgeId; 

      if (isAutoAdvanceEnabled || !lastReviewedEdgeId || justLoadedNewPageForLargeCluster) { 
          actions.selectNextUnreviewedEdge();
      }
    }
  }, [selectedClusterId, currentVisualizationDataForSelection, selectedEdgeId, actions, isAutoAdvanceEnabled, lastReviewedEdgeId, activelyPagingClusterId, largeClusterConnectionsPage]);

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
      const needsProcessing = reviewQueue.some(b => !b.isTerminalFailure && (b.failedOperations.size === 0 || b.attempt < MAX_REVIEW_ATTEMPTS));
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
        (visualizationData[selectedClusterId]?.data?.links || getClusterById(selectedClusterId)) 
      ) {
        reconstructed[selectedClusterId] =
          queries.getClusterProgress(selectedClusterId);
      }
      return reconstructed;
    }, [clusters.data, selectedClusterId, visualizationData, queries, getClusterById]), 
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

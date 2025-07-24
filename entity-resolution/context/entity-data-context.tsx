/*
================================================================================
|
|   File: /context/entity-data-context.tsx - Phase 2: Data Loading & Caching
|
|   Description: Data management context for Entity Resolution
|   - Manages all API calls and data loading
|   - Handles caching and memory management
|   - Provides request deduplication
|   - Controls data invalidation and cleanup
|   - Consumes EntityStateContext for selection-driven loading
|   - 🔧 FIXED: Added optimistic update functions for immediate UI feedback
|
================================================================================
*/
"use client";

import {
  EntityCluster,
  EntityVisualizationDataResponse,
  EntityConnectionDataResponse,
  NodeDetailResponse,
  NodeIdentifier,
  BulkConnectionRequestItem,
  BulkVisualizationRequestItem,
  ClusterProgress,
  OverallProgress,
  ClusterProgressResponse,
  ResolutionMode,
  BaseLink,
} from "@/types/entity-resolution";
import {
  getServiceClusters,
  getBulkNodeDetails,
  getBulkConnections,
  getBulkVisualizations,
  getOrganizationClusters,
  getClusterProgress,
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
const CONNECTION_PAGE_SIZE = 200;

// ============================================================================
// State Interfaces (unchanged)
// ============================================================================

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

// ============================================================================
// Context Type - ENHANCED with Optimistic Updates
// ============================================================================

export interface EntityDataContextType {
  // Data stores
  clusters: ClustersState;
  visualizationData: Record<string, VisualizationState>;
  connectionData: Record<string, ConnectionState>;
  nodeDetails: Record<string, NodeDetailResponse | null | "loading" | "error">;

  // Server-side progress data
  serverProgress: Record<string, ClusterProgress>;
  overallServerProgress: OverallProgress | null;

  // Loading functions
  loadClusters: (page: number, limit?: number) => Promise<void>;
  loadClusterProgress: (page: number, limit?: number) => Promise<void>;
  loadVisualizationDataForClusters: (items: BulkVisualizationRequestItem[]) => Promise<void>;
  loadBulkConnections: (items: BulkConnectionRequestItem[]) => Promise<void>;
  loadBulkNodeDetails: (nodesToFetch: NodeIdentifier[]) => Promise<void>;
  loadSingleConnectionData: (edgeId: string) => Promise<EntityConnectionDataResponse | null>;

  // Cache management
  invalidateVisualizationData: (clusterId: string) => Promise<void>;
  invalidateConnectionData: (edgeId: string) => Promise<void>;
  performThreeClusterCleanup: (force?: boolean) => void;
  clearAllData: () => void;

  // 🔧 NEW: Optimistic update functions
  updateEdgeStatusOptimistically: (
    clusterId: string, 
    edgeId: string, 
    newStatus: BaseLink["status"],
    wasReviewed?: boolean
  ) => () => void; // Returns a revert function
  
  updateClusterCompletionOptimistically: (
    clusterId: string, 
    wasReviewed: boolean
  ) => () => void; // Returns a revert function

  // Data queries
  isVisualizationDataLoaded: (clusterId: string) => boolean;
  isVisualizationDataLoading: (clusterId: string) => boolean;
  isConnectionDataLoaded: (edgeId: string) => boolean;
  isConnectionDataLoading: (edgeId: string) => boolean;
  getVisualizationError: (clusterId: string) => string | null;
  getConnectionError: (edgeId: string) => string | null;
  getClusterById: (clusterId: string) => EntityCluster | undefined;
  getNodeDetail: (nodeId: string) => NodeDetailResponse | null | "loading" | "error";

  // Cache statistics
  getCacheStats: () => any;
}

// ============================================================================
// Context Creation (unchanged)
// ============================================================================

const EntityDataContext = createContext<EntityDataContextType | undefined>(
  undefined
);

// ============================================================================
// Initial State (unchanged)
// ============================================================================

const initialClustersState: ClustersState = {
  data: [],
  total: 0,
  page: 1,
  limit: 10,
  loading: false,
  error: null,
};

// ============================================================================
// Utility Functions (unchanged)
// ============================================================================

function uniqueBy<T>(items: T[], keySelector: (item: T) => string): T[] {
  return Array.from(
    new Map(items.map((item) => [keySelector(item), item])).values()
  );
}

// ============================================================================
// Provider Component - ENHANCED with Optimistic Updates
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
  } = state;

  // ========================================================================
  // Internal State (unchanged)
  // ========================================================================

  const [clusters, setClusters] = useState<ClustersState>(initialClustersState);
  const [visualizationData, setVisualizationData] = useState<Record<string, VisualizationState>>({});
  const [connectionData, setConnectionData] = useState<Record<string, ConnectionState>>({});
  const [nodeDetails, setNodeDetails] = useState<Record<string, NodeDetailResponse | null | "loading" | "error">>({});
  const [serverProgress, setServerProgress] = useState<Record<string, ClusterProgress>>({});
  const [overallServerProgress, setOverallServerProgress] = useState<OverallProgress | null>(null);
  const [pendingNodeFetches, setPendingNodeFetches] = useState<Set<string>>(new Set());

  // ========================================================================
  // Request Management Refs - ENHANCED
  // ========================================================================

  const activeRequestsRef = useRef<Set<string>>(new Set());
  const lastRequestTimeRef = useRef<number>(0);
  const cleanupTimeoutRef = useRef<NodeJS.Timeout | null>(null);
  const lastCleanupStateRef = useRef<string>("");
  
  // 🔧 FIX: Add refs for stable state access in effects
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

  // 🔧 FIX: Add initialization flag to prevent multiple initial loads
  const initializationRef = useRef<{
    hasInitialized: boolean;
    lastInitParams: string;
  }>({
    hasInitialized: false,
    lastInitParams: "",
  });

  // ========================================================================
  // 🔧 NEW: Optimistic Update Functions
  // ========================================================================

  const updateEdgeStatusOptimistically = useCallback(
    (
      clusterId: string, 
      edgeId: string, 
      newStatus: BaseLink["status"],
      wasReviewed: boolean = true
    ): (() => void) => {
      console.log(`🚀 [EntityData] Optimistic update: Edge ${edgeId} in cluster ${clusterId} -> ${newStatus}`);
      
      // Store original state for revert function
      const originalVizState = visualizationData[clusterId];
      const originalConnectionState = connectionData[edgeId];
      
      // Update visualization data
      setVisualizationData(prev => {
        if (!prev[clusterId]?.data) return prev;
        
        return produce(prev, draft => {
          const vizState = draft[clusterId];
          if (vizState?.data?.links) {
            const link = vizState.data.links.find(l => l.id === edgeId);
            if (link) {
              link.status = newStatus;
              link.wasReviewed = wasReviewed;
              console.log(`✅ [EntityData] Optimistically updated edge ${edgeId} status to ${newStatus}`);
            }
          }
        });
      });
      
      // Update connection data if it exists
      setConnectionData(prev => {
        if (!prev[edgeId]?.data) return prev;
        
        return produce(prev, draft => {
          const connState = draft[edgeId];
          if (connState?.data?.edge) {
            // FIX: Handle undefined newStatus by coalescing to null
            connState.data.edge.status = newStatus ?? null;
            connState.data.edge.wasReviewed = wasReviewed;
            console.log(`✅ [EntityData] Optimistically updated connection ${edgeId} status to ${newStatus}`);
          }
        });
      });
      
      // Return revert function
      return () => {
        console.log(`↩️ [EntityData] Reverting optimistic update for edge ${edgeId}`);
        
        if (originalVizState) {
          setVisualizationData(prev => ({
            ...prev,
            [clusterId]: originalVizState
          }));
        }
        
        if (originalConnectionState) {
          setConnectionData(prev => ({
            ...prev,
            [edgeId]: originalConnectionState
          }));
        }
      };
    },
    [visualizationData, connectionData]
  );

  const updateClusterCompletionOptimistically = useCallback(
    (clusterId: string, wasReviewed: boolean): (() => void) => {
      console.log(`🚀 [EntityData] Optimistic cluster completion update: ${clusterId} -> ${wasReviewed}`);
      
      // Store original state for revert function
      const originalClusterState = clusters;
      
      // Update cluster completion status
      setClusters(prev => {
        return produce(prev, draft => {
          const cluster = draft.data.find(c => c.id === clusterId);
          if (cluster) {
            cluster.wasReviewed = wasReviewed;
            console.log(`✅ [EntityData] Optimistically updated cluster ${clusterId} completion to ${wasReviewed}`);
          }
        });
      });
      
      // Return revert function
      return () => {
        console.log(`↩️ [EntityData] Reverting optimistic cluster completion update for ${clusterId}`);
        setClusters(originalClusterState);
      };
    },
    [clusters]
  );

  // ========================================================================
  // Helper Functions (mostly unchanged)
  // ========================================================================

  const getThreeClustersToKeep = useCallback(
    (currentClusterId: string | null, clustersData: EntityCluster[]): Set<string> => {
      const clustersToKeep = new Set<string>();

      if (!currentClusterId || clustersData.length === 0) {
        return clustersToKeep;
      }

      clustersToKeep.add(currentClusterId);
      const currentIndex = clustersData.findIndex((c) => c.id === currentClusterId);

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
          } else if (currentIndex === clustersData.length - 1 && clustersData.length > 2) {
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
  // Cache Management - FIXED
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
        const keepSet = activelyPagingClusterId ? new Set([activelyPagingClusterId]) : new Set<string>();

        setVisualizationData((prev) => {
          const cleaned = Object.fromEntries(
            Object.entries(prev).filter(([clusterId]) => keepSet.has(clusterId))
          );
          if (Object.keys(cleaned).length !== Object.keys(prev).length) {
            console.log(
              `🧹 [EntityData] Three-cluster cleanup (no selection): ${Object.keys(prev).length} -> ${Object.keys(cleaned).length} clusters`
            );
          }
          return cleaned;
        });
        return;
      }

      const clustersToKeep = getThreeClustersToKeep(selectedClusterId, clusters.data);

      if (activelyPagingClusterId) {
        clustersToKeep.add(activelyPagingClusterId);
      }

      const currentClusterIds = new Set(Object.keys(visualizationData));
      const clustersToRemove = [...currentClusterIds].filter((id) => !clustersToKeep.has(id));

      if (clustersToRemove.length === 0 && !force) {
        return;
      }

      console.log(
        `🧹 [EntityData] Three-cluster cleanup: keeping [${Array.from(clustersToKeep).join(", ")}], removing [${clustersToRemove.join(", ")}]`
      );

      // Clean visualization data
      setVisualizationData((prev) => {
        const cleaned = Object.fromEntries(
          Object.entries(prev).filter(([clusterId]) => clustersToKeep.has(clusterId))
        );
        return cleaned;
      });

      // Clean server progress data
      setServerProgress((prev) => {
        const cleaned = Object.fromEntries(
          Object.entries(prev).filter(([clusterId]) => clustersToKeep.has(clusterId))
        );
        return cleaned;
      });

      // Determine edges to keep
      const edgesToKeep = new Set<string>();
      if (state.selectedEdgeId) {
        edgesToKeep.add(state.selectedEdgeId);
      }

      Object.entries(visualizationData).forEach(([clusterId, vizState]) => {
        if (clustersToKeep.has(clusterId) && vizState.data?.links) {
          vizState.data.links.forEach((link) => edgesToKeep.add(link.id));
        }
      });

      // Clean connection data
      setConnectionData((prev) => {
        const cleaned = Object.fromEntries(
          Object.entries(prev).filter(([edgeId]) => edgesToKeep.has(edgeId))
        );
        if (Object.keys(cleaned).length !== Object.keys(prev).length) {
          console.log(
            `🧹 [EntityData] Connection cleanup: ${Object.keys(prev).length} -> ${Object.keys(cleaned).length} edges`
          );
        }
        return cleaned;
      });

      // Determine nodes to keep
      const nodeIdsToKeep = new Set<string>();
      Object.entries(visualizationData).forEach(([clusterId, vizState]) => {
        if (clustersToKeep.has(clusterId) && vizState.data?.nodes) {
          vizState.data.nodes.forEach((node) => nodeIdsToKeep.add(node.id));
        }
      });

      Object.entries(connectionData).forEach(([edgeId, connState]) => {
        if (edgesToKeep.has(edgeId) && connState.data) {
          if (connState.data.entity1) nodeIdsToKeep.add(connState.data.entity1.id);
          if (connState.data.entity2) nodeIdsToKeep.add(connState.data.entity2.id);
        }
      });

      // Clean node details
      setNodeDetails((prev) => {
        const cleaned = Object.fromEntries(
          Object.entries(prev).filter(([nodeId]) => nodeIdsToKeep.has(nodeId))
        );
        if (Object.keys(cleaned).length !== Object.keys(prev).length) {
          console.log(
            `🧹 [EntityData] Node cleanup: ${Object.keys(prev).length} -> ${Object.keys(cleaned).length} nodes`
          );
        }
        return cleaned;
      });

      // Clean pending fetches
      setPendingNodeFetches((prev) => {
        const cleaned = new Set([...prev].filter((nodeId) => nodeIdsToKeep.has(nodeId)));
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

  const clearAllData = useCallback(() => {
    console.log("🧹 [EntityData] Clearing all data");
    setClusters(initialClustersState);
    setVisualizationData({});
    setConnectionData({});
    setNodeDetails({});
    setPendingNodeFetches(new Set());
    setServerProgress({});
    setOverallServerProgress(null);
    activeRequestsRef.current.clear();
    lastRequestTimeRef.current = 0;
    
    // 🔧 FIX: Reset initialization flag
    initializationRef.current = {
      hasInitialized: false,
      lastInitParams: "",
    };
  }, []);
  // Create a stable ref for the clearAllData function
  const clearAllDataRef = useRef(clearAllData);
  clearAllDataRef.current = clearAllData;


  // ========================================================================
  // Data Loading Functions - ENHANCED
  // ========================================================================

  // 🔧 FIX: Enhanced request deduplication helpers
  const isRequestInProgress = useCallback((requestKey: string): boolean => {
    return activeRequestsRef.current.has(requestKey);
  }, []);

  const addActiveRequest = useCallback((requestKey: string) => {
    activeRequestsRef.current.add(requestKey);
  }, []);

  const removeActiveRequest = useCallback((requestKey: string) => {
    activeRequestsRef.current.delete(requestKey);
  }, []);

  // ... (rest of the data loading functions remain the same) ...
  const loadBulkNodeDetails = useCallback(
    async (nodesToFetch: NodeIdentifier[]) => {
      if (!selectedOpinionRef.current) {
        console.log("🚫 [EntityData] No opinion selected, skipping node details fetch");
        return;
      }

      const caller = new Error().stack?.split("\n")[2]?.match(/at (\w+)/)?.[1] || "unknown";
      console.log(`🔍 [EntityData] loadBulkNodeDetails called by: ${caller}`);
      console.log(`🔍 [EntityData] Requested: ${nodesToFetch.length} nodes, First 3 IDs:`, nodesToFetch.slice(0, 3).map((n) => n.id));

      if (nodesToFetch.length === 0) {
        console.log("🚫 [EntityData] No nodes to fetch, early return");
        return;
      }

      // 🔧 FIX: Enhanced request deduplication
      const requestKey = `nodes-${nodesToFetch.map(n => `${n.id}-${n.nodeType}`).sort().join(",")}`;
      if (isRequestInProgress(requestKey)) {
        console.log("🚫 [EntityData] Duplicate node request in progress, skipping");
        return;
      }

      const uniqueNodesToFetch = uniqueBy(nodesToFetch, (node) => `${node.id}-${node.nodeType}`);
      const trulyNeedsFetching = uniqueNodesToFetch.filter((node) => {
        const currentState = nodeDetails[node.id];
        const isPending = pendingNodeFetches.has(node.id);
        const shouldSkip = (currentState && currentState !== "error") || isPending;

        if (shouldSkip) {
          console.log(`🚫 [EntityData] Skipping node ${node.id}: state=${currentState}, pending=${isPending}`);
        }
        return !shouldSkip;
      });

      if (trulyNeedsFetching.length === 0) {
        console.log("🚫 [EntityData] All nodes already loaded/loading, early return");
        return;
      }

      console.log(`📦 [EntityData] Actually fetching: ${trulyNeedsFetching.length} nodes`);

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
            const response = await getBulkNodeDetails({ items: chunk }, selectedOpinionRef.current);

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
    [nodeDetails, pendingNodeFetches, isRequestInProgress, addActiveRequest, removeActiveRequest]
  );

  const loadVisualizationDataForClusters = useCallback(async (items: BulkVisualizationRequestItem[]) => {
      if (!selectedOpinionRef.current) {
        console.log("🚫 [EntityData] No opinion selected, skipping visualization fetch");
        return;
      }
      if (items.length === 0) return;
      
      const requestKey = `viz-${items.map(i => i.clusterId).sort().join(',')}`;
      if (isRequestInProgress(requestKey)) {
        console.log("🚫 [EntityData] Duplicate visualization request in progress, skipping");
        return;
      }

      const uniqueItems = uniqueBy(items, (item) => `${item.clusterId}-${item.itemType}`);
      addActiveRequest(requestKey);

      setVisualizationData((prev) =>
        produce(prev, (draft) => {
          uniqueItems.forEach((item) => {
            if (!draft[item.clusterId] || draft[item.clusterId]?.error) {
              draft[item.clusterId] = { data: null, loading: true, error: null, lastUpdated: null };
            } else if (draft[item.clusterId] && !draft[item.clusterId]?.loading) {
              draft[item.clusterId]!.loading = true;
              draft[item.clusterId]!.error = null;
            }
          });
        })
      );

      try {
        const response = await getBulkVisualizations({ items: uniqueItems }, selectedOpinionRef.current);
        
        setVisualizationData((prev) =>
          produce(prev, (draft) => {
            response.forEach((vizData) => {
              draft[vizData.clusterId] = { data: vizData, loading: false, error: null, lastUpdated: Date.now() };
            });
            uniqueItems.forEach((reqItem) => {
              if (!response.find((r) => r.clusterId === reqItem.clusterId)) {
                if (draft[reqItem.clusterId]) {
                    draft[reqItem.clusterId] = { data: null, loading: false, error: "Not Found", lastUpdated: null };
                }
              }
            });
          })
        );
        
        const allNodeIdentifiers = response.flatMap(viz => viz.nodes.map(node => ({ id: node.id, nodeType: resolutionModeRef.current })));
        if (allNodeIdentifiers.length > 0) {
            loadBulkNodeDetails(allNodeIdentifiers);
        }

      } catch (error) {
        console.error("❌ [EntityData] Failed to load visualization data:", error);
        setVisualizationData((prev) =>
          produce(prev, (draft) => {
            uniqueItems.forEach((item) => {
                if (draft[item.clusterId]) {
                    draft[item.clusterId] = { data: null, loading: false, error: (error as Error).message, lastUpdated: null };
                }
            });
          })
        );
      } finally {
        removeActiveRequest(requestKey);
      }
    }, [isRequestInProgress, addActiveRequest, removeActiveRequest, loadBulkNodeDetails]);
  const loadVisualizationDataForClustersRef = useRef(loadVisualizationDataForClusters);
  loadVisualizationDataForClustersRef.current = loadVisualizationDataForClusters;


  // 🔧 FIX: Enhanced loadClusterProgress with stable dependencies
  const loadClusterProgress = useCallback(
    async (page: number, limit: number = 10) => {
      const currentOpinion = selectedOpinionRef.current;
      const currentMode = resolutionModeRef.current;
      const currentFilterStatus = clusterFilterStatusRef.current;
      const currentWorkflowFilter = workflowFilterRef.current;

      if (!currentOpinion) {
        console.log("🚫 [EntityData] No opinion selected, skipping cluster progress fetch");
        return;
      }

      // 🔧 FIX: Enhanced request deduplication
      const requestKey = `cluster-progress-${page}-${limit}-${currentFilterStatus}-${currentWorkflowFilter}-${currentMode}-${currentOpinion}`;
      if (isRequestInProgress(requestKey)) {
        console.log("🚫 [EntityData] Duplicate cluster progress request in progress, skipping");
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

        // Handle empty page
        if (response.clusters.length === 0 && response.page > 1) {
          console.warn(`No clusters on page ${response.page}. Attempting to load page 1.`);
          setClusters((prev) => ({ ...prev, loading: false, error: null }));
          removeActiveRequest(requestKey);
          // 🔧 FIX: Use setTimeout to prevent stack overflow
          setTimeout(() => loadClusterProgress(1, limit), 0);
          return;
        }

        // Update server progress state
        const progressMap: Record<string, ClusterProgress> = {};
        response.clusters.forEach((cluster) => {
          progressMap[cluster.id] = cluster.progress;
        });

        setServerProgress(progressMap);
        setOverallServerProgress(response.overallProgress);

        // Update clusters data for compatibility
        setClusters({
          data: response.clusters.map((c) => ({
            ...c,
            // Map progress data to existing cluster structure for compatibility
            wasReviewed: c.progress.currentView.isComplete,
          })),
          total: response.total,
          page: response.page,
          limit: response.limit,
          loading: false,
          error: null,
        });

        if (response.clusters.length === 0) {
          // Don't clear selectedClusterId here - let EntityWorkflowContext handle this
        }

        // 🔧 FIX: Debounce visualization loading to prevent cascading loads
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
              itemType: currentMode,
            });
          }
        });

        if (vizRequestItemsNormal.length > 0) {
          // 🔧 FIX: Delay visualization loading to prevent cascade
          setTimeout(() => {
            loadVisualizationDataForClustersRef.current(vizRequestItemsNormal);
          }, 100);
        }
      } catch (error) {
        console.error("❌ [EntityData] Failed to load cluster progress:", error);
        setClusters((prev) => ({
          ...prev,
          loading: false,
          error: (error as Error).message,
          data: [],
          total: 0,
        }));

        // Clear server progress on error
        setServerProgress({});
        setOverallServerProgress(null);
      } finally {
        removeActiveRequest(requestKey);
      }
    },
    [isRequestInProgress, addActiveRequest, removeActiveRequest]
  );

  // 🔧 FIX: Create stable reference for loadClusterProgress
  const loadClusterProgressRef = useRef(loadClusterProgress);
  loadClusterProgressRef.current = loadClusterProgress;

  const loadClusters = loadClusterProgress; // Alias for backward compatibility if needed

  const loadBulkConnections = useCallback(async (items: BulkConnectionRequestItem[]) => {
      if (!selectedOpinionRef.current) {
        console.log("🚫 [EntityData] No opinion selected, skipping connections fetch");
        return;
      }
      if (items.length === 0) return;

      const requestKey = `conns-${items.map(i => i.edgeId).sort().join(',')}`;
      if (isRequestInProgress(requestKey)) {
        console.log("🚫 [EntityData] Duplicate connection request in progress, skipping");
        return;
      }
      addActiveRequest(requestKey);

      const uniqueItems = uniqueBy(items, (item) => `${item.edgeId}-${item.itemType}`);
      setConnectionData((prev) =>
        produce(prev, (draft) => {
          uniqueItems.forEach((item) => {
            if (!draft[item.edgeId] || draft[item.edgeId]?.error) {
              draft[item.edgeId] = { data: null, loading: true, error: null, lastUpdated: null };
            } else if (draft[item.edgeId] && !draft[item.edgeId]?.loading) {
              draft[item.edgeId]!.loading = true;
              draft[item.edgeId]!.error = null;
            }
          });
        })
      );

      try {
        const response = await getBulkConnections({ items: uniqueItems }, selectedOpinionRef.current);
        setConnectionData((prev) =>
          produce(prev, (draft) => {
            response.forEach((connData) => {
              draft[connData.edge.id] = { data: connData, loading: false, error: null, lastUpdated: Date.now() };
            });
            uniqueItems.forEach((reqItem) => {
              if (!response.find((r) => r.edge.id === reqItem.edgeId)) {
                if(draft[reqItem.edgeId]) {
                    draft[reqItem.edgeId] = { data: null, loading: false, error: "Not Found", lastUpdated: null };
                }
              }
            });
          })
        );
      } catch (error) {
        console.error("❌ [EntityData] Failed to load bulk connections:", error);
        setConnectionData((prev) =>
          produce(prev, (draft) => {
            uniqueItems.forEach((item) => {
              if (draft[item.edgeId]) {
                draft[item.edgeId] = { data: null, loading: false, error: (error as Error).message, lastUpdated: null };
              }
            });
          })
        );
      } finally {
        removeActiveRequest(requestKey);
      }
  }, [isRequestInProgress, addActiveRequest, removeActiveRequest]);


  const loadSingleConnectionData = useCallback(async (edgeId: string): Promise<EntityConnectionDataResponse | null> => {
      // This function can be simplified to just use the bulk loader for consistency
      await loadBulkConnections([{ edgeId, itemType: resolutionModeRef.current }]);
      // The data will be in the state, but we can return it if needed for immediate use
      // Note: This might not be the most up-to-date data if the state update hasn't completed.
      // For most UI, reacting to state change is better.
      return connectionData[edgeId]?.data ?? null;
  }, [loadBulkConnections, connectionData]);

  const invalidateVisualizationData = useCallback(async (clusterId: string) => {
      console.log(`🔄 [EntityData] Invalidating and reloading visualization data for cluster: ${clusterId}`);
      await loadVisualizationDataForClustersRef.current([{ clusterId, itemType: resolutionModeRef.current }]);
  }, []);

  const invalidateConnectionData = useCallback(async (edgeId: string) => {
      console.log(`🔄 [EntityData] Invalidating and reloading connection data for edge: ${edgeId}`);
      await loadBulkConnections([{ edgeId, itemType: resolutionModeRef.current }]);
  }, [loadBulkConnections]);


  // ========================================================================
  // Effects - FIXED
  // ========================================================================

  // 🔧 FIX: Stable effect for initial cluster loading
  useEffect(() => {
    const currentOpinion = selectedOpinionRef.current;
    const currentMode = resolutionModeRef.current;
    const currentFilterStatus = clusterFilterStatusRef.current;
    const currentWorkflowFilter = workflowFilterRef.current;
    const currentClusters = clustersRef.current;

    if (!currentOpinion) return;

    const initParams = `${currentMode}-${currentFilterStatus}-${currentWorkflowFilter}-${currentOpinion}`;
    
    // 🔧 FIX: Prevent multiple initializations with same parameters
    if (initializationRef.current.hasInitialized && 
        initializationRef.current.lastInitParams === initParams) {
      return;
    }

    // This logic determines if we are in a state where an initial load is required.
    const shouldInitialize = (
      (currentClusters.data.length === 0 || currentClusters.error) &&
      !currentClusters.loading
    );

    if (shouldInitialize) {
      console.log(
        `[EntityData] Initial cluster progress load triggered for filter: ${currentFilterStatus}, workflow: ${currentWorkflowFilter}`
      );
      
      initializationRef.current = {
        hasInitialized: true,
        lastInitParams: initParams,
      };
      
      loadClusterProgressRef.current(1, currentClusters.limit);
    }
  }, [
    // 🔧 FIX: Only depend on primitive values that actually change
    resolutionMode,
    clusterFilterStatus,
    workflowFilter,
    selectedOpinion,
    refreshTrigger, // This will trigger reinitialization when needed
  ]);

  // 🔧 FIX: Debounced cleanup effect
  useEffect(() => {
    if (!selectedClusterId || clusters.data.length === 0) return;

    // Clear any existing timeout
    if (cleanupTimeoutRef.current) {
      clearTimeout(cleanupTimeoutRef.current);
    }

    // 🔧 FIX: Much longer debounce to prevent frequent cleanup
    cleanupTimeoutRef.current = setTimeout(() => {
      const currentState = JSON.stringify({
        selectedClusterId,
        clustersCount: clusters.data.length,
        visualizationKeys: Object.keys(visualizationData).length,
      });

      // Only cleanup if state actually changed
      if (currentState !== lastCleanupStateRef.current) {
        console.log("🧹 [EntityData] Debounced cleanup triggered");
        performThreeClusterCleanup();
        lastCleanupStateRef.current = currentState;
      }
      cleanupTimeoutRef.current = null;
    }, 5000); // 🔧 FIX: Increased from 3000ms to 5000ms

    return () => {
      if (cleanupTimeoutRef.current) {
        clearTimeout(cleanupTimeoutRef.current);
        cleanupTimeoutRef.current = null;
      }
    };
  }, [selectedClusterId, clusters.data.length, visualizationData, performThreeClusterCleanup]); 

  // 🔧 FIX: Effect to clear data when refresh trigger changes
  useEffect(() => {
    if (refreshTrigger > 0) {
      console.log("🔄 [EntityData] Refresh trigger activated - clearing data");
      clearAllDataRef.current();
    }
  }, [refreshTrigger]);

  // ========================================================================
  // Context Value (ENHANCED with optimistic update functions)
  // ========================================================================

  const contextValue: EntityDataContextType = {
    // Data stores
    clusters,
    visualizationData,
    connectionData,
    nodeDetails,
    serverProgress,
    overallServerProgress,

    // Loading functions
    loadClusters,
    loadClusterProgress,
    loadVisualizationDataForClusters,
    loadBulkConnections,
    loadBulkNodeDetails,
    loadSingleConnectionData,

    // Cache management
    invalidateVisualizationData,
    invalidateConnectionData,
    performThreeClusterCleanup,
    clearAllData,

    // 🔧 NEW: Optimistic update functions
    updateEdgeStatusOptimistically,
    updateClusterCompletionOptimistically,

    // Data queries
    isVisualizationDataLoaded: useCallback((clusterId: string) => 
      !!visualizationData[clusterId]?.data &&
      !visualizationData[clusterId]?.loading &&
      !visualizationData[clusterId]?.error, [visualizationData]),
    
    isVisualizationDataLoading: useCallback((clusterId: string) =>
      !!visualizationData[clusterId]?.loading, [visualizationData]),
    
    isConnectionDataLoaded: useCallback((edgeId: string) =>
      !!connectionData[edgeId]?.data &&
      !connectionData[edgeId]?.loading &&
      !connectionData[edgeId]?.error, [connectionData]),
    
    isConnectionDataLoading: useCallback((edgeId: string) =>
      !!connectionData[edgeId]?.loading, [connectionData]),
    
    getVisualizationError: useCallback((clusterId: string) =>
      visualizationData[clusterId]?.error || null, [visualizationData]),
    
    getConnectionError: useCallback((edgeId: string) =>
      connectionData[edgeId]?.error || null, [connectionData]),
    
    getClusterById: useCallback((clusterId: string) =>
      clusters.data.find((c) => c.id === clusterId), [clusters.data]),
    
    getNodeDetail: useCallback((nodeId: string) => 
      nodeDetails[nodeId] || null, [nodeDetails]),

    getCacheStats: useCallback(() => {
      const stats = {
        clusters: Object.keys(visualizationData).length,
        connections: Object.keys(connectionData).length,
        nodes: Object.keys(nodeDetails).length,
        pendingFetches: pendingNodeFetches.size,
        activeRequests: activeRequestsRef.current.size,
      };
      console.table(stats);
      return stats;
    }, [visualizationData, connectionData, nodeDetails, pendingNodeFetches]),
  };

  return <EntityDataContext.Provider value={contextValue}>{children}</EntityDataContext.Provider>;
}

// ============================================================================
// Hook for Consuming Context (unchanged)
// ============================================================================

export function useEntityData(): EntityDataContextType {
  const context = useContext(EntityDataContext);

  if (context === undefined) {
    throw new Error("useEntityData must be used within an EntityDataProvider");
  }

  return context;
}


// ============================================================================
// Utility Hooks (unchanged)
// ============================================================================

/**
 * Hook for components that only need cluster data
 */
export function useClusterData() {
  const { clusters, loadClusters, loadClusterProgress, getClusterById } = useEntityData();

  return {
    clusters,
    loadClusters,
    loadClusterProgress,
    getClusterById,
  };
}

/**
 * Hook for components that only need visualization data
 */
export function useVisualizationData() {
  const {
    visualizationData,
    loadVisualizationDataForClusters,
    isVisualizationDataLoaded,
    isVisualizationDataLoading,
    getVisualizationError,
    invalidateVisualizationData,
    updateEdgeStatusOptimistically,
  } = useEntityData();

  return {
    visualizationData,
    loadVisualizationDataForClusters,
    isVisualizationDataLoaded,
    isVisualizationDataLoading,
    getVisualizationError,
    invalidateVisualizationData,
    updateEdgeStatusOptimistically,
  };
}

/**
 * Hook for components that only need connection data
 */
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

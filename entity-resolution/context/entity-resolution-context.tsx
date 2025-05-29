// context/entity-resolution-context.tsx

"use client"

import {
  VisualizationDataResponse,
  ConnectionDataResponse,
  ClustersResponse,
  Cluster,
  EntityLink,
  EntityGroup,
} from "@/types/entity-resolution";
import {
  getClusters,
  getVisualizationData,
  getConnectionData,
} from "@/utils/api-client";
import {
  createContext,
  useCallback,
  useContext,
  useState,
  useEffect,
  useMemo,
  type ReactNode,
} from "react";

// Enhanced data state interfaces
interface ClustersState {
  data: Cluster[];
  total: number;
  page: number;
  limit: number;
  loading: boolean;
  error: string | null;
}

interface VisualizationState {
  data: VisualizationDataResponse | null;
  loading: boolean;
  error: string | null;
  lastUpdated: number | null;
}

interface ConnectionState {
  data: ConnectionDataResponse | null;
  loading: boolean;
  error: string | null;
  lastUpdated: number | null;
}

// SIMPLIFIED: Computed state interfaces
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
  // Core UI state (read-only for components)
  selectedClusterId: string | null;
  selectedEdgeId: string | null;
  reviewerId: string;
  lastReviewedEdgeId: string | null;
  refreshTrigger: number;

  // Core data state (read-only for components)
  clusters: ClustersState;
  visualizationData: Record<string, VisualizationState>;
  connectionData: Record<string, ConnectionState>;

  // Computed state (read-only for components)
  clusterProgress: Record<string, ClusterProgress>;
  edgeSelectionInfo: EdgeSelectionInfo;
  currentVisualizationData: VisualizationDataResponse | null;
  currentConnectionData: ConnectionDataResponse | null;

  // Actions (the only way components should modify state)
  actions: {
    setSelectedClusterId: (id: string | null) => void;
    setSelectedEdgeId: (id: string | null) => void;
    setReviewerId: (id: string) => void;
    setLastReviewedEdgeId: (id: string | null) => void;
    triggerRefresh: () => void;
    loadClusters: (page: number, limit?: number) => Promise<void>;
    preloadVisualizationData: (clusterIds: string[]) => Promise<void>;
    loadConnectionData: (edgeId: string) => Promise<ConnectionDataResponse | null>;
    invalidateVisualizationData: (clusterId: string) => void;
    invalidateConnectionData: (edgeId: string) => void;
    clearAllData: () => void;
    selectNextUnreviewedEdge: (afterEdgeId?: string | null) => void;
    advanceToNextCluster: () => void;
    checkAndAdvanceIfComplete: () => void; // NEW: Only advance after review completion
  };

  // Utility functions (read-only queries)
  queries: {
    isVisualizationDataLoaded: (clusterId: string) => boolean;
    isVisualizationDataLoading: (clusterId: string) => boolean;
    isConnectionDataLoaded: (edgeId: string) => boolean;
    isConnectionDataLoading: (edgeId: string) => boolean;
    getVisualizationError: (clusterId: string) => string | null;
    getConnectionError: (edgeId: string) => string | null;
    getClusterProgress: (clusterId: string) => ClusterProgress | null;
    canAdvanceToNextCluster: () => boolean;
    isEdgeReviewed: (edgeId: string) => boolean;
    getEdgeStatus: (edgeId: string) => 'PENDING_REVIEW' | 'CONFIRMED_MATCH' | 'CONFIRMED_NON_MATCH' | null;
  };
}

const EntityResolutionContext = createContext<EntityResolutionContextType | undefined>(undefined);

export function EntityResolutionProvider({ children }: { children: ReactNode }) {
  // Core state
  const [selectedClusterId, setSelectedClusterIdState] = useState<string | null>(null);
  const [selectedEdgeId, setSelectedEdgeIdState] = useState<string | null>(null);
  const [reviewerId, setReviewerId] = useState<string>("default-reviewer");
  const [refreshTrigger, setRefreshTrigger] = useState<number>(0);
  const [lastReviewedEdgeId, setLastReviewedEdgeId] = useState<string | null>(null);

  // Data state
  const [clusters, setClusters] = useState<ClustersState>({
    data: [],
    total: 0,
    page: 1,
    limit: 10,
    loading: false,
    error: null,
  });

  const [visualizationData, setVisualizationData] = useState<Record<string, VisualizationState>>({});
  const [connectionData, setConnectionData] = useState<Record<string, ConnectionState>>({});

  // MASSIVELY SIMPLIFIED: Progress calculation now just counts edge statuses directly
  const clusterProgress = useMemo((): Record<string, ClusterProgress> => {
    const progress: Record<string, ClusterProgress> = {};

    Object.entries(visualizationData).forEach(([clusterId, vizState]) => {
      if (!vizState.data) {
        progress[clusterId] = {
          clusterId,
          totalEdges: 0,
          reviewedEdges: 0,
          pendingEdges: 0,
          confirmedMatches: 0,
          confirmedNonMatches: 0,
          progressPercentage: 0,
          isComplete: false,
        };
        return;
      }

      const { links } = vizState.data;
      const totalEdges = links.length;

      if (totalEdges === 0) {
        progress[clusterId] = {
          clusterId,
          totalEdges: 0,
          reviewedEdges: 0,
          pendingEdges: 0,
          confirmedMatches: 0,
          confirmedNonMatches: 0,
          progressPercentage: 100,
          isComplete: true,
        };
        return;
      }

      // SIMPLE: Just count by edge status directly!
      const confirmedMatches = links.filter(link => link.status === 'CONFIRMED_MATCH').length;
      const confirmedNonMatches = links.filter(link => link.status === 'CONFIRMED_NON_MATCH').length;
      const pendingEdges = links.filter(link => link.status === 'PENDING_REVIEW').length;
      const reviewedEdges = confirmedMatches + confirmedNonMatches;
      const progressPercentage = totalEdges > 0 ? Math.round((reviewedEdges / totalEdges) * 100) : 0;

      progress[clusterId] = {
        clusterId,
        totalEdges,
        reviewedEdges,
        pendingEdges,
        confirmedMatches,
        confirmedNonMatches,
        progressPercentage,
        isComplete: progressPercentage === 100,
      };
    });

    return progress;
  }, [visualizationData]);

  // Computed state - Current visualization data
  const currentVisualizationData = useMemo((): VisualizationDataResponse | null => {
    if (!selectedClusterId) return null;
    return visualizationData[selectedClusterId]?.data || null;
  }, [selectedClusterId, visualizationData]);

  // Computed state - Current connection data
  const currentConnectionData = useMemo((): ConnectionDataResponse | null => {
    if (!selectedEdgeId) return null;
    return connectionData[selectedEdgeId]?.data || null;
  }, [selectedEdgeId, connectionData]);

  // SIMPLIFIED: Edge selection now just looks at edge status directly
  const edgeSelectionInfo = useMemo((): EdgeSelectionInfo => {
    if (!currentVisualizationData) {
      return {
        currentEdgeId: selectedEdgeId,
        nextUnreviewedEdgeId: null,
        hasUnreviewedEdges: false,
        currentEdgeIndex: -1,
        totalEdges: 0,
      };
    }

    const { links } = currentVisualizationData;
    const totalEdges = links.length;

    // SIMPLE: Find unreviewed edges by status
    const unreviewedEdges = links.filter(link => link.status === 'PENDING_REVIEW');

    // Find next unreviewed edge after a specific edge
    const findNextUnreviewedAfter = (afterEdgeId: string | null): string | null => {
      if (!afterEdgeId) return unreviewedEdges[0]?.id || null;

      const startIndex = links.findIndex(link => link.id === afterEdgeId);
      if (startIndex === -1) return unreviewedEdges[0]?.id || null;

      // Search from startIndex + 1 to end
      for (let i = startIndex + 1; i < links.length; i++) {
        if (links[i].status === 'PENDING_REVIEW') {
          return links[i].id;
        }
      }

      // Wrap around to beginning
      for (let i = 0; i < startIndex; i++) {
        if (links[i].status === 'PENDING_REVIEW') {
          return links[i].id;
        }
      }

      return null;
    };

    const currentEdgeIndex = selectedEdgeId ? links.findIndex(link => link.id === selectedEdgeId) : -1;
    const nextUnreviewedEdgeId = findNextUnreviewedAfter(lastReviewedEdgeId);

    return {
      currentEdgeId: selectedEdgeId,
      nextUnreviewedEdgeId,
      hasUnreviewedEdges: unreviewedEdges.length > 0,
      currentEdgeIndex,
      totalEdges,
    };
  }, [currentVisualizationData, selectedEdgeId, lastReviewedEdgeId]);

  // Action: Load clusters with automatic preloading
  const loadClusters = useCallback(async (page: number, limit: number = 10) => {
    setClusters((prev) => ({ ...prev, loading: true, error: null }));
    
    try {
      const response: ClustersResponse = await getClusters(page, limit);
      setClusters({
        data: response.clusters,
        total: response.total,
        page,
        limit,
        loading: false,
        error: null,
      });
      
      // Automatically preload visualization data for all clusters on this page
      const clusterIds = response.clusters.map(cluster => cluster.id);
      if (clusterIds.length > 0) {
        preloadVisualizationData(clusterIds).catch(error => {
          console.error("Background preloading failed:", error);
        });
      }
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : "Failed to load clusters";
      setClusters((prev) => ({
        ...prev,
        loading: false,
        error: errorMessage,
      }));
    }
  }, []);

  // Action: Preload visualization data for multiple clusters
  const preloadVisualizationData = useCallback(async (clusterIds: string[]) => {
    const clustersToLoad = clusterIds.filter(
      (clusterId) =>
        !visualizationData[clusterId] ||
        visualizationData[clusterId].error !== null
    );

    if (clustersToLoad.length === 0) {
      return;
    }

    // Set loading state for all clusters being loaded
    setVisualizationData((prev) => {
      const updated = { ...prev };
      clustersToLoad.forEach((clusterId) => {
        updated[clusterId] = {
          data: null,
          loading: true,
          error: null,
          lastUpdated: null,
        };
      });
      return updated;
    });

    // Load data for all clusters in parallel
    const loadPromises = clustersToLoad.map(async (clusterId) => {
      try {
        const data = await getVisualizationData(clusterId);
        setVisualizationData((prev) => ({
          ...prev,
          [clusterId]: {
            data,
            loading: false,
            error: null,
            lastUpdated: Date.now(),
          },
        }));
        return { clusterId, success: true };
      } catch (error) {
        const errorMessage = error instanceof Error ? error.message : "Failed to load visualization data";
        setVisualizationData((prev) => ({
          ...prev,
          [clusterId]: {
            data: null,
            loading: false,
            error: errorMessage,
            lastUpdated: null,
          },
        }));
        return { clusterId, success: false, error: errorMessage };
      }
    });

    const results = await Promise.allSettled(loadPromises);
    const failures = results
      .filter((result) => result.status === "rejected")
      .map((result) => (result as PromiseRejectedResult).reason);

    if (failures.length > 0) {
      console.warn("Some visualization data failed to preload:", failures);
    }
  }, [visualizationData]);

  // Action: Load connection data for a specific edge
  const loadConnectionData = useCallback(async (edgeId: string): Promise<ConnectionDataResponse | null> => {
    // Return cached data if available and not stale
    const cached = connectionData[edgeId];
    if (cached?.data && !cached.loading && !cached.error) {
      const isStale = cached.lastUpdated && (Date.now() - cached.lastUpdated) > 5 * 60 * 1000; // 5 minutes
      if (!isStale) {
        return cached.data;
      }
    }

    setConnectionData((prev) => ({
      ...prev,
      [edgeId]: {
        data: prev[edgeId]?.data || null,
        loading: true,
        error: null,
        lastUpdated: null,
      },
    }));

    try {
      const data = await getConnectionData(edgeId);
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
      const errorMessage = error instanceof Error ? error.message : "Failed to load connection data";
      setConnectionData((prev) => ({
        ...prev,
        [edgeId]: {
          data: null,
          loading: false,
          error: errorMessage,
          lastUpdated: null,
        },
      }));
      return null;
    }
  }, [connectionData]);

  // SIMPLIFIED: Select next unreviewed edge
  const selectNextUnreviewedEdge = useCallback((afterEdgeId?: string | null) => {
    if (!selectedClusterId) return;
    
    const afterId = afterEdgeId !== undefined ? afterEdgeId : lastReviewedEdgeId;
    const vizData = visualizationData[selectedClusterId]?.data;
    
    if (!vizData) return;

    const { links } = vizData;
    
    // Find next unreviewed edge
    const findNextUnreviewed = (afterId: string | null): string | null => {
      const startIndex = afterId ? links.findIndex(link => link.id === afterId) + 1 : 0;

      // Search from startIndex to end
      for (let i = startIndex; i < links.length; i++) {
        if (links[i].status === 'PENDING_REVIEW') {
          return links[i].id;
        }
      }

      // Wrap around if we started mid-array
      if (startIndex > 0) {
        for (let i = 0; i < startIndex - 1; i++) {
          if (links[i].status === 'PENDING_REVIEW') {
            return links[i].id;
          }
        }
      }

      return null;
    };

    const nextEdgeId = findNextUnreviewed(afterId);
    setSelectedEdgeIdState(nextEdgeId);
    
    if (afterId) {
      setLastReviewedEdgeId(null); // Reset after finding next
    }
  }, [selectedClusterId, visualizationData, lastReviewedEdgeId]);

  // Action: Advance to next cluster
  const advanceToNextCluster = useCallback(async () => {
    if (!selectedClusterId) return;

    const currentIndex = clusters.data.findIndex(c => c.id === selectedClusterId);
    if (currentIndex >= 0) {
      if (currentIndex < clusters.data.length - 1) {
        // Move to next cluster on same page
        const nextCluster = clusters.data[currentIndex + 1];
        setSelectedClusterIdState(nextCluster.id);
        setSelectedEdgeIdState(null);
        setLastReviewedEdgeId(null);
      } else if (clusters.page < Math.ceil(clusters.total / clusters.limit)) {
        // Move to next page
        await loadClusters(clusters.page + 1, clusters.limit);
      }
    }
  }, [selectedClusterId, clusters, loadClusters]);

  // NEW: Check if cluster is complete and advance - only called after review actions
  const checkAndAdvanceIfComplete = useCallback(async () => {
    if (!selectedClusterId) return;
    
    const progress = clusterProgress[selectedClusterId];
    if (progress?.isComplete) {
      // Small delay to let user see completion
      setTimeout(() => {
        advanceToNextCluster();
      }, 1000);
    }
  }, [selectedClusterId, clusterProgress, advanceToNextCluster]);

  // Action: Set selected cluster with cleanup
  const setSelectedClusterId = useCallback((id: string | null) => {
    if (selectedClusterId !== id) {
      setSelectedClusterIdState(id);
      setSelectedEdgeIdState(null);
      setLastReviewedEdgeId(null);
    }
  }, [selectedClusterId]);

  // Action: Set selected edge
  const setSelectedEdgeId = useCallback((id: string | null) => {
    setSelectedEdgeIdState(id);
  }, []);

  // Action: Trigger refresh
  const triggerRefresh = useCallback(() => {
    setRefreshTrigger((prev) => prev + 1);
  }, []);

  // Invalidation functions
  const invalidateVisualizationData = useCallback((clusterId: string) => {
    setVisualizationData((prev) => {
      const updated = { ...prev };
      delete updated[clusterId];
      return updated;
    });
  }, []);

  const invalidateConnectionData = useCallback((edgeId: string) => {
    setConnectionData((prev) => {
      const updated = { ...prev };
      delete updated[edgeId];
      return updated;
    });
  }, []);

  const clearAllData = useCallback(() => {
    setVisualizationData({});
    setConnectionData({});
    setClusters({
      data: [],
      total: 0,
      page: 1,
      limit: 10,
      loading: false,
      error: null,
    });
  }, []);

  // Query functions
  const queries = useMemo(() => ({
    isVisualizationDataLoaded: (clusterId: string): boolean => {
      return visualizationData[clusterId]?.data !== null && visualizationData[clusterId]?.data !== undefined;
    },
    isVisualizationDataLoading: (clusterId: string): boolean => {
      return visualizationData[clusterId]?.loading || false;
    },
    isConnectionDataLoaded: (edgeId: string): boolean => {
      return connectionData[edgeId]?.data !== null && connectionData[edgeId]?.data !== undefined;
    },
    isConnectionDataLoading: (edgeId: string): boolean => {
      return connectionData[edgeId]?.loading || false;
    },
    getVisualizationError: (clusterId: string): string | null => {
      return visualizationData[clusterId]?.error || null;
    },
    getConnectionError: (edgeId: string): string | null => {
      return connectionData[edgeId]?.error || null;
    },
    getClusterProgress: (clusterId: string): ClusterProgress | null => {
      return clusterProgress[clusterId] || null;
    },
    canAdvanceToNextCluster: (): boolean => {
      if (!selectedClusterId) return false;
      const progress = clusterProgress[selectedClusterId];
      return progress?.isComplete || false;
    },
    isEdgeReviewed: (edgeId: string): boolean => {
      if (!currentVisualizationData) return false;
      const edge = currentVisualizationData.links.find(l => l.id === edgeId);
      return edge ? edge.status !== 'PENDING_REVIEW' : false;
    },
    getEdgeStatus: (edgeId: string): 'PENDING_REVIEW' | 'CONFIRMED_MATCH' | 'CONFIRMED_NON_MATCH' | null => {
      if (!currentVisualizationData) return null;
      const edge = currentVisualizationData.links.find(l => l.id === edgeId);
      return edge?.status ?? null;
    },
  }), [visualizationData, connectionData, clusterProgress, selectedClusterId, currentVisualizationData]);

  // Auto-load clusters on mount
  useEffect(() => {
    loadClusters(1, 10);
  }, [loadClusters]);

  // Handle refresh trigger
  useEffect(() => {
    if (refreshTrigger > 0) {
      // Reload current clusters page
      loadClusters(clusters.page, clusters.limit);
      
      // Invalidate data for current cluster
      if (selectedClusterId) {
        invalidateVisualizationData(selectedClusterId);
      }
      
      // Invalidate connection data for current edge
      if (selectedEdgeId) {
        invalidateConnectionData(selectedEdgeId);
      }
    }
  }, [refreshTrigger, clusters.page, clusters.limit, selectedClusterId, selectedEdgeId, loadClusters, invalidateVisualizationData, invalidateConnectionData]);

  // Auto-select first cluster when clusters load
  useEffect(() => {
    if (!clusters.loading && clusters.data.length > 0 && !selectedClusterId) {
      setSelectedClusterId(clusters.data[0].id);
    }
  }, [clusters.loading, clusters.data, selectedClusterId, setSelectedClusterId]);

  // Auto-select first unreviewed edge when cluster changes (only if there are unreviewed edges)
  useEffect(() => {
    if (selectedClusterId && currentVisualizationData && edgeSelectionInfo.hasUnreviewedEdges) {
      if (!selectedEdgeId) {
        selectNextUnreviewedEdge(null); // Start from beginning
      }
    }
  }, [selectedClusterId, currentVisualizationData, selectedEdgeId, edgeSelectionInfo.hasUnreviewedEdges, selectNextUnreviewedEdge]);

  // REMOVED: Auto-advance cluster when current cluster is complete
  // This was causing unwanted advancement when browsing already-complete clusters
  // Auto-advancement now only happens via checkAndAdvanceIfComplete() after reviews

  const actions = useMemo(() => ({
    setSelectedClusterId,
    setSelectedEdgeId,
    setReviewerId,
    setLastReviewedEdgeId,
    triggerRefresh,
    loadClusters,
    preloadVisualizationData,
    loadConnectionData,
    invalidateVisualizationData,
    invalidateConnectionData,
    clearAllData,
    selectNextUnreviewedEdge,
    advanceToNextCluster,
    checkAndAdvanceIfComplete,
  }), [
    setSelectedClusterId,
    setSelectedEdgeId,
    setReviewerId,
    setLastReviewedEdgeId,
    triggerRefresh,
    loadClusters,
    preloadVisualizationData,
    loadConnectionData,
    invalidateVisualizationData,
    invalidateConnectionData,
    clearAllData,
    selectNextUnreviewedEdge,
    advanceToNextCluster,
    checkAndAdvanceIfComplete,
  ]);

  return (
    <EntityResolutionContext.Provider
      value={{
        // Core state (read-only)
        selectedClusterId,
        selectedEdgeId,
        reviewerId,
        lastReviewedEdgeId,
        refreshTrigger,

        // Data state (read-only)
        clusters,
        visualizationData,
        connectionData,

        // Computed state (read-only)
        clusterProgress,
        edgeSelectionInfo,
        currentVisualizationData,
        currentConnectionData,

        // Actions
        actions,

        // Queries
        queries,
      }}
    >
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
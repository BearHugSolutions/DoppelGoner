// context/entity-resolution-context.tsx
"use client";

import {
  ResolutionMode,
  // Entity types
  EntityClustersResponse,
  EntityCluster,
  EntityVisualizationDataResponse,
  EntityConnectionDataResponse,
  EntityGroup,
  EntityLink,
  VisualizationEntityEdge,
  EntityGroupReviewApiPayload,
  // Service types
  ServiceClustersResponse,
  ServiceCluster,
  ServiceVisualizationDataResponse,
  ServiceConnectionDataResponse,
  ServiceGroup,
  ServiceLink,
  VisualizationServiceEdge,
  ServiceGroupReviewApiPayload,
  // Common types
  GroupReviewDecision,
  ClusterFinalizationStatusResponse,
  QueuedReviewBatch,
  BaseCluster, // Using BaseCluster for generic state
  BaseLink,
  BaseNode, // BaseNode now includes source_system and source_id
} from "@/types/entity-resolution";
import {
  // Entity API calls
  getEntityClusters,
  getEntityVisualizationData,
  getEntityConnectionData,
  postEntityGroupFeedback,
  triggerEntityClusterFinalization,
  // Service API calls
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
import { useAuth } from "./auth-context"; // Assuming auth-context.tsx is in the same directory
import { useToast } from "@/hooks/use-toast";
import { Button } from "@/components/ui/button";
import { v4 as uuidv4 } from 'uuid';

// Enhanced data state interfaces (now generic)
interface ClustersState<TCluster extends BaseCluster> {
  data: TCluster[];
  total: number;
  page: number;
  limit: number;
  loading: boolean;
  error: string | null;
}

interface VisualizationState<TVisData> { // TVisData will be EntityVisualizationDataResponse or ServiceVisualizationDataResponse
  data: TVisData | null;
  loading: boolean;
  error: string | null;
  lastUpdated: number | null;
}

interface ConnectionState<TConnData> { // TConnData will be EntityConnectionDataResponse or ServiceConnectionDataResponse
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

  // State will now hold data based on resolutionMode
  clusters: ClustersState<EntityCluster | ServiceCluster>;
  visualizationData: Record<string, VisualizationState<EntityVisualizationDataResponse | ServiceVisualizationDataResponse>>;
  connectionData: Record<string, ConnectionState<EntityConnectionDataResponse | ServiceConnectionDataResponse>>;

  reviewQueue: QueuedReviewBatch[];
  isProcessingQueue: boolean;

  clusterProgress: Record<string, ClusterProgress>;
  edgeSelectionInfo: EdgeSelectionInfo;

  // These will provide data cast to the correct type based on mode
  currentVisualizationData: EntityVisualizationDataResponse | ServiceVisualizationDataResponse | null;
  currentConnectionData: EntityConnectionDataResponse | ServiceConnectionDataResponse | null;

  actions: {
    setResolutionMode: (mode: ResolutionMode) => void;
    setSelectedClusterId: (id: string | null) => void;
    setSelectedEdgeId: (id: string | null) => void;
    setReviewerId: (id: string) => void;
    setLastReviewedEdgeId: (id: string | null) => void;
    triggerRefresh: (target?: 'all' | 'clusters' | 'current_visualization' | 'current_connection') => void;
    loadClusters: (page: number, limit?: number) => Promise<void>;
    preloadVisualizationData: (clusterIds: string[]) => Promise<void>;
    loadConnectionData: (edgeId: string) => Promise<EntityConnectionDataResponse | ServiceConnectionDataResponse | null>;
    invalidateVisualizationData: (clusterId: string) => void;
    invalidateConnectionData: (edgeId: string) => void;
    clearAllData: () => void;
    selectNextUnreviewedEdge: (afterEdgeId?: string | null) => void;
    advanceToNextCluster: () => Promise<void>;
    checkAndAdvanceIfComplete: (clusterIdToCheck?: string) => Promise<void>;
    submitEdgeReview: (edgeId: string, decision: GroupReviewDecision) => Promise<void>;
    retryFailedBatch: (batchId: string) => void;
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
    getEdgeStatus: (edgeId: string) => BaseLink['status'] | null;
    getQueueItemStatus: (edgeId: string) => 'pending' | 'processing' | 'failed' | null;
  };
}

const EntityResolutionContext = createContext<EntityResolutionContextType | undefined>(undefined);

const MAX_REVIEW_ATTEMPTS = 3;

const initialClustersState: ClustersState<EntityCluster | ServiceCluster> = {
  data: [], total: 0, page: 1, limit: 10, loading: false, error: null,
};

export function EntityResolutionProvider({ children }: { children: ReactNode }) {
  const { currentUser } = useAuth();
  const { toast } = useToast();

  const [resolutionMode, setResolutionModeState] = useState<ResolutionMode>("entity");
  const [selectedClusterId, setSelectedClusterIdState] = useState<string | null>(null);
  const [selectedEdgeId, setSelectedEdgeIdState] = useState<string | null>(null);
  const [reviewerId, setReviewerId] = useState<string>("default-reviewer");
  const [refreshTrigger, setRefreshTrigger] = useState<number>(0);
  const [lastReviewedEdgeId, setLastReviewedEdgeId] = useState<string | null>(null);

  const [clusters, setClusters] = useState<ClustersState<EntityCluster | ServiceCluster>>(initialClustersState);
  const [visualizationData, setVisualizationData] = useState<Record<string, VisualizationState<EntityVisualizationDataResponse | ServiceVisualizationDataResponse>>>({});
  const [connectionData, setConnectionData] = useState<Record<string, ConnectionState<EntityConnectionDataResponse | ServiceConnectionDataResponse>>>({});

  const [reviewQueue, setReviewQueue] = useState<QueuedReviewBatch[]>([]);
  const [isProcessingQueue, setIsProcessingQueue] = useState<boolean>(false);
  const processingBatchIdRef = useRef<string | null>(null);


  const setResolutionMode = useCallback((mode: ResolutionMode) => {
    if (mode === resolutionMode) return;
    console.log(`Switching resolution mode from ${resolutionMode} to ${mode}`);
    setResolutionModeState(mode);
    // Reset selections and data when mode changes
    setSelectedClusterIdState(null);
    setSelectedEdgeIdState(null);
    setLastReviewedEdgeId(null);
    setClusters(initialClustersState); // Reset clusters
    setVisualizationData({}); // Clear visualization cache
    setConnectionData({}); // Clear connection cache
    // The useEffect for loadClusters will trigger based on the new mode.
  }, [resolutionMode]);


  const preloadVisualizationData = useCallback(async (clusterIds: string[]) => {
    const getVizData = resolutionMode === 'entity' ? getEntityVisualizationData : getServiceVisualizationData;
    console.log(`Preloading visualization data for mode: ${resolutionMode}, clusters:`, clusterIds);

    clusterIds.forEach(clusterId => {
      if (!visualizationData[clusterId]?.data || visualizationData[clusterId]?.error) {
        setVisualizationData(prev => ({
          ...prev,
          [clusterId]: { data: prev[clusterId]?.data || null, loading: true, error: null, lastUpdated: prev[clusterId]?.lastUpdated || null },
        }));
        getVizData(clusterId)
          .then(data => {
            console.log(`Successfully preloaded viz data for cluster ${clusterId}`, data);
            setVisualizationData(prev => ({
            ...prev, [clusterId]: { data, loading: false, error: null, lastUpdated: Date.now() },
          }))})
          .catch(error => {
            console.error(`Error preloading viz data for cluster ${clusterId}:`, error);
            setVisualizationData(prev => ({
            ...prev, [clusterId]: { data: null, loading: false, error: (error as Error).message, lastUpdated: null },
          }))});
      }
    });
  }, [visualizationData, resolutionMode]);

  const loadConnectionData = useCallback(async (edgeId: string): Promise<EntityConnectionDataResponse | ServiceConnectionDataResponse | null> => {
    const getConnData = resolutionMode === 'entity' ? getEntityConnectionData : getServiceConnectionData;
    const cached = connectionData[edgeId];
    console.log(`Loading connection data for edge: ${edgeId}, mode: ${resolutionMode}`);

    if (cached?.data && !cached.loading && !cached.error && cached.lastUpdated && (Date.now() - cached.lastUpdated < 300000)) { // 5 min cache
      console.log(`Using cached connection data for edge ${edgeId}`);
      return cached.data;
    }
    setConnectionData(prev => ({
      ...prev,
      [edgeId]: { data: prev[edgeId]?.data || null, loading: true, error: null, lastUpdated: prev[edgeId]?.lastUpdated || null }
    }));
    try {
      const data = await getConnData(edgeId);
      console.log(`Successfully loaded connection data for edge ${edgeId}`, data);
      setConnectionData(prev => ({
        ...prev,
        [edgeId]: { data, loading: false, error: null, lastUpdated: Date.now() }
      }));
      return data;
    } catch (error) {
      console.error(`Error loading connection data for edge ${edgeId}:`, error);
      setConnectionData(prev => ({
        ...prev,
        [edgeId]: { data: null, loading: false, error: (error as Error).message, lastUpdated: null }
      }));
      return null;
    }
  }, [connectionData, resolutionMode]);

  const invalidateVisualizationData = useCallback((clusterId: string) => {
    console.log(`Invalidating visualization data for cluster: ${clusterId}`);
    setVisualizationData(prev => ({ ...prev, [clusterId]: { data: null, loading: false, error: `Invalidated by user action`, lastUpdated: null }}));
    // Optionally, trigger a reload if it's the currently selected cluster
    if (selectedClusterId === clusterId) {
        preloadVisualizationData([clusterId]);
    }
  }, [selectedClusterId, preloadVisualizationData]);

  const invalidateConnectionData = useCallback((edgeId: string) => {
    console.log(`Invalidating connection data for edge: ${edgeId}`);
    setConnectionData(prev => ({
      ...prev,
      [edgeId]: { data: null, loading: false, error: `Invalidated by user action`, lastUpdated: null }
    }));
     if (selectedEdgeId === edgeId) {
        loadConnectionData(edgeId);
    }
  }, [selectedEdgeId, loadConnectionData]);

  const loadClusters = useCallback(async (page: number, limit: number = 10) => {
    console.log(`Loading clusters for mode: ${resolutionMode}, page: ${page}, limit: ${limit}`);
    setClusters((prev) => ({ ...prev, loading: true, error: null, page, limit })); // Update page and limit optimistically
    const fetcher = resolutionMode === 'entity' ? getEntityClusters : getServiceClusters;
    try {
      // The fetcher function (getEntityClusters or getServiceClusters) should return a response
      // that matches the ClustersResponse<EntityCluster> or ClustersResponse<ServiceCluster> structure.
      const response = await fetcher(page, limit);
      console.log(`Successfully loaded clusters for mode ${resolutionMode}`, response);
      setClusters({
        data: response.clusters as (EntityCluster[] | ServiceCluster[]), // Cast is okay if fetcher is typed
        total: response.total, page, limit, loading: false, error: null,
      });
      const clusterIds = response.clusters.map(c => c.id);
      if (clusterIds.length > 0) {
        preloadVisualizationData(clusterIds);
      }
    } catch (error) {
      console.error(`Error loading clusters for mode ${resolutionMode}:`, error);
      setClusters((prev) => ({ ...prev, loading: false, error: (error as Error).message, data: [], total:0, page:1, limit:10 }));
    }
  }, [preloadVisualizationData, resolutionMode]);

  const setSelectedClusterId = useCallback((id: string | null) => {
    console.log(`Setting selected cluster ID to: ${id}`);
    setSelectedClusterIdState(id);
    setSelectedEdgeIdState(null); // Reset edge selection when cluster changes
    setLastReviewedEdgeId(null); // Reset last reviewed edge
    if (id && (!visualizationData[id]?.data && !visualizationData[id]?.loading)) {
      preloadVisualizationData([id]);
    }
  }, [visualizationData, preloadVisualizationData]);

  // Renamed to avoid conflict with state setter, and to be explicit about its action.
  const setSelectedEdgeIdAction = useCallback((id: string | null) => {
    console.log(`Setting selected edge ID to: ${id}`);
    setSelectedEdgeIdState(id);
  }, []);


  const clearAllData = useCallback(() => {
      console.log("Clearing all data.");
      setClusters(initialClustersState);
      setVisualizationData({});
      setConnectionData({});
      setReviewQueue([]);
      setSelectedClusterIdState(null);
      setSelectedEdgeIdState(null);
      setLastReviewedEdgeId(null);
   }, []);

  const retryFailedBatch = useCallback((batchId: string) => {
    console.log(`Retrying failed batch: ${batchId}`);
    setReviewQueue(prev => prev.map(b => {
      if (b.batchId === batchId && b.isTerminalFailure) {
        return { ...b, attempt: 0, isTerminalFailure: false, error: undefined, failedOperations: new Set(), processedOperations: new Set() };
      }
      return b;
    }));
  }, []);

  const triggerRefresh = useCallback((target: 'all' | 'clusters' | 'current_visualization' | 'current_connection' = 'all') => {
    console.log(`Triggering refresh for target: ${target}`);
    if (target === 'all' || target === 'clusters') {
        loadClusters(clusters.page, clusters.limit);
    }
    if (selectedClusterId && (target === 'all' || target === 'current_visualization')) {
        invalidateVisualizationData(selectedClusterId);
    }
    if (selectedEdgeId && (target === 'all' || target === 'current_connection')) {
        invalidateConnectionData(selectedEdgeId);
    }
    setRefreshTrigger(prev => prev + 1); // For components that might want to react to a general refresh
  }, [clusters.page, clusters.limit, selectedClusterId, selectedEdgeId, loadClusters, invalidateVisualizationData, invalidateConnectionData]);


  const submitEdgeReview = useCallback(async (edgeId: string, decision: GroupReviewDecision) => {
    if (!currentUser?.id) {
      toast({ title: "Authentication Error", description: "You must be logged in to submit reviews.", variant: "destructive" });
      return;
    }
    if (!selectedClusterId) {
      toast({ title: "Selection Error", description: "A cluster must be selected.", variant: "destructive" });
      return;
    }
    console.log(`Submitting edge review for edge: ${edgeId}, decision: ${decision}, mode: ${resolutionMode}`);

    const connData = connectionData[edgeId]?.data;
    const vizData = visualizationData[selectedClusterId]?.data;

    if (!connData || !vizData) {
      toast({ title: "Data Error", description: "Connection or visualization data not loaded for review.", variant: "destructive" });
      if (!connData) loadConnectionData(edgeId); // Attempt to load if missing
      if (!vizData && selectedClusterId) preloadVisualizationData([selectedClusterId]); // Attempt to load if missing
      return;
    }

    let relevantGroups: Array<EntityGroup | ServiceGroup> = [];
    let currentEdgeLink: EntityLink | ServiceLink | undefined; // These are BaseLink

    // Type guards and specific data access based on resolutionMode
    if (resolutionMode === 'entity' && 'entityGroups' in connData && 'entity1' in connData && 'links' in vizData) {
        const entityConnData = connData as EntityConnectionDataResponse;
        const entityVizData = vizData as EntityVisualizationDataResponse;
        relevantGroups = entityConnData.entityGroups.filter(group =>
            (group.entity_id_1 === entityConnData.edge.entity_id_1 && group.entity_id_2 === entityConnData.edge.entity_id_2) ||
            (group.entity_id_1 === entityConnData.edge.entity_id_2 && group.entity_id_2 === entityConnData.edge.entity_id_1)
        );
        currentEdgeLink = entityVizData.links.find(l => l.id === edgeId);
    } else if (resolutionMode === 'service' && 'entityGroups' in connData && 'entity1' in connData && 'links' in vizData) {
        // Note: ServiceConnectionDataResponse also uses 'entityGroups' for its groups array, but they are ServiceGroup[]
        const serviceConnData = connData as ServiceConnectionDataResponse;
        const serviceVizData = vizData as ServiceVisualizationDataResponse;
        relevantGroups = (serviceConnData.entityGroups as ServiceGroup[]).filter(group =>
            (group.service_id_1 === serviceConnData.edge.service_id_1 && group.service_id_2 === serviceConnData.edge.service_id_2) ||
            (group.service_id_1 === serviceConnData.edge.service_id_2 && group.service_id_2 === serviceConnData.edge.service_id_1)
        );
        currentEdgeLink = serviceVizData.links.find(l => l.id === edgeId);
    }


    if (relevantGroups.length === 0) {
      toast({ title: "Info", description: "No underlying groups for this edge. Marking as reviewed based on decision." });
      // Optimistic update for edges with no groups (directly update the link status)
      setVisualizationData(prev => {
        const newVizData = { ...prev };
        const targetClusterVizState = newVizData[selectedClusterId!]?.data;
        if (targetClusterVizState?.links) {
          // Create a new object for the targetClusterViz to ensure state update
          const newTargetClusterViz = { ...targetClusterVizState };
          const linkIndex = newTargetClusterViz.links.findIndex(l => l.id === edgeId);
          if (linkIndex !== -1) {
            // Ensure we're creating a new array for links to trigger re-render
            const newLinks = [...newTargetClusterViz.links];
            newLinks[linkIndex] = { ...newLinks[linkIndex], status: decision === 'ACCEPTED' ? 'CONFIRMED_MATCH' : 'CONFIRMED_NON_MATCH' };
            newTargetClusterViz.links = newLinks;
          }
           // Update the state with the modified targetClusterViz
           newVizData[selectedClusterId!] = { ...newVizData[selectedClusterId!], data: newTargetClusterViz, lastUpdated: Date.now() };
        }
        return newVizData;
      });
      setLastReviewedEdgeId(edgeId);
      return;
    }

    if (!currentEdgeLink) {
        console.error("Critical Error: Current edge link not found in visualization data for optimistic update.");
        toast({title: "Internal Error", description: "Could not find edge in visualization for update. Please refresh.", variant: "destructive"});
        return;
    }

    const operations = relevantGroups.map(group => ({
      groupId: group.id,
      originalGroupStatus: group.confirmed_status, // This is 'PENDING_REVIEW' | 'CONFIRMED_MATCH' | 'CONFIRMED_NON_MATCH'
    }));

    const optimisticEdgeStatus = decision === 'ACCEPTED' ? 'CONFIRMED_MATCH' : 'CONFIRMED_NON_MATCH';
    const optimisticGroupStatus = decision === 'ACCEPTED' ? 'CONFIRMED_MATCH' : 'CONFIRMED_NON_MATCH';

    const batch: QueuedReviewBatch = {
      batchId: uuidv4(),
      edgeId,
      clusterId: selectedClusterId,
      decision,
      reviewerId: currentUser.id, // Safe due to earlier check
      operations,
      originalEdgeStatus: currentEdgeLink.status || 'PENDING_REVIEW', // BaseLink status
      optimisticEdgeStatus,
      optimisticGroupStatus, // This is for EntityGroup/ServiceGroup confirmed_status
      attempt: 0,
      processedOperations: new Set(),
      failedOperations: new Set(),
      mode: resolutionMode,
    };

    // Optimistic UI updates
    setVisualizationData(prevVizData => {
        const newVizDataState = { ...prevVizData };
        const targetClusterVizState = newVizDataState[batch.clusterId];

        if (targetClusterVizState?.data) {
            // Create a shallow copy of the specific cluster's data to modify it
            const targetClusterViz = { ...targetClusterVizState.data };
            
            const linkIndex = targetClusterViz.links.findIndex(l => l.id === batch.edgeId);
            if (linkIndex !== -1) {
                // Create a new links array for immutability
                targetClusterViz.links = [...targetClusterViz.links];
                targetClusterViz.links[linkIndex] = { ...targetClusterViz.links[linkIndex], status: batch.optimisticEdgeStatus };
            }

            // Update groups within the visualization data
            if ('entityGroups' in targetClusterViz && Array.isArray(targetClusterViz.entityGroups)) {
                if (batch.mode === 'entity') {
                    const currentEntityGroups = targetClusterViz.entityGroups as EntityGroup[];
                    const updatedEntityGroups: EntityGroup[] = currentEntityGroups.map(group => {
                        if (batch.operations.some(op => op.groupId === group.id)) {
                            return { ...group, confirmed_status: batch.optimisticGroupStatus };
                        }
                        return group;
                    });
                    (targetClusterViz as EntityVisualizationDataResponse).entityGroups = updatedEntityGroups;
                } else if (batch.mode === 'service') {
                    const currentServiceGroups = targetClusterViz.entityGroups as ServiceGroup[];
                    const updatedServiceGroups: ServiceGroup[] = currentServiceGroups.map(group => {
                        if (batch.operations.some(op => op.groupId === group.id)) {
                            return { ...group, confirmed_status: batch.optimisticGroupStatus };
                        }
                        return group;
                    });
                    (targetClusterViz as ServiceVisualizationDataResponse).entityGroups = updatedServiceGroups;
                }
            }
            // Update the state with the modified targetClusterViz
            newVizDataState[batch.clusterId] = { ...targetClusterVizState, data: targetClusterViz, lastUpdated: Date.now() };
        }
        return newVizDataState;
    });

    setConnectionData(prevConnData => {
        const newConnDataState = { ...prevConnData };
        const targetConnState = newConnDataState[batch.edgeId];

        if (targetConnState?.data) {
            // Create a shallow copy of the specific connection's data
            const targetConn = { ...targetConnState.data };
            
            if ('edge' in targetConn) {
              // The edge in ConnectionDataResponse (VisualizationEntityEdge/VisualizationServiceEdge) also has a status.
              (targetConn.edge as VisualizationEntityEdge | VisualizationServiceEdge).status = batch.optimisticEdgeStatus;
              // And it might have confirmed_status if it's directly on the edge object
              if ('confirmed_status' in targetConn.edge) {
                (targetConn.edge as any).confirmed_status = batch.optimisticGroupStatus;
              }
            }

            if ('entityGroups' in targetConn && Array.isArray(targetConn.entityGroups)) {
                if (batch.mode === 'entity') {
                    const currentEntityGroups = targetConn.entityGroups as EntityGroup[];
                    const updatedEntityGroups: EntityGroup[] = currentEntityGroups.map(group => {
                        if (batch.operations.some(op => op.groupId === group.id)) {
                            return { ...group, confirmed_status: batch.optimisticGroupStatus };
                        }
                        return group;
                    });
                    (targetConn as EntityConnectionDataResponse).entityGroups = updatedEntityGroups;
                } else if (batch.mode === 'service') {
                     const currentServiceGroups = targetConn.entityGroups as ServiceGroup[];
                     const updatedServiceGroups: ServiceGroup[] = currentServiceGroups.map(group => {
                        if (batch.operations.some(op => op.groupId === group.id)) {
                            return { ...group, confirmed_status: batch.optimisticGroupStatus };
                        }
                        return group;
                    });
                    (targetConn as ServiceConnectionDataResponse).entityGroups = updatedServiceGroups;
                }
            }
            // Update the state with the modified targetConn
            newConnDataState[batch.edgeId] = { ...targetConnState, data: targetConn, lastUpdated: Date.now() };
        }
        return newConnDataState;
    });

    setReviewQueue(prevQueue => [...prevQueue, batch]);
    setLastReviewedEdgeId(edgeId);

  }, [currentUser, selectedClusterId, connectionData, visualizationData, toast, resolutionMode, loadConnectionData, preloadVisualizationData]);


  const advanceToNextCluster = useCallback(async () => {
    if (!selectedClusterId) return;
    console.log(`Advancing to next cluster from: ${selectedClusterId}`);
    const currentIndex = clusters.data.findIndex(c => c.id === selectedClusterId);
    if (currentIndex === -1) {
        console.warn("Current cluster not found in cluster list, cannot advance.");
        return;
    }

    if (currentIndex < clusters.data.length - 1) {
      const nextClusterId = clusters.data[currentIndex + 1].id;
      console.log(`Selecting next cluster in current page: ${nextClusterId}`);
      setSelectedClusterIdState(nextClusterId); // This will trigger edge selection logic via useEffect
    } else if (clusters.page < Math.ceil(clusters.total / clusters.limit)) {
      const nextPage = clusters.page + 1;
      console.log(`Loading next page of clusters: ${nextPage}`);
      await loadClusters(nextPage, clusters.limit);
      // After new clusters are loaded, a useEffect will select the first one if none is selected.
    } else {
      toast({ title: "Workflow Complete", description: "All clusters have been processed." });
      console.log("All clusters processed.");
    }
  }, [selectedClusterId, clusters, loadClusters, toast]); // resolutionMode removed as it's part of loadClusters' closure


  const checkAndAdvanceIfComplete = useCallback(async (clusterIdToCheck?: string) => {
    const targetClusterId = clusterIdToCheck || selectedClusterId;
    if (!targetClusterId) return;
    console.log(`Checking if cluster ${targetClusterId} is complete.`);

    const currentVizData = visualizationData[targetClusterId]?.data;
    if (!currentVizData?.links) {
        console.log(`No visualization data or links for cluster ${targetClusterId}, cannot determine completeness.`);
        return;
    }

    const { links } = currentVizData;
    const totalEdges = links.length;
    const reviewedEdges = links.filter(link => link.status !== 'PENDING_REVIEW').length;
    const isComplete = totalEdges > 0 && reviewedEdges === totalEdges;

    console.log(`Cluster ${targetClusterId}: Total=${totalEdges}, Reviewed=${reviewedEdges}, Complete=${isComplete}`);

    if (isComplete) {
      toast({ title: "Cluster Review Complete", description: `Review of cluster ${targetClusterId.substring(0,8)}... is complete.` });
      const finalizer = resolutionMode === 'entity' ? triggerEntityClusterFinalization : triggerServiceClusterFinalization;
      try {
        console.log(`Finalizing cluster ${targetClusterId} for mode ${resolutionMode}.`);
        const finalizationResponse = await finalizer(targetClusterId);
        toast({ title: "Cluster Finalization", description: `${finalizationResponse.message}. Status: ${finalizationResponse.status}` });
        console.log(`Finalization response for ${targetClusterId}:`, finalizationResponse);

        // Refresh cluster list to reflect changes (e.g., if a cluster was split or modified)
        // and invalidate the specific cluster's viz data as it might have changed or been removed.
        if (finalizationResponse.status === 'COMPLETED_SPLIT_OCCURRED' || finalizationResponse.status === 'COMPLETED_NO_SPLIT_NEEDED') {
           loadClusters(clusters.page, clusters.limit); // Reload current page
           invalidateVisualizationData(targetClusterId); // Invalidate the just-finalized cluster
           if (finalizationResponse.newClusterIds && finalizationResponse.newClusterIds.length > 0) {
                preloadVisualizationData(finalizationResponse.newClusterIds); // Preload new clusters if any
           }
        }
        // If the finalized cluster was the selected one, advance.
        if (targetClusterId === selectedClusterId) {
            advanceToNextCluster();
        }

      } catch (finalizationError) {
        toast({ title: "Finalization Error", description: `Could not finalize cluster ${targetClusterId.substring(0,8)}...: ${(finalizationError as Error).message}`, variant: "destructive" });
        console.error(`Error finalizing cluster ${targetClusterId}:`, finalizationError);
      }
    }
  }, [selectedClusterId, visualizationData, advanceToNextCluster, toast, invalidateVisualizationData, loadClusters, clusters.page, clusters.limit, resolutionMode, preloadVisualizationData]);


  const processReviewQueue = useCallback(async () => {
    if (isProcessingQueue || reviewQueue.length === 0) return;

    const batchToProcess = reviewQueue.find(b => !b.isTerminalFailure && (b.failedOperations.size === 0 || b.attempt < MAX_REVIEW_ATTEMPTS));
    if (!batchToProcess) {
      setIsProcessingQueue(false);
      return;
    }
    // Ensure only one batch is processed at a time by this instance of the hook
    if (processingBatchIdRef.current && processingBatchIdRef.current !== batchToProcess.batchId) {
        console.log(`Queue processor is busy with batch ${processingBatchIdRef.current}, skipping ${batchToProcess.batchId}`);
        return;
    }

    setIsProcessingQueue(true);
    processingBatchIdRef.current = batchToProcess.batchId;
    console.log(`Processing review batch: ${batchToProcess.batchId}, attempt: ${batchToProcess.attempt + 1}, mode: ${batchToProcess.mode}`);

    let currentBatch = { ...batchToProcess, attempt: batchToProcess.attempt + 1, failedOperations: new Set(batchToProcess.failedOperations) };
    let batchOverallSuccess = true;
    const stillPendingOperations = currentBatch.operations.filter(op => !currentBatch.processedOperations.has(op.groupId));

    // Determine the correct API call based on the batch's mode
    const feedbackPoster = currentBatch.mode === 'entity' ? postEntityGroupFeedback : postServiceGroupFeedback;

    for (const op of stillPendingOperations) {
      try {
        // Payload is generic enough for both entity and service feedback
        const payload: EntityGroupReviewApiPayload | ServiceGroupReviewApiPayload = {
          decision: currentBatch.decision,
          reviewerId: currentBatch.reviewerId,
          // notes: op.notes, // If you add notes per operation
        };
        console.log(`Submitting feedback for group ${op.groupId} in batch ${currentBatch.batchId}`);
        await feedbackPoster(op.groupId, payload);
        currentBatch.processedOperations.add(op.groupId);
        console.log(`Successfully submitted feedback for group ${op.groupId}`);
      } catch (error) {
        console.error(`Failed to submit review for group ${op.groupId} (mode: ${currentBatch.mode}) in batch ${currentBatch.batchId}:`, error);
        currentBatch.failedOperations.add(op.groupId);
        currentBatch.error = `Group ${op.groupId.substring(0,8)}...: ${(error as Error).message}`; // Store specific error
        batchOverallSuccess = false;
        // Do not break, try to process other operations in the batch
      }
    }

    if (!batchOverallSuccess) {
        console.warn(`Batch ${currentBatch.batchId} encountered errors. Attempt ${currentBatch.attempt}/${MAX_REVIEW_ATTEMPTS}.`);
        if (currentBatch.attempt >= MAX_REVIEW_ATTEMPTS) {
            currentBatch.isTerminalFailure = true;
            toast({
                title: "Review Submission Failed Permanently",
                description: (
                <>
                    Failed to save review for connection {currentBatch.edgeId.substring(0,8)}... ({currentBatch.mode}) after {currentBatch.attempt} attempts. Error: {currentBatch.error}
                    <Button variant="link" className="p-0 h-auto ml-2 text-destructive-foreground underline" onClick={() => setSelectedEdgeIdState(currentBatch.edgeId)}>
                    View Connection
                    </Button>
                    <Button variant="link" className="p-0 h-auto ml-2 text-destructive-foreground underline" onClick={() => retryFailedBatch(currentBatch.batchId)}>
                    Retry Manually
                    </Button>
                </>
                ),
                variant: "destructive",
                duration: 10000,
            });
            console.error(`Batch ${currentBatch.batchId} reached max attempts and failed permanently.`);

            // Revert optimistic updates for the failed batch
            setVisualizationData(prevVizData => {
                const newVizDataState = { ...prevVizData };
                const targetClusterVizState = newVizDataState[currentBatch.clusterId];
                if (targetClusterVizState?.data) {
                    const targetClusterViz = { ...targetClusterVizState.data }; // Shallow copy
                    const linkIndex = targetClusterViz.links.findIndex(l => l.id === currentBatch.edgeId);
                    if (linkIndex !== -1) {
                        targetClusterViz.links = [...targetClusterViz.links]; // New array for links
                        targetClusterViz.links[linkIndex] = { ...targetClusterViz.links[linkIndex], status: currentBatch.originalEdgeStatus };
                    }
                     if ('entityGroups' in targetClusterViz && Array.isArray(targetClusterViz.entityGroups)) {
                        if (currentBatch.mode === 'entity') {
                            const currentEntityGroups = targetClusterViz.entityGroups as EntityGroup[];
                            const updatedEntityGroups: EntityGroup[] = currentEntityGroups.map(eg => {
                                const op = currentBatch.operations.find(o => o.groupId === eg.id);
                                if (op) return { ...eg, confirmed_status: op.originalGroupStatus };
                                return eg;
                            });
                            (targetClusterViz as EntityVisualizationDataResponse).entityGroups = updatedEntityGroups;
                        } else if (currentBatch.mode === 'service') {
                            const currentServiceGroups = targetClusterViz.entityGroups as ServiceGroup[];
                            const updatedServiceGroups: ServiceGroup[] = currentServiceGroups.map(eg => {
                                const op = currentBatch.operations.find(o => o.groupId === eg.id);
                                if (op) return { ...eg, confirmed_status: op.originalGroupStatus };
                                return eg;
                            });
                            (targetClusterViz as ServiceVisualizationDataResponse).entityGroups = updatedServiceGroups;
                        }
                    }
                    newVizDataState[currentBatch.clusterId] = { ...targetClusterVizState, data: targetClusterViz, lastUpdated: Date.now() };
                }
                return newVizDataState;
            });
            setConnectionData(prevConnData => {
                const newConnDataState = { ...prevConnData };
                const targetConnState = newConnDataState[currentBatch.edgeId];
                if (targetConnState?.data) {
                    const targetConn = { ...targetConnState.data }; // Shallow copy
                     if ('edge' in targetConn) {
                        (targetConn.edge as VisualizationEntityEdge | VisualizationServiceEdge).status = currentBatch.originalEdgeStatus;
                         if ('confirmed_status' in targetConn.edge) { // If edge itself has confirmed_status
                            (targetConn.edge as any).confirmed_status = currentBatch.originalEdgeStatus; // This might be too broad, check specific edge type
                         }
                     }
                     if ('entityGroups' in targetConn && Array.isArray(targetConn.entityGroups)) {
                        if (currentBatch.mode === 'entity') {
                            const currentEntityGroups = targetConn.entityGroups as EntityGroup[];
                            const updatedEntityGroups: EntityGroup[] = currentEntityGroups.map(eg => {
                                const op = currentBatch.operations.find(o => o.groupId === eg.id);
                                if (op) return { ...eg, confirmed_status: op.originalGroupStatus };
                                return eg;
                            });
                            (targetConn as EntityConnectionDataResponse).entityGroups = updatedEntityGroups;
                        } else if (currentBatch.mode === 'service') {
                            const currentServiceGroups = targetConn.entityGroups as ServiceGroup[];
                            const updatedServiceGroups: ServiceGroup[] = currentServiceGroups.map(eg => {
                                const op = currentBatch.operations.find(o => o.groupId === eg.id);
                                if (op) return { ...eg, confirmed_status: op.originalGroupStatus };
                                return eg;
                            });
                            (targetConn as ServiceConnectionDataResponse).entityGroups = updatedServiceGroups;
                        }
                     }
                    newConnDataState[currentBatch.edgeId] = { ...targetConnState, data: targetConn, lastUpdated: Date.now() };
                }
                return newConnDataState;
            });
            setReviewQueue(prevQ => prevQ.map(b => b.batchId === currentBatch.batchId ? { ...currentBatch, isTerminalFailure: true } : b));
        } else {
            // Not a terminal failure yet, will retry automatically
            toast({
                title: "Review Submission Issue",
                description: `Attempt ${currentBatch.attempt}/${MAX_REVIEW_ATTEMPTS} failed for connection ${currentBatch.edgeId.substring(0,8)}... (${currentBatch.mode}). Will retry. Error: ${currentBatch.error}`,
                variant: "default",
                duration: 5000,
            });
            setReviewQueue(prevQ => prevQ.map(b => b.batchId === currentBatch.batchId ? currentBatch : b));
        }
    } else {
      // Batch processed successfully
      console.log(`Batch ${currentBatch.batchId} processed successfully.`);
      setReviewQueue(prevQ => prevQ.filter(b => b.batchId !== currentBatch.batchId));
      // After successful processing, check if the cluster this edge belonged to is now complete.
      if (currentBatch.clusterId) {
        checkAndAdvanceIfComplete(currentBatch.clusterId);
      }
    }
    processingBatchIdRef.current = null; // Release the lock
    setIsProcessingQueue(false); // Allow next batch processing
  }, [reviewQueue, isProcessingQueue, toast, currentUser, checkAndAdvanceIfComplete, retryFailedBatch]); // resolutionMode removed from deps, using currentBatch.mode


  const selectNextUnreviewedEdge = useCallback((afterEdgeId?: string | null) => {
    const currentClusterId = selectedClusterId;
    if (!currentClusterId) return;
    console.log(`Selecting next unreviewed edge in cluster ${currentClusterId}, after: ${afterEdgeId}`);

    const currentViz = visualizationData[currentClusterId]?.data;
    if (!currentViz?.links || currentViz.links.length === 0) {
        console.log("No links in current visualization data to select from.");
        checkAndAdvanceIfComplete(currentClusterId); // Check if empty cluster is "complete"
        return;
    }

    const { links } = currentViz;
    let startIdx = 0;
    const referenceEdgeId = afterEdgeId || lastReviewedEdgeId || selectedEdgeId; // Prioritize afterEdgeId, then lastReviewed, then current

    if(referenceEdgeId) {
        const idx = links.findIndex(l => l.id === referenceEdgeId);
        if (idx !== -1) startIdx = idx + 1;
    }

    for (let i = 0; i < links.length; i++) {
      const link = links[(startIdx + i) % links.length]; // Loop through links starting from startIdx
      if (link.status === 'PENDING_REVIEW') {
        console.log(`Next unreviewed edge found: ${link.id}`);
        setSelectedEdgeIdState(link.id);
        return;
      }
    }
    // If no PENDING_REVIEW edges are found
    console.log(`No more unreviewed edges in cluster ${currentClusterId}.`);
    toast({ title: "Cluster Review Status", description: "All connections in this cluster have been reviewed or are being processed." });
    checkAndAdvanceIfComplete(currentClusterId); // This will handle advancing if truly complete

  }, [selectedClusterId, selectedEdgeId, lastReviewedEdgeId, visualizationData, toast, checkAndAdvanceIfComplete]);


  // Memoized current visualization data based on selectedClusterId
  const currentVisualizationData = useMemo((): EntityVisualizationDataResponse | ServiceVisualizationDataResponse | null => {
    if (!selectedClusterId) return null;
    return visualizationData[selectedClusterId]?.data || null;
  }, [selectedClusterId, visualizationData]);

  // Memoized current connection data based on selectedEdgeId
  const currentConnectionData = useMemo((): EntityConnectionDataResponse | ServiceConnectionDataResponse | null => {
    if (!selectedEdgeId) return null;
    return connectionData[selectedEdgeId]?.data || null;
  }, [selectedEdgeId, connectionData]);


  // Memoized information about the current edge selection and availability of next edges
  const edgeSelectionInfo = useMemo((): EdgeSelectionInfo => {
    if (!currentVisualizationData?.links) return { currentEdgeId: selectedEdgeId, nextUnreviewedEdgeId: null, hasUnreviewedEdges: false, currentEdgeIndex: -1, totalEdges: 0 };
    const { links } = currentVisualizationData;
    const totalEdges = links.length;

    const findNextAfter = (afterId: string | null): string | null => {
        const startIndex = afterId ? links.findIndex(l => l.id === afterId) + 1 : 0;
        for (let i = 0; i < links.length; i++) {
            const currentIndex = (startIndex + i) % links.length;
            if (links[currentIndex].status === 'PENDING_REVIEW') return links[currentIndex].id;
        }
        return null;
    };
    // Try to find next unreviewed after the last one actually reviewed, or the current one if none reviewed yet
    const nextUnreviewedEdgeId = findNextAfter(lastReviewedEdgeId || selectedEdgeId);
    const unreviewedLinksCount = links.filter(link => link.status === 'PENDING_REVIEW').length;

    return {
      currentEdgeId: selectedEdgeId,
      nextUnreviewedEdgeId,
      hasUnreviewedEdges: unreviewedLinksCount > 0,
      currentEdgeIndex: selectedEdgeId ? links.findIndex(link => link.id === selectedEdgeId) : -1,
      totalEdges,
    };
  }, [currentVisualizationData, selectedEdgeId, lastReviewedEdgeId]);


  // Effect to load clusters when the component mounts or resolutionMode changes
  useEffect(() => {
    loadClusters(1, clusters.limit);
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [resolutionMode]); // loadClusters, clusters.limit removed to prevent loops. resolutionMode is the primary trigger.

  // Effect to select the first cluster if none is selected and clusters are loaded
  useEffect(() => {
    if (!selectedClusterId && clusters.data.length > 0 && !clusters.loading) {
      console.log("Auto-selecting first cluster:", clusters.data[0].id);
      setSelectedClusterIdState(clusters.data[0].id);
    }
  }, [selectedClusterId, clusters.data, clusters.loading]);

  // Effect to automatically select the next unreviewed edge when a cluster is selected or visualization data changes
  useEffect(() => {
    if (selectedClusterId && currentVisualizationData?.links && !selectedEdgeId && edgeSelectionInfo.hasUnreviewedEdges) {
       if (edgeSelectionInfo.nextUnreviewedEdgeId) {
           console.log(`Auto-selecting next unreviewed edge: ${edgeSelectionInfo.nextUnreviewedEdgeId}`);
           setSelectedEdgeIdState(edgeSelectionInfo.nextUnreviewedEdgeId);
       } else {
            // This case might happen if edgeSelectionInfo is stale, try to find one.
            console.log("No nextUnreviewedEdgeId in edgeSelectionInfo, attempting to selectNextUnreviewedEdge.");
            selectNextUnreviewedEdge();
       }
    }
  }, [selectedClusterId, currentVisualizationData, selectedEdgeId, selectNextUnreviewedEdge, edgeSelectionInfo.hasUnreviewedEdges, edgeSelectionInfo.nextUnreviewedEdgeId]);

  // Memoize actions to prevent unnecessary re-renders of consumer components
  const actions = useMemo(() => ({
    setResolutionMode,
    setSelectedClusterId,
    setSelectedEdgeId: setSelectedEdgeIdAction, // Use the renamed action
    setReviewerId,
    setLastReviewedEdgeId,
    triggerRefresh, loadClusters, preloadVisualizationData, loadConnectionData,
    invalidateVisualizationData, invalidateConnectionData,
    clearAllData,
    selectNextUnreviewedEdge, advanceToNextCluster, checkAndAdvanceIfComplete,
    submitEdgeReview, retryFailedBatch,
  }), [
    setResolutionMode, setSelectedClusterId, setSelectedEdgeIdAction,
    setReviewerId, setLastReviewedEdgeId,
    triggerRefresh, loadClusters, preloadVisualizationData, loadConnectionData,
    invalidateVisualizationData, invalidateConnectionData,
    clearAllData,
    selectNextUnreviewedEdge, advanceToNextCluster, checkAndAdvanceIfComplete,
    submitEdgeReview, retryFailedBatch,
  ]);

  // Effect to load connection data when selectedEdgeId changes and data is not fresh
  useEffect(() => {
    if (selectedEdgeId) {
      const currentEdgeState = connectionData[selectedEdgeId];
      // Check if data is missing, stale, or there was an error
      if (!currentEdgeState?.data || currentEdgeState?.error || (currentEdgeState?.lastUpdated && (Date.now() - currentEdgeState.lastUpdated > 300000))) {
        if (!currentEdgeState?.loading) { // Avoid re-fetching if already loading
          console.log(`Fetching connection data for selected edge: ${selectedEdgeId}`);
          actions.loadConnectionData(selectedEdgeId);
        }
      }
    }
  }, [selectedEdgeId, connectionData, actions]); // actions includes loadConnectionData

  // Effect to set reviewerId from authenticated user
  useEffect(() => {
    if (currentUser?.id) {
      setReviewerId(currentUser.id);
    }
  }, [currentUser]);

  // Effect to process the review queue
  useEffect(() => {
    if (reviewQueue.length > 0 && !isProcessingQueue && processReviewQueue) {
      // Delay processing slightly to allow UI updates and batching
      const timeoutId = setTimeout(processReviewQueue, 500);
      return () => clearTimeout(timeoutId);
    }
  }, [reviewQueue, isProcessingQueue, processReviewQueue]);


  // Memoize queries for stable references
  const queries = useMemo(() => ({
    isVisualizationDataLoaded: (clusterId: string) => !!visualizationData[clusterId]?.data && !visualizationData[clusterId]?.loading && !visualizationData[clusterId]?.error,
    isVisualizationDataLoading: (clusterId: string) => !!visualizationData[clusterId]?.loading,
    isConnectionDataLoaded: (edgeId: string) => !!connectionData[edgeId]?.data && !connectionData[edgeId]?.loading && !connectionData[edgeId]?.error,
    isConnectionDataLoading: (edgeId: string) => !!connectionData[edgeId]?.loading,
    getVisualizationError: (clusterId: string) => visualizationData[clusterId]?.error || null,
    getConnectionError: (edgeId: string) => connectionData[edgeId]?.error || null,
    getClusterProgress: (clusterId: string): ClusterProgress => {
        const vizState = visualizationData[clusterId];
        if (!vizState?.data?.links) return { clusterId, totalEdges: 0, reviewedEdges: 0, pendingEdges: 0, confirmedMatches: 0, confirmedNonMatches: 0, progressPercentage: 0, isComplete: false };
        const { links } = vizState.data;
        const totalEdges = links.length;
        if (totalEdges === 0) return { clusterId, totalEdges: 0, reviewedEdges: 0, pendingEdges: 0, confirmedMatches: 0, confirmedNonMatches: 0, progressPercentage: 100, isComplete: true };

        const confirmedMatches = links.filter(link => link.status === 'CONFIRMED_MATCH').length;
        const confirmedNonMatches = links.filter(link => link.status === 'CONFIRMED_NON_MATCH').length;
        const reviewedEdges = confirmedMatches + confirmedNonMatches;
        const pendingEdges = totalEdges - reviewedEdges;
        const progressPercentage = totalEdges > 0 ? Math.round((reviewedEdges / totalEdges) * 100) : 0;
        return { clusterId, totalEdges, reviewedEdges, pendingEdges, confirmedMatches, confirmedNonMatches, progressPercentage, isComplete: pendingEdges === 0 && totalEdges > 0 };
    },
    canAdvanceToNextCluster: () => {
        if (!selectedClusterId) return false;
        const progress = queries.getClusterProgress(selectedClusterId);
        return progress?.isComplete || false;
    },
    isEdgeReviewed: (edgeId: string) => {
        const viz = currentVisualizationData;
        if (!viz?.links) return false;
        const edge = viz.links.find(l => l.id === edgeId);
        return edge ? edge.status !== 'PENDING_REVIEW' : false;
    },
    getEdgeStatus: (edgeId: string) => {
        const viz = currentVisualizationData;
        if (!viz?.links) return null;
        const edge = viz.links.find(l => l.id === edgeId);
        return edge?.status ?? null;
    },
    getQueueItemStatus: (edgeId: string) => {
        const item = reviewQueue.find(b => b.edgeId === edgeId);
        if (!item) return null;
        if (item.isTerminalFailure) return 'failed';
        if (processingBatchIdRef.current === item.batchId) return 'processing';
        return 'pending';
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }), [visualizationData, connectionData, selectedClusterId, currentVisualizationData, reviewQueue, processingBatchIdRef.current]);

  // Construct the context value
  const contextValue: EntityResolutionContextType = {
    resolutionMode,
    selectedClusterId, selectedEdgeId, reviewerId, lastReviewedEdgeId, refreshTrigger,
    clusters, visualizationData, connectionData, reviewQueue, isProcessingQueue,
    clusterProgress: useMemo(() => {
        const reconstructed: Record<string, ClusterProgress> = {};
        clusters.data.forEach(c => {
            reconstructed[c.id] = queries.getClusterProgress(c.id);
        });
        if (selectedClusterId && !reconstructed[selectedClusterId] && visualizationData[selectedClusterId]?.data?.links) {
            reconstructed[selectedClusterId] = queries.getClusterProgress(selectedClusterId);
        }
        return reconstructed;
    }, [clusters.data, selectedClusterId, visualizationData, queries]),
    edgeSelectionInfo, currentVisualizationData, currentConnectionData,
    actions,
    queries,
  };

  return (
    <EntityResolutionContext.Provider value={contextValue}>
      {children}
    </EntityResolutionContext.Provider>
  );
}

// Export the hook for easy consumption
export function useEntityResolution(): EntityResolutionContextType {
  const context = useContext(EntityResolutionContext);
  if (context === undefined) {
    throw new Error("useEntityResolution must be used within an EntityResolutionProvider");
  }
  return context;
}

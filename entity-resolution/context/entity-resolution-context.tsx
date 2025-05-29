// context/entity-resolution-context.tsx

"use client"

import {
  VisualizationDataResponse,
  ConnectionDataResponse,
  ClustersResponse,
  Cluster,
  EntityLink,
  EntityGroup,
  EntityGroupReviewDecision,
  EntityGroupReviewApiPayload,
  ClusterFinalizationStatusResponse,
  QueuedReviewBatch, 
  VisualizationEntityEdge, 
} from "@/types/entity-resolution";
import {
  getClusters,
  getVisualizationData,
  getConnectionData,
  postEntityGroupFeedback,
  triggerClusterFinalization,
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
import { v4 as uuidv4 } from 'uuid';


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
  selectedClusterId: string | null;
  selectedEdgeId: string | null;
  reviewerId: string; 
  lastReviewedEdgeId: string | null;
  refreshTrigger: number;

  clusters: ClustersState;
  visualizationData: Record<string, VisualizationState>;
  connectionData: Record<string, ConnectionState>;

  reviewQueue: QueuedReviewBatch[];
  isProcessingQueue: boolean;

  clusterProgress: Record<string, ClusterProgress>; 
  edgeSelectionInfo: EdgeSelectionInfo;
  currentVisualizationData: VisualizationDataResponse | null;
  currentConnectionData: ConnectionDataResponse | null;

  actions: {
    setSelectedClusterId: (id: string | null) => void;
    setSelectedEdgeId: (id: string | null) => void;
    setReviewerId: (id: string) => void; 
    setLastReviewedEdgeId: (id: string | null) => void;
    triggerRefresh: (target?: 'all' | 'clusters' | 'current_visualization' | 'current_connection') => void;
    loadClusters: (page: number, limit?: number) => Promise<void>;
    preloadVisualizationData: (clusterIds: string[]) => Promise<void>;
    loadConnectionData: (edgeId: string) => Promise<ConnectionDataResponse | null>;
    invalidateVisualizationData: (clusterId: string) => void;
    invalidateConnectionData: (edgeId: string) => void; // This will be the corrected version
    clearAllData: () => void;
    selectNextUnreviewedEdge: (afterEdgeId?: string | null) => void;
    advanceToNextCluster: () => Promise<void>;
    checkAndAdvanceIfComplete: (clusterIdToCheck?: string) => Promise<void>;
    submitEdgeReview: (edgeId: string, decision: EntityGroupReviewDecision) => Promise<void>;
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
    getEdgeStatus: (edgeId: string) => EntityLink['status'] | null;
    getQueueItemStatus: (edgeId: string) => 'pending' | 'processing' | 'failed' | null;
  };
}

const EntityResolutionContext = createContext<EntityResolutionContextType | undefined>(undefined);

const MAX_REVIEW_ATTEMPTS = 3;

export function EntityResolutionProvider({ children }: { children: ReactNode }) {
  const { currentUser } = useAuth();
  const { toast } = useToast();

  const [selectedClusterId, setSelectedClusterIdState] = useState<string | null>(null);
  const [selectedEdgeId, setSelectedEdgeIdState] = useState<string | null>(null);
  const [reviewerId, setReviewerId] = useState<string>("default-reviewer"); 
  const [refreshTrigger, setRefreshTrigger] = useState<number>(0);
  const [lastReviewedEdgeId, setLastReviewedEdgeId] = useState<string | null>(null);

  const [clusters, setClusters] = useState<ClustersState>({
    data: [], total: 0, page: 1, limit: 10, loading: false, error: null,
  });
  const [visualizationData, setVisualizationData] = useState<Record<string, VisualizationState>>({});
  const [connectionData, setConnectionData] = useState<Record<string, ConnectionState>>({});

  const [reviewQueue, setReviewQueue] = useState<QueuedReviewBatch[]>([]);
  const [isProcessingQueue, setIsProcessingQueue] = useState<boolean>(false);
  const processingBatchIdRef = useRef<string | null>(null);


  // --- DATA LOADING AND MANAGEMENT ---

  const preloadVisualizationData = useCallback(async (clusterIds: string[]) => {
    clusterIds.forEach(clusterId => {
      // Check if data needs loading (not present, or has an error)
      if (!visualizationData[clusterId]?.data || visualizationData[clusterId]?.error) {
        setVisualizationData(prev => ({
          ...prev,
          [clusterId]: { data: prev[clusterId]?.data || null, loading: true, error: null, lastUpdated: prev[clusterId]?.lastUpdated || null },
        }));
        getVisualizationData(clusterId)
          .then(data => setVisualizationData(prev => ({
            ...prev, [clusterId]: { data, loading: false, error: null, lastUpdated: Date.now() },
          })))
          .catch(error => setVisualizationData(prev => ({
            ...prev, [clusterId]: { data: null, loading: false, error: (error as Error).message, lastUpdated: null },
          })));
      }
    });
  }, [visualizationData]); // Depends on visualizationData to read cache state

  const loadConnectionData = useCallback(async (edgeId: string): Promise<ConnectionDataResponse | null> => {
    const cached = connectionData[edgeId];
    if (cached?.data && !cached.loading && !cached.error && cached.lastUpdated && (Date.now() - cached.lastUpdated < 300000)) { // 5 min cache
      return cached.data;
    }
    setConnectionData(prev => ({
      ...prev,
      [edgeId]: {
        data: null, 
        loading: true,
        error: null,
        lastUpdated: prev[edgeId]?.lastUpdated || null 
      }
    }));
    try {
      const data = await getConnectionData(edgeId);
      setConnectionData(prev => ({
        ...prev,
        [edgeId]: { data, loading: false, error: null, lastUpdated: Date.now() }
      }));
      return data;
    } catch (error) {
      setConnectionData(prev => ({
        ...prev,
        [edgeId]: { data: null, loading: false, error: (error as Error).message, lastUpdated: null }
      }));
      return null;
    }
  }, [connectionData]); // Depends on connectionData to read cache state

  const invalidateVisualizationData = useCallback((clusterId: string) => {
    setVisualizationData(prev => ({ ...prev, [clusterId]: { data: null, loading: false, error: `Invalidated by user`, lastUpdated: null }}));
    // If the invalidated cluster is the selected one, preload its data again.
    // The preloadVisualizationData function has its own guards.
    if (selectedClusterId === clusterId) {
        preloadVisualizationData([clusterId]);
    }
  }, [selectedClusterId, preloadVisualizationData]); // Added selectedClusterId and preloadVisualizationData

  // CORRECTED invalidateConnectionData
  const invalidateConnectionData = useCallback((edgeId: string) => {
    setConnectionData(prev => ({
      ...prev,
      [edgeId]: { data: null, loading: false, error: `Invalidated by user`, lastUpdated: null }
    }));
    // The main useEffect watching selectedEdgeId and connectionData will handle reloading if this edgeId is selected.
  }, []); // setConnectionData is stable, so empty dependency array is appropriate.

  const loadClusters = useCallback(async (page: number, limit: number = 10) => {
    setClusters((prev) => ({ ...prev, loading: true, error: null }));
    try {
      const response = await getClusters(page, limit);
      setClusters({
        data: response.clusters, total: response.total, page, limit, loading: false, error: null,
      });
      const clusterIds = response.clusters.map(c => c.id);
      if (clusterIds.length > 0) {
        preloadVisualizationData(clusterIds);
      }
    } catch (error) {
      setClusters((prev) => ({ ...prev, loading: false, error: (error as Error).message }));
    }
  }, [preloadVisualizationData]); 

  const setSelectedClusterId = useCallback((id: string | null) => {
    setSelectedClusterIdState(id);
    setSelectedEdgeIdState(null); 
    setLastReviewedEdgeId(null);
    if (id && (!visualizationData[id]?.data && !visualizationData[id]?.loading)) {
      preloadVisualizationData([id]);
    }
  }, [visualizationData, preloadVisualizationData]);

  const setSelectedEdgeIdAction = useCallback((id: string | null) => {
    setSelectedEdgeIdState(id);
  }, [setSelectedEdgeIdState]);


  const clearAllData = useCallback(() => {
      setClusters({ data: [], total: 0, page: 1, limit: 10, loading: false, error: null });
      setVisualizationData({});
      setConnectionData({});
      setReviewQueue([]);
      setSelectedClusterIdState(null);
      setSelectedEdgeIdState(null);
      setLastReviewedEdgeId(null);
   }, []);

  const retryFailedBatch = useCallback((batchId: string) => {
    setReviewQueue(prev => prev.map(b => {
      if (b.batchId === batchId && b.isTerminalFailure) {
        return { ...b, attempt: 0, isTerminalFailure: false, error: undefined, failedOperations: new Set(), processedOperations: new Set() };
      }
      return b;
    }));
  }, []); 

  const triggerRefresh = useCallback((target: 'all' | 'clusters' | 'current_visualization' | 'current_connection' = 'all') => {
    if (target === 'all' || target === 'clusters') {
        loadClusters(clusters.page, clusters.limit);
    }
    if (selectedClusterId && (target === 'all' || target === 'current_visualization')) {
        // invalidateVisualizationData will also trigger preload if it's the selected cluster
        invalidateVisualizationData(selectedClusterId); 
    }
    if (selectedEdgeId && (target === 'all' || target === 'current_connection')) {
        invalidateConnectionData(selectedEdgeId); 
        // The main useEffect will pick up the invalidation and reload.
    }
    setRefreshTrigger(prev => prev + 1);
  }, [clusters.page, clusters.limit, selectedClusterId, selectedEdgeId, loadClusters, invalidateVisualizationData, invalidateConnectionData]);


  const submitEdgeReview = useCallback(async (edgeId: string, decision: EntityGroupReviewDecision) => {
    if (!currentUser?.id) {
      toast({ title: "Authentication Error", description: "You must be logged in to submit reviews.", variant: "destructive" });
      return;
    }
    if (!selectedClusterId) {
      toast({ title: "Selection Error", description: "A cluster must be selected.", variant: "destructive" });
      return;
    }

    const connData = connectionData[edgeId]?.data;
    const vizData = visualizationData[selectedClusterId]?.data;

    if (!connData || !vizData) {
      toast({ title: "Data Error", description: "Connection or visualization data not loaded.", variant: "destructive" });
      if (!connData && loadConnectionData) loadConnectionData(edgeId); 
      if (!vizData && selectedClusterId && preloadVisualizationData) preloadVisualizationData([selectedClusterId]); 
      return;
    }

    const relevantGroups = connData.entityGroups.filter(group =>
      (group.entity_id_1 === connData.edge.entity_id_1 && group.entity_id_2 === connData.edge.entity_id_2) ||
      (group.entity_id_1 === connData.edge.entity_id_2 && group.entity_id_2 === connData.edge.entity_id_1)
    );

    if (relevantGroups.length === 0) {
      toast({ title: "Info", description: "No underlying entity groups for this edge. Marking as reviewed." });
      setVisualizationData(prev => {
        const newVizData = { ...prev };
        const targetClusterViz = newVizData[selectedClusterId!]?.data;
        if (targetClusterViz?.links) {
          const linkIndex = targetClusterViz.links.findIndex(l => l.id === edgeId);
          if (linkIndex !== -1) {
            targetClusterViz.links[linkIndex].status = decision === 'ACCEPTED' ? 'CONFIRMED_MATCH' : 'CONFIRMED_NON_MATCH';
            targetClusterViz.links = [...targetClusterViz.links]; 
          }
        }
        return newVizData;
      });
      setLastReviewedEdgeId(edgeId);
      return;
    }

    const currentEdgeLink = vizData.links.find(l => l.id === edgeId);
    if (!currentEdgeLink) {
        console.error("Current edge link not found in visualization data for optimistic update.");
        toast({title: "Internal Error", description: "Could not find edge in visualization for update.", variant: "destructive"});
        return;
    }
    
    const operations: QueuedReviewBatch['operations'] = relevantGroups.map(group => ({
      groupId: group.id,
      originalGroupStatus: group.confirmed_status,
    }));

    const optimisticEdgeStatus: EntityLink['status'] = decision === 'ACCEPTED' ? 'CONFIRMED_MATCH' : 'CONFIRMED_NON_MATCH';
    const optimisticGroupStatus: EntityGroup['confirmed_status'] = decision === 'ACCEPTED' ? 'CONFIRMED_MATCH' : 'CONFIRMED_NON_MATCH';

    const batch: QueuedReviewBatch = {
      batchId: uuidv4(),
      edgeId,
      clusterId: selectedClusterId,
      decision,
      reviewerId: currentUser.id,
      operations,
      originalEdgeStatus: currentEdgeLink.status,
      optimisticEdgeStatus,
      optimisticGroupStatus,
      attempt: 0,
      processedOperations: new Set(),
      failedOperations: new Set(),
    };

    setVisualizationData(prev => {
      const newVizData = { ...prev };
      const targetClusterViz = newVizData[batch.clusterId]?.data;
      if (targetClusterViz?.links) {
        const linkIndex = targetClusterViz.links.findIndex(l => l.id === batch.edgeId);
        if (linkIndex !== -1) {
          targetClusterViz.links[linkIndex] = { ...targetClusterViz.links[linkIndex], status: batch.optimisticEdgeStatus };
          targetClusterViz.links = [...targetClusterViz.links];
        }
        if (targetClusterViz.entityGroups) {
            targetClusterViz.entityGroups = targetClusterViz.entityGroups.map(eg => {
            if (batch.operations.some(op => op.groupId === eg.id)) {
                return { ...eg, confirmed_status: batch.optimisticGroupStatus };
            }
            return eg;
            });
        }
      }
      return newVizData;
    });

    setConnectionData(prev => {
      const newConnData = { ...prev };
      const targetConnState = newConnData[batch.edgeId];
      if (targetConnState?.data) { // Optimistically update if data exists
        const targetConn = { ...targetConnState.data }; 
        targetConn.edge = {
            ...targetConn.edge,
            status: batch.optimisticEdgeStatus,
            confirmed_status: batch.optimisticGroupStatus // Assuming edge confirmed_status mirrors group for simplicity
        };
        if (targetConn.entityGroups) {
            targetConn.entityGroups = targetConn.entityGroups.map(eg => {
            if (batch.operations.some(op => op.groupId === eg.id)) {
                return { ...eg, confirmed_status: batch.optimisticGroupStatus };
            }
            return eg;
            });
        }
        newConnData[batch.edgeId] = { ...targetConnState, data: targetConn };
      } else { // If no data (e.g., edge was just selected), store optimistic status for when data loads
          newConnData[batch.edgeId] = {
              ...(prev[batch.edgeId] || { data: null, loading: false, error: null, lastUpdated: null }), // keep other state fields
              // Store some minimal optimistic data or rely on viz data for display until full load
              // This part is tricky; for now, primarily relying on viz data for optimistic UI for the edge itself
          };
      }
      return newConnData;
    });

    setReviewQueue(prevQueue => [...prevQueue, batch]);
    setLastReviewedEdgeId(edgeId);

  }, [currentUser, selectedClusterId, connectionData, visualizationData, toast, loadConnectionData, preloadVisualizationData, setReviewQueue, setVisualizationData, setConnectionData, setLastReviewedEdgeId]);


  const advanceToNextCluster = useCallback(async () => {
    if (!selectedClusterId) return;
    const currentIndex = clusters.data.findIndex(c => c.id === selectedClusterId);
    if (currentIndex === -1) return;

    if (currentIndex < clusters.data.length - 1) {
      setSelectedClusterIdState(clusters.data[currentIndex + 1].id);
    } else if (clusters.page < Math.ceil(clusters.total / clusters.limit)) {
      const nextPage = clusters.page + 1;
      await loadClusters(nextPage, clusters.limit);
    } else {
      toast({ title: "Workflow Complete", description: "All clusters have been processed." });
    }
  }, [selectedClusterId, clusters, loadClusters, toast, setSelectedClusterIdState]);


  const checkAndAdvanceIfComplete = useCallback(async (clusterIdToCheck?: string) => {
    const targetClusterId = clusterIdToCheck || selectedClusterId;
    if (!targetClusterId) return;

    const currentVizData = visualizationData[targetClusterId]?.data;
    if (!currentVizData) return;

    const { links } = currentVizData;
    const totalEdges = links.length;
    const reviewedEdges = links.filter(link => link.status !== 'PENDING_REVIEW').length;
    const isComplete = totalEdges > 0 && reviewedEdges === totalEdges;

    if (isComplete) {
      toast({ title: "Cluster Complete", description: `Review of cluster ${targetClusterId.substring(0,8)} is complete.` });

      try {
        const finalizationResponse = await triggerClusterFinalization(targetClusterId);
        toast({ title: "Cluster Finalization", description: `${finalizationResponse.message}. Status: ${finalizationResponse.status}` });

        if (finalizationResponse.status === 'COMPLETED_SPLIT_OCCURRED' || finalizationResponse.status === 'COMPLETED_NO_SPLIT_NEEDED') {
           if (loadClusters) loadClusters(clusters.page, clusters.limit); 
          if (finalizationResponse.originalClusterId && finalizationResponse.originalClusterId === selectedClusterId && invalidateVisualizationData) {
             invalidateVisualizationData(selectedClusterId);
          }
        }
      } catch (finalizationError) {
        toast({ title: "Finalization Error", description: `Could not finalize cluster ${targetClusterId.substring(0,8)}: ${(finalizationError as Error).message}`, variant: "destructive" });
      }
      if (advanceToNextCluster) advanceToNextCluster();
    }
  }, [selectedClusterId, visualizationData, advanceToNextCluster, toast, invalidateVisualizationData, loadClusters, clusters.page, clusters.limit]);


  const processReviewQueue = useCallback(async () => {
    if (isProcessingQueue || reviewQueue.length === 0) return;

    const batchToProcess = reviewQueue.find(b => !b.isTerminalFailure && (b.failedOperations.size === 0 || b.attempt < MAX_REVIEW_ATTEMPTS));
    if (!batchToProcess) {
      setIsProcessingQueue(false);
      return;
    }

    if (processingBatchIdRef.current && processingBatchIdRef.current !== batchToProcess.batchId) {
        return;
    }

    setIsProcessingQueue(true);
    processingBatchIdRef.current = batchToProcess.batchId;

    let currentBatch = { ...batchToProcess, attempt: batchToProcess.attempt + 1, failedOperations: new Set(batchToProcess.failedOperations) };

    let batchOverallSuccess = true;
    const stillPendingOperations = currentBatch.operations.filter(op => !currentBatch.processedOperations.has(op.groupId));

    for (const op of stillPendingOperations) {
      try {
        const payload: EntityGroupReviewApiPayload = {
          decision: currentBatch.decision,
          reviewerId: currentBatch.reviewerId,
        };
        await postEntityGroupFeedback(op.groupId, payload);
        currentBatch.processedOperations.add(op.groupId);
      } catch (error) {
        console.error(`Failed to submit review for group ${op.groupId} in batch ${currentBatch.batchId}:`, error);
        currentBatch.failedOperations.add(op.groupId);
        currentBatch.error = `Group ${op.groupId}: ${(error as Error).message}`;
        batchOverallSuccess = false;
      }
    }

    if (!batchOverallSuccess) {
        if (currentBatch.attempt >= MAX_REVIEW_ATTEMPTS) {
            currentBatch.isTerminalFailure = true;
            toast({
                title: "Review Submission Failed Permanently",
                description: (
                <>
                    Failed to save review for connection {currentBatch.edgeId.substring(0,8)} after {currentBatch.attempt} attempts. Error: {currentBatch.error}
                    <Button variant="link" className="p-0 h-auto ml-2 text-destructive-foreground underline" onClick={() => setSelectedEdgeIdState(currentBatch.edgeId)}>
                    View Connection
                    </Button>
                </>
                ),
                variant: "destructive",
                duration: 10000,
            });

            setVisualizationData(prev => {
                const newVizData = { ...prev };
                const targetClusterViz = newVizData[currentBatch.clusterId]?.data;
                if (targetClusterViz?.links) {
                    const linkIndex = targetClusterViz.links.findIndex(l => l.id === currentBatch.edgeId);
                    if (linkIndex !== -1) {
                        targetClusterViz.links[linkIndex] = { ...targetClusterViz.links[linkIndex], status: currentBatch.originalEdgeStatus };
                        targetClusterViz.links = [...targetClusterViz.links];
                    }
                    if (targetClusterViz.entityGroups){
                        targetClusterViz.entityGroups = targetClusterViz.entityGroups.map(eg => {
                            const op = currentBatch.operations.find(o => o.groupId === eg.id);
                            if (op) return { ...eg, confirmed_status: op.originalGroupStatus };
                            return eg;
                        });
                    }
                }
                return newVizData;
            });
            setConnectionData(prev => {
                const newConnData = { ...prev };
                const targetConnState = newConnData[currentBatch.edgeId];
                if (targetConnState?.data) {
                    const targetConn = { ...targetConnState.data };
                    targetConn.edge = { ...targetConn.edge, status: currentBatch.originalEdgeStatus, confirmed_status: currentBatch.originalEdgeStatus as EntityGroup['confirmed_status'] };
                     if (targetConn.entityGroups) {
                        targetConn.entityGroups = targetConn.entityGroups.map(eg => {
                            const op = currentBatch.operations.find(o => o.groupId === eg.id);
                            if (op) return { ...eg, confirmed_status: op.originalGroupStatus };
                            return eg;
                        });
                     }
                    newConnData[currentBatch.edgeId] = { ...targetConnState, data: targetConn };
                }
                return newConnData;
            });
             setReviewQueue(prevQ => prevQ.map(b => b.batchId === currentBatch.batchId ? { ...currentBatch, isTerminalFailure: true } : b));
        } else {
            toast({
                title: "Review Submission Issue",
                description: `Attempt ${currentBatch.attempt}/${MAX_REVIEW_ATTEMPTS} failed for conn ${currentBatch.edgeId.substring(0,8)}. Will retry. Error: ${currentBatch.error}`,
                variant: "default",
                duration: 5000,
            });
            setReviewQueue(prevQ => prevQ.map(b => b.batchId === currentBatch.batchId ? currentBatch : b));
        }
    } else {
      setReviewQueue(prevQ => prevQ.filter(b => b.batchId !== currentBatch.batchId));
      if (currentBatch.clusterId && checkAndAdvanceIfComplete) {
        checkAndAdvanceIfComplete(currentBatch.clusterId);
      }
    }

    processingBatchIdRef.current = null;
    setIsProcessingQueue(false);
  }, [reviewQueue, isProcessingQueue, toast, currentUser, checkAndAdvanceIfComplete, setReviewQueue, setVisualizationData, setConnectionData, setIsProcessingQueue, setSelectedEdgeIdState]);


  const selectNextUnreviewedEdge = useCallback((afterEdgeId?: string | null) => {
    const currentClusterId = selectedClusterId;
    if (!currentClusterId) return;
    const currentViz = visualizationData[currentClusterId]?.data;
    if (!currentViz?.links) return;

    const { links } = currentViz;
    let startIdx = 0;
    const referenceEdgeId = afterEdgeId || selectedEdgeId; 

    if(referenceEdgeId) {
        const idx = links.findIndex(l => l.id === referenceEdgeId);
        if (idx !== -1) startIdx = idx + 1;
    }

    for (let i = 0; i < links.length; i++) {
      const link = links[(startIdx + i) % links.length];
      if (link.status === 'PENDING_REVIEW') {
        setSelectedEdgeIdState(link.id); 
        return;
      }
    }
    toast({ title: "Cluster Review", description: "All connections in this cluster have been reviewed." });
    if (checkAndAdvanceIfComplete) checkAndAdvanceIfComplete(currentClusterId);

  }, [selectedClusterId, selectedEdgeId, visualizationData, toast, checkAndAdvanceIfComplete, setSelectedEdgeIdState]);


  // --- COMPUTED STATE --- 
  const currentVisualizationData = useMemo((): VisualizationDataResponse | null => {
    if (!selectedClusterId) return null;
    return visualizationData[selectedClusterId]?.data || null;
  }, [selectedClusterId, visualizationData]);

  const currentConnectionData = useMemo((): ConnectionDataResponse | null => {
    if (!selectedEdgeId) return null;
    return connectionData[selectedEdgeId]?.data || null;
  }, [selectedEdgeId, connectionData]);


  const edgeSelectionInfo = useMemo((): EdgeSelectionInfo => {
    if (!currentVisualizationData) return { currentEdgeId: selectedEdgeId, nextUnreviewedEdgeId: null, hasUnreviewedEdges: false, currentEdgeIndex: -1, totalEdges: 0 };
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


  // --- LIFECYCLE & INITIALIZATION ---
  useEffect(() => {
    if (loadClusters) { 
        loadClusters(1, clusters.limit);
    }
  }, [loadClusters, clusters.limit]); 

  useEffect(() => {
    if (!selectedClusterId && clusters.data.length > 0 && !clusters.loading) {
      setSelectedClusterIdState(clusters.data[0].id);
    }
  }, [selectedClusterId, clusters.data, clusters.loading, setSelectedClusterIdState]);

  useEffect(() => {
    if (selectedClusterId && currentVisualizationData && !selectedEdgeId && edgeSelectionInfo.hasUnreviewedEdges) {
       if (edgeSelectionInfo.nextUnreviewedEdgeId) {
           setSelectedEdgeIdState(edgeSelectionInfo.nextUnreviewedEdgeId); 
       } else if (selectNextUnreviewedEdge) { 
            selectNextUnreviewedEdge(); 
       }
    }
  }, [selectedClusterId, currentVisualizationData, selectedEdgeId, selectNextUnreviewedEdge, edgeSelectionInfo.hasUnreviewedEdges, edgeSelectionInfo.nextUnreviewedEdgeId, setSelectedEdgeIdState]);

  const actions = useMemo(() => ({
    setSelectedClusterId, 
    setSelectedEdgeId: setSelectedEdgeIdAction, 
    setReviewerId, 
    setLastReviewedEdgeId,
    triggerRefresh, loadClusters, preloadVisualizationData, loadConnectionData,
    invalidateVisualizationData, invalidateConnectionData, // invalidateConnectionData is now the corrected stable one
    clearAllData,
    selectNextUnreviewedEdge, advanceToNextCluster, checkAndAdvanceIfComplete,
    submitEdgeReview, retryFailedBatch,
  }), [
    setSelectedClusterId, setSelectedEdgeIdAction, 
    triggerRefresh, loadClusters, preloadVisualizationData, loadConnectionData, 
    invalidateVisualizationData, invalidateConnectionData, 
    clearAllData,
    selectNextUnreviewedEdge, advanceToNextCluster, checkAndAdvanceIfComplete,
    submitEdgeReview, retryFailedBatch,
  ]);

  // Main useEffect to load connection data when selectedEdgeId changes, with guards
  useEffect(() => {
    if (selectedEdgeId) {
      const currentEdgeState = connectionData[selectedEdgeId];

      // Guard 1: If already loading this specific edge, do nothing.
      if (currentEdgeState?.loading) {
        return;
      }

      // Guard 2: If data is already present and fresh, do nothing.
      if (currentEdgeState?.data && !currentEdgeState.error && currentEdgeState.lastUpdated && (Date.now() - currentEdgeState.lastUpdated < 300000)) { // 5 min cache
        return;
      }

      // If not loading and data is not fresh/present, then proceed to load.
      // actions.loadConnectionData is memoized and safe to call here.
      if (actions.loadConnectionData) actions.loadConnectionData(selectedEdgeId);
    }
  }, [selectedEdgeId, connectionData, actions.loadConnectionData]); // actions.loadConnectionData changes if connectionData changes

  useEffect(() => {
    if (currentUser?.id) {
      setReviewerId(currentUser.id);
    }
  }, [currentUser, setReviewerId]);

  useEffect(() => {
    if (reviewQueue.length > 0 && !isProcessingQueue && processReviewQueue) { 
      const timeoutId = setTimeout(processReviewQueue, 1000); 
      return () => clearTimeout(timeoutId);
    }
  }, [reviewQueue, isProcessingQueue, processReviewQueue]);


  const queries = useMemo(() => ({
    isVisualizationDataLoaded: (clusterId: string) => !!visualizationData[clusterId]?.data,
    isVisualizationDataLoading: (clusterId: string) => !!visualizationData[clusterId]?.loading,
    isConnectionDataLoaded: (edgeId: string) => !!connectionData[edgeId]?.data,
    isConnectionDataLoading: (edgeId: string) => !!connectionData[edgeId]?.loading,
    getVisualizationError: (clusterId: string) => visualizationData[clusterId]?.error || null,
    getConnectionError: (edgeId: string) => connectionData[edgeId]?.error || null,
    getClusterProgress: (clusterId: string): ClusterProgress => {
        const vizState = visualizationData[clusterId];
        if (!vizState?.data) return { clusterId, totalEdges: 0, reviewedEdges: 0, pendingEdges: 0, confirmedMatches: 0, confirmedNonMatches: 0, progressPercentage: 0, isComplete: false };
        const { links } = vizState.data;
        const totalEdges = links.length;
        if (totalEdges === 0) return { clusterId, totalEdges: 0, reviewedEdges: 0, pendingEdges: 0, confirmedMatches: 0, confirmedNonMatches: 0, progressPercentage: 100, isComplete: true };
        const confirmedMatches = links.filter(link => link.status === 'CONFIRMED_MATCH').length;
        const confirmedNonMatches = links.filter(link => link.status === 'CONFIRMED_NON_MATCH').length;
        const reviewedEdges = confirmedMatches + confirmedNonMatches;
        const pendingEdges = totalEdges - reviewedEdges;
        const progressPercentage = totalEdges > 0 ? Math.round((reviewedEdges / totalEdges) * 100) : 0;
        return { clusterId, totalEdges, reviewedEdges, pendingEdges, confirmedMatches, confirmedNonMatches, progressPercentage, isComplete: pendingEdges === 0 };
    },
    canAdvanceToNextCluster: () => {
        if (!selectedClusterId || !queries.getClusterProgress) return false; // Added !queries.getClusterProgress check
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
  }), [visualizationData, connectionData, selectedClusterId, currentVisualizationData, reviewQueue, processingBatchIdRef]); 

  return (
    <EntityResolutionContext.Provider
      value={{
        selectedClusterId, selectedEdgeId, reviewerId, lastReviewedEdgeId, refreshTrigger,
        clusters, visualizationData, connectionData, reviewQueue, isProcessingQueue,
        clusterProgress: {} ,
        edgeSelectionInfo, currentVisualizationData, currentConnectionData,
        actions,
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
  const reconstructedClusterProgress: Record<string, ClusterProgress> = {};
  if (context.clusters.data && context.queries.getClusterProgress) {
      context.clusters.data.forEach(c => {
          reconstructedClusterProgress[c.id] = context.queries.getClusterProgress(c.id);
      });
      if (context.selectedClusterId && !reconstructedClusterProgress[context.selectedClusterId] && context.visualizationData[context.selectedClusterId]?.data) { // Ensure viz data exists before getting progress
          reconstructedClusterProgress[context.selectedClusterId] = context.queries.getClusterProgress(context.selectedClusterId);
      }
  }

  return {...context, clusterProgress: reconstructedClusterProgress };
}

// components/connection-review-tools.tsx
"use client";

import { useEffect, useState, useCallback, Key } from "react";
import { useEntityResolution } from "@/context/entity-resolution-context";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@/components/ui/collapsible";
import { Badge } from "@/components/ui/badge";
import {
  Check,
  ChevronDown,
  ChevronUp,
  X,
  AlertCircle,
  Info,
  Loader2,
  AlertTriangle,
  RefreshCw,
  CheckCircle,
  XCircle,
  SkipForward,
  RotateCcw,
  ChevronLeft,
  ChevronRight,
  DownloadCloud, // For fetch connections button
  ChevronsRight // For load more connections
} from "lucide-react";
import type {
  VisualizationEntityEdge,
  EntityGroup,
  Entity,
  GroupReviewDecision,
  QueuedReviewBatch,
  ServiceGroup,
  VisualizationServiceEdge,
  Service,
  EntityConnectionDataResponse,
  ServiceConnectionDataResponse,
  NodeDetailResponse,
  EntityCluster, // For groupCount
  ServiceCluster, // For serviceGroupCount
} from "@/types/entity-resolution";
import {
  isEntityConnectionData,
  isServiceConnectionData,
} from "@/types/entity-resolution";
import { useToast } from "@/hooks/use-toast";
import NodeAttributesDisplay from "./node-attribute-display";

const LARGE_CLUSTER_THRESHOLD = 200; // Should match context
const CONNECTION_PAGE_SIZE = 200; // Should match context

export default function ConnectionReviewTools() {
  const {
    resolutionMode,
    selectedClusterId,
    selectedEdgeId,
    currentConnectionData, // This is data for the *selectedEdgeId*
    currentVisualizationData, // This is the *paged* visualization data for large clusters
    selectedClusterDetails,
    actions,
    queries,
    reviewQueue,
    edgeSelectionInfo,
    // Large cluster specific state from context
    activelyPagingClusterId,
    largeClusterConnectionsPage,
    isLoadingConnectionPageData,
  } = useEntityResolution();
  const { toast } = useToast();

  const [isSubmitting, setIsSubmitting] = useState(false);
  const [isExpanded, setIsExpanded] = useState(true);

  const edgeStatus = selectedEdgeId
    ? queries.getEdgeStatus(selectedEdgeId)
    : null;
  const isEdgeReviewed = selectedEdgeId
    ? queries.isEdgeReviewed(selectedEdgeId)
    : false;
  const queueStatus = selectedEdgeId
    ? queries.getQueueItemStatus(selectedEdgeId)
    : null;

  const isClusterSplit = selectedClusterDetails?.wasSplit || false;

  // Determine if the selected cluster is large and if paging is active for it
  const connectionCount = selectedClusterDetails
    ? resolutionMode === "entity"
      ? (selectedClusterDetails as EntityCluster).groupCount
      : (selectedClusterDetails as ServiceCluster).serviceGroupCount
    : 0;
  
  const isSelectedClusterLarge = connectionCount && connectionCount > LARGE_CLUSTER_THRESHOLD;
  const isPagingActiveForSelectedCluster = selectedClusterId === activelyPagingClusterId && largeClusterConnectionsPage > 0;


  useEffect(() => {
    // This effect ensures that if an edge is selected (e.g. by auto-advance or click),
    // its connection data is loaded if not already present or loading.
    // This is independent of large cluster paging, but works with it.
    if (
      selectedEdgeId &&
      !currentConnectionData && // currentConnectionData is for the specific selectedEdgeId
      !queries.isConnectionDataLoading(selectedEdgeId) &&
      !isLoadingConnectionPageData // Don't interfere if a page is loading
    ) {
      actions.loadSingleConnectionData(selectedEdgeId);
    }
  }, [selectedEdgeId, currentConnectionData, queries, actions, isLoadingConnectionPageData]);

  useEffect(() => {
    setIsSubmitting(false);
    // setIsExpanded(true); // Consider if panel should always expand on new edge
  }, [selectedEdgeId]);

  const handleReviewDecision = async (decision: GroupReviewDecision) => {
    if (!selectedEdgeId) {
      toast({
        title: "Error",
        description: "No connection selected.",
        variant: "destructive",
      });
      return;
    }
    if (isClusterSplit) {
      toast({
        title: "Info",
        description: "This cluster has been split and cannot be reviewed.",
      });
      return;
    }
    if (isEdgeReviewed && queueStatus !== "failed") {
      toast({
        title: "Already Reviewed",
        description: "This connection has already been reviewed.",
      });
      return;
    }
    if (queueStatus === "processing" || queueStatus === "pending") {
      toast({
        title: "In Progress",
        description: "This connection review is already being processed.",
      });
      return;
    }

    setIsSubmitting(true);
    try {
      await actions.submitEdgeReview(selectedEdgeId, decision);
    } catch (error) {
      toast({
        title: "Submission Error",
        description: (error as Error).message,
        variant: "destructive",
      });
    } finally {
      setIsSubmitting(false);
    }
  };

  const handleRetryLoadConnection = useCallback(() => {
    if (selectedEdgeId) {
      // For a paged large cluster, retrying might mean re-requesting the current page's connection data
      // For a non-paged or single edge, it's simpler.
      if (isSelectedClusterLarge && isPagingActiveForSelectedCluster) {
        // Re-load connection data for the current page
        actions.viewNextConnectionPage(selectedClusterId!); // This reloads current page if called with same page, effectively
        // A more direct "reloadCurrentPageConnectionData" might be better in context if viewNext always advances.
        // For now, let's assume viewNextConnectionPage can handle reloading current page if needed, or we add a specific action.
        // A simpler approach for retry might be to invalidate and let useEffect pick it up.
        actions.invalidateConnectionData(selectedEdgeId);

      } else {
        actions.invalidateConnectionData(selectedEdgeId);
      }
    }
  }, [selectedEdgeId, actions, isSelectedClusterLarge, isPagingActiveForSelectedCluster, selectedClusterId]);

  const handleSkipToNext = useCallback(() => {
    actions.selectNextUnreviewedEdge(selectedEdgeId || undefined);
  }, [actions, selectedEdgeId]);

  const handleRetryQueueItem = () => {
    if (selectedEdgeId && queueStatus === "failed") {
      const batchToRetry = reviewQueue.find(
        (b: QueuedReviewBatch) =>
          b.edgeId === selectedEdgeId && b.isTerminalFailure
      );
      if (batchToRetry) {
        actions.retryFailedBatch(batchToRetry.batchId);
        toast({
          title: "Retrying Submission",
          description: `Retrying review for connection ${selectedEdgeId.substring(
            0,
            8
          )}...`,
        });
      } else {
        toast({
          title: "Retry Error",
          description: "Could not find failed batch to retry.",
          variant: "destructive",
        });
      }
    }
  };
  
  const handleInitializeLargeClusterPaging = async () => {
    if (selectedClusterId) {
        await actions.initializeLargeClusterConnectionPaging(selectedClusterId);
    }
  };

  const handleViewNextConnectionPage = async () => {
    if (selectedClusterId) {
        await actions.viewNextConnectionPage(selectedClusterId);
    }
  };


  const handlePreviousUnreviewed = useCallback(() => {
    actions.selectPreviousUnreviewedInCluster();
  }, [actions]);

  const handleNextUnreviewed = useCallback(() => {
    actions.selectNextUnreviewedInCluster();
  }, [actions]);

  const isLoadingUI = selectedEdgeId ? queries.isConnectionDataLoading(selectedEdgeId) : false;
  const errorUI = selectedEdgeId ? queries.getConnectionError(selectedEdgeId) : null;
  const nodeLabel = resolutionMode === "entity" ? "Entity" : "Service";

  // UI display conditions
  if (!selectedClusterId) { // If no cluster is selected at all
    return (
      <div className="flex justify-center items-center h-[100px] text-muted-foreground p-4 border rounded-md bg-card shadow">
        Select a cluster to begin reviewing connections.
      </div>
    );
  }

  // If selected cluster is large AND connection paging has NOT been initialized for it
  if (isSelectedClusterLarge && !isPagingActiveForSelectedCluster) {
    return (
      <Card className="h-full flex flex-col items-center justify-center p-4">
        <CardHeader className="text-center">
          <CardTitle className="flex items-center justify-center">
            <AlertTriangle className="h-6 w-6 mr-2 text-amber-500" /> Large Cluster
          </CardTitle>
        </CardHeader>
        <CardContent className="text-center space-y-3">
          <p className="text-sm text-muted-foreground">
            This cluster has a large number of connections ({connectionCount}). 
            To optimize performance and memory usage, connections are loaded in batches.
          </p>
          <Button 
            onClick={handleInitializeLargeClusterPaging} 
            disabled={isLoadingConnectionPageData}
            size="lg"
            className="w-full"
          >
            {isLoadingConnectionPageData ? (
              <Loader2 className="h-5 w-5 mr-2 animate-spin" />
            ) : (
              <DownloadCloud className="h-5 w-5 mr-2" />
            )}
            Load Connections ({CONNECTION_PAGE_SIZE} at a time)
          </Button>
        </CardContent>
      </Card>
    );
  }
  
  // If no edge is selected WITHIN an initialized (possibly paged) cluster
  if (!selectedEdgeId && (isPagingActiveForSelectedCluster || !isSelectedClusterLarge)) {
     if (isLoadingConnectionPageData && isSelectedClusterLarge) { // Loading first page for large cluster
        return (
             <div className="flex justify-center items-center h-[100px] border rounded-md bg-card shadow">
                <Loader2 className="h-6 w-6 animate-spin text-primary mr-2" /> Initializing connections...
            </div>
        );
     }
    return (
      <div className="flex justify-center items-center h-[100px] text-muted-foreground p-4 border rounded-md bg-card shadow">
        Select a connection from the graph to review its details.
        {isPagingActiveForSelectedCluster && edgeSelectionInfo.totalEdges === 0 && !isLoadingConnectionPageData && (
            <p className="text-xs mt-1">No connections on this page, or all reviewed. Try loading more.</p>
        )}
      </div>
    );
  }

  // Loading state for a specific edge's connection data (currentConnectionData)
  if (isLoadingUI && !currentConnectionData) {
    return (
      <div className="flex justify-center items-center h-[100px] border rounded-md bg-card shadow">
        <Loader2 className="h-6 w-6 animate-spin text-primary" />
      </div>
    );
  }

  // Error state for a specific edge's connection data
  if (errorUI && !currentConnectionData) {
    return (
      <Card className="h-full flex flex-col items-center justify-center">
        <CardHeader>
          <CardTitle className="flex items-center">
            <AlertCircle className="h-6 w-6 mr-2 text-destructive" /> Error Loading Details
          </CardTitle>
        </CardHeader>
        <CardContent className="text-center">
          <p className="text-destructive mb-2">
            Could not load details for the selected connection.
          </p>
          {errorUI && (
            <p className="text-xs mt-1 text-muted-foreground">{errorUI}</p>
          )}
          <div className="flex gap-2 mt-3">
            <Button variant="outline" size="sm" onClick={handleRetryLoadConnection}>
              <RefreshCw className="h-4 w-4 mr-1" /> Retry
            </Button>
            <Button
              variant="outline"
              size="sm"
              onClick={() => actions.setSelectedEdgeId(null)}
            >
              Clear Selection
            </Button>
          </div>
        </CardContent>
      </Card>
    );
  }
  
  // If currentConnectionData is still null after checks (should be rare if selectedEdgeId is set)
  if (!currentConnectionData && selectedEdgeId) {
     if (!isLoadingUI && !errorUI) { // Not loading, no error, but no data. Potentially still initializing.
        return (
            <div className="flex justify-center items-center h-[100px] border rounded-md bg-card shadow">
            <Loader2 className="h-6 w-6 animate-spin text-primary mr-2" /> Loading connection data...
            </div>
        );
     }
     return null; // Should be covered by loading/error states
  }


  let edgeDetails:
    | VisualizationEntityEdge
    | VisualizationServiceEdge
    | undefined;
  let groupsForEdge: Array<EntityGroup | ServiceGroup> = [];
  let node1: Entity | Service | undefined;
  let node2: Entity | Service | undefined;

  if (currentConnectionData) {
    const data: EntityConnectionDataResponse | ServiceConnectionDataResponse =
      currentConnectionData;
    if (isEntityConnectionData(data, resolutionMode)) {
      edgeDetails = data.edge;
      groupsForEdge = data.entityGroups;
      node1 = data.entity1;
      node2 = data.entity2;
    } else if (isServiceConnectionData(data, resolutionMode)) {
      edgeDetails = data.edge;
      groupsForEdge = data.serviceGroups;
      node1 = data.service1;
      node2 = data.service2;
    } else {
      console.error(
        "Connection data type and resolution mode mismatch or data is of an unexpected non-null type."
      );
      // UI for this error state
    }
  }

  if (!node1 || !node2 || !edgeDetails) {
    // This state might occur if selectedEdgeId is set, but its data hasn't loaded yet,
    // or if it's a new edge on a paged large cluster whose data is still fetching.
    if (isLoadingUI || (isLoadingConnectionPageData && isPagingActiveForSelectedCluster)) {
      return (
        <div className="flex justify-center items-center h-[100px] border rounded-md bg-card shadow">
          <Loader2 className="h-6 w-6 animate-spin text-primary mr-2" /> Loading details...
        </div>
      );
    }
    console.error(
      "Critical data (node1, node2, or edgeDetails) is undefined. SelectedEdgeID:",
      selectedEdgeId,
      "HasData:",
      !!currentConnectionData
    );
    return (
      <Card className="h-full flex flex-col items-center justify-center">
        <CardHeader>
          <CardTitle>Data Error</CardTitle>
        </CardHeader>
        <CardContent>
          <p>Essential connection details are missing or not yet loaded.</p>
          <Button
            variant="outline"
            size="sm"
            onClick={handleRetryLoadConnection}
            className="mt-2"
          >
            <RefreshCw className="h-4 w-4 mr-1" /> Retry Load
          </Button>
        </CardContent>
      </Card>
    );
  }

  const node1Details = queries.getNodeDetail(node1.id);
  const node2Details = queries.getNodeDetail(node2.id);

  const showReviewButtons =
    (!isEdgeReviewed || queueStatus === "failed") && !isClusterSplit;
  
  const totalLinksInEntireCluster = edgeSelectionInfo.totalEdgesInEntireCluster;
  const canLoadMoreConnections = isPagingActiveForSelectedCluster && 
                                 (largeClusterConnectionsPage * CONNECTION_PAGE_SIZE) < totalLinksInEntireCluster;


  return (
    <Collapsible
      open={isExpanded}
      onOpenChange={setIsExpanded}
      className="h-full flex flex-col"
    >
      <div className="flex justify-between items-center flex-shrink-0 pb-2 border-b mb-3">
        <div className="flex items-center gap-2 flex-wrap">
          <h3 className="text-lg font-medium">{nodeLabel} Connection Review</h3>
          {(isLoadingUI || (isLoadingConnectionPageData && isPagingActiveForSelectedCluster)) && (
            <Loader2 className="h-4 w-4 animate-spin text-muted-foreground" />
          )}
          {/* Status Badges */}
          {queueStatus === "processing" && ( <Badge variant="outline" className="bg-blue-50 text-blue-700 border-blue-300"> <Loader2 className="h-3 w-3 mr-1 animate-spin" /> Processing </Badge> )}
          {queueStatus === "pending" && ( <Badge variant="outline" className="bg-yellow-50 text-yellow-700 border-yellow-300"> <Info className="h-3 w-3 mr-1" /> Queued </Badge> )}
          {queueStatus === "failed" && ( <Badge variant="destructive"> <AlertTriangle className="h-3 w-3 mr-1" /> Failed </Badge> )}
          {isEdgeReviewed && !queueStatus && !isClusterSplit && ( <Badge variant={ edgeStatus === "CONFIRMED_MATCH" ? "default" : "secondary" } className={`${ edgeStatus === "CONFIRMED_MATCH" ? "bg-green-100 text-green-800 border-green-300" : "bg-red-100 text-red-800 border-red-300" }`}> {edgeStatus === "CONFIRMED_MATCH" ? <CheckCircle className="h-3 w-3 mr-1" /> : <XCircle className="h-3 w-3 mr-1" />} {edgeStatus === "CONFIRMED_MATCH" ? "Match" : "Non-Match"} </Badge> )}
          {isClusterSplit && ( <Badge variant="outline" className="bg-gray-100 text-gray-700 border-gray-300"> <Info className="h-3 w-3 mr-1" /> Cluster Split </Badge> )}
          {isPagingActiveForSelectedCluster && (
            <Badge variant="outline" className="border-purple-300 bg-purple-50 text-purple-700">
                Page {largeClusterConnectionsPage} (Connections {(largeClusterConnectionsPage-1)*CONNECTION_PAGE_SIZE + 1} - {Math.min(largeClusterConnectionsPage*CONNECTION_PAGE_SIZE, totalLinksInEntireCluster)} of {totalLinksInEntireCluster})
            </Badge>
          )}
        </div>
        <CollapsibleTrigger asChild>
          <Button variant="ghost" size="sm">
            {isExpanded ? (
              <ChevronUp className="h-4 w-4" />
            ) : (
              <ChevronDown className="h-4 w-4" />
            )}
            <span className="sr-only">Toggle review panel</span>
          </Button>
        </CollapsibleTrigger>
      </div>

      <CollapsibleContent className="flex-1 min-h-0">
        <div className="h-full overflow-y-auto space-y-3 pr-2 custom-scrollbar">
          {showReviewButtons ? (
            <div className="space-y-3 p-1">
              <p className="text-sm text-muted-foreground">
                Do these records represent the same real-world {resolutionMode}?
              </p>

              {/* Navigation for unreviewed connections (operates on current view) */}
              {edgeSelectionInfo.totalUnreviewedEdgesInCluster > 0 && // Check total unreviewed in *entire* cluster
                !isClusterSplit && edgeSelectionInfo.totalEdges > 0 && // Check edges in current view
                (
                  <div className="flex justify-center items-center gap-2 mb-3">
                    <Button
                      variant="outline"
                      size="icon"
                      onClick={handlePreviousUnreviewed}
                      disabled={ isSubmitting || isLoadingUI || queueStatus === "processing" || queueStatus === "pending" || edgeSelectionInfo.totalEdges <=0 }
                      aria-label="Previous unreviewed connection in view"
                    >
                      <ChevronLeft className="h-4 w-4" />
                    </Button>
                    <span className="text-xs text-muted-foreground whitespace-nowrap px-2 py-1 bg-muted rounded-md">
                      {edgeSelectionInfo.currentUnreviewedEdgeIndexInCluster !== -1 && edgeSelectionInfo.totalUnreviewedEdgesInCluster > 0
                        ? `Viewing unreviewed ${edgeSelectionInfo.currentUnreviewedEdgeIndexInCluster + 1} of ${edgeSelectionInfo.totalUnreviewedEdgesInCluster} (total in cluster)`
                        : edgeSelectionInfo.totalUnreviewedEdgesInCluster > 0
                        ? `${edgeSelectionInfo.totalUnreviewedEdgesInCluster} unreviewed in cluster`
                        : `No unreviewed in cluster`}
                    </span>
                    <Button
                      variant="outline"
                      size="icon"
                      onClick={handleNextUnreviewed}
                      disabled={ isSubmitting || isLoadingUI || queueStatus === "processing" || queueStatus === "pending" || edgeSelectionInfo.totalEdges <=0 }
                      aria-label="Next unreviewed connection in view"
                    >
                      <ChevronRight className="h-4 w-4" />
                    </Button>
                  </div>
                )}

              <div className="flex flex-col sm:flex-row justify-between gap-2">
                <Button variant="outline" className="border-red-500 text-red-600 hover:bg-red-50 hover:text-red-700 flex-1" size="sm" onClick={() => handleReviewDecision("REJECTED")} disabled={ isSubmitting || isLoadingUI || queueStatus === "processing" || queueStatus === "pending" || isClusterSplit || !selectedEdgeId } > <X className="h-4 w-4 mr-1" /> Not a Match </Button>
                <Button variant="default" className="bg-green-600 hover:bg-green-700 flex-1" size="sm" onClick={() => handleReviewDecision("ACCEPTED")} disabled={ isSubmitting || isLoadingUI || queueStatus === "processing" || queueStatus === "pending" || isClusterSplit || !selectedEdgeId } > <Check className="h-4 w-4 mr-1" /> Confirm Match </Button>
              </div>
              {queueStatus === "failed" && !isClusterSplit && ( <Button variant="outline" size="sm" onClick={handleRetryQueueItem} className="w-full mt-2 border-amber-500 text-amber-600 hover:bg-amber-50" disabled={isSubmitting || isLoadingUI} > <RotateCcw className="h-4 w-4 mr-1" /> Retry Failed Submission </Button> )}
            </div>
          ) : (
            <div className="space-y-3 p-1">
              <Card className={`border-2 ${ isClusterSplit ? "border-gray-200 bg-gray-50" : edgeStatus === "CONFIRMED_MATCH" ? "border-green-200 bg-green-50" : "border-red-200 bg-red-50" }`} >
                <CardContent className="p-3">
                  <div className="flex items-center gap-3">
                    {isClusterSplit ? <Info className="h-6 w-6 text-gray-600" /> : edgeStatus === "CONFIRMED_MATCH" ? <CheckCircle className="h-6 w-6 text-green-600" /> : <XCircle className="h-6 w-6 text-red-600" />}
                    <div>
                      <h4 className="font-semibold text-sm"> {isClusterSplit ? "Cluster Split" : edgeStatus === "CONFIRMED_MATCH" ? "Match Confirmed" : "Non-Match Confirmed"} </h4>
                      <p className="text-xs text-muted-foreground"> {isClusterSplit ? "This cluster has been processed and split." : "This connection has been reviewed."} </p>
                    </div>
                  </div>
                </CardContent>
              </Card>
              <Button variant="outline" size="sm" onClick={handleSkipToNext} className="w-full" disabled={isLoadingUI || (isLoadingConnectionPageData && isPagingActiveForSelectedCluster)} > <SkipForward className="h-4 w-4 mr-1" /> Continue to Next Unreviewed/Cluster </Button>
            </div>
          )}
          
          {/* Load More Button for Large Clusters */}
          {isPagingActiveForSelectedCluster && canLoadMoreConnections && (
            <Button 
                onClick={handleViewNextConnectionPage} 
                disabled={isLoadingConnectionPageData}
                variant="secondary"
                className="w-full mt-2"
            >
                {isLoadingConnectionPageData ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : <ChevronsRight className="h-4 w-4 mr-2" />}
                Load Next {CONNECTION_PAGE_SIZE} Connections
            </Button>
          )}
           {isPagingActiveForSelectedCluster && !canLoadMoreConnections && !isLoadingConnectionPageData && largeClusterConnectionsPage > 0 && (
            <p className="text-xs text-center text-muted-foreground mt-2">All connections for this large cluster have been loaded into view.</p>
           )}


          <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
            <Card> <CardHeader className="p-3"> <CardTitle className="text-sm">{nodeLabel} 1 Details</CardTitle> </CardHeader> <CardContent className="p-3 pt-0"> <NodeAttributesDisplay nodeDetails={node1Details} /> </CardContent> </Card>
            <Card> <CardHeader className="p-3"> <CardTitle className="text-sm">{nodeLabel} 2 Details</CardTitle> </CardHeader> <CardContent className="p-3 pt-0"> <NodeAttributesDisplay nodeDetails={node2Details} /> </CardContent> </Card>
          </div>

          <Tabs defaultValue="matching-methods">
            <TabsList className="grid w-full grid-cols-1"> <TabsTrigger value="matching-methods"> Matching Methods </TabsTrigger> </TabsList>
            <TabsContent value="matching-methods" className="space-y-3">
              <Card>
                <CardHeader className="p-3"> <CardTitle className="text-sm flex justify-between items-center"> <span>Matching Methods</span> <span className="text-xs font-normal"> Overall Confidence: <span className="font-medium"> {edgeDetails?.edgeWeight?.toFixed(2) ?? "N/A"} </span> </span> </CardTitle> </CardHeader>
                <CardContent className="p-3 pt-0">
                  {resolutionMode === "entity" && currentConnectionData && isEntityConnectionData( currentConnectionData as EntityConnectionDataResponse | ServiceConnectionDataResponse, resolutionMode ) ? (
                    ( (currentConnectionData as EntityConnectionDataResponse) .edge as VisualizationEntityEdge ).details?.methods && ( (currentConnectionData as EntityConnectionDataResponse) .edge as VisualizationEntityEdge ).details!.methods.length > 0 ? (
                      <div className="space-y-1">
                        {( ( (currentConnectionData as EntityConnectionDataResponse) .edge as VisualizationEntityEdge ).details!.methods.map( ( method: { method_type: string; pre_rl_confidence: number; rl_confidence: number; combined_confidence: number; }, index: Key | null | undefined ) => ( <div key={ method.method_type ? `${method.method_type}-${index}` : index } className="grid grid-cols-[1fr_auto_auto_auto] gap-2 items-center text-xs" > <div> {method.method_type?.replace(/_/g, " ") ?? "Unknown Method"} </div> <div className="text-right"> <span className="text-xs text-muted-foreground"> Pre-RL: </span> {method.pre_rl_confidence?.toFixed(2) ?? "N/A"} </div> <div className="text-right"> <span className="text-xs text-muted-foreground"> RL: </span> {method.rl_confidence?.toFixed(2) ?? "N/A"} </div> <div className="text-right font-medium"> <span className="text-xs text-muted-foreground"> Combined: </span> {method.combined_confidence?.toFixed(2) ?? "N/A"} </div> </div> ) ) )}
                      </div>
                    ) : (currentConnectionData as EntityConnectionDataResponse) .entityGroups.length > 0 ? (
                      <div className="space-y-1">
                        {( (currentConnectionData as EntityConnectionDataResponse) .entityGroups as EntityGroup[] ).map((group, index) => ( <div key={group.id || index} className="grid grid-cols-[1fr_auto_auto] gap-2 items-center text-xs" > <div> {group.methodType?.replace(/_/g, " ") ?? "Unknown Group Method"} </div> <div className="text-right"> <span className="text-xs text-muted-foreground"> Pre-RL: </span> {group.preRlConfidenceScore?.toFixed(2) ?? "N/A"} </div> <div className="text-right font-medium"> <span className="text-xs text-muted-foreground"> Combined: </span> {group.confidenceScore?.toFixed(2) ?? "N/A"} </div> </div> ))}
                      </div>
                    ) : ( <p className="text-xs text-muted-foreground"> No detailed matching methods or entity groups available for this connection. </p> )
                  ) : resolutionMode === "service" && currentConnectionData && isServiceConnectionData( currentConnectionData as EntityConnectionDataResponse | ServiceConnectionDataResponse, resolutionMode ) ? (
                    (currentConnectionData as ServiceConnectionDataResponse) .edge?.details ? ( <pre className="text-xs bg-muted p-2 rounded-md overflow-x-auto"> {JSON.stringify( ( currentConnectionData as ServiceConnectionDataResponse ).edge.details, null, 2 )} </pre> ) : ( <p className="text-xs text-muted-foreground"> No specific matching methods detailed for this service connection. </p> )
                  ) : ( <p className="text-xs text-muted-foreground"> Matching method details unavailable or mode/data mismatch. </p> )}
                </CardContent>
              </Card>

              {groupsForEdge.length > 0 && (
                <Card>
                  <CardHeader className="p-3"> <CardTitle className="text-sm"> Underlying Group Details ({groupsForEdge.length}) </CardTitle> </CardHeader>
                  <CardContent className="p-3 pt-0 space-y-2">
                    {groupsForEdge.map((group) => ( <div key={group.id} className="rounded-md border bg-muted/30 p-2" > <div className="flex justify-between items-center mb-1"> <span className="text-xs font-medium capitalize"> {group.methodType.replace(/_/g, " ")} Match Group </span> <div className="flex gap-2 items-center"> <Badge variant={ group.confirmedStatus === "CONFIRMED_MATCH" ? "default" : group.confirmedStatus === "CONFIRMED_NON_MATCH" ? "destructive" : "outline" } className={`text-xs ${ group.confirmedStatus === "CONFIRMED_MATCH" ? "border-green-300 bg-green-50 text-green-700" : group.confirmedStatus === "CONFIRMED_NON_MATCH" ? "border-red-300 bg-red-50 text-red-700" : "" }`} > {group.confirmedStatus?.replace(/_/g, " ") ?? "Pending"} </Badge> <span className="text-xs text-muted-foreground"> Score: {group.confidenceScore ? group.confidenceScore.toFixed(3) : "N/A"} </span> </div> </div> <div className="text-xs text-muted-foreground space-y-0.5"> {Object.entries(group.matchValues?.values || {}) .map(([key, val]) => ( <div key={key} className="truncate"> <span className="font-medium capitalize"> {key .replace(/_/g, " ") .replace(/normalized|original|1|2/gi, "") .trim()} : </span> {String(val)} </div> )) .slice(0, 3)} </div> </div> ))}
                  </CardContent>
                </Card>
              )}
            </TabsContent>
          </Tabs>
        </div>
      </CollapsibleContent>
    </Collapsible>
  );
}

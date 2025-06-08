// components/connection-review-tools.tsx
"use client";

import { useEffect, useState, useCallback, Key, useMemo } from "react";
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
  DownloadCloud,
  ChevronsRight,
  Maximize,
  Minimize,
} from "lucide-react";
import type {
  VisualizationEntityEdge,
  EntityGroup,
  GroupReviewDecision,
  QueuedReviewBatch,
  Service,
  EntityConnectionDataResponse,
  Organization,
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
    currentConnectionData,
    currentVisualizationData,
    selectedClusterDetails,
    isReviewToolsMaximized, // Get the new state from context
    actions,
    queries,
    reviewQueue,
    edgeSelectionInfo,
    activelyPagingClusterId,
    largeClusterConnectionsPage,
    isLoadingConnectionPageData,
    isProcessingQueue,
  } = useEntityResolution();
  const { toast } = useToast();

  const [isSubmitting, setIsSubmitting] = useState(false);
  const [isContinuing, setIsContinuing] = useState(false);
  const [isExpanded, setIsExpanded] = useState(true);
  const [isAttributesOpen, setIsAttributesOpen] = useState(false);

  useEffect(() => {
    if (
      selectedEdgeId &&
      !currentConnectionData &&
      !queries.isConnectionDataLoading(selectedEdgeId) &&
      !isLoadingConnectionPageData
    ) {
      actions.loadSingleConnectionData(selectedEdgeId);
    }
  }, [
    selectedEdgeId,
    currentConnectionData,
    queries,
    actions,
    isLoadingConnectionPageData,
  ]);

  useEffect(() => {
    setIsSubmitting(false);
  }, [selectedEdgeId]);

  const handleReviewDecision = useCallback(
    async (decision: GroupReviewDecision) => {
      if (!selectedEdgeId) {
        toast({
          title: "Error",
          description: "No connection selected.",
          variant: "destructive",
        });
        return;
      }
      if (selectedClusterDetails?.wasSplit) {
        toast({
          title: "Info",
          description: "This cluster has been split and cannot be reviewed.",
        });
        return;
      }
      const currentEdgeIsReviewed = selectedEdgeId
        ? queries.isEdgeReviewed(selectedEdgeId)
        : false;
      const currentQueueStatus = selectedEdgeId
        ? queries.getQueueItemStatus(selectedEdgeId)
        : null;

      if (currentEdgeIsReviewed && currentQueueStatus !== "failed") {
        toast({
          title: "Already Reviewed",
          description: "This connection has already been reviewed.",
        });
        return;
      }
      if (
        currentQueueStatus === "processing" ||
        currentQueueStatus === "pending"
      ) {
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
    },
    [selectedEdgeId, toast, selectedClusterDetails?.wasSplit, queries, actions]
  );

  const handleRetryLoadConnection = useCallback(() => {
    if (
      selectedEdgeId &&
      selectedClusterDetails &&
      selectedClusterDetails.groupCount
    ) {
      const isLarge =
        selectedClusterDetails.groupCount > LARGE_CLUSTER_THRESHOLD;
      const isPaging =
        selectedClusterId === activelyPagingClusterId &&
        largeClusterConnectionsPage > 0;

      if (isLarge && isPaging && selectedClusterId) {
        actions.viewNextConnectionPage(selectedClusterId);
      } else {
        actions.invalidateConnectionData(selectedEdgeId);
      }
    } else if (selectedEdgeId) {
      actions.invalidateConnectionData(selectedEdgeId);
    }
  }, [
    selectedEdgeId,
    selectedClusterDetails,
    selectedClusterId,
    activelyPagingClusterId,
    largeClusterConnectionsPage,
    actions,
  ]);

  const handleContinueToNext = useCallback(async () => {
    if (!selectedClusterId) {
      toast({
        title: "No Cluster Selected",
        description: "Cannot continue.",
        variant: "destructive",
      });
      return;
    }
    setIsContinuing(true);

    try {
      const clusterDetails = queries.getClusterById(selectedClusterId);
      const isClusterMarkedSplit = clusterDetails?.wasSplit || false;

      let isCriticallyProcessing = false;
      if (isProcessingQueue && currentVisualizationData?.links) {
        for (const link of currentVisualizationData.links) {
          if (queries.getQueueItemStatus(link.id) === "processing") {
            isCriticallyProcessing = true;
            break;
          }
        }
      }

      if (isCriticallyProcessing) {
        toast({
          title: "Processing Active",
          description:
            "Current cluster reviews are saving. Advancing to next cluster.",
          duration: 2500,
        });
        await actions.advanceToNextCluster();
        return;
      }

      if (isClusterMarkedSplit) {
        console.log(`Cluster ${selectedClusterId} is split. Advancing.`);
        await actions.advanceToNextCluster();
        return;
      }

      if (edgeSelectionInfo.totalUnreviewedEdgesInCluster > 0) {
        const unreviewedInCurrentView =
          currentVisualizationData?.links?.filter(
            (l) => !queries.isEdgeReviewed(l.id)
          ) || [];
        let attemptPagingOrAdvanceCluster = false;

        if (unreviewedInCurrentView.length > 0) {
          const currentSelectedEdgeIsReviewed = selectedEdgeId
            ? queries.isEdgeReviewed(selectedEdgeId)
            : true;
          if (selectedEdgeId && !currentSelectedEdgeIsReviewed) {
            const otherUnreviewedInView = unreviewedInCurrentView.filter(
              (l) => l.id !== selectedEdgeId
            );
            if (otherUnreviewedInView.length > 0) {
              console.log(
                "Continuing to next unreviewed item in current view."
              );
              actions.selectNextUnreviewedInCluster();
            } else {
              console.log(
                "Current selection is the only unreviewed in view. Attempting to page or advance cluster."
              );
              attemptPagingOrAdvanceCluster = true;
            }
          } else {
            console.log("Selecting first unreviewed item in current view.");
            actions.selectNextUnreviewedInCluster();
          }
        } else {
          console.log(
            "Current view exhausted of unreviewed. Attempting to page or advance cluster."
          );
          attemptPagingOrAdvanceCluster = true;
        }

        if (attemptPagingOrAdvanceCluster) {
          const isLargeAndPaged =
            selectedClusterId === activelyPagingClusterId &&
            largeClusterConnectionsPage > 0;
          const totalLinksInEntireCluster =
            edgeSelectionInfo.totalEdgesInEntireCluster;
          const canLoadMorePages =
            isLargeAndPaged &&
            largeClusterConnectionsPage * CONNECTION_PAGE_SIZE <
              totalLinksInEntireCluster;

          if (isLargeAndPaged && canLoadMorePages) {
            console.log(
              `Loading next page for large cluster ${selectedClusterId}.`
            );
            await actions.viewNextConnectionPage(selectedClusterId);
          } else {
            console.warn(
              `Cluster ${selectedClusterId} has unreviewed items, but current view is exhausted and paging options are limited/done. Advancing to next cluster.`
            );
            await actions.advanceToNextCluster();
          }
        }
      } else {
        console.log(
          `All connections in cluster ${selectedClusterId} are reviewed. Advancing.`
        );
        await actions.advanceToNextCluster();
      }
    } catch (error) {
      console.error("Error in handleContinueToNext:", error);
      toast({
        title: "Error",
        description: "Could not proceed with navigation.",
        variant: "destructive",
      });
    } finally {
      setTimeout(() => setIsContinuing(false), 300);
    }
  }, [
    selectedClusterId,
    selectedEdgeId,
    actions,
    queries,
    toast,
    edgeSelectionInfo,
    currentVisualizationData,
    activelyPagingClusterId,
    largeClusterConnectionsPage,
    isProcessingQueue,
  ]);

  const handleRetryQueueItem = useCallback(() => {
    if (selectedEdgeId) {
      const currentQueueStatus = queries.getQueueItemStatus(selectedEdgeId);
      if (currentQueueStatus === "failed") {
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
    }
  }, [selectedEdgeId, reviewQueue, actions, queries, toast]);

  const handleInitializeLargeClusterPaging = useCallback(async () => {
    if (selectedClusterId) {
      await actions.initializeLargeClusterConnectionPaging(selectedClusterId);
    }
  }, [selectedClusterId, actions]);

  const handleViewNextConnectionPage = useCallback(async () => {
    if (selectedClusterId) {
      await actions.viewNextConnectionPage(selectedClusterId);
    }
  }, [selectedClusterId, actions]);

  const handlePreviousUnreviewed = useCallback(() => {
    actions.selectPreviousUnreviewedInCluster();
  }, [actions]);

  const handleNextUnreviewed = useCallback(() => {
    actions.selectNextUnreviewedInCluster();
  }, [actions]);

  const isLoadingUI = selectedEdgeId
    ? queries.isConnectionDataLoading(selectedEdgeId)
    : false;

  const isAnyGeneralOperationPending = useMemo(() => {
    const isPagingActiveForSelectedCluster =
      selectedClusterId === activelyPagingClusterId &&
      largeClusterConnectionsPage > 0;
    return (
      isSubmitting ||
      isLoadingUI ||
      (isLoadingConnectionPageData && isPagingActiveForSelectedCluster) ||
      isProcessingQueue
    );
  }, [
    isSubmitting,
    isLoadingUI,
    isLoadingConnectionPageData,
    selectedClusterId,
    activelyPagingClusterId,
    largeClusterConnectionsPage,
    isProcessingQueue,
  ]);

  const isContinueButtonDisabled = useMemo(() => {
    if (isContinuing) return true;

    if (
      selectedClusterId &&
      edgeSelectionInfo.totalUnreviewedEdgesInCluster === 0 &&
      queries.getClusterProgress(selectedClusterId)?.isComplete &&
      !queries.canAdvanceToNextCluster()
    ) {
      return true;
    }
    return false;
  }, [
    isContinuing,
    selectedClusterId,
    edgeSelectionInfo.totalUnreviewedEdgesInCluster,
    queries,
  ]);

  if (!selectedClusterId) {
    return (
      <div className="flex justify-center items-center h-full text-muted-foreground p-4 border rounded-md bg-card shadow">
        Select a cluster to begin reviewing connections.
      </div>
    );
  }

  const connectionCount = selectedClusterDetails?.groupCount || 0;
  const isSelectedClusterLarge = connectionCount > LARGE_CLUSTER_THRESHOLD;

  const isPagingActiveForSelectedCluster =
    selectedClusterId === activelyPagingClusterId &&
    largeClusterConnectionsPage > 0;

  if (isSelectedClusterLarge && !isPagingActiveForSelectedCluster) {
    return (
      <Card className="h-full flex flex-col items-center justify-center p-4">
        <CardHeader className="text-center">
          <CardTitle className="flex items-center justify-center">
            <AlertTriangle className="h-6 w-6 mr-2 text-amber-500" /> Large
            Cluster
          </CardTitle>
        </CardHeader>
        <CardContent className="text-center space-y-3">
          <p className="text-sm text-muted-foreground">
            This cluster has a large number of connections ({connectionCount}).
            To optimize performance and memory usage, connections are loaded in
            batches.
          </p>
          <Button
            onClick={handleInitializeLargeClusterPaging}
            disabled={
              isLoadingConnectionPageData || isAnyGeneralOperationPending
            }
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

  if (
    !selectedEdgeId &&
    (isPagingActiveForSelectedCluster || !isSelectedClusterLarge)
  ) {
    if (isLoadingConnectionPageData && isSelectedClusterLarge) {
      return (
        <div className="flex justify-center items-center h-full border rounded-md bg-card shadow">
          <Loader2 className="h-6 w-6 animate-spin text-primary mr-2" />{" "}
          Initializing connections...
        </div>
      );
    }
    return (
      <div className="flex justify-center items-center h-full text-muted-foreground p-4 border rounded-md bg-card shadow">
        Select a connection from the graph to review its details.
        {isPagingActiveForSelectedCluster &&
          edgeSelectionInfo.totalEdges === 0 &&
          !isLoadingConnectionPageData && (
            <p className="text-xs mt-1">
              No connections on this page, or all reviewed. Try loading more.
            </p>
          )}
      </div>
    );
  }

  if (isLoadingUI && !currentConnectionData) {
    return (
      <div className="flex justify-center items-center h-full border rounded-md bg-card shadow">
        <Loader2 className="h-6 w-6 animate-spin text-primary" />
      </div>
    );
  }

  const errorUI = selectedEdgeId
    ? queries.getConnectionError(selectedEdgeId)
    : null;

  if (errorUI && !currentConnectionData) {
    return (
      <Card className="h-full flex flex-col items-center justify-center">
        <CardHeader>
          <CardTitle className="flex items-center">
            <AlertCircle className="h-6 w-6 mr-2 text-destructive" /> Error
            Loading Details
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
            <Button
              variant="outline"
              size="sm"
              onClick={handleRetryLoadConnection}
              disabled={isAnyGeneralOperationPending}
            >
              <RefreshCw className="h-4 w-4 mr-1" /> Retry
            </Button>
            <Button
              variant="outline"
              size="sm"
              onClick={() => actions.setSelectedEdgeId(null)}
              disabled={isAnyGeneralOperationPending}
            >
              Clear Selection
            </Button>
          </div>
        </CardContent>
      </Card>
    );
  }

  if (!currentConnectionData && selectedEdgeId) {
    if (!isLoadingUI && !errorUI) {
      return (
        <div className="flex justify-center items-center h-full border rounded-md bg-card shadow">
          <Loader2 className="h-6 w-6 animate-spin text-primary mr-2" /> Loading
          connection data...
        </div>
      );
    }
    return null;
  }

  const data = currentConnectionData;
  const edgeDetails = data?.edge as VisualizationEntityEdge | undefined;
  const groupsForEdge = data?.entityGroups || [];
  const node1 = data?.entity1;
  const node2 = data?.entity2;

  if (!node1 || !node2 || !edgeDetails) {
    if (
      isLoadingUI ||
      (isLoadingConnectionPageData && isPagingActiveForSelectedCluster)
    ) {
      return (
        <div className="flex justify-center items-center h-full border rounded-md bg-card shadow">
          <Loader2 className="h-6 w-6 animate-spin text-primary mr-2" /> Loading
          details...
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
            disabled={isAnyGeneralOperationPending}
          >
            <RefreshCw className="h-4 w-4 mr-1" /> Retry Load
          </Button>
        </CardContent>
      </Card>
    );
  }

  const node1Details = queries.getNodeDetail(node1.id);
  const node2Details = queries.getNodeDetail(node2.id);

  const edgeStatus = selectedEdgeId
    ? queries.getEdgeStatus(selectedEdgeId)
    : null;
  const isEdgeReviewed = selectedEdgeId
    ? queries.isEdgeReviewed(selectedEdgeId)
    : false;
  const queueStatusForSelectedEdge = selectedEdgeId
    ? queries.getQueueItemStatus(selectedEdgeId)
    : null;
  const isClusterSplit = selectedClusterDetails?.wasSplit || false;

  const showReviewButtons =
    (!isEdgeReviewed || queueStatusForSelectedEdge === "failed") &&
    !isClusterSplit;

  const totalLinksInEntireCluster = edgeSelectionInfo.totalEdgesInEntireCluster;
  const canLoadMoreConnections =
    isPagingActiveForSelectedCluster &&
    largeClusterConnectionsPage * CONNECTION_PAGE_SIZE <
      totalLinksInEntireCluster;
  const nodeLabel = resolutionMode === "entity" ? "Entity" : "Service";

  return (
    <Collapsible
      open={isExpanded}
      onOpenChange={setIsExpanded}
      className="h-full flex flex-col"
    >
      <div className="flex justify-between items-center flex-shrink-0 pb-2 border-b mb-3">
        <div className="flex items-center gap-2 flex-wrap">
          <h3 className="text-lg font-medium">{nodeLabel} Connection Review</h3>
          {(isLoadingUI ||
            (isLoadingConnectionPageData && isPagingActiveForSelectedCluster) ||
            isContinuing) && (
            <Loader2 className="h-4 w-4 animate-spin text-muted-foreground" />
          )}
          {queueStatusForSelectedEdge === "processing" && (
            <Badge
              variant="outline"
              className="bg-blue-50 text-blue-700 border-blue-300"
            >
              <Loader2 className="h-3 w-3 mr-1 animate-spin" /> Processing
            </Badge>
          )}
          {queueStatusForSelectedEdge === "pending" && (
            <Badge
              variant="outline"
              className="bg-yellow-50 text-yellow-700 border-yellow-300"
            >
              <Info className="h-3 w-3 mr-1" /> Queued
            </Badge>
          )}
          {queueStatusForSelectedEdge === "failed" && (
            <Badge variant="destructive">
              <AlertTriangle className="h-3 w-3 mr-1" /> Failed
            </Badge>
          )}
          {isEdgeReviewed && !queueStatusForSelectedEdge && !isClusterSplit && (
            <Badge
              variant={
                edgeStatus === "CONFIRMED_MATCH" ? "default" : "secondary"
              }
              className={`${
                edgeStatus === "CONFIRMED_MATCH"
                  ? "bg-green-100 text-green-800 border-green-300"
                  : "bg-red-100 text-red-800 border-red-300"
              }`}
            >
              {edgeStatus === "CONFIRMED_MATCH" ? (
                <CheckCircle className="h-3 w-3 mr-1" />
              ) : (
                <XCircle className="h-3 w-3 mr-1" />
              )}
              {edgeStatus === "CONFIRMED_MATCH" ? "Match" : "Non-Match"}
            </Badge>
          )}
          {isClusterSplit && (
            <Badge
              variant="outline"
              className="bg-gray-100 text-gray-700 border-gray-300"
            >
              <Info className="h-3 w-3 mr-1" /> Cluster Split
            </Badge>
          )}
          {isPagingActiveForSelectedCluster && (
            <Badge
              variant="outline"
              className="border-purple-300 bg-purple-50 text-purple-700"
            >
              Page {largeClusterConnectionsPage} (Connections{" "}
              {(largeClusterConnectionsPage - 1) * CONNECTION_PAGE_SIZE + 1} -{" "}
              {Math.min(
                largeClusterConnectionsPage * CONNECTION_PAGE_SIZE,
                totalLinksInEntireCluster
              )}{" "}
              of {totalLinksInEntireCluster})
            </Badge>
          )}
        </div>
        <div className="flex items-center">
          <Button
            variant="ghost"
            size="sm"
            onClick={() =>
              actions.setIsReviewToolsMaximized(!isReviewToolsMaximized)
            }
            title={isReviewToolsMaximized ? "Restore" : "Maximize"}
          >
            {isReviewToolsMaximized ? (
              <Minimize className="h-4 w-4" />
            ) : (
              <Maximize className="h-4 w-4" />
            )}
            <span className="sr-only">
              {isReviewToolsMaximized ? "Restore" : "Maximize"}
            </span>
          </Button>
          <CollapsibleTrigger asChild>
            <Button
              variant="ghost"
              size="sm"
              disabled={isAnyGeneralOperationPending || isContinuing}
            >
              {isExpanded ? (
                <ChevronUp className="h-4 w-4" />
              ) : (
                <ChevronDown className="h-4 w-4" />
              )}
              <span className="sr-only">Toggle review panel</span>
            </Button>
          </CollapsibleTrigger>
        </div>
      </div>

      <CollapsibleContent className="flex-1 min-h-0">
        <div className="h-full overflow-y-auto space-y-3 pr-2 custom-scrollbar">
          {showReviewButtons ? (
            <div className="space-y-3 p-1">
              <p className="text-sm text-muted-foreground">
                Do these records represent the same real-world {resolutionMode}?
              </p>
              {edgeSelectionInfo.totalUnreviewedEdgesInCluster > 0 &&
                !isClusterSplit &&
                edgeSelectionInfo.totalEdges > 0 && (
                  <div className="flex justify-center items-center gap-2 mb-3">
                    <Button
                      variant="outline"
                      size="icon"
                      onClick={handlePreviousUnreviewed}
                      disabled={
                        isAnyGeneralOperationPending ||
                        isContinuing ||
                        edgeSelectionInfo.totalEdges <= 0
                      }
                      aria-label="Previous unreviewed connection in view"
                    >
                      <ChevronLeft className="h-4 w-4" />
                    </Button>
                    <span className="text-xs text-muted-foreground whitespace-nowrap px-2 py-1 bg-muted rounded-md">
                      {edgeSelectionInfo.currentUnreviewedEdgeIndexInCluster !==
                        -1 &&
                      edgeSelectionInfo.totalUnreviewedEdgesInCluster > 0
                        ? `Unreviewed ${
                            edgeSelectionInfo.currentUnreviewedEdgeIndexInCluster +
                            1
                          } of ${
                            edgeSelectionInfo.totalUnreviewedEdgesInCluster
                          } (cluster total)`
                        : edgeSelectionInfo.totalUnreviewedEdgesInCluster > 0
                        ? `${edgeSelectionInfo.totalUnreviewedEdgesInCluster} unreviewed in cluster`
                        : `No unreviewed in cluster`}
                    </span>
                    <Button
                      variant="outline"
                      size="icon"
                      onClick={handleNextUnreviewed}
                      disabled={
                        isAnyGeneralOperationPending ||
                        isContinuing ||
                        edgeSelectionInfo.totalEdges <= 0
                      }
                      aria-label="Next unreviewed connection in view"
                    >
                      <ChevronRight className="h-4 w-4" />
                    </Button>
                  </div>
                )}

              <div className="flex flex-col sm:flex-row justify-between gap-2">
                <Button
                  variant="outline"
                  className="border-red-500 text-red-600 hover:bg-red-50 hover:text-red-700 flex-1"
                  size="sm"
                  onClick={() => handleReviewDecision("REJECTED")}
                  disabled={
                    isAnyGeneralOperationPending ||
                    isClusterSplit ||
                    !selectedEdgeId
                  }
                >
                  <X className="h-4 w-4 mr-1" /> Not a Match
                </Button>
                <Button
                  variant="default"
                  className="bg-green-600 hover:bg-green-700 flex-1"
                  size="sm"
                  onClick={() => handleReviewDecision("ACCEPTED")}
                  disabled={
                    isAnyGeneralOperationPending ||
                    isClusterSplit ||
                    !selectedEdgeId
                  }
                >
                  <Check className="h-4 w-4 mr-1" /> Confirm Match
                </Button>
              </div>
              {queueStatusForSelectedEdge === "failed" && !isClusterSplit && (
                <Button
                  variant="outline"
                  size="sm"
                  onClick={handleRetryQueueItem}
                  className="w-full mt-2 border-amber-500 text-amber-600 hover:bg-amber-50"
                  disabled={isAnyGeneralOperationPending}
                >
                  <RotateCcw className="h-4 w-4 mr-1" /> Retry Failed Submission
                </Button>
              )}
            </div>
          ) : (
            <div className="space-y-3 p-1">
              <Card
                className={`border-2 ${
                  isClusterSplit
                    ? "border-gray-200 bg-gray-50"
                    : edgeStatus === "CONFIRMED_MATCH"
                    ? "border-green-200 bg-green-50"
                    : "border-red-200 bg-red-50"
                }`}
              >
                <CardContent className="p-3">
                  <div className="flex items-center gap-3">
                    {isClusterSplit ? (
                      <Info className="h-6 w-6 text-gray-600" />
                    ) : edgeStatus === "CONFIRMED_MATCH" ? (
                      <CheckCircle className="h-6 w-6 text-green-600" />
                    ) : (
                      <XCircle className="h-6 w-6 text-red-600" />
                    )}
                    <div>
                      <h4 className="font-semibold text-sm">
                        {isClusterSplit
                          ? "Cluster Split"
                          : edgeStatus === "CONFIRMED_MATCH"
                          ? "Match Confirmed"
                          : "Non-Match Confirmed"}
                      </h4>
                      <p className="text-xs text-muted-foreground">
                        {isClusterSplit
                          ? "This cluster has been processed and split."
                          : "This connection has been reviewed."}
                      </p>
                    </div>
                  </div>
                </CardContent>
              </Card>
              <Button
                variant="outline"
                size="sm"
                onClick={handleContinueToNext}
                className="w-full"
                disabled={isContinueButtonDisabled}
              >
                {isContinuing ? (
                  <Loader2 className="h-4 w-4 mr-1 animate-spin" />
                ) : (
                  <SkipForward className="h-4 w-4 mr-1" />
                )}
                Continue to Next Unreviewed Connection or Cluster
              </Button>
            </div>
          )}

          {isPagingActiveForSelectedCluster && canLoadMoreConnections && (
            <Button
              onClick={handleViewNextConnectionPage}
              disabled={
                isLoadingConnectionPageData ||
                isAnyGeneralOperationPending ||
                isContinuing
              }
              variant="secondary"
              className="w-full mt-2"
            >
              {isLoadingConnectionPageData ? (
                <Loader2 className="h-4 w-4 mr-2 animate-spin" />
              ) : (
                <ChevronsRight className="h-4 w-4 mr-2" />
              )}
              Load Next {CONNECTION_PAGE_SIZE} Connections
            </Button>
          )}
          {isPagingActiveForSelectedCluster &&
            !canLoadMoreConnections &&
            !isLoadingConnectionPageData &&
            largeClusterConnectionsPage > 0 && (
              <p className="text-xs text-center text-muted-foreground mt-2">
                All connections for this large cluster have been loaded into
                view.
              </p>
            )}

          <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
            <Card>
              <CardHeader className="p-3">
                <CardTitle className="text-sm">{nodeLabel} 1 Details</CardTitle>
              </CardHeader>
              <CardContent className="p-3 pt-0">
                <NodeAttributesDisplay
                  node={node1}
                  nodeDetails={node1Details}
                  setIsAttributesOpen={setIsAttributesOpen}
                  isAttributesOpen={isAttributesOpen}
                />
              </CardContent>
            </Card>
            <Card>
              <CardHeader className="p-3">
                <CardTitle className="text-sm">{nodeLabel} 2 Details</CardTitle>
              </CardHeader>
              <CardContent className="p-3 pt-0">
                <NodeAttributesDisplay
                  node={node2}
                  nodeDetails={node2Details}
                  setIsAttributesOpen={setIsAttributesOpen}
                  isAttributesOpen={isAttributesOpen}
                />
              </CardContent>
            </Card>
          </div>

          <Tabs defaultValue="matching-methods">
            <TabsList className="grid w-full grid-cols-1">
              <TabsTrigger value="matching-methods">
                Matching Methods
              </TabsTrigger>
            </TabsList>
            <TabsContent value="matching-methods" className="space-y-3">
              <Card>
                <CardHeader className="p-3">
                  <CardTitle className="text-sm flex justify-between items-center">
                    <span>Matching Methods</span>
                    <span className="text-xs font-normal">
                      Overall Confidence:{" "}
                      <span className="font-medium">
                        {edgeDetails?.edgeWeight?.toFixed(2) ?? "N/A"}
                      </span>
                    </span>
                  </CardTitle>
                </CardHeader>
                <CardContent className="p-3 pt-0">
                  {currentConnectionData ? (
                    (() => {
                      const edge =
                        currentConnectionData.edge as VisualizationEntityEdge;
                      const methods = edge.details?.methods;
                      const groups = currentConnectionData.entityGroups;

                      if (
                        methods &&
                        Array.isArray(methods) &&
                        methods.length > 0
                      ) {
                        return (
                          <div className="space-y-1">
                            {methods.map(
                              (
                                method: {
                                  method_type: string;
                                  pre_rl_confidence: number;
                                  rl_confidence: number;
                                  combined_confidence: number;
                                },
                                index: React.Key
                              ) => (
                                <div
                                  key={
                                    method.method_type
                                      ? `${method.method_type}-${index}`
                                      : index
                                  }
                                  className="grid grid-cols-[1fr_auto_auto_auto] gap-2 items-center text-xs"
                                >
                                  <div className="truncate">
                                    {method.method_type?.replace(/_/g, " ") ??
                                      "Unknown Method"}
                                  </div>
                                  <div className="text-right">
                                    <span className="text-xs text-muted-foreground">
                                      Pre-RL:{" "}
                                    </span>
                                    {method.pre_rl_confidence?.toFixed(2) ??
                                      "N/A"}
                                  </div>
                                  <div className="text-right">
                                    <span className="text-xs text-muted-foreground">
                                      RL:{" "}
                                    </span>
                                    {method.rl_confidence?.toFixed(2) ?? "N/A"}
                                  </div>
                                  <div className="text-right font-medium">
                                    <span className="text-xs text-muted-foreground">
                                      Combined:{" "}
                                    </span>
                                    {method.combined_confidence?.toFixed(2) ??
                                      "N/A"}
                                  </div>
                                </div>
                              )
                            )}
                          </div>
                        );
                      } else if (groups && groups.length > 0) {
                        return (
                          <div className="space-y-1">
                            {groups.map((group, index) => (
                              <div
                                key={group.id || index}
                                className="grid grid-cols-[1fr_auto_auto] gap-2 items-center text-xs"
                              >
                                <div className="truncate">
                                  {group.methodType?.replace(/_/g, " ") ??
                                    "Unknown Group Method"}
                                </div>
                                <div className="text-right">
                                  <span className="text-xs text-muted-foreground">
                                    Pre-RL:{" "}
                                  </span>
                                  {group.preRlConfidenceScore?.toFixed(2) ??
                                    "N/A"}
                                </div>
                                <div className="text-right font-medium">
                                  <span className="text-xs text-muted-foreground">
                                    Combined:{" "}
                                  </span>
                                  {group.confidenceScore?.toFixed(2) ?? "N/A"}
                                </div>
                              </div>
                            ))}
                          </div>
                        );
                      } else {
                        return (
                          <p className="text-xs text-muted-foreground">
                            No detailed matching methods or entity groups
                            available for this connection.
                          </p>
                        );
                      }
                    })()
                  ) : (
                    <p className="text-xs text-muted-foreground">
                      Matching method details unavailable.
                    </p>
                  )}
                </CardContent>
              </Card>

              {groupsForEdge.length > 0 && (
                <Card>
                  <CardHeader className="p-3">
                    <CardTitle className="text-sm">
                      Underlying Group Details ({groupsForEdge.length})
                    </CardTitle>
                  </CardHeader>
                  <CardContent className="p-3 pt-0 space-y-2">
                    {groupsForEdge.map((group) => (
                      <div
                        key={group.id}
                        className="rounded-md border bg-muted/30 p-2"
                      >
                        <div className="flex justify-between items-center mb-1">
                          <span className="text-xs font-medium capitalize truncate">
                            {group.methodType.replace(/_/g, " ")} Match Group
                          </span>
                          <div className="flex gap-2 items-center flex-shrink-0">
                            <Badge
                              variant={
                                group.confirmedStatus === "CONFIRMED_MATCH"
                                  ? "default"
                                  : group.confirmedStatus ===
                                    "CONFIRMED_NON_MATCH"
                                  ? "destructive"
                                  : "outline"
                              }
                              className={`text-xs ${
                                group.confirmedStatus === "CONFIRMED_MATCH"
                                  ? "border-green-300 bg-green-50 text-green-700"
                                  : group.confirmedStatus ===
                                    "CONFIRMED_NON_MATCH"
                                  ? "border-red-300 bg-red-50 text-red-700"
                                  : ""
                              }`}
                            >
                              {group.confirmedStatus?.replace(/_/g, " ") ??
                                "Pending"}
                            </Badge>
                            <span className="text-xs text-muted-foreground">
                              Score:{" "}
                              {group.confidenceScore
                                ? group.confidenceScore.toFixed(3)
                                : "N/A"}
                            </span>
                          </div>
                        </div>
                        <div className="text-xs text-muted-foreground space-y-0.5">
                          {Object.entries(group.matchValues?.values || {})
                            .map(([key, val]) => (
                              <div key={key} className="truncate">
                                <span className="font-medium capitalize">
                                  {key
                                    .replace(/_/g, " ")
                                    .replace(/normalized|original|1|2/gi, "")
                                    .trim()}
                                  :{" "}
                                </span>
                                {String(val)}
                              </div>
                            ))
                            .slice(0, 3)}
                        </div>
                      </div>
                    ))}
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

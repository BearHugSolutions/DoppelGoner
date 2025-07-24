// components/connection-review-tools.tsx - ENHANCED: Optimistic Continue & Smart Navigation
"use client";

import { useEffect, useState, useCallback, useMemo } from "react";
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
  ChevronLeft,
  ChevronRight,
  DownloadCloud,
  ChevronsRight,
  Maximize,
  Minimize,
  Link,
  GitBranch,
  Filter as FilterIcon,
  Clock,
} from "lucide-react";
import type {
  VisualizationEntityEdge,
  GroupReviewDecision,
  EntityConnectionDataResponse,
} from "@/types/entity-resolution";
import { useToast } from "@/hooks/use-toast";
import NodeAttributesDisplay from "./node-attribute-display";

const LARGE_CLUSTER_THRESHOLD = 200;
const CONNECTION_PAGE_SIZE = 200;

export default function ConnectionReviewTools() {
  const {
    resolutionMode,
    selectedClusterId,
    selectedEdgeId,
    currentConnectionData,
    currentVisualizationData,
    selectedClusterDetails,
    isReviewToolsMaximized,
    disconnectDependentServicesEnabled,
    workflowFilter,
    clusters,
    actions,
    queries,
    edgeSelectionInfo,
    activelyPagingClusterId,
    largeClusterConnectionsPage,
    isLoadingConnectionPageData,
  } = useEntityResolution();
  const { toast } = useToast();

  // Local state for optimistic UI
  const [isContinuing, setIsContinuing] = useState(false);
  const [isExpanded, setIsExpanded] = useState(true);
  const [isAttributesOpen, setIsAttributesOpen] = useState(false);
  const [showOptimisticContinue, setShowOptimisticContinue] = useState(false); // 🔧 NEW: Optimistic continue state

  // Extract data early and use in useMemo with proper fallbacks
  const data = currentConnectionData;
  const edgeDetails = data?.edge as VisualizationEntityEdge | undefined;
  const node1 = data?.entity1;
  const node2 = data?.entity2;

  // All useMemo hooks at the top with proper null checks
  const node1Details = useMemo(() => {
    return node1 ? queries.getNodeDetail(node1.id) : null;
  }, [node1, queries]);

  const node2Details = useMemo(() => {
    return node2 ? queries.getNodeDetail(node2.id) : null;
  }, [node2, queries]);

  // Check if current connection is cross-source
  const isCrossSourceConnection = useMemo(() => {
    if (!node1 || !node2) return false;
    const sourceSystem1 = node1.sourceSystem;
    const sourceSystem2 = node2.sourceSystem;
    return sourceSystem1 && sourceSystem2 && sourceSystem1 !== sourceSystem2;
  }, [node1, node2]);

  const { isSubmitting, error: submissionError } = useMemo(() => {
    return selectedEdgeId
      ? queries.getEdgeSubmissionStatus(selectedEdgeId)
      : { isSubmitting: false, error: null };
  }, [selectedEdgeId, queries]);

  const isLoadingUI = useMemo(() => {
    return selectedEdgeId ? queries.isConnectionDataLoading(selectedEdgeId) : false;
  }, [selectedEdgeId, queries]);

  const isAnyGeneralOperationPending = useMemo(() => {
    const isPagingActiveForSelectedCluster =
      selectedClusterId === activelyPagingClusterId &&
      largeClusterConnectionsPage > 0;
    return (
      isSubmitting ||
      isLoadingUI ||
      (isLoadingConnectionPageData && isPagingActiveForSelectedCluster)
    );
  }, [
    isSubmitting,
    isLoadingUI,
    isLoadingConnectionPageData,
    selectedClusterId,
    activelyPagingClusterId,
    largeClusterConnectionsPage,
  ]);

  const connectionCount = useMemo(() => {
    return selectedClusterDetails?.groupCount || 0;
  }, [selectedClusterDetails?.groupCount]);

  const isSelectedClusterLarge = useMemo(() => {
    return connectionCount > LARGE_CLUSTER_THRESHOLD;
  }, [connectionCount]);

  const isPagingActiveForSelectedCluster = useMemo(() => {
    return (
      selectedClusterId === activelyPagingClusterId &&
      largeClusterConnectionsPage > 0
    );
  }, [selectedClusterId, activelyPagingClusterId, largeClusterConnectionsPage]);

  // Computed values that depend on data
  const isEdgeReviewed = useMemo(() => {
    return selectedEdgeId ? queries.isEdgeReviewed(selectedEdgeId) : false;
  }, [selectedEdgeId, queries]);

  const isClusterFinalized = useMemo(() => {
    return selectedClusterDetails?.wasReviewed || false;
  }, [selectedClusterDetails?.wasReviewed]);

  // 🔧 ENHANCED: Determine if current cluster is complete for current filter
  const isCurrentClusterCompleteForFilter = useMemo(() => {
    if (!selectedClusterId) return false;
    return queries.isClusterCompleteForCurrentFilter(selectedClusterId);
  }, [selectedClusterId, queries]);

  const showReviewButtons = useMemo(() => {
    return !isEdgeReviewed && !isClusterFinalized;
  }, [isEdgeReviewed, isClusterFinalized]);

  const edgeStatus = useMemo(() => {
    return selectedEdgeId ? queries.getEdgeStatus(selectedEdgeId) : null;
  }, [selectedEdgeId, queries]);

  const nodeLabel = useMemo(() => {
    return resolutionMode === 'entity' ? 'Entity' : 'Service';
  }, [resolutionMode]);

  // 🔧 NEW: Smart cluster navigation that respects workflow filter
  const findNextUnreviewedCluster = useCallback(() => {
    return queries.findNextViableCluster(selectedClusterId || undefined);
  }, [selectedClusterId, queries]);

  // All useEffect hooks after useMemo hooks
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

  // 🔧 NEW: Reset optimistic continue state when edge changes
  useEffect(() => {
    setShowOptimisticContinue(false);
  }, [selectedEdgeId]);

  // All useCallback hooks after useEffect hooks
  const handleReviewDecision = useCallback(
    async (decision: GroupReviewDecision) => {
      if (!selectedEdgeId || !selectedClusterId) {
        toast({
          title: "Error",
          description: "No connection or cluster selected.",
          variant: "destructive",
        });
        return;
      }
      if (selectedClusterDetails?.wasReviewed) {
        toast({
          title: "Info",
          description: "This cluster has already been finalized.",
        });
        return;
      }
      if (queries.isEdgeReviewed(selectedEdgeId)) {
        toast({
          title: "Already Reviewed",
          description: "This connection has already been reviewed.",
        });
        return;
      }
      if (isSubmitting) {
        toast({ title: "In Progress", description: "Submission is already in progress." });
        return;
      }

      // 🔧 NEW: Show optimistic continue button immediately
      setShowOptimisticContinue(true);

      try {
        await actions.submitEdgeReview(selectedEdgeId, decision);
        // The optimistic update happens inside submitEdgeReview
        // Continue button is already shown optimistically
      } catch (error) {
        // Error handling is done inside submitEdgeReview
        // Revert optimistic continue state on error
        setShowOptimisticContinue(false);
      }
    },
    [
      selectedEdgeId,
      selectedClusterId,
      toast,
      selectedClusterDetails?.wasReviewed,
      queries,
      actions,
      isSubmitting,
    ]
  );

  const handleRetryLoadConnection = useCallback(() => {
    if (selectedEdgeId) {
      actions.invalidateConnectionData(selectedEdgeId);
    }
  }, [selectedEdgeId, actions]);

  // 🔧 ENHANCED: Smart continue logic with filter awareness
  const handleContinueToNext = useCallback(async () => {
    if (!selectedClusterId) {
      return;
    }
    
    setIsContinuing(true);
    setShowOptimisticContinue(false); // Hide the optimistic button
    
    try {
      // Check if current cluster is complete for the current filter
      if (isCurrentClusterCompleteForFilter) {
        console.log(`🚀 [ConnectionReview] Current cluster ${selectedClusterId} is complete for filter ${workflowFilter}. Finding next cluster.`);
        
        const nextCluster = findNextUnreviewedCluster();
        
        if (nextCluster === "next_page") {
          // Load next page
          const nextPageToLoad = clusters.page + 1;
          console.log(`📄 [ConnectionReview] Loading next page (${nextPageToLoad}) for unreviewed clusters.`);
          await actions.loadClusterProgress(nextPageToLoad, clusters.limit);
        } else if (nextCluster) {
          // Move to next cluster
          console.log(`➡️ [ConnectionReview] Advancing to next unreviewed cluster: ${nextCluster.id}`);
          await actions.setSelectedClusterId(nextCluster.id);
        } else {
          // No more clusters
          toast({
            title: "Workflow Complete",
            description: `All clusters have been reviewed for the current filter (${workflowFilter}).`,
          });
          console.log("✅ [ConnectionReview] All clusters reviewed for current filter.");
        }
      } else {
        // Current cluster still has unreviewed edges, find next edge
        console.log(`🔍 [ConnectionReview] Current cluster ${selectedClusterId} still has unreviewed edges. Finding next edge.`);
        actions.selectNextUnreviewedEdge(selectedEdgeId);
      }
    } catch (error) {
      console.error("❌ [ConnectionReview] Error in handleContinueToNext:", error);
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
    isCurrentClusterCompleteForFilter,
    workflowFilter,
    findNextUnreviewedCluster,
    actions,
    toast,
  ]);

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

  // All early returns happen after all hooks are called
  if (!selectedClusterId) {
    return (
      <div className="flex justify-center items-center h-full text-muted-foreground p-4 border rounded-md bg-card shadow">
        Select a cluster to begin reviewing connections.
      </div>
    );
  }
  
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
            To optimize performance, connections are loaded in batches.
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
    return (
      <div className="flex justify-center items-center h-full text-muted-foreground p-4 border rounded-md bg-card shadow">
        Select a connection from the graph to review its details.
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

  if (!node1 || !node2 || !edgeDetails) {
     return (
      <div className="flex justify-center items-center h-full border rounded-md bg-card shadow">
        <Loader2 className="h-6 w-6 animate-spin text-primary mr-2" /> Loading details...
      </div>
    );
  }

  console.log("🙏 currentConnectionData", currentConnectionData);

  // 🔧 ENHANCED: Determine if we should show continue button (optimistic or confirmed)
  const shouldShowContinue = showOptimisticContinue || (!showReviewButtons && !isSubmitting);

  return (
    <Collapsible
      open={isExpanded}
      onOpenChange={setIsExpanded}
      className="h-full flex flex-col"
    >
      <div className="flex justify-between items-center flex-shrink-0 pb-2 border-b">
        <div className="flex items-center gap-2 flex-wrap">
          <h3 className="text-lg font-medium">{nodeLabel} Connection Review</h3>
          {isSubmitting && <Loader2 className="h-4 w-4 animate-spin text-muted-foreground" />}
          
          {/* Cross-source connection indicator */}
          {isCrossSourceConnection && (
            <Badge variant="outline" className="bg-green-50 text-green-700 border-green-200">
              <GitBranch className="h-3 w-3 mr-1" />
              Cross-Source
            </Badge>
          )}
          
          {/* Workflow filter indicator when active */}
          {workflowFilter === "cross-source-only" && (
            <Badge variant="outline" className="bg-blue-50 text-blue-700 border-blue-200">
              <FilterIcon className="h-3 w-3 mr-1" />
              Filtered View
            </Badge>
          )}
          
          {/* Show dependent services indicator when enabled */}
          {disconnectDependentServicesEnabled && resolutionMode === 'entity' && (
            <Badge variant="outline" className="bg-blue-50 text-blue-700 border-blue-200">
              <Link className="h-3 w-3 mr-1" />
              Auto-disconnect Services
            </Badge>
          )}
          
          {/* UPDATED: Badge logic based on new flags */}
          {isClusterFinalized && (
            <Badge variant="outline" className="bg-purple-100 text-purple-800 border-purple-300">
              <CheckCircle className="h-3 w-3 mr-1" /> Cluster Finalized
            </Badge>
          )}
          
          {!isClusterFinalized && isEdgeReviewed && (
             <Badge
              variant={edgeStatus === "CONFIRMED_MATCH" ? "default" : "secondary"}
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
              Reviewed
            </Badge>
          )}

          {/* 🔧 NEW: Optimistic processing indicator */}
          {showOptimisticContinue && isSubmitting && (
            <Badge variant="outline" className="bg-blue-50 text-blue-700 border-blue-200">
              <Clock className="h-3 w-3 mr-1 animate-pulse" />
              Processing...
            </Badge>
          )}

        </div>
        <div className="flex items-center">
            <Button variant="ghost" size="sm" onClick={() => actions.setIsReviewToolsMaximized(!isReviewToolsMaximized)} title={isReviewToolsMaximized ? "Restore" : "Maximize"}>
                {isReviewToolsMaximized ? <Minimize className="h-4 w-4" /> : <Maximize className="h-4 w-4" />}
                <span className="sr-only">{isReviewToolsMaximized ? "Restore" : "Maximize"}</span>
            </Button>
            <CollapsibleTrigger asChild>
                <Button variant="ghost" size="sm" disabled={isSubmitting || isContinuing}>
                    {isExpanded ? <ChevronUp className="h-4 w-4" /> : <ChevronDown className="h-4 w-4" />}
                    <span className="sr-only">Toggle review panel</span>
                </Button>
            </CollapsibleTrigger>
        </div>
      </div>

      <CollapsibleContent className="flex-1 min-h-0">
        <div className="h-full overflow-y-auto flex flex-col gap-2 pr-2 custom-scrollbar">
          {showReviewButtons ? (
            <div className="flex flex-col gap-2 p-1">
              <div className="flex flex-col">
                {/* Place for future useful notes if necessary */}
              </div>
              <div className="flex justify-center items-center gap-2">
                 <Button variant="outline" size="icon" onClick={handlePreviousUnreviewed} disabled={isSubmitting || isContinuing}>
                    <ChevronLeft className="h-4 w-4" />
                </Button>
                 <span className="text-xs text-muted-foreground whitespace-nowrap px-2 py-1 bg-muted rounded-md">
                    Unreviewed {edgeSelectionInfo.currentUnreviewedEdgeIndexInCluster + 1} of {edgeSelectionInfo.totalUnreviewedEdgesInCluster}
                </span>
                <Button variant="outline" size="icon" onClick={handleNextUnreviewed} disabled={isSubmitting || isContinuing}>
                    <ChevronRight className="h-4 w-4" />
                </Button>
              </div>
              <div className="flex flex-col sm:flex-row justify-between gap-2">
                <Button
                  variant="outline"
                  className="border-red-500 text-red-600 hover:bg-red-50 hover:text-red-700 flex-1"
                  size="sm"
                  onClick={() => handleReviewDecision("REJECTED")}
                  disabled={isSubmitting}
                >
                  <X className="h-4 w-4 mr-1" /> Not a Match
                </Button>
                <Button
                  variant="default"
                  className="bg-green-600 hover:bg-green-700 flex-1"
                  size="sm"
                  onClick={() => handleReviewDecision("ACCEPTED")}
                  disabled={isSubmitting}
                >
                  <Check className="h-4 w-4 mr-1" /> Confirm Match
                </Button>
              </div>
            </div>
          ) : shouldShowContinue ? (
            <div className="space-y-3 p-1">
                <Card className={`border-2 ${
                  showOptimisticContinue 
                    ? "border-blue-200 bg-blue-50"  // Optimistic processing state
                    : isClusterFinalized 
                      ? "border-purple-200 bg-purple-50" 
                      : (edgeStatus === "CONFIRMED_MATCH" ? "border-green-200 bg-green-50" : "border-red-200 bg-red-50")
                }`}>
                    <CardContent className="p-3">
                        <div className="flex items-center gap-3">
                            {showOptimisticContinue ? (
                              <Clock className="h-6 w-6 text-blue-600 animate-pulse" />
                            ) : isClusterFinalized ? (
                              <CheckCircle className="h-6 w-6 text-purple-600" />
                            ) : (edgeStatus === "CONFIRMED_MATCH" ? (
                              <CheckCircle className="h-6 w-6 text-green-600" />
                            ) : (
                              <XCircle className="h-6 w-6 text-red-600" />
                            ))}
                            <div>
                                <h4 className="font-semibold text-sm">
                                    {showOptimisticContinue 
                                      ? "Review Submitted" 
                                      : isClusterFinalized 
                                        ? "Cluster Finalized" 
                                        : "Connection Reviewed"}
                                </h4>
                                <p className="text-xs text-muted-foreground">
                                    {showOptimisticContinue 
                                      ? "Processing in background. You can continue reviewing." 
                                      : isClusterFinalized 
                                        ? "All connections in this cluster have been reviewed." 
                                        : "You can continue to the next unreviewed item."}
                                </p>
                            </div>
                        </div>
                    </CardContent>
                </Card>
                
                {/* 🔧 ENHANCED: Continue button with smart navigation */}
                <Button
                  variant="outline"
                  size="sm"
                  onClick={handleContinueToNext}
                  className="w-full"
                  disabled={isContinuing}
                >
                  {isContinuing ? (
                    <Loader2 className="h-4 w-4 mr-1 animate-spin" />
                  ) : (
                    <SkipForward className="h-4 w-4 mr-1" />
                  )}
                  {isCurrentClusterCompleteForFilter 
                    ? `Continue to Next Cluster (${workflowFilter})`
                    : "Continue to Next Connection"
                  }
                </Button>
             </div>
          ) : null}
          
          <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
             <Card>
                <CardHeader className="p-3"><CardTitle className="text-sm">{nodeLabel} 1 Details</CardTitle></CardHeader>
                <CardContent className="p-3 pt-0">
                    <NodeAttributesDisplay node={node1} nodeDetails={node1Details} setIsAttributesOpen={setIsAttributesOpen} isAttributesOpen={isAttributesOpen} />
                </CardContent>
             </Card>
             <Card>
                <CardHeader className="p-3"><CardTitle className="text-sm">{nodeLabel} 2 Details</CardTitle></CardHeader>
                <CardContent className="p-3 pt-0">
                    <NodeAttributesDisplay node={node2} nodeDetails={node2Details} setIsAttributesOpen={setIsAttributesOpen} isAttributesOpen={isAttributesOpen}/>
                </CardContent>
             </Card>
          </div>
          
           <Tabs defaultValue="matching-methods">
            <TabsList className="grid w-full grid-cols-1"><TabsTrigger value="matching-methods">Matching Methods</TabsTrigger></TabsList>
            <TabsContent value="matching-methods" className="space-y-3">
              <Card>
                <CardHeader className="p-3">
                  <CardTitle className="text-sm flex justify-between items-center">
                    <span>Matching Methods</span>
                    <span className="text-xs font-normal">Overall Confidence: <span className="font-medium">{edgeDetails?.edgeWeight?.toFixed(2) ?? "N/A"}</span></span>
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
                          console.log("‼️ Methods:", methods),
                          <div className="space-y-1">
                            {methods.map(
                              (
                                method: {
                                  methodType: string;
                                  preRlConfidence: number;
                                  rlConfidence: number;
                                  combinedConfidence: number;
                                },
                                index: React.Key
                              ) => (
                                <div
                                  key={
                                    method.methodType
                                      ? `${method.methodType}-${index}`
                                      : index
                                  }
                                  className="grid grid-cols-[1fr_auto_auto_auto] gap-2 items-center text-xs"
                                >
                                  <div className="truncate">
                                    {method.methodType?.replace(/_/g, " ") ??
                                      "Unknown Method"}
                                  </div>
                                  <div className="text-right">
                                    <span className="text-xs text-muted-foreground">
                                      Pre-RL:{" "}
                                    </span>
                                    {method.preRlConfidence?.toFixed(2) ??
                                      "N/A"}
                                  </div>
                                  <div className="text-right">
                                    <span className="text-xs text-muted-foreground">
                                      RL:{" "}
                                    </span>
                                    {method.rlConfidence?.toFixed(2) ?? "N/A"}
                                  </div>
                                  <div className="text-right font-medium">
                                    <span className="text-xs text-muted-foreground">
                                      Combined:{" "}
                                    </span>
                                    {method.combinedConfidence?.toFixed(2) ??
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
              {currentConnectionData?.entityGroups?.length && currentConnectionData.entityGroups.length > 0 && (
                <Card>
                  <CardHeader className="p-3">
                    <CardTitle className="text-sm">
                      Underlying Group Details ({currentConnectionData?.entityGroups?.length})
                    </CardTitle>
                  </CardHeader>
                  <CardContent className="p-3 pt-0 space-y-2">
                    {currentConnectionData?.entityGroups?.map((group) => (
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
// components/connection-review-tools.tsx
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
    actions,
    queries,
    edgeSelectionInfo,
    activelyPagingClusterId,
    largeClusterConnectionsPage,
    isLoadingConnectionPageData,
  } = useEntityResolution();
  const { toast } = useToast();

  const [isContinuing, setIsContinuing] = useState(false);
  const [isExpanded, setIsExpanded] = useState(true);
  const [isAttributesOpen, setIsAttributesOpen] = useState(false);

  // Get submission status for the currently selected edge
  const { isSubmitting, error: submissionError } = selectedEdgeId
    ? queries.getEdgeSubmissionStatus(selectedEdgeId)
    : { isSubmitting: false, error: null };

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

      try {
        // ACTION UPDATED: Call the new, simplified submitEdgeReview
        await actions.submitEdgeReview(selectedEdgeId, decision);
      } catch (error) {
        // Error is now handled inside the context, which shows a toast.
        // This catch block is for any unexpected errors bubbling up.
        toast({
          title: "Submission Error",
          description: (error as Error).message,
          variant: "destructive",
        });
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

  const handleContinueToNext = useCallback(async () => {
    if (!selectedClusterId) {
      return;
    }
    setIsContinuing(true);
    try {
      // Check if the current cluster is fully reviewed
      if (queries.getClusterProgress(selectedClusterId).isComplete) {
         await actions.advanceToNextCluster();
      } else {
        // If not, find the next unreviewed edge
        actions.selectNextUnreviewedEdge(selectedEdgeId);
      }
    } catch (error) {
        console.error("Error in handleContinueToNext:", error);
        toast({
            title: "Error",
            description: "Could not proceed with navigation.",
            variant: "destructive",
        });
    }
    finally {
      setTimeout(() => setIsContinuing(false), 300);
    }
  }, [selectedClusterId, selectedEdgeId, actions, queries, toast]);


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
  
  const data = currentConnectionData;
  const edgeDetails = data?.edge as VisualizationEntityEdge | undefined;
  const node1 = data?.entity1;
  const node2 = data?.entity2;

  if (!node1 || !node2 || !edgeDetails) {
     return (
      <div className="flex justify-center items-center h-full border rounded-md bg-card shadow">
        <Loader2 className="h-6 w-6 animate-spin text-primary mr-2" /> Loading details...
      </div>
    );
  }
  
  const node1Details = queries.getNodeDetail(node1.id);
  const node2Details = queries.getNodeDetail(node2.id);

  // UPDATED: Simplified status checks
  const isEdgeReviewed = selectedEdgeId ? queries.isEdgeReviewed(selectedEdgeId) : false;
  const isClusterFinalized = selectedClusterDetails?.wasReviewed || false;
  const showReviewButtons = !isEdgeReviewed && !isClusterFinalized;
  const edgeStatus = selectedEdgeId ? queries.getEdgeStatus(selectedEdgeId) : null;
  const nodeLabel = resolutionMode === 'entity' ? 'Entity' : 'Service';

  return (
    <Collapsible
      open={isExpanded}
      onOpenChange={setIsExpanded}
      className="h-full flex flex-col"
    >
      <div className="flex justify-between items-center flex-shrink-0 pb-2 border-b mb-3">
        <div className="flex items-center gap-2 flex-wrap">
          <h3 className="text-lg font-medium">{nodeLabel} Connection Review</h3>
          {isSubmitting && <Loader2 className="h-4 w-4 animate-spin text-muted-foreground" />}
          
          {/* REMOVED: All queue-related badges ('Processing', 'Queued', 'Failed') */}
          
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
        <div className="h-full overflow-y-auto space-y-3 pr-2 custom-scrollbar">
          {showReviewButtons ? (
            <div className="space-y-3 p-1">
              <p className="text-sm text-muted-foreground">
                Do these records represent the same real-world {resolutionMode}?
              </p>
              <div className="flex justify-center items-center gap-2 mb-3">
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
              {/* REMOVED: Retry submission button for failed batches */}
            </div>
          ) : (
             <div className="space-y-3 p-1">
                <Card className={`border-2 ${isClusterFinalized ? "border-purple-200 bg-purple-50" : (edgeStatus === "CONFIRMED_MATCH" ? "border-green-200 bg-green-50" : "border-red-200 bg-red-50")}`}>
                    <CardContent className="p-3">
                        <div className="flex items-center gap-3">
                            {isClusterFinalized ? <CheckCircle className="h-6 w-6 text-purple-600" /> : (edgeStatus === "CONFIRMED_MATCH" ? <CheckCircle className="h-6 w-6 text-green-600" /> : <XCircle className="h-6 w-6 text-red-600" />)}
                            <div>
                                <h4 className="font-semibold text-sm">
                                    {isClusterFinalized ? "Cluster Finalized" : "Connection Reviewed"}
                                </h4>
                                <p className="text-xs text-muted-foreground">
                                    {isClusterFinalized ? "All connections in this cluster have been reviewed." : "You can continue to the next unreviewed item."}
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
                  disabled={isContinuing || isSubmitting}
                >
                  {isContinuing ? <Loader2 className="h-4 w-4 mr-1 animate-spin" /> : <SkipForward className="h-4 w-4 mr-1" />}
                  Continue
                </Button>
             </div>
          )}
          
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
                                  methodType: string; // Changed from method_type
                                  preRlConfidence: number; // Changed from pre_rl_confidence
                                  rlConfidence: number; // Changed from rl_confidence
                                  combinedConfidence: number; // Changed from combined_confidence
                                },
                                index: React.Key
                              ) => (
                                <div
                                  key={
                                    method.methodType // Changed from method_type
                                      ? `${method.methodType}-${index}` // Changed from method_type
                                      : index
                                  }
                                  className="grid grid-cols-[1fr_auto_auto_auto] gap-2 items-center text-xs"
                                >
                                  <div className="truncate">
                                    {method.methodType?.replace(/_/g, " ") ?? // Changed from method_type
                                      "Unknown Method"}
                                  </div>
                                  <div className="text-right">
                                    <span className="text-xs text-muted-foreground">
                                      Pre-RL:{" "}
                                    </span>
                                    {method.preRlConfidence?.toFixed(2) ?? // Changed from pre_rl_confidence
                                      "N/A"}
                                  </div>
                                  <div className="text-right">
                                    <span className="text-xs text-muted-foreground">
                                      RL:{" "}
                                    </span>
                                    {method.rlConfidence?.toFixed(2) ?? "N/A"} {/* Changed from rl_confidence */}
                                  </div>
                                  <div className="text-right font-medium">
                                    <span className="text-xs text-muted-foreground">
                                      Combined:{" "}
                                    </span>
                                    {method.combinedConfidence?.toFixed(2) ?? // Changed from combined_confidence
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
// components/connection-review-tools.tsx
"use client";

import { useEffect, useState, useCallback } from "react";
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
  ChevronLeft, // Added
  ChevronRight, // Added
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
  BaseNode,
  EntityConnectionDataResponse, // Explicit import for clarity
  ServiceConnectionDataResponse // Explicit import for clarity
} from "@/types/entity-resolution";
// Import the type guards
import {
  isEntityConnectionData,
  isServiceConnectionData,
} from "@/types/entity-resolution";
import { useToast } from "@/hooks/use-toast";

// Helper function to extract contact information
const getContactInfo = (
  groups: Array<EntityGroup | ServiceGroup>,
  nodeId: string,
  mode: "entity" | "service"
) => {
  const contactInfo: {
    email?: string;
    phone?: string;
    address?: string;
    url?: string;
  } = {};

  for (const group of groups) {
    const values = group.matchValues?.values;
    let isNode1 = false;
    let isNode2 = false;

    if (mode === "entity") {
      const eg = group as EntityGroup; 
      isNode1 = eg.entityId1 === nodeId;
      isNode2 = eg.entityId2 === nodeId;
    } else {
      const sg = group as ServiceGroup; 
      isNode1 = sg.serviceId1 === nodeId;
      isNode2 = sg.serviceId2 === nodeId;
    }

    if (!isNode1 && !isNode2) continue;

    switch (group.matchValues?.type?.toLowerCase()) {
      case "email":
        contactInfo.email = isNode1
          ? values?.original_email1 || values?.email1
          : values?.original_email2 || values?.email2;
        break;
      case "phone":
        contactInfo.phone = isNode1
          ? values?.original_phone1 || values?.phone1
          : values?.original_phone2 || values?.phone2;
        break;
      case "address":
        contactInfo.address = isNode1
          ? values?.original_address1 || values?.address1
          : values?.original_address2 || values?.address2;
        break;
      case "url":
        contactInfo.url = isNode1
          ? values?.original_url1 || values?.url1
          : values?.original_url2 || values?.url2;
        break;
    }
  }
  return contactInfo;
};

export default function ConnectionReviewTools() {
  const {
    resolutionMode,
    selectedClusterId, // Added to get cluster details for split status
    selectedEdgeId,
    currentConnectionData, 
    actions,
    queries,
    reviewQueue,
    edgeSelectionInfo, // Added for unreviewed navigation
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

  const currentClusterDetails = selectedClusterId ? queries.getClusterById(selectedClusterId) : null;
  const isClusterSplit = currentClusterDetails?.wasSplit || false;


  useEffect(() => {
    if (
      selectedEdgeId &&
      !currentConnectionData &&
      !queries.isConnectionDataLoading(selectedEdgeId)
    ) {
      actions.loadConnectionData(selectedEdgeId);
    }
  }, [selectedEdgeId, currentConnectionData, queries, actions]);

  useEffect(() => {
    setIsSubmitting(false);
    // Keep panel expanded by default on new edge selection, unless user collapses it manually
    // setIsExpanded(true); 
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
      toast({ title: "Info", description: "This cluster has been split and cannot be reviewed."});
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

  const handleRetryLoad = useCallback(() => {
    if (selectedEdgeId) {
      actions.invalidateConnectionData(selectedEdgeId);
      // loadConnectionData will be called by useEffect if selectedEdgeId is still the same
    }
  }, [selectedEdgeId, actions]);

  const handleSkipToNext = useCallback(() => {
    // This uses the auto-advance version, which might go to the next cluster
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
          description: `Retrying review for connection ${selectedEdgeId.substring(0,8)}...`,
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

  const handlePreviousUnreviewed = useCallback(() => {
    actions.selectPreviousUnreviewedInCluster();
  }, [actions]);

  const handleNextUnreviewed = useCallback(() => {
    actions.selectNextUnreviewedInCluster();
  }, [actions]);


  const isLoading = selectedEdgeId
    ? queries.isConnectionDataLoading(selectedEdgeId)
    : false;
  const error = selectedEdgeId
    ? queries.getConnectionError(selectedEdgeId)
    : null;

  const nodeLabel = resolutionMode === "entity" ? "Entity" : "Service";

  if (!selectedEdgeId && !isLoading) {
    return (
      <div className="flex justify-center items-center h-[100px] text-muted-foreground p-4 border rounded-md bg-card shadow">
        Select a connection from the graph to review its details.
      </div>
    );
  }
  if (isLoading && !currentConnectionData) { // Show loader only if data is truly absent
    return (
      <div className="flex justify-center items-center h-[100px] border rounded-md bg-card shadow">
        <Loader2 className="h-6 w-6 animate-spin text-primary" />
      </div>
    );
  }
  if (error && !currentConnectionData) { // Show error only if data is truly absent
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
          {error && (
            <p className="text-xs mt-1 text-muted-foreground">{error}</p>
          )}
          <div className="flex gap-2 mt-3">
            <Button variant="outline" size="sm" onClick={handleRetryLoad}>
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

  if (!currentConnectionData && selectedEdgeId) {
      if (!isLoading && !error) {
        return (
             <div className="flex justify-center items-center h-[100px] border rounded-md bg-card shadow">
                <Loader2 className="h-6 w-6 animate-spin text-primary mr-2" /> Loading connection data...
            </div>
        )
      }
      return null;
  }


  let edgeDetails:
    | VisualizationEntityEdge
    | VisualizationServiceEdge
    | undefined;
  let groupsForEdge: Array<EntityGroup | ServiceGroup> = []; 
  let node1: Entity | Service | undefined;
  let node2: Entity | Service | undefined;

  // Check if currentConnectionData is non-null before using it with type guards
  if (currentConnectionData) {
    // Assign to a new const to help TypeScript with type narrowing for function arguments
    const data: EntityConnectionDataResponse | ServiceConnectionDataResponse = currentConnectionData;

    if (isEntityConnectionData(data, resolutionMode)) {
      edgeDetails = data.edge;
      groupsForEdge = data.entityGroups; 
      node1 = data.entity1;
      node2 = data.entity2;
    } else if (isServiceConnectionData(data, resolutionMode)) {
      edgeDetails = data.edge;
      groupsForEdge = data.entityGroups; 
      node1 = data.entity1;
      node2 = data.entity2;
    } else {
      // This case implies 'data' is non-null but not one of the expected types.
      // This should ideally not be reached if type guards are comprehensive.
      console.error(
        "Connection data type and resolution mode mismatch or data is of an unexpected non-null type."
      );
      return (
        <Card className="h-full flex flex-col items-center justify-center">
          <CardHeader><CardTitle>Type Error</CardTitle></CardHeader>
          <CardContent><p>Could not determine the specific type of connection data.</p></CardContent>
        </Card>
      );
    }
  }


  if (!node1 || !node2 || !edgeDetails) {
    // This can be reached if currentConnectionData was null and the above block was skipped.
    console.error(
      "Critical data (node1, node2, or edgeDetails) is undefined. SelectedEdgeID:", selectedEdgeId, "HasData:", !!currentConnectionData
    );
     if (isLoading) { 
        return (
            <div className="flex justify-center items-center h-[100px] border rounded-md bg-card shadow">
                <Loader2 className="h-6 w-6 animate-spin text-primary" />
            </div>
        );
    }
    return (
      <Card className="h-full flex flex-col items-center justify-center">
        <CardHeader><CardTitle>Data Error</CardTitle></CardHeader>
        <CardContent><p>Essential connection details are missing or not yet loaded.</p>
         <Button variant="outline" size="sm" onClick={handleRetryLoad} className="mt-2">
              <RefreshCw className="h-4 w-4 mr-1" /> Retry Load
            </Button>
        </CardContent>
      </Card>
    );
  }

  const node1ContactInfo = getContactInfo(
    groupsForEdge,
    node1.id,
    resolutionMode
  );
  const node2ContactInfo = getContactInfo(
    groupsForEdge,
    node2.id,
    resolutionMode
  );

  const showReviewButtons = (!isEdgeReviewed || queueStatus === "failed") && !isClusterSplit;

  return (
    <Collapsible
      open={isExpanded}
      onOpenChange={setIsExpanded}
      className="h-full flex flex-col"
    >
      <div className="flex justify-between items-center flex-shrink-0 pb-2 border-b mb-3">
        <div className="flex items-center gap-2">
          <h3 className="text-lg font-medium">{nodeLabel} Connection Review</h3>
          {isLoading && <Loader2 className="h-4 w-4 animate-spin text-muted-foreground" />}
          {queueStatus === "processing" && (
            <Badge variant="outline" className="bg-blue-50 text-blue-700 border-blue-300">
              <Loader2 className="h-3 w-3 mr-1 animate-spin" /> Processing
            </Badge>
          )}
          {queueStatus === "pending" && (
            <Badge variant="outline" className="bg-yellow-50 text-yellow-700 border-yellow-300">
              <Info className="h-3 w-3 mr-1" /> Queued
            </Badge>
          )}
          {queueStatus === "failed" && (
            <Badge variant="destructive">
              <AlertTriangle className="h-3 w-3 mr-1" /> Failed
            </Badge>
          )}
          {isEdgeReviewed && !queueStatus && !isClusterSplit && ( // Don't show if cluster is split
            <Badge
              variant={ edgeStatus === "CONFIRMED_MATCH" ? "default" : "secondary" }
              className={`${
                edgeStatus === "CONFIRMED_MATCH"
                  ? "bg-green-100 text-green-800 border-green-300"
                  : "bg-red-100 text-red-800 border-red-300"
              }`}
            >
              {edgeStatus === "CONFIRMED_MATCH" ? ( <CheckCircle className="h-3 w-3 mr-1" /> ) : ( <XCircle className="h-3 w-3 mr-1" /> )}
              {edgeStatus === "CONFIRMED_MATCH" ? "Match" : "Non-Match"}
            </Badge>
          )}
           {isClusterSplit && (
            <Badge variant="outline" className="bg-gray-100 text-gray-700 border-gray-300">
              <Info className="h-3 w-3 mr-1" /> Cluster Split
            </Badge>
          )}
        </div>
        <CollapsibleTrigger asChild>
          <Button variant="ghost" size="sm">
            {isExpanded ? ( <ChevronUp className="h-4 w-4" /> ) : ( <ChevronDown className="h-4 w-4" /> )}
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

              {/* Navigation for unreviewed connections */}
              {edgeSelectionInfo.totalUnreviewedEdgesInCluster > 0 && !isClusterSplit && (
                <div className="flex justify-center items-center gap-2 mb-3">
                  <Button
                    variant="outline"
                    size="icon"
                    onClick={handlePreviousUnreviewed}
                    disabled={
                      isSubmitting ||
                      isLoading ||
                      queueStatus === "processing" ||
                      queueStatus === "pending" ||
                      edgeSelectionInfo.totalUnreviewedEdgesInCluster <= 0 // Disable if no unreviewed edges
                    }
                    aria-label="Previous unreviewed connection"
                  >
                    <ChevronLeft className="h-4 w-4" />
                  </Button>
                  <span className="text-xs text-muted-foreground whitespace-nowrap px-2 py-1 bg-muted rounded-md">
                    {edgeSelectionInfo.currentUnreviewedEdgeIndexInCluster !== -1 && edgeSelectionInfo.totalUnreviewedEdgesInCluster > 0
                      ? `${edgeSelectionInfo.currentUnreviewedEdgeIndexInCluster + 1} of ${edgeSelectionInfo.totalUnreviewedEdgesInCluster} unreviewed`
                      : edgeSelectionInfo.totalUnreviewedEdgesInCluster > 0
                      ? `${edgeSelectionInfo.totalUnreviewedEdgesInCluster} unreviewed`
                      : `No unreviewed`}
                  </span>
                  <Button
                    variant="outline"
                    size="icon"
                    onClick={handleNextUnreviewed}
                    disabled={
                      isSubmitting ||
                      isLoading ||
                      queueStatus === "processing" ||
                      queueStatus === "pending" ||
                      edgeSelectionInfo.totalUnreviewedEdgesInCluster <= 0 // Disable if no unreviewed edges
                    }
                    aria-label="Next unreviewed connection"
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
                    isSubmitting ||
                    isLoading ||
                    queueStatus === "processing" ||
                    queueStatus === "pending" ||
                    isClusterSplit || 
                    !selectedEdgeId // Disable if no edge is selected
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
                    isSubmitting ||
                    isLoading ||
                    queueStatus === "processing" ||
                    queueStatus === "pending" ||
                    isClusterSplit || 
                    !selectedEdgeId 
                  }
                >
                  <Check className="h-4 w-4 mr-1" /> Confirm Match
                </Button>
              </div>
              {queueStatus === "failed" && !isClusterSplit && (
                <Button
                  variant="outline"
                  size="sm"
                  onClick={handleRetryQueueItem}
                  className="w-full mt-2 border-amber-500 text-amber-600 hover:bg-amber-50"
                  disabled={isSubmitting || isLoading}
                >
                  <RotateCcw className="h-4 w-4 mr-1" /> Retry Failed Submission
                </Button>
              )}
            </div>
          ) : ( // This block is for when !showReviewButtons (i.e., edge is reviewed or cluster is split)
            <div className="space-y-3 p-1">
              <Card
                className={`border-2 ${ isClusterSplit ? "border-gray-200 bg-gray-50" :
                  edgeStatus === "CONFIRMED_MATCH"
                    ? "border-green-200 bg-green-50"
                    : "border-red-200 bg-red-50"
                }`}
              >
                <CardContent className="p-3">
                  <div className="flex items-center gap-3">
                    {isClusterSplit ? <Info className="h-6 w-6 text-gray-600" /> :
                     edgeStatus === "CONFIRMED_MATCH" ? ( <CheckCircle className="h-6 w-6 text-green-600" /> ) : ( <XCircle className="h-6 w-6 text-red-600" />)}
                    <div>
                      <h4 className="font-semibold text-sm">
                        {isClusterSplit ? "Cluster Split" :
                         edgeStatus === "CONFIRMED_MATCH"
                          ? "Match Confirmed"
                          : "Non-Match Confirmed"}
                      </h4>
                      <p className="text-xs text-muted-foreground">
                        {isClusterSplit ? "This cluster has been processed and split." : "This connection has been reviewed."}
                      </p>
                    </div>
                  </div>
                </CardContent>
              </Card>
              <Button
                variant="outline"
                size="sm"
                onClick={handleSkipToNext} // This uses auto-advance logic
                className="w-full"
                disabled={isLoading}
              >
                <SkipForward className="h-4 w-4 mr-1" /> Continue to Next Unreviewed/Cluster
              </Button>
            </div>
          )}

          <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
            <Card>
              <CardHeader className="p-3">
                <CardTitle className="text-sm">{nodeLabel} 1</CardTitle>
              </CardHeader>
              <CardContent className="p-3 pt-0">
                <div className="space-y-1 text-xs">
                  <div> <span className="font-medium">Name:</span>{" "} {node1.name || "N/A"} </div>
                  <div> <span className="font-medium">Source:</span>{" "} {node1.source_system || "N/A"}{" "} {node1.source_id && `(${node1.source_id})`} </div>
                  {node1ContactInfo.address && ( <div> <span className="font-medium">Address:</span>{" "} {node1ContactInfo.address} </div> )}
                  {node1ContactInfo.phone && ( <div> <span className="font-medium">Phone:</span>{" "} {node1ContactInfo.phone} </div> )}
                  {node1ContactInfo.email && ( <div> <span className="font-medium">Email:</span>{" "} {node1ContactInfo.email} </div> )}
                  {node1ContactInfo.url && ( <div> <span className="font-medium">URL:</span>{" "} <a href={node1ContactInfo.url} target="_blank" rel="noopener noreferrer" className="text-blue-600 hover:underline break-all" > {node1ContactInfo.url} </a> </div> )}
                </div>
              </CardContent>
            </Card>
            <Card>
              <CardHeader className="p-3">
                <CardTitle className="text-sm">{nodeLabel} 2</CardTitle>
              </CardHeader>
              <CardContent className="p-3 pt-0">
                <div className="space-y-1 text-xs">
                  <div> <span className="font-medium">Name:</span>{" "} {node2.name || "N/A"} </div>
                  <div> <span className="font-medium">Source:</span>{" "} {node2.source_system || "N/A"}{" "} {node2.source_id && `(${node2.source_id})`} </div>
                  {node2ContactInfo.address && ( <div> <span className="font-medium">Address:</span>{" "} {node2ContactInfo.address} </div> )}
                  {node2ContactInfo.phone && ( <div> <span className="font-medium">Phone:</span>{" "} {node2ContactInfo.phone} </div> )}
                  {node2ContactInfo.email && ( <div> <span className="font-medium">Email:</span>{" "} {node2ContactInfo.email} </div> )}
                  {node2ContactInfo.url && ( <div> <span className="font-medium">URL:</span>{" "} <a href={node2ContactInfo.url} target="_blank" rel="noopener noreferrer" className="text-blue-600 hover:underline break-all" > {node2ContactInfo.url} </a> </div> )}
                </div>
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
                  {resolutionMode === "entity" &&
                  currentConnectionData && // ensure data is non-null before type guard
                  isEntityConnectionData(
                    currentConnectionData as EntityConnectionDataResponse | ServiceConnectionDataResponse, // Cast here after null check
                    resolutionMode
                  ) ? ( // Type guard will refine currentConnectionData
                    ((currentConnectionData as EntityConnectionDataResponse).edge as VisualizationEntityEdge).details?.methods &&
                    ((currentConnectionData as EntityConnectionDataResponse).edge as VisualizationEntityEdge).details!.methods
                      .length > 0 ? (
                      <div className="space-y-1">
                        {(
                          ((currentConnectionData as EntityConnectionDataResponse).edge as VisualizationEntityEdge)
                        ).details!.methods.map((method, index) => (
                          <div
                            key={
                              method.method_type
                                ? `${method.method_type}-${index}`
                                : index
                            }
                            className="grid grid-cols-[1fr_auto_auto_auto] gap-2 items-center text-xs"
                          >
                            <div>
                              {method.method_type?.replace(/_/g, " ") ??
                                "Unknown Method"}
                            </div>
                            <div className="text-right">
                              <span className="text-xs text-muted-foreground">
                                Pre-RL:
                              </span>{" "}
                              {method.pre_rl_confidence?.toFixed(2) ?? "N/A"}
                            </div>
                            <div className="text-right">
                              <span className="text-xs text-muted-foreground">
                                RL:
                              </span>{" "}
                              {method.rl_confidence?.toFixed(2) ?? "N/A"}
                            </div>
                            <div className="text-right font-medium">
                              <span className="text-xs text-muted-foreground">
                                Combined:
                              </span>{" "}
                              {method.combined_confidence?.toFixed(2) ?? "N/A"}
                            </div>
                          </div>
                        ))}
                      </div>
                    ) : (currentConnectionData as EntityConnectionDataResponse).entityGroups.length > 0 ? ( 
                      <div className="space-y-1">
                        {((currentConnectionData as EntityConnectionDataResponse).entityGroups as EntityGroup[]).map(
                          (group, index) => (
                            <div
                              key={group.id || index}
                              className="grid grid-cols-[1fr_auto_auto] gap-2 items-center text-xs" 
                            >
                              <div>
                                {group.methodType?.replace(/_/g, " ") ??
                                  "Unknown Group Method"}
                              </div>
                              <div className="text-right">
                                <span className="text-xs text-muted-foreground">
                                  Pre-RL:
                                </span>{" "}
                                {group.pre_rl_confidence?.toFixed(2) ?? "N/A"}
                              </div>
                              <div className="text-right font-medium">
                              <span className="text-xs text-muted-foreground">
                                Combined:
                              </span>{" "}
                                {group.rl_confidence?.toFixed(2) ?? "N/A"}
                              </div>
                            </div>
                          )
                        )}
                      </div>
                    ) : (
                      <p className="text-xs text-muted-foreground">
                        No detailed matching methods or entity groups available
                        for this connection.
                      </p>
                    )
                  ) : resolutionMode === "service" &&
                    currentConnectionData && // ensure data is non-null
                    isServiceConnectionData(
                      currentConnectionData as EntityConnectionDataResponse | ServiceConnectionDataResponse, // Cast here
                      resolutionMode
                    ) ? ( // Type guard refines
                    (currentConnectionData as ServiceConnectionDataResponse).edge?.details ? (
                      <pre className="text-xs bg-muted p-2 rounded-md overflow-x-auto">
                        {JSON.stringify((currentConnectionData as ServiceConnectionDataResponse).edge.details, null, 2)}
                      </pre>
                    ) : (
                      <p className="text-xs text-muted-foreground">
                        No specific matching methods detailed for this service
                        connection.
                      </p>
                    )
                  ) : (
                    <p className="text-xs text-muted-foreground">
                      Matching method details unavailable or mode/data mismatch.
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
                          <span className="text-xs font-medium capitalize">
                            {group.methodType.replace(/_/g, " ")} Match Group
                          </span>
                          <div className="flex gap-2 items-center">
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
                              {group.rl_confidence
                                ? group.rl_confidence.toFixed(3)
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
                                  :
                                </span>{" "}
                                {String(val)}
                              </div>
                            ))
                            .slice(0, 3)}{" "}
                          {/* Show first 3 match values */}
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

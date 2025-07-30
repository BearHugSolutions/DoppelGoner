// components/connection-review-tools/connection-review-tools.tsx - ENHANCED: Full Audit Mode Implementation with Paginated Connections
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
  Bot,
  Eye,
  Plus,
} from "lucide-react";
import type {
  VisualizationEntityEdge,
  GroupReviewDecision,
  EntityConnectionDataResponse,
} from "@/types/entity-resolution";
import { useToast } from "@/hooks/use-toast";
import NodeAttributesDisplay from "./node-attribute-display";
import AuditDecisionCard from "../audit/audit-decision-card"; // NEW: Import the audit decision card

const LARGE_CLUSTER_THRESHOLD = 200;
const CONNECTION_PAGE_SIZE = 200;

export default function ConnectionReviewTools() {
  const {
    resolutionMode,
    selectedClusterId,
    selectedEdgeId,
    auditMode,
    postProcessingFilter,
    currentConnectionData,
    currentVisualizationData,
    selectedClusterDetails,
    isReviewToolsMaximized,
    disconnectDependentServicesEnabled,
    workflowFilter,
    clusters,
    postProcessingAuditData, // NEW: Access to audit data
    clustersWithAuditData, // ✨ NEW: Access to clusters with audit data
    reviewerId, // ✅ FIX: Destructure reviewerId
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
  const [showOptimisticContinue, setShowOptimisticContinue] = useState(false);

  // Extract data early and use in useMemo with proper fallbacks
  const data = currentConnectionData;
  const edgeDetails = data?.edge as VisualizationEntityEdge | undefined;
  const node1 = data?.entity1;
  const node2 = data?.entity2;

  // NEW: Get audit decisions for current edge
  const edgeAuditDecisions = useMemo(() => {
    if (!selectedEdgeId || !postProcessingAuditData?.data?.decisions) return [];

    return postProcessingAuditData.data.decisions.filter(
      (decision) => decision.edgeId === selectedEdgeId
    );
  }, [selectedEdgeId, postProcessingAuditData]);

  // All useMemo hooks at the top with proper fallbacks
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

  // Get pagination state for the selected cluster
  const { hasMore, isLoadingMore } = useMemo(() => {
    return selectedClusterId
      ? queries.getClusterPaginationState(selectedClusterId)
      : { hasMore: false, isLoadingMore: false };
  }, [selectedClusterId, queries]);

  const { isSubmitting, error: submissionError } = useMemo(() => {
    return selectedEdgeId
      ? queries.getEdgeSubmissionStatus(selectedEdgeId)
      : { isSubmitting: false, error: null };
  }, [selectedEdgeId, queries]);

  const isLoadingUI = useMemo(() => {
    return selectedEdgeId
      ? queries.isConnectionDataLoading(selectedEdgeId)
      : false;
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

  const isCurrentClusterCompleteForFilter = useMemo(() => {
    if (!selectedClusterId) return false;
    return queries.isClusterCompleteForCurrentFilter(selectedClusterId);
  }, [selectedClusterId, queries]);

  const edgeStatus = useMemo(() => {
    return selectedEdgeId ? queries.getEdgeStatus(selectedEdgeId) : null;
  }, [selectedEdgeId, queries]);

  const nodeLabel = useMemo(() => {
    return resolutionMode === "entity" ? "Entity" : "Service";
  }, [resolutionMode]);

  const findNextUnreviewedCluster = useCallback(() => {
    return queries.findNextViableCluster(selectedClusterId || undefined);
  }, [selectedClusterId, queries]);

  // FIX: This useEffect is redundant as the logic is (and should be) handled in the workflow context.
  // By removing it, we eliminate the "duplicate request" logs and centralize data loading logic.
  /*
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
  */

  useEffect(() => {
    setShowOptimisticContinue(false);
  }, [selectedEdgeId]);

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

      const isReReview = isEdgeReviewed;

      if (isReReview) {
        const currentDecision =
          edgeStatus === "CONFIRMED_MATCH" ? "Match" : "Not a Match";
        const newDecision = decision === "ACCEPTED" ? "Match" : "Not a Match";

        if (
          (decision === "ACCEPTED" && edgeStatus === "CONFIRMED_MATCH") ||
          (decision === "REJECTED" && edgeStatus === "CONFIRMED_NON_MATCH")
        ) {
          toast({
            title: "Same Decision",
            description: "This connection already has that decision.",
          });
          return;
        }

        const confirmMessage = selectedClusterDetails?.wasReviewed
          ? `Change decision from "${currentDecision}" to "${newDecision}"?\n\nNote: This cluster was previously finalized but you can still modify individual connection decisions.`
          : `Change decision from "${currentDecision}" to "${newDecision}"?\n\nThis will update your previous review.`;

        const confirmed = window.confirm(confirmMessage);
        if (!confirmed) return;
      }

      if (isSubmitting) {
        toast({
          title: "In Progress",
          description: "Submission is already in progress.",
        });
        return;
      }

      setShowOptimisticContinue(true);

      try {
        await actions.submitEdgeReview(selectedEdgeId, decision);

        if (isReReview) {
          const reviewType = selectedClusterDetails?.wasReviewed
            ? "finalized cluster"
            : "connection";
          toast({
            title: "Decision Updated",
            description: `Successfully changed decision to "${
              decision === "ACCEPTED" ? "Match" : "Not a Match"
            }" for ${reviewType}.`,
          });
        }
      } catch (error) {
        setShowOptimisticContinue(false);
      }
    },
    [
      selectedEdgeId,
      selectedClusterId,
      isEdgeReviewed,
      edgeStatus,
      toast,
      selectedClusterDetails?.wasReviewed,
      actions,
      isSubmitting,
    ]
  );

  const handleContinueToNext = useCallback(async () => {
    if (!selectedClusterId) {
      return;
    }

    setIsContinuing(true);
    setShowOptimisticContinue(false);

    try {
      if (isCurrentClusterCompleteForFilter) {
        const nextCluster = findNextUnreviewedCluster();

        if (nextCluster === "next_page") {
          await actions.loadClusterProgress(clusters.page + 1, clusters.limit);
        } else if (nextCluster) {
          await actions.setSelectedClusterId(nextCluster.id);
        } else {
          toast({
            title: "Workflow Complete",
            description: `All clusters have been reviewed for the current filter (${workflowFilter}).`,
          });
        }
      } else {
        actions.selectNextUnreviewedEdge(selectedEdgeId);
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
    isCurrentClusterCompleteForFilter,
    workflowFilter,
    findNextUnreviewedCluster,
    actions,
    toast,
    clusters.page,
    clusters.limit,
  ]);

  const handleInitializeLargeClusterPaging = useCallback(async () => {
    if (selectedClusterId) {
      await actions.initializeLargeClusterConnectionPaging(selectedClusterId);
    }
  }, [selectedClusterId, actions]);

  // NEW: Load more connections handler (from refactoring plan)
  const handleLoadMore = useCallback(async () => {
    if (selectedClusterId) {
      await actions.loadMoreConnectionsForCluster(selectedClusterId);
    }
  }, [selectedClusterId, actions]);

  const handlePreviousUnreviewed = useCallback(() => {
    actions.selectPreviousUnreviewedInCluster();
  }, [actions]);

  const handleNextUnreviewed = useCallback(() => {
    actions.selectNextUnreviewedInCluster();
  }, [actions]);

  // NEW: Handle marking individual audit decisions as reviewed
  const handleSingleMarkReviewed = useCallback(
    async (decisionId: string) => {
      // ✅ FIX: Use reviewerId from context instead of getNodeDetail
      if (!reviewerId) {
        toast({
          title: "Authentication Error",
          description: "User ID not found. Please ensure you are logged in.",
          variant: "destructive",
        });
        return;
      }

      try {
        await actions.bulkMarkDecisionsReviewed([decisionId], reviewerId);
        toast({
          title: "Decision Marked as Reviewed",
          description: "The automated decision has been marked as reviewed.",
        });
      } catch (error) {
        toast({
          title: "Error",
          description: "Failed to mark decision as reviewed.",
          variant: "destructive",
        });
      }
    },
    [actions, reviewerId, toast]
  ); // ✅ FIX: Add reviewerId to dependency array

  // ====================================================================
  // AUDIT MODE UI - Full Implementation with Empty State Handling
  // ====================================================================
  if (auditMode === "post_processing_audit") {
    // ✨ NEW: Check if we have any valid clusters
    const hasValidClusters =
      (clustersWithAuditData?.data?.clusters?.length ?? 0) > 0;

    if (!hasValidClusters && !queries.isClustersWithAuditDataLoading()) {
      return (
        <div className="h-full flex flex-col">
          <div className="flex justify-between items-center flex-shrink-0 pb-2 border-b">
            <h3 className="text-lg font-medium flex items-center gap-2">
              <Bot className="h-5 w-5 text-blue-600" />
              Audit Review Mode
            </h3>
            <p className="text-xs text-muted-foreground">
              Reminder: These are already reviewed. Your review would override
              the automated decision.
            </p>
          </div>

          <div className="flex-1 flex items-center justify-center">
            <Card className="max-w-md">
              <CardContent className="p-6 text-center">
                <Bot className="h-12 w-12 text-blue-600 mx-auto mb-4" />
                <h4 className="font-medium mb-2">No Reviewable Audit Data</h4>
                <p className="text-sm text-muted-foreground mb-4">
                  All automated decisions have been reviewed or the clusters no
                  longer contain active data.
                </p>
                <Button
                  variant="outline"
                  onClick={() => actions.setAuditMode("normal")}
                >
                  <Eye className="h-4 w-4 mr-1" />
                  Return to Manual Review
                </Button>
              </CardContent>
            </Card>
          </div>
        </div>
      );
    }

    return (
      <div className="h-full flex flex-col">
        <div className="flex justify-between items-center flex-shrink-0 pb-2 border-b">
          <div className="flex items-center gap-2 sm:gap-4 flex-wrap">
            <h3 className="text-lg font-medium flex flex-col sm:flex-row items-center gap-2">
              <Bot className="h-5 w-5 text-blue-600" />
              Audit Review Mode
            </h3>
            <p className="text-xs text-muted-foreground">
              Reminder: These are already reviewed. Your review would override
              the automated decision.
            </p>
          </div>

          <div className="flex items-center gap-2">
            {postProcessingFilter && (
              <Badge
                variant="outline"
                className="bg-blue-50 text-blue-700 border-blue-200"
              >
                {queries.getAuditFilterDisplayName()}
              </Badge>
            )}
          </div>

          <div className="flex items-center gap-2">
            <Button
              variant="outline"
              size="sm"
              onClick={() => actions.setAuditMode("normal")}
              title="Switch to manual review mode"
            >
              <Eye className="h-4 w-4 mr-1" />
              Manual Review
            </Button>

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
            </Button>
          </div>
        </div>

        <div className="flex-1 min-h-0 space-y-4 mt-4 overflow-y-auto">
          {/* Manual Review Section - Same as normal mode */}
          {currentConnectionData && (
            <div className="flex flex-col gap-2 p-3 border rounded-lg bg-card">
              <h4 className="font-medium text-sm">Manual Review</h4>
              <p className="text-xs text-muted-foreground">
                Review this connection manually. This will override any
                automated decisions.
              </p>

              <div className="flex justify-center items-center gap-2 mb-2">
                <Button
                  variant="outline"
                  size="icon"
                  onClick={handlePreviousUnreviewed}
                  disabled={isSubmitting || isContinuing}
                >
                  <ChevronLeft className="h-4 w-4" />
                </Button>
                <span className="text-xs text-muted-foreground whitespace-nowrap px-2 py-1 bg-muted rounded-md">
                  {auditMode === "post_processing_audit"
                    ? `Decision ${
                        edgeSelectionInfo.currentUnreviewedEdgeIndexInCluster +
                        1
                      } of ${edgeSelectionInfo.totalUnreviewedEdgesInCluster}`
                    : `Unreviewed ${
                        edgeSelectionInfo.currentUnreviewedEdgeIndexInCluster +
                        1
                      } of ${edgeSelectionInfo.totalUnreviewedEdgesInCluster}`}
                </span>
                <Button
                  variant="outline"
                  size="icon"
                  onClick={handleNextUnreviewed}
                  disabled={isSubmitting || isContinuing}
                >
                  <ChevronRight className="h-4 w-4" />
                </Button>
              </div>

              <div className="flex gap-2">
                <Button
                  variant="outline"
                  className="border-red-500 text-red-600 hover:bg-red-50 flex-1"
                  onClick={() => handleReviewDecision("REJECTED")}
                  disabled={isSubmitting}
                >
                  <X className="h-4 w-4 mr-1" /> Not a Match
                </Button>
                <Button
                  variant="default"
                  className="bg-green-600 hover:bg-green-700 flex-1"
                  onClick={() => handleReviewDecision("ACCEPTED")}
                  disabled={isSubmitting}
                >
                  <Check className="h-4 w-4 mr-1" /> Confirm Match
                </Button>
              </div>
            </div>
          )}

          {/* Connection Details - Same as normal mode */}
          {currentConnectionData && (
            <Card>
              <CardHeader>
                <CardTitle className="text-base">Connection Details</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                  <Card>
                    <CardHeader className="p-3">
                      <CardTitle className="text-sm">{nodeLabel} 1</CardTitle>
                    </CardHeader>
                    <CardContent className="p-3 pt-0">
                      <NodeAttributesDisplay
                        node={node1!}
                        nodeDetails={node1Details}
                        isAttributesOpen={isAttributesOpen}
                        setIsAttributesOpen={setIsAttributesOpen}
                      />
                    </CardContent>
                  </Card>
                  <Card>
                    <CardHeader className="p-3">
                      <CardTitle className="text-sm">{nodeLabel} 2</CardTitle>
                    </CardHeader>
                    <CardContent className="p-3 pt-0">
                      <NodeAttributesDisplay
                        node={node2!}
                        nodeDetails={node2Details}
                        isAttributesOpen={isAttributesOpen}
                        setIsAttributesOpen={setIsAttributesOpen}
                      />
                    </CardContent>
                  </Card>
                </div>
              </CardContent>
            </Card>
          )}

          {/* Audit Decisions Section - NEW */}
          {edgeAuditDecisions.length > 0 && (
            <Card>
              <CardHeader>
                <CardTitle className="text-base flex items-center gap-2">
                  <Bot className="h-4 w-4 text-blue-600" />
                  Automated Decisions ({edgeAuditDecisions.length})
                </CardTitle>
              </CardHeader>
              <CardContent>
                <div className="space-y-3">
                  {edgeAuditDecisions.map((decision) => (
                    <AuditDecisionCard
                      key={decision.id}
                      decision={decision}
                      onMarkReviewed={handleSingleMarkReviewed}
                    />
                  ))}
                </div>
                <div className="mt-3 p-2 bg-blue-50 rounded text-sm text-blue-800">
                  <Info className="h-4 w-4 inline mr-1" />
                  Manual review will automatically mark these decisions as
                  reviewed.
                </div>
              </CardContent>
            </Card>
          )}

          {/* Matching Methods Section - Same as normal mode */}
          {currentConnectionData && (
            <Card>
              <CardHeader>
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
              <CardContent>
                {(() => {
                  const edge =
                    currentConnectionData.edge as VisualizationEntityEdge;
                  const methods = edge.details?.methods;
                  const groups = currentConnectionData.entityGroups;

                  if (methods && Array.isArray(methods) && methods.length > 0) {
                    return (
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
                                {method.preRlConfidence?.toFixed(2) ?? "N/A"}
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
                                {method.combinedConfidence?.toFixed(2) ?? "N/A"}
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
                              {group.preRlConfidenceScore?.toFixed(2) ?? "N/A"}
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
                        No detailed matching methods or entity groups available
                        for this connection.
                      </p>
                    );
                  }
                })()}
              </CardContent>
            </Card>
          )}

          {!selectedEdgeId && (
            <div className="flex items-center justify-center h-32 text-muted-foreground">
              Select a connection from the graph to review automated decisions
            </div>
          )}
        </div>
      </div>
    );
  }

  // ====================================================================
  // NORMAL MODE UI - Enhanced with Pagination Support
  // ====================================================================
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

  if (!selectedEdgeId) {
    return (
      <div className="flex flex-col justify-center items-center h-full text-muted-foreground p-4 border rounded-md bg-card shadow">
        <p>Select a connection from the graph to review.</p>
        {/* NEW: Load More Connections button when hasMore is true */}
        {hasMore && (
          <Button
            onClick={handleLoadMore}
            disabled={isLoadingMore}
            className="mt-4"
          >
            {isLoadingMore ? (
              <Loader2 className="h-4 w-4 mr-2 animate-spin" />
            ) : (
              <Plus className="h-4 w-4 mr-2" />
            )}
            Load More Connections ({edgeSelectionInfo.loadedLinksCount} /{" "}
            {edgeSelectionInfo.totalConnectionsInFilter})
          </Button>
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

  if (!node1 || !node2 || !edgeDetails) {
    return (
      <div className="flex justify-center items-center h-full border rounded-md bg-card shadow">
        <Loader2 className="h-6 w-6 animate-spin text-primary mr-2" /> Loading
        details...
      </div>
    );
  }

  const shouldShowContinue =
    showOptimisticContinue || (isEdgeReviewed && !isSubmitting);

  return (
    <Collapsible
      open={isExpanded}
      onOpenChange={setIsExpanded}
      className="h-full flex flex-col"
    >
      <div className="flex justify-between items-center flex-shrink-0 pb-2 border-b">
        <div className="flex items-center gap-2 flex-wrap">
          <h3 className="text-lg font-medium">{nodeLabel} Connection Review</h3>
          {isSubmitting && (
            <Loader2 className="h-4 w-4 animate-spin text-muted-foreground" />
          )}

          {/* NEW: Connection count badge (from refactoring plan) */}
          <Badge variant="outline">
            {edgeSelectionInfo.loadedLinksCount} /{" "}
            {edgeSelectionInfo.totalConnectionsInFilter ?? "..."} Loaded
          </Badge>

          {isCrossSourceConnection && (
            <Badge
              variant="outline"
              className="bg-green-50 text-green-700 border-green-200"
            >
              <GitBranch className="h-3 w-3 mr-1" />
              Cross-Source
            </Badge>
          )}

          {workflowFilter === "cross-source-only" && (
            <Badge
              variant="outline"
              className="bg-blue-50 text-blue-700 border-blue-200"
            >
              <FilterIcon className="h-3 w-3 mr-1" />
              Filtered View
            </Badge>
          )}

          {disconnectDependentServicesEnabled &&
            resolutionMode === "entity" && (
              <Badge
                variant="outline"
                className="bg-blue-50 text-blue-700 border-blue-200"
              >
                <Link className="h-3 w-3 mr-1" />
                Auto-disconnect Services
              </Badge>
            )}

          {isClusterFinalized && (
            <Badge
              variant="outline"
              className="bg-purple-100 text-purple-800 border-purple-300"
            >
              <CheckCircle className="h-3 w-3 mr-1" /> Cluster Finalized
            </Badge>
          )}

          {!isClusterFinalized && isEdgeReviewed && (
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
              Reviewed
            </Badge>
          )}

          {showOptimisticContinue && isSubmitting && (
            <Badge
              variant="outline"
              className="bg-blue-50 text-blue-700 border-blue-200"
            >
              <Clock className="h-3 w-3 mr-1 animate-pulse" />
              Processing...
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
              disabled={isSubmitting || isContinuing}
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
        <div className="h-full overflow-y-auto flex flex-col gap-2 pr-2 custom-scrollbar">
          <div className="flex flex-col gap-2 p-1">
            <div className="flex justify-center items-center gap-2">
              <Button
                variant="outline"
                size="icon"
                onClick={handlePreviousUnreviewed}
                disabled={isSubmitting || isContinuing}
              >
                <ChevronLeft className="h-4 w-4" />
              </Button>
              <span className="text-xs text-muted-foreground whitespace-nowrap px-2 py-1 bg-muted rounded-md">
                {isEdgeReviewed
                  ? "Reviewed Connection"
                  : `Unreviewed ${
                      edgeSelectionInfo.currentUnreviewedEdgeIndexInCluster + 1
                    } of ${edgeSelectionInfo.totalUnreviewedEdgesInCluster}`}
              </span>
              <Button
                variant="outline"
                size="icon"
                onClick={handleNextUnreviewed}
                disabled={isSubmitting || isContinuing}
              >
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

          {shouldShowContinue && (
            <div className="space-y-3 p-1">
              <Card
                className={`border-2 ${
                  showOptimisticContinue
                    ? "border-blue-200 bg-blue-50"
                    : isClusterFinalized
                    ? "border-purple-200 bg-purple-50"
                    : edgeStatus === "CONFIRMED_MATCH"
                    ? "border-green-200 bg-green-50"
                    : "border-red-200 bg-red-50"
                }`}
              >
                <CardContent className="p-3">
                  <div className="flex items-center gap-3">
                    {showOptimisticContinue ? (
                      <Clock className="h-6 w-6 text-blue-600 animate-pulse" />
                    ) : isClusterFinalized ? (
                      <CheckCircle className="h-6 w-6 text-purple-600" />
                    ) : edgeStatus === "CONFIRMED_MATCH" ? (
                      <CheckCircle className="h-6 w-6 text-green-600" />
                    ) : (
                      <XCircle className="h-6 w-6 text-red-600" />
                    )}
                    <div className="flex-1">
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
                          : `Current decision: ${
                              edgeStatus === "CONFIRMED_MATCH"
                                ? "Match"
                                : "Not a Match"
                            }. Use buttons above to change.`}
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
                disabled={isContinuing}
              >
                {isContinuing ? (
                  <Loader2 className="h-4 w-4 mr-1 animate-spin" />
                ) : (
                  <SkipForward className="h-4 w-4 mr-1" />
                )}
                {isCurrentClusterCompleteForFilter
                  ? `Continue to Next Cluster (${workflowFilter})`
                  : "Continue to Next Connection"}
              </Button>
            </div>
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

          {/* NEW: Load More Connections button in review panel (from refactoring plan) */}
          {hasMore && (
            <Button
              onClick={handleLoadMore}
              disabled={isLoadingMore}
              className="w-full"
            >
              {isLoadingMore ? (
                <Loader2 className="h-4 w-4 mr-2 animate-spin" />
              ) : (
                <Plus className="h-4 w-4 mr-2" />
              )}
              Load More Connections
            </Button>
          )}

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
              {currentConnectionData?.entityGroups?.length &&
                currentConnectionData.entityGroups.length > 0 && (
                  <Card>
                    <CardHeader className="p-3">
                      <CardTitle className="text-sm">
                        Underlying Group Details (
                        {currentConnectionData?.entityGroups?.length})
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

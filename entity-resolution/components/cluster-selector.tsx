// components/cluster-selector.tsx - FIXED: Restore pagination visibility
"use client";

import { useCallback, useState, useEffect, Fragment } from "react";
import { useEntityResolution } from "@/context/entity-resolution-context";
import { Card, CardContent } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Checkbox } from "@/components/ui/checkbox";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog";
import {
  CheckCircle,
  ChevronLeft,
  ChevronRight,
  Loader2,
  HelpCircle,
  Filter,
  AlertTriangle,
  GitBranch,
  Layers,
  Info,
  Clock,
  Bot,
  Hand,
} from "lucide-react";
import { Progress } from "@/components/ui/progress";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Badge } from "@/components/ui/badge";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import type {
  EntityCluster,
  ClusterReviewProgress,
  ClusterFilterStatus,
  WorkflowFilter,
} from "@/types/entity-resolution";
import ResolutionModeSwitcher from "./resolution-mode-switcher";
import { useToast } from "@/hooks/use-toast";
import { useAuth } from "@/context/auth-context";
import {
  getOpinionPreferences,
  updateOpinionPreferences,
} from "@/utils/api-client";
import { formatDistanceToNow } from "date-fns";

const LARGE_CLUSTER_THRESHOLD = 200;

const ClusterListContent = ({
  page,
  loading,
}: {
  page: number;
  loading: boolean;
}) => {
  const {
    resolutionMode,
    selectedClusterId,
    clusters,
    clusterProgress,
    workflowFilter,
    actions,
    queries,
    visualizationData,
  } = useEntityResolution();

  const { data: clustersData, error } = clusters;

  const handleClusterSelection = useCallback(
    async (clusterId: string) => {
      if (selectedClusterId !== clusterId) {
        actions.setSelectedClusterId(clusterId, true);
      } else if (!queries.isVisualizationDataLoaded(clusterId)) {
        const clusterDetail = queries.getClusterById(clusterId);
        const connectionCount = clusterDetail ? clusterDetail.groupCount : 0;
        const isLarge =
          connectionCount && connectionCount > LARGE_CLUSTER_THRESHOLD;

        if (!isLarge) {
          actions.invalidateVisualizationData(clusterId);
        }
      }
    },
    [selectedClusterId, actions, queries]
  );

  const getCoherenceColor = (score: number | null) => {
    if (score === null) return "bg-gray-300";
    if (score < 0.4) return "bg-red-500";
    if (score < 0.7) return "bg-yellow-500";
    return "bg-green-500";
  };

  const renderClusterProgressWithFilterInfo = useCallback(
    (cluster: EntityCluster) => {
      const currentProgress = queries.getClusterProgress(cluster.id);
      const totalProgress = queries.getClusterProgressUnfiltered(cluster.id);
      const crossSourceProgress = queries.getClusterProgressCrossSource(
        cluster.id
      );

      const isFiltered = workflowFilter === "cross-source-only";
      const hasServerData = currentProgress.totalEdges !== -1;

      return (
        <div className="mt-1">
          <div className="flex justify-between text-xs mb-0.5 text-muted-foreground">
            <div className="flex items-center gap-1">
              <span>Review Progress</span>
              {isFiltered &&
                crossSourceProgress.totalEdges === 0 &&
                hasServerData && (
                  <TooltipProvider key={`${cluster.id}-no-cross-source`}>
                    <Tooltip>
                      <TooltipTrigger>
                        <Info className="h-3 w-3 text-amber-500" />
                      </TooltipTrigger>
                      <TooltipContent>
                        <p>No cross-source connections in this cluster</p>
                      </TooltipContent>
                    </Tooltip>
                  </TooltipProvider>
                )}
            </div>
            <span className="font-medium text-card-foreground">
              {hasServerData
                ? `${currentProgress.reviewedEdges} / ${currentProgress.totalEdges}`
                : `${currentProgress.reviewedEdges} / ?`}
              {isFiltered &&
                crossSourceProgress.totalEdges === 0 &&
                hasServerData && (
                  <span className="ml-1 text-amber-600">(filtered)</span>
                )}
            </span>
          </div>

          <Progress
            value={
              currentProgress.progressPercentage === -1
                ? 0
                : currentProgress.progressPercentage
            }
            className={`h-1.5 ${
              currentProgress.progressPercentage === -1
                ? "bg-gray-200 [&>div]:bg-gray-400"
                : isFiltered &&
                  crossSourceProgress.totalEdges === 0 &&
                  hasServerData
                ? "bg-amber-100 [&>div]:bg-amber-400"
                : ""
            }`}
          />

          {hasServerData && (
            <div className="mt-1 space-y-0.5">
              <div className="flex justify-between text-xs text-muted-foreground">
                <span>
                  {isFiltered ? "Cross-source decisions" : "All decisions"}
                </span>
                <span>
                  {currentProgress.confirmedMatches} ✔ /{" "}
                  {currentProgress.confirmedNonMatches} X
                </span>
              </div>

              {isFiltered &&
                crossSourceProgress.totalEdges === 0 &&
                hasServerData && (
                  <div className="text-xs text-amber-600 bg-amber-50 px-2 py-1 rounded">
                    No cross-source connections to review
                  </div>
                )}

              {isFiltered &&
                hasServerData &&
                totalProgress.totalEdges > currentProgress.totalEdges && (
                  <div className="flex justify-between text-xs text-muted-foreground/70">
                    <span>Total (all connections):</span>
                    <span>
                      {totalProgress.reviewedEdges} / {totalProgress.totalEdges}
                    </span>
                  </div>
                )}

              {!isFiltered &&
                hasServerData &&
                crossSourceProgress.totalEdges > 0 && (
                  <div className="flex justify-between text-xs text-muted-foreground/70">
                    <span>Cross-source available:</span>
                    <span>
                      {crossSourceProgress.reviewedEdges} /{" "}
                      {crossSourceProgress.totalEdges}
                    </span>
                  </div>
                )}
            </div>
          )}
        </div>
      );
    },
    [queries, workflowFilter]
  );

  const entityLabel = resolutionMode === "entity" ? "Entities" : "Services";
  const groupLabel = "Potential Connections";

  if (loading && clustersData.length === 0) {
    return (
      <div className="flex justify-center items-center flex-grow">
        <Loader2 className="h-8 w-8 animate-spin text-primary" />
      </div>
    );
  }

  return (
    <div className="flex flex-col h-full">
      <div className="space-y-3 flex-grow overflow-y-auto pr-1 custom-scrollbar">
        {error && (
          <div className="text-red-600 text-sm p-2 bg-red-50 rounded border">
            Error: {error}
          </div>
        )}
        {clustersData.length === 0 && !loading && (
          <div className="text-center text-muted-foreground py-10">
            No clusters found for this filter.
          </div>
        )}
        {clustersData.map((cluster: EntityCluster) => {
          const isSelected = selectedClusterId === cluster.id;
          const isLoadingViz = queries.isVisualizationDataLoading(cluster.id);
          const vizForCluster = visualizationData[cluster.id];
          const hasVizDataWithLinks =
            queries.isVisualizationDataLoaded(cluster.id) &&
            vizForCluster?.data?.links?.length !== undefined;
          const vizError = queries.getVisualizationError(cluster.id);

          const progress: ClusterReviewProgress = clusterProgress[
            cluster.id
          ] || {
            totalEdges: -1,
            reviewedEdges: 0,
            progressPercentage: -1,
            isComplete: false,
            pendingEdges: -1,
            confirmedMatches: 0,
            confirmedNonMatches: 0,
          };

          const entityCount = cluster.entityCount;
          const groupCount = cluster.groupCount;
          const isProcessing = progress.isComplete && !cluster.wasReviewed;

          return (
            <Card
              key={cluster.id}
              className={`cursor-pointer transition-all hover:shadow-lg border-l-4 ${
                isSelected
                  ? "ring-2 ring-primary border-primary shadow-md"
                  : isProcessing
                  ? "border-blue-300 bg-blue-50"
                  : "border-transparent hover:border-muted-foreground/30"
              }`}
              onClick={() => handleClusterSelection(cluster.id)}
            >
              <CardContent className="p-3">
                <div className="flex justify-between items-start mb-1.5">
                  <div
                    className="font-semibold text-sm text-card-foreground truncate"
                    title={
                      cluster.name ||
                      `${
                        resolutionMode === "entity" ? "Entity" : "Service"
                      } Cluster ${cluster.id.substring(0, 8)}...`
                    }
                  >
                    {cluster.name ||
                      `${
                        resolutionMode === "entity" ? "Entity" : "Service"
                      } Cluster ${cluster.id.substring(0, 8)}...`}
                  </div>
                  <div className="flex items-center gap-1">
                    {isLoadingViz && (
                      <div
                        className="h-2 w-2 bg-blue-500 rounded-full animate-pulse"
                        title="Loading visualization data..."
                      />
                    )}
                    {hasVizDataWithLinks && !isLoadingViz && (
                      <div
                        className="h-2 w-2 bg-green-500 rounded-full"
                        title="Visualization data loaded"
                      />
                    )}
                    {vizError && (
                      <div
                        className="h-2 w-2 bg-red-500 rounded-full"
                        title={`Error: ${vizError}`}
                      />
                    )}
                    {groupCount &&
                      groupCount > LARGE_CLUSTER_THRESHOLD &&
                      !hasVizDataWithLinks &&
                      !isLoadingViz &&
                      !vizError && (
                        <span title="Large cluster: Load connections to see details.">
                          <HelpCircle className="h-3 w-3 text-amber-500" />
                        </span>
                      )}
                  </div>
                </div>

                <div className="grid grid-cols-2 gap-2 text-xs text-muted-foreground mb-1.5">
                  <div>
                    {entityLabel}:{" "}
                    <span className="font-medium text-card-foreground">
                      {entityCount ?? "?"}
                    </span>
                  </div>
                  <div>
                    {groupLabel}:{" "}
                    <span className="font-medium text-card-foreground">
                      {groupCount ?? "?"}
                    </span>
                  </div>
                </div>

                <div className="flex items-center gap-1.5 mb-2 text-xs">
                  <div className="text-muted-foreground">Coherence:</div>
                  <div
                    className={`h-2.5 w-2.5 rounded-full ${getCoherenceColor(
                      cluster.averageCoherenceScore
                    )}`}
                  ></div>
                  <div className="font-medium text-card-foreground">
                    {cluster.averageCoherenceScore !== null
                      ? (cluster.averageCoherenceScore * 100).toFixed(0) + "%"
                      : "N/A"}
                  </div>
                </div>

                {renderClusterProgressWithFilterInfo(cluster)}

                {cluster.wasReviewed && (
                  <div className="flex items-center mt-1.5 text-green-600 text-xs font-medium">
                    <CheckCircle className="h-3.5 w-3.5 mr-1" />
                    Review Complete
                  </div>
                )}

                {isProcessing && (
                  <div className="flex items-center mt-1.5 text-blue-600 text-xs font-medium">
                    <Clock className="h-3.5 w-3.5 mr-1 animate-pulse" />
                    Finalizing Review...
                  </div>
                )}
              </CardContent>
            </Card>
          );
        })}
      </div>
    </div>
  );
};

const PostProcessingFiltersDialog = () => {
  const { disconnectDependentServicesEnabled, actions } = useEntityResolution();
  const { selectedOpinion } = useAuth();
  const { toast } = useToast();
  const [isOpen, setIsOpen] = useState(false);
  const [tempEnabled, setTempEnabled] = useState(
    disconnectDependentServicesEnabled
  );
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [isLoadingPreferences, setIsLoadingPreferences] = useState(false);

  useEffect(() => {
    if (isOpen && selectedOpinion) {
      setIsLoadingPreferences(true);
      getOpinionPreferences(selectedOpinion)
        .then((response) => {
          setTempEnabled(response.preferences.disconnectDependentServices);
        })
        .catch((error) => {
          toast({
            title: "Warning",
            description:
              "Could not load saved preferences. Using current settings.",
            variant: "destructive",
          });
        })
        .finally(() => {
          setIsLoadingPreferences(false);
        });
    } else if (isOpen) {
      setTempEnabled(disconnectDependentServicesEnabled);
    }
  }, [isOpen, selectedOpinion, disconnectDependentServicesEnabled, toast]);

  const handleConfirm = async () => {
    if (!selectedOpinion) {
      toast({
        title: "Error",
        description:
          "No opinion selected. Please select an opinion to save preferences.",
        variant: "destructive",
      });
      return;
    }

    if (tempEnabled === disconnectDependentServicesEnabled) {
      setIsOpen(false);
      return;
    }

    setIsSubmitting(true);
    try {
      await updateOpinionPreferences(
        { disconnectDependentServices: tempEnabled },
        selectedOpinion
      );

      actions.setDisconnectDependentServicesEnabled(tempEnabled);

      if (tempEnabled && !disconnectDependentServicesEnabled) {
        try {
          await actions.enableDisconnectDependentServices();
        } catch (bulkError) {
          toast({
            title: "Preference Saved with Warning",
            description:
              "Your preference was saved, but bulk processing of historical data failed. Future reviews will use the new setting.",
            variant: "destructive",
          });
        }
      } else {
        toast({
          title: "Preferences Saved",
          description: `Post-processing filters have been updated for opinion: ${selectedOpinion}`,
        });
      }

      setIsOpen(false);
    } catch (error) {
      toast({
        title: "Error Saving Preferences",
        description: `Failed to save preferences: ${(error as Error).message}`,
        variant: "destructive",
      });
      setTempEnabled(disconnectDependentServicesEnabled);
    } finally {
      setIsSubmitting(false);
    }
  };

  const handleCancel = () => {
    setTempEnabled(disconnectDependentServicesEnabled);
    setIsOpen(false);
  };

  const handleOpenChange = (open: boolean) => {
    if (!open && !isSubmitting) {
      handleCancel();
    } else if (open) {
      setIsOpen(true);
    }
  };

  const isConfirmDisabled =
    isSubmitting ||
    isLoadingPreferences ||
    tempEnabled === disconnectDependentServicesEnabled ||
    !selectedOpinion;

  return (
    <Dialog open={isOpen} onOpenChange={handleOpenChange}>
      <DialogTrigger asChild>
        <Button variant="outline" size="sm" className="flex-1 justify-center text-xs h-auto py-2 px-2 max-h-10">
          <Filter className="h-4 w-4 mr-0.5" />
          Post Processing Filters
        </Button>
      </DialogTrigger>
      <DialogContent className="sm:max-w-[600px]">
        <DialogHeader>
          <DialogTitle>Post Processing Filters</DialogTitle>
          <DialogDescription>
            Configure automatic actions that apply to your review decisions.
            {selectedOpinion && (
              <span className="block mt-1 text-xs text-muted-foreground">
                Current opinion: <strong>{selectedOpinion}</strong>
              </span>
            )}
            {!selectedOpinion && (
              <span className="block mt-1 text-xs text-destructive">
                Please select an opinion to configure filters.
              </span>
            )}
          </DialogDescription>
        </DialogHeader>

        <div className="space-y-4 py-4">
          {isLoadingPreferences && (
            <div className="flex items-center justify-center py-4">
              <Loader2 className="h-4 w-4 animate-spin mr-2" />
              <span className="text-sm text-muted-foreground">
                Loading saved preferences...
              </span>
            </div>
          )}

          {!isLoadingPreferences && (
            <div className="flex items-start space-x-3">
              <Checkbox
                id="disconnect-dependent"
                checked={tempEnabled}
                onCheckedChange={(checked) => {
                  setTempEnabled(checked === true);
                }}
                disabled={isSubmitting || !selectedOpinion}
              />
              <div className="grid gap-2 flex-1">
                <label
                  htmlFor="disconnect-dependent"
                  className="text-sm font-medium leading-none peer-disabled:cursor-not-allowed peer-disabled:opacity-70"
                >
                  Disconnect All Dependent Service Matches
                  {disconnectDependentServicesEnabled && (
                    <span className="ml-2 text-xs bg-green-100 text-green-800 px-2 py-1 rounded">
                      Currently Active
                    </span>
                  )}
                  {tempEnabled && !disconnectDependentServicesEnabled && (
                    <span className="ml-2 text-xs bg-amber-100 text-amber-800 px-2 py-1 rounded">
                      Will Enable
                    </span>
                  )}
                </label>
                {!tempEnabled && (
                  <p className="text-sm text-muted-foreground">
                    When enabled, service matches will be automatically
                    disconnected when their parent entities are marked as
                    non-matches.
                  </p>
                )}
              </div>
            </div>
          )}
        </div>

        <DialogFooter>
          <Button
            variant="outline"
            onClick={handleCancel}
            disabled={isSubmitting}
          >
            Cancel
          </Button>
          <Button onClick={handleConfirm} disabled={isConfirmDisabled}>
            {isSubmitting && <Loader2 className="h-4 w-4 mr-2 animate-spin" />}
            {isLoadingPreferences && !isSubmitting && "Loading..."}
            {!isLoadingPreferences && !isSubmitting && (
              <>
                {tempEnabled && !disconnectDependentServicesEnabled
                  ? "Enable & Process History"
                  : tempEnabled === disconnectDependentServicesEnabled
                  ? "No Changes"
                  : "Save Changes"}
              </>
            )}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
};

const WorkflowFilterSelector = () => {
  const { workflowFilter, actions } = useEntityResolution();
  return (
    <div className="space-y-2">
      <div className="grid grid-cols-2 gap-2">
        <Button
          variant={workflowFilter === "all" ? "default" : "outline"}
          onClick={() => actions.setWorkflowFilter("all")}
          size="sm"
          className="flex-1 justify-center text-xs h-auto py-2 px-2"
        >
          <Layers className="h-3 w-2 mr-1" />
          <div className="text-left truncate">All Connections</div>
        </Button>
        <Button
          variant={
            workflowFilter === "cross-source-only" ? "default" : "outline"
          }
          onClick={() => actions.setWorkflowFilter("cross-source-only")}
          size="sm"
          className="flex-1 justify-center text-xs h-auto py-2 px-2 gap-0"
        >
          <GitBranch className="h-3 w-2 mr-1" />
          <div className="text-left truncate">Cross-Source</div>
        </Button>
      </div>
    </div>
  );
};

const AuditModeToggle = () => {
  const { auditMode, clustersWithAuditData, actions } = useEntityResolution();
  const unviewedCount =
    clustersWithAuditData?.data?.auditCounts?.totalUnreviewed || 0;

  return (
    <div className="flex items-center gap-2">
      <Button
        variant={auditMode === "normal" ? "default" : "outline"}
        onClick={() => actions.setAuditMode("normal")}
        size="sm"
        className="flex-1 justify-center text-xs h-auto py-2 px-2"
      >
        <Hand className="h-3 w-3 mr-1" />
        Manual Review
      </Button>
      <Button
        variant={auditMode === "post_processing_audit" ? "default" : "outline"}
        onClick={() => actions.setAuditMode("post_processing_audit")}
        size="sm"
        className="flex-1 justify-center text-xs h-auto py-2 px-2"
      >
        <Bot className="h-3 w-3 mr-1" />
        Audit Mode
        {auditMode === "normal" && unviewedCount > 0 && (
          <Badge
            variant="destructive"
            className="absolute -top-2 -right-2 h-5 min-w-5 p-0 text-xs flex items-center justify-center"
          >
            {unviewedCount}
          </Badge>
        )}
      </Button>
    </div>
  );
};

const PostProcessingFilterSelector = () => {
  const { postProcessingFilter, actions } = useEntityResolution();

  return (
    <div className="space-y-2">
      <label className="text-sm font-medium">Audit Filter</label>
      <div className="flex flex-col sm:flex-row gap-2">
        <Button
          variant={postProcessingFilter === null ? "default" : "outline"}
          onClick={() => actions.setPostProcessingFilter(null)}
          size="sm"
          className="flex-1 justify-center text-xs h-auto py-2 px-2"
        >
          All Decisions
        </Button>
        <Button
          variant={
            postProcessingFilter === "disconnectDependentServices"
              ? "default"
              : "outline"
          }
          onClick={() =>
            actions.setPostProcessingFilter("disconnectDependentServices")
          }
          size="sm"
          className="flex-1 justify-center text-xs h-auto py-2 px-2"
        >
          Dependent Services
        </Button>
      </div>
    </div>
  );
};

const AuditClusterList = () => {
  const { selectedClusterId, actions, queries } = useEntityResolution();
  const { clustersWithAuditData, visualizationData, postProcessingAuditData } =
    useEntityResolution();

  const clusters = clustersWithAuditData?.data?.clusters || [];

  return (
    <div className="space-y-2">
      {clusters.map((cluster) => {
        const vizState = visualizationData[cluster.id];
        const hasVizData = !!vizState?.data;
        const isLoadingViz = !!vizState?.loading;

        const clusterAuditDecisions =
          postProcessingAuditData?.data?.decisions?.filter(
            (d) => d.clusterId === cluster.id
          ) || [];
        const affectedEdgeCount = new Set(
          clusterAuditDecisions.map((d) => d.edgeId)
        ).size;

        return (
          <Card
            key={cluster.id}
            className={`cursor-pointer transition-colors hover:bg-accent ${
              selectedClusterId === cluster.id ? "ring-2 ring-primary" : ""
            }`}
            onClick={() => actions.setSelectedClusterId(cluster.id)}
          >
            <CardContent className="p-3">
              <div className="flex items-center justify-between gap-2">
                <div className="flex items-center gap-2">
                  <Bot className="h-4 w-4 text-blue-600" />
                  <span className="font-medium text-sm">
                    {cluster.entityType === "entity" ? "Entity" : "Service"}{" "}
                    Cluster
                  </span>

                  {isLoadingViz && (
                    <div
                      className="h-2 w-2 bg-blue-500 rounded-full animate-pulse"
                      title="Loading visualization data"
                    />
                  )}
                  {hasVizData && !isLoadingViz && (
                    <div
                      className="h-2 w-2 bg-green-500 rounded-full"
                      title="Rich data loaded"
                    />
                  )}
                </div>

                <div className="flex items-center gap-2">
                  <Badge variant="outline" className="text-xs">
                    {cluster.unreviewedDecisionsCount} unreviewed
                  </Badge>
                </div>
              </div>

              {hasVizData && vizState.data && (
                <div className="mt-2 text-xs text-muted-foreground">
                  <div className="grid grid-cols-2 gap-2">
                    <span>Affected Edges: {affectedEdgeCount}</span>
                    <span>Total Decisions: {cluster.totalDecisionsCount}</span>
                  </div>
                </div>
              )}

              <div className="mt-2 text-sm text-muted-foreground">
                <div className="flex items-center gap-1">
                  <Clock className="h-3 w-3" />
                  <span className="text-xs">
                    Updated:{" "}
                    {formatDistanceToNow(new Date(cluster.lastDecisionAt))} ago
                  </span>
                </div>
              </div>

              {cluster.totalDecisionsCount > 0 && (
                <div className="mt-2">
                  <div className="flex justify-between text-xs text-muted-foreground mb-1">
                    <span>Review Progress</span>
                    <span>
                      {cluster.totalDecisionsCount -
                        cluster.unreviewedDecisionsCount}{" "}
                      / {cluster.totalDecisionsCount}
                    </span>
                  </div>
                  <div className="w-full bg-gray-200 rounded-full h-1.5">
                    <div
                      className="bg-green-600 h-1.5 rounded-full transition-all duration-300"
                      style={{
                        width: `${Math.round(
                          ((cluster.totalDecisionsCount -
                            cluster.unreviewedDecisionsCount) /
                            cluster.totalDecisionsCount) *
                            100
                        )}%`,
                      }}
                    />
                  </div>
                </div>
              )}
            </CardContent>
          </Card>
        );
      })}
    </div>
  );
};

const AuditClusterSection = () => {
  const { auditMode, clustersWithAuditData } = useEntityResolution();

  if (auditMode !== "post_processing_audit") {
    return null;
  }

  return (
    <div className="space-y-4 flex flex-col flex-grow min-h-0">
      <PostProcessingFilterSelector />

      <div className="flex-grow overflow-y-auto pr-1 custom-scrollbar">
        <AuditClusterList />
      </div>
    </div>
  );
};

export default function ClusterSelector() {
  const { resolutionMode, clusterFilterStatus, auditMode, clusters, actions } =
    useEntityResolution();

  const { page, total, limit, loading } = clusters;
  const [pageInput, setPageInput] = useState(page.toString());
  const totalPages = Math.ceil(total / limit);

  useEffect(() => {
    setPageInput(page.toString());
  }, [page]);

  // Load clusters when filter parameters change (but not in audit mode)
  useEffect(() => {
    if (auditMode === "post_processing_audit") {
      return; // Skip in audit mode - data is loaded differently
    }

    console.log("🔄 [ClusterSelector] Loading clusters for filter change:", {
      resolutionMode,
      clusterFilterStatus,
    });

    // Load first page when filters change
    actions.loadClusterProgress(1);
  }, [clusterFilterStatus, resolutionMode, auditMode]);

  const handleValueChange = (value: string) => {
    actions.setClusterFilterStatus(value as ClusterFilterStatus);
  };

  const handlePageChange = useCallback(
    (newPage: number) => {
      if (newPage >= 1 && newPage <= totalPages) {
        console.log(`🔄 [ClusterSelector] Loading page ${newPage}`);
        actions.loadClusterProgress(newPage, limit);
      }
    },
    [actions, totalPages, limit]
  );

  const handlePageInputSubmit = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === "Enter") {
      const newPage = parseInt(pageInput, 10);
      if (!isNaN(newPage) && newPage >= 1 && newPage <= totalPages) {
        handlePageChange(newPage);
      } else {
        setPageInput(page.toString());
      }
      e.currentTarget.blur();
    }
  };

  const handleInputBlur = () => {
    const newPage = parseInt(pageInput, 10);
    if (isNaN(newPage) || newPage < 1 || newPage > totalPages) {
      setPageInput(page.toString());
    }
  };

  return (
    <TooltipProvider>
      <div className="space-y-4 h-full flex flex-col bg-card p-3 rounded-lg shadow">
        <h3 className="text-lg font-semibold text-card-foreground border-b pb-2">
          {resolutionMode === "entity" ? "Entity Clusters" : "Service Clusters"}
        </h3>

        {auditMode === "normal" && (
          <>
            <WorkflowFilterSelector />
            <ResolutionModeSwitcher />
          </>
        )}

        <AuditModeToggle />

        <PostProcessingFiltersDialog />

        <div className="flex flex-col flex-grow min-h-0">
          {auditMode === "normal" ? (
            <>
              <Tabs
                value={clusterFilterStatus}
                onValueChange={handleValueChange}
                className="flex flex-col flex-grow min-h-0"
              >
                <TabsList className="grid w-full grid-cols-2 flex-shrink-0">
                  <TabsTrigger value="unreviewed">Unreviewed</TabsTrigger>
                  <TabsTrigger value="reviewed">Reviewed</TabsTrigger>
                </TabsList>
                <TabsContent
                  value="unreviewed"
                  className="flex-grow flex flex-col mt-2 min-h-0"
                >
                  <ClusterListContent page={page} loading={loading} />
                </TabsContent>
                <TabsContent
                  value="reviewed"
                  className="flex-grow flex flex-col mt-2 min-h-0"
                >
                  <ClusterListContent page={page} loading={loading} />
                </TabsContent>
              </Tabs>

              {/* ✅ FIX: Always show pagination section when conditions are met */}
              <div className="flex justify-between items-center pt-3 border-t flex-shrink-0 mt-2">
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => handlePageChange(page - 1)}
                  disabled={page === 1 || loading}
                  className="text-xs"
                >
                  <ChevronLeft className="h-3 w-3 mr-1" />
                  Prev
                </Button>
                <div className="text-xs text-muted-foreground flex items-center gap-2">
                  Page
                  <Input
                    type="number"
                    value={pageInput}
                    onChange={(e) => setPageInput(e.target.value)}
                    onKeyDown={handlePageInputSubmit}
                    onBlur={handleInputBlur}
                    className="h-8 w-12 text-center"
                    min="1"
                    max={totalPages}
                    disabled={loading}
                  />
                  of {totalPages}
                </div>
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => handlePageChange(page + 1)}
                  disabled={page >= totalPages || loading}
                  className="text-xs"
                >
                  Next
                  <ChevronRight className="h-3 w-3 ml-1" />
                </Button>
              </div>
            </>
          ) : (
            <AuditClusterSection />
          )}
        </div>
      </div>
    </TooltipProvider>
  );
}

// components/cluster-selector.tsx
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
} from "lucide-react";
import { Progress } from "@/components/ui/progress";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import type {
  EntityCluster,
  ClusterReviewProgress,
  ClusterFilterStatus,
} from "@/types/entity-resolution";
import ResolutionModeSwitcher from "./resolution-mode-switcher";
import { useToast } from "@/hooks/use-toast";

const LARGE_CLUSTER_THRESHOLD = 200;

// To avoid duplicating the list rendering logic, it's extracted into its own component.
// This component will be rendered inside each Tab's content.
const ClusterListContent = ({
  page, // Receive page as prop
  loading, // Receive loading as prop
}: {
  page: number;
  loading: boolean;
}) => {
  const {
    resolutionMode,
    selectedClusterId,
    clusters,
    clusterProgress,
    actions,
    queries,
    visualizationData,
  } = useEntityResolution();

  const { data: clustersData, error } = clusters; // Only use data and error from clusters here

  useEffect(() => {
    // This useEffect is no longer needed here as pageInput state is managed higher up
    // setPageInput(page.toString());
  }, [page]); // Keep dependency for completeness if it were used, but it's not anymore

  const handleClusterSelection = useCallback(
    async (clusterId: string) => {
      if (selectedClusterId !== clusterId) {
        actions.setSelectedClusterId(clusterId);
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
    [selectedClusterId, actions, queries, resolutionMode]
  );

  const getCoherenceColor = (score: number | null) => {
    if (score === null) return "bg-gray-300";
    if (score < 0.4) return "bg-red-500";
    if (score < 0.7) return "bg-yellow-500";
    return "bg-green-500";
  };

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
      {/* Parent container for flex column behavior*/}
      <div className="space-y-3 flex-grow overflow-y-auto pr-1 custom-scrollbar">
        {/* Scrollable content area*/}
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

          return (
            <Card
              key={cluster.id}
              className={`cursor-pointer transition-all hover:shadow-lg border-l-4 ${
                isSelected
                  ? "ring-2 ring-primary border-primary shadow-md"
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

                <div className="mt-1">
                  <div className="flex justify-between text-xs mb-0.5 text-muted-foreground">
                    <span>Review Progress</span>
                    <span className="font-medium text-card-foreground">
                      {progress.totalEdges === -1 ||
                      progress.progressPercentage === -1
                        ? `${progress.reviewedEdges} / ?`
                        : `${progress.reviewedEdges} / ${progress.totalEdges}`}
                    </span>
                  </div>
                  <Progress
                    value={
                      progress.progressPercentage === -1
                        ? 0
                        : progress.progressPercentage
                    }
                    className={`h-1.5 ${
                      progress.progressPercentage === -1
                        ? "bg-gray-200 [&>div]:bg-gray-400"
                        : ""
                    }`}
                  />
                </div>

                {progress.isComplete && progress.totalEdges !== -1 && (
                  <div className="flex items-center mt-1.5 text-green-600 text-xs font-medium">
                    <CheckCircle className="h-3.5 w-3.5 mr-1" />
                    Review Complete
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

// NEW: Post Processing Filters Dialog Component
const PostProcessingFiltersDialog = () => {
  const { disconnectDependentServicesEnabled, actions } = useEntityResolution();
  const { toast } = useToast();
  const [isOpen, setIsOpen] = useState(false);
  const [tempEnabled, setTempEnabled] = useState(disconnectDependentServicesEnabled);
  const [isSubmitting, setIsSubmitting] = useState(false);

  // Reset temp state when dialog opens
  useEffect(() => {
    if (isOpen) {
      setTempEnabled(disconnectDependentServicesEnabled);
    }
  }, [isOpen, disconnectDependentServicesEnabled]);

  const handleConfirm = async () => {
    if (tempEnabled && !disconnectDependentServicesEnabled) {
      setIsSubmitting(true);
      try {
        await actions.enableDisconnectDependentServices();
        setIsOpen(false);
      } catch (error) {
        // Error handling is done in the action
      } finally {
        setIsSubmitting(false);
      }
    } else if (!tempEnabled && disconnectDependentServicesEnabled) {
      // Allow disabling without API call (since it's irreversible according to spec)
      actions.setDisconnectDependentServicesEnabled(false);
      setIsOpen(false);
      toast({
        title: "Setting Updated",
        description: "Dependent service disconnection has been disabled for future reviews.",
      });
    } else {
      // No change
      setIsOpen(false);
    }
  };

  const handleCancel = () => {
    setTempEnabled(disconnectDependentServicesEnabled);
    setIsOpen(false);
  };

  return (
    <Dialog open={isOpen} onOpenChange={setIsOpen}>
      <DialogTrigger asChild>
        <Button variant="outline" size="sm" className="w-full mb-2">
          <Filter className="h-4 w-4 mr-2" />
          Post Processing Filters
        </Button>
      </DialogTrigger>
      <DialogContent className="sm:max-w-[600px]">
        <DialogHeader>
          <DialogTitle>Post Processing Filters</DialogTitle>
          <DialogDescription>
            Configure automatic actions that apply to your review decisions.
          </DialogDescription>
        </DialogHeader>
        
        <div className="space-y-4 py-4">
          <div className="flex items-start space-x-3">
            <Checkbox
              id="disconnect-dependent"
              checked={tempEnabled}
              onCheckedChange={(checked) => {
                if (checked === "indeterminate") {
                  setTempEnabled(false);
                } else {
                  setTempEnabled(checked);
                }
              }}
              disabled={isSubmitting || disconnectDependentServicesEnabled}
            />
            <div className="grid gap-2 flex-1">
              <label
                htmlFor="disconnect-dependent"
                className="text-sm font-medium leading-none peer-disabled:cursor-not-allowed peer-disabled:opacity-70"
              >
                Disconnect All Dependent Service Matches
                {disconnectDependentServicesEnabled && (
                  <span className="ml-2 text-xs bg-green-100 text-green-800 px-2 py-1 rounded">
                    Active
                  </span>
                )}
              </label>
              
              {tempEnabled && !disconnectDependentServicesEnabled && (
                <div className="bg-amber-50 border border-amber-200 rounded-md p-3">
                  <div className="flex items-start space-x-2">
                    <AlertTriangle className="h-4 w-4 text-amber-600 mt-0.5 flex-shrink-0" />
                    <div className="text-sm text-amber-800">
                      <p className="font-medium mb-2">Important: This action is irreversible</p>
                      <p className="mb-3">
                        This setting will automatically disconnect service matches when you mark two entities as non-matches. 
                        It applies to all future decisions and will also process your existing review history.
                      </p>
                      
                      <p className="mb-3">
                        <strong>How it works:</strong> When two organizations are marked as different entities, 
                        any service matches between those organizations will also be automatically marked as non-matches.
                      </p>
                      
                      <p className="mb-3">
                        <strong>Example:</strong> If you mark "Salvation Army - Seattle" and "Salvation Army - Bellevue" 
                        as different organizations, but want their identical "Coat Drive" services to remain matched, 
                        you should NOT enable this setting. However, if you want to reduce service management overhead 
                        by ensuring service matches are consistent with entity decisions, enable this setting.
                      </p>
                      
                      <p className="text-xs text-amber-700">
                        Once confirmed, this setting cannot be disabled for this opinion set, and the checkbox will be locked.
                      </p>
                    </div>
                  </div>
                </div>
              )}
              
              {!tempEnabled && (
                <p className="text-sm text-muted-foreground">
                  When enabled, service matches will be automatically disconnected when their parent entities are marked as non-matches.
                </p>
              )}
            </div>
          </div>
        </div>

        <DialogFooter>
          <Button variant="outline" onClick={handleCancel} disabled={isSubmitting}>
            Cancel
          </Button>
          <Button 
            onClick={handleConfirm} 
            disabled={isSubmitting || tempEnabled === disconnectDependentServicesEnabled}
          >
            {isSubmitting && <Loader2 className="h-4 w-4 mr-2 animate-spin" />}
            {tempEnabled && !disconnectDependentServicesEnabled ? "Enable & Process History" : "Confirm"}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
};

export default function ClusterSelector() {
  const { resolutionMode, actions, clusterFilterStatus, clusters } =
    useEntityResolution();

  const { page, total, limit, loading } = clusters;
  const [pageInput, setPageInput] = useState(page.toString());
  const totalPages = Math.ceil(total / limit);

  // Update pageInput when the actual page changes (e.g., after loading new clusters)
  useEffect(() => {
    setPageInput(page.toString());
  }, [page]);

  // This effect triggers a reload of clusters whenever the filter status changes.
  useEffect(() => {
    // When a new tab is selected, fetch the clusters for that tab, resetting to page 1.
    actions.loadClusters(1);
  }, [clusterFilterStatus, actions.loadClusters]);

  const handleValueChange = (value: string) => {
    // This updates the filter status in the global context.
    actions.setClusterFilterStatus(value as ClusterFilterStatus);
  };

  const handlePageChange = useCallback(
    (newPage: number) => {
      if (newPage >= 1 && newPage <= totalPages) {
        actions.loadClusters(newPage, limit);
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
        setPageInput(page.toString()); // Revert to current page if invalid input
      }
      e.currentTarget.blur();
    }
  };

  const handleInputBlur = () => {
    const newPage = parseInt(pageInput, 10);
    if (isNaN(newPage) || newPage < 1 || newPage > totalPages) {
      setPageInput(page.toString()); // Revert to current page if invalid input on blur
    }
  };

  return (
    <div className="space-y-4 h-full flex flex-col bg-card p-3 rounded-lg shadow">
      <h3 className="text-lg font-semibold text-card-foreground border-b pb-2">
        {resolutionMode === "entity" ? "Entity Clusters" : "Service Clusters"}{" "}
        for Review
      </h3>
      <ResolutionModeSwitcher />
      
      {/* NEW: Post Processing Filters Button */}
      <PostProcessingFiltersDialog />
      
      {/* This is the key change: apply flex-grow only to this div */}
      <div className="flex flex-col flex-grow min-h-0">
        <Tabs
          value={clusterFilterStatus}
          onValueChange={handleValueChange}
          className="flex flex-col flex-grow min-h-0"
        >
          <TabsList className="grid w-full grid-cols-2 flex-shrink-0">
            <TabsTrigger value="unreviewed">Unreviewed</TabsTrigger>
            <TabsTrigger value="reviewed">Reviewed</TabsTrigger>
          </TabsList>
          {/* The two TabsContent components will mount/unmount based on the selected tab.
              The list content itself is now a separate component to avoid code duplication. */}
          <TabsContent
            value="unreviewed"
            className="flex-grow flex flex-col mt-2 min-h-0"
          >
            <ClusterListContent page={page} loading={loading} />
          </TabsContent>
          <TabsContent value="reviewed" className="flex-grow flex flex-col mt-2 min-h-0">
            <ClusterListContent page={page} loading={loading} />
          </TabsContent>
        </Tabs>
        {totalPages > 1 && (
          <div className="flex justify-between items-center pt-3 border-t flex-shrink-0 min-h-0">
            <Button
              variant="outline"
              size="sm"
              onClick={() => handlePageChange(page - 1)}
              disabled={page === 1 || loading}
              className="text-xs"
            >
              <ChevronLeft className="h-2 w-2 mr-0.5" />
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
              <ChevronRight className="h-2 w-2 ml-0.5" />
            </Button>
          </div>
        )}
      </div>
    </div>
  );
}
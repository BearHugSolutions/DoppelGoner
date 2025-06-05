// components/cluster-selector.tsx
"use client";

import { useCallback } from "react";
import { useEntityResolution } from "@/context/entity-resolution-context";
import { Card, CardContent } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { CheckCircle, ChevronLeft, ChevronRight, Loader2, HelpCircle, Info } from "lucide-react"; 
import { Progress } from "@/components/ui/progress";
import type { EntityCluster, ClusterReviewProgress } from "@/types/entity-resolution"; 

const LARGE_CLUSTER_THRESHOLD = 200; // Ensure this matches context if used here

export default function ClusterSelector() {
  const {
    resolutionMode, 
    selectedClusterId,
    clusters, 
    clusterProgress, // This is Record<string, ClusterReviewProgress>
    actions,
    queries,
    visualizationData, // Added to solve 'Cannot find name' error
  } = useEntityResolution();

  const handleClusterSelection = useCallback(async (clusterId: string) => {
    if (selectedClusterId !== clusterId) {
      actions.setSelectedClusterId(clusterId);
    } else if (!queries.isVisualizationDataLoaded(clusterId)) {
      const clusterDetail = queries.getClusterById(clusterId);
      const connectionCount = clusterDetail ? clusterDetail.groupCount : 0;
      // Use the constant from context if available, or define locally
      const isLarge = connectionCount && connectionCount > LARGE_CLUSTER_THRESHOLD; 

      if(!isLarge){ 
        actions.invalidateVisualizationData(clusterId);
      }
    }
  }, [selectedClusterId, actions, queries, resolutionMode]);

  const handlePageChange = useCallback((newPage: number) => {
    const { total, limit } = clusters;
    if (newPage >= 1 && newPage <= Math.ceil(total / limit)) {
      actions.loadClusters(newPage, limit); 
    }
  }, [clusters, actions]);

  const getCoherenceColor = (score: number | null) => {
    if (score === null) return "bg-gray-300";
    if (score < 0.4) return "bg-red-500";
    if (score < 0.7) return "bg-yellow-500";
    return "bg-green-500";
  };

  const { data: clustersData, loading, error, page, total, limit } = clusters;

  const entityLabel = resolutionMode === 'entity' ? 'Entities' : 'Services';
  const groupLabel = resolutionMode === 'entity' ? 'Potential Connections' : 'Potential Connections';

  return (
    <div className="space-y-4 h-full flex flex-col bg-card p-3 rounded-lg shadow">
      <h3 className="text-lg font-semibold text-card-foreground border-b pb-2">
        {resolutionMode === 'entity' ? 'Entity Clusters' : 'Service Clusters'} for Review
      </h3>

      {error && (
        <div className="text-red-600 text-sm p-2 bg-red-50 rounded border">
          Error: {error}
        </div>
      )}

      {loading && clustersData.length === 0 ? (
        <div className="flex justify-center items-center flex-grow">
          <Loader2 className="h-8 w-8 animate-spin text-primary" />
        </div>
      ) : (
        <>
          <div className="space-y-3 flex-grow overflow-auto pr-1 custom-scrollbar">
            {clustersData.length === 0 && !loading && (
              <div className="text-center text-muted-foreground py-10">No clusters found.</div>
            )}
            {clustersData.map((cluster) => {
              const isSelected = selectedClusterId === cluster.id;
              const isLoadingViz = queries.isVisualizationDataLoading(cluster.id);
              const vizForCluster = visualizationData[cluster.id]; // Get the specific viz data
              const hasVizDataWithLinks = queries.isVisualizationDataLoaded(cluster.id) && 
                                          vizForCluster?.data?.links?.length !== undefined;
              const vizError = queries.getVisualizationError(cluster.id);
              
              const progress: ClusterReviewProgress = clusterProgress[cluster.id] || {
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
                      <div className="font-semibold text-sm text-card-foreground truncate"
                           title={cluster.name || `${resolutionMode === 'entity' ? 'Entity' : 'Service'} Cluster ${cluster.id.substring(0,8)}...`}>
                        {cluster.name || `${resolutionMode === 'entity' ? 'Entity' : 'Service'} Cluster ${cluster.id.substring(0,8)}...`}
                      </div>
                      <div className="flex items-center gap-1">
                        {isLoadingViz &&
                          <div className="h-2 w-2 bg-blue-500 rounded-full animate-pulse" title="Loading visualization data..." />}
                        {hasVizDataWithLinks && !isLoadingViz &&
                          <div className="h-2 w-2 bg-green-500 rounded-full" title="Visualization data loaded" />}
                        {vizError &&
                          <div className="h-2 w-2 bg-red-500 rounded-full" title={`Error: ${vizError}`} />}
                        {(groupCount && groupCount > LARGE_CLUSTER_THRESHOLD && !hasVizDataWithLinks && !isLoadingViz && !vizError) && (
                          <span title="Large cluster: Load connections to see details.">
                            <HelpCircle className="h-3 w-3 text-amber-500" />
                          </span>
                        )}
                      </div>
                    </div>

                    <div className="grid grid-cols-2 gap-2 text-xs text-muted-foreground mb-1.5">
                      <div>{entityLabel}: <span className="font-medium text-card-foreground">{entityCount ?? '?'}</span></div>
                      <div>{groupLabel}: <span className="font-medium text-card-foreground">{groupCount ?? '?'}</span></div>
                    </div>

                    <div className="flex items-center gap-1.5 mb-2 text-xs">
                      <div className="text-muted-foreground">Coherence:</div>
                      <div className={`h-2.5 w-2.5 rounded-full ${getCoherenceColor(cluster.averageCoherenceScore)}`}></div>
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
                          {progress.totalEdges === -1 || progress.progressPercentage === -1
                            ? `${progress.reviewedEdges} / ?`
                            : `${progress.reviewedEdges} / ${progress.totalEdges}`}
                        </span>
                      </div>
                      <Progress 
                        value={progress.progressPercentage === -1 ? 0 : progress.progressPercentage} 
                        // Removed indicatorClassName, apply conditional background to the main className for the track
                        className={`h-1.5 ${progress.progressPercentage === -1 ? 'bg-gray-200 [&>div]:bg-gray-400' : ''}`} 
                      />
                    </div>

                    {progress.isComplete && progress.totalEdges !== -1 && ( 
                      <div className="flex items-center mt-1.5 text-green-600 text-xs font-medium">
                        <CheckCircle className="h-3.5 w-3.5 mr-1" />
                        Review Complete
                      </div>
                    )}
                     {cluster.wasSplit && (
                       <div className="flex items-center mt-1.5 text-orange-600 text-xs font-medium">
                        <Info className="h-3.5 w-3.5 mr-1" />
                        Cluster Processed (Split)
                      </div>
                    )}
                  </CardContent>
                </Card>
              )
            })}
          </div>

          {total > limit && (
            <div className="flex justify-between items-center pt-3 border-t mt-auto">
              <Button
                variant="outline"
                size="sm"
                onClick={() => handlePageChange(page - 1)}
                disabled={page === 1 || loading}
              >
                <ChevronLeft className="h-4 w-4 mr-1" />
                Prev
              </Button>
              <span className="text-xs text-muted-foreground">
                Page {page} of {Math.ceil(total / limit)}
              </span>
              <Button
                variant="outline"
                size="sm"
                onClick={() => handlePageChange(page + 1)}
                disabled={page >= Math.ceil(total / limit) || loading}
              >
                Next
                <ChevronRight className="h-4 w-4 ml-1" />
              </Button>
            </div>
          )}
        </>
      )}
    </div>
  );
}

"use client"

import { useEffect, useState, useCallback } from "react"
import { useEntityResolution } from "@/context/entity-resolution-context"
import type { Cluster, EntityLink, EntityGroup, VisualizationDataResponse } from "@/types/entity-resolution"
import { Card, CardContent } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { CheckCircle, ChevronLeft, ChevronRight, Loader2 } from "lucide-react"
import { Progress } from "@/components/ui/progress"
import { getClusters, getVisualizationData } from "@/utils/api-client"

export default function ClusterSelector() {
  const {
    selectedClusterId,
    setSelectedClusterId,
    selectedEdgeId, // Current selected edge
    setSelectedEdgeId,
    reviewProgress,
    updateReviewProgress,
    refreshTrigger,
    lastReviewedEdgeId, // From context - NEW
    setLastReviewedEdgeId,  // From context - NEW
    // Assuming setCurrentClusterVisualizationData exists if we want to cache viz data in context
    // setCurrentClusterVisualizationData 
  } = useEntityResolution()

  const [clusters, setClusters] = useState<Cluster[]>([])
  const [total, setTotal] = useState(0)
  const [page, setPage] = useState(1)
  const [loading, setLoading] = useState(true)
  const [isAutoSelectingEdge, setIsAutoSelectingEdge] = useState(false);
  const limit = 10 // Number of clusters per page

  // Memoized function to find and select an appropriate edge in a given cluster
  const findAndSelectEdge = useCallback(async (clusterId: string, attemptAfterEdgeId?: string | null) => {
    if (!clusterId) {
      setSelectedEdgeId(null);
      // setCurrentClusterVisualizationData?.(null); // Clear context viz data if used
      return;
    }
    setIsAutoSelectingEdge(true);
    try {
      const vizData: VisualizationDataResponse = await getVisualizationData(clusterId);
      // setCurrentClusterVisualizationData?.(vizData); // Store full data in context

      const { links: allLinksInCluster, entityGroups: allEntityGroupsInCluster } = vizData;
      let nextUnreviewedEdgeId: string | null = null;
      
      // Determine start index for searching for the next edge
      const startIndex = attemptAfterEdgeId && allLinksInCluster.some(link => link.id === attemptAfterEdgeId)
        ? allLinksInCluster.findIndex(link => link.id === attemptAfterEdgeId) + 1
        : 0;

      // Search from startIndex to the end
      for (let i = startIndex; i < allLinksInCluster.length; i++) {
        const link = allLinksInCluster[i];
        const groupsForThisLink = allEntityGroupsInCluster.filter(group =>
          ((group.entity_id_1 === link.source && group.entity_id_2 === link.target) ||
           (group.entity_id_1 === link.target && group.entity_id_2 === link.source))
        );
        if (groupsForThisLink.some(g => g.confirmed_status === 'PENDING_REVIEW')) {
          nextUnreviewedEdgeId = link.id;
          break;
        }
      }

      // If no edge found after startIndex, and we did skip some, search from the beginning up to startIndex
      if (!nextUnreviewedEdgeId && startIndex > 0) {
        for (let i = 0; i < startIndex -1 ; i++) { // up to (but not including) the original startIndex
          const link = allLinksInCluster[i];
          const groupsForThisLink = allEntityGroupsInCluster.filter(group =>
            ((group.entity_id_1 === link.source && group.entity_id_2 === link.target) ||
             (group.entity_id_1 === link.target && group.entity_id_2 === link.source))
          );
          if (groupsForThisLink.some(g => g.confirmed_status === 'PENDING_REVIEW')) {
            nextUnreviewedEdgeId = link.id;
            break;
          }
        }
      }
      
      setSelectedEdgeId(nextUnreviewedEdgeId); // Set to found edge or null if none

      // Update review progress for the cluster
      if (allLinksInCluster.length > 0) {
        const reviewedCount = allLinksInCluster.filter(link => {
          const groupsForLink = allEntityGroupsInCluster.filter(g =>
            ((g.entity_id_1 === link.source && g.entity_id_2 === link.target) ||
             (g.entity_id_1 === link.target && g.entity_id_2 === link.source))
          );
          // An edge is reviewed if all its groups are not PENDING_REVIEW (and there's at least one group)
          return groupsForLink.length > 0 && groupsForLink.every(g => g.confirmed_status !== 'PENDING_REVIEW');
        }).length;
        const progress = Math.round((reviewedCount / allLinksInCluster.length) * 100);
        updateReviewProgress(clusterId, progress);
      } else {
        // No edges in the cluster, consider it 100% reviewed (or 0% if preferred)
        // If there are entity groups but no visualizable edges, this might need adjustment.
        // For now, no edges means nothing to review via edges.
        updateReviewProgress(clusterId, allEntityGroupsInCluster.length === 0 ? 100 : 0);
      }

    } catch (error) {
      console.error(`Failed to auto-select edge for cluster ${clusterId}:`, error);
      setSelectedEdgeId(null); // Clear edge on error
      // setCurrentClusterVisualizationData?.(null);
    } finally {
      setIsAutoSelectingEdge(false);
      if (attemptAfterEdgeId) { // Reset lastReviewedEdgeId after attempting to find next
        setLastReviewedEdgeId(null);
      }
    }
  }, [setSelectedEdgeId, updateReviewProgress, setLastReviewedEdgeId /*, setCurrentClusterVisualizationData */]);


  // Main effect for loading clusters and handling auto-advancement
  useEffect(() => {
    let isMounted = true;
    async function loadClustersAndManageSelection() {
      if (!isMounted) return;
      setLoading(true);
      try {
        const { clusters: fetchedClusters, total: fetchedTotal } = await getClusters(page, limit);
        if (!isMounted) return;

        setClusters(fetchedClusters);
        setTotal(fetchedTotal);

        let currentTargetClusterId = selectedClusterId;
        let didClusterChangeDueToCompletion = false;
        let initialClusterSelection = false;

        // Scenario 1: No cluster selected, or selected cluster not in current page view. Auto-select first from list.
        if ((!currentTargetClusterId && fetchedClusters.length > 0) ||
            (currentTargetClusterId && !fetchedClusters.some(c => c.id === currentTargetClusterId))) {
          if (fetchedClusters.length > 0) {
            currentTargetClusterId = fetchedClusters[0].id;
            setSelectedClusterId(currentTargetClusterId);
            // setSelectedEdgeId(null); // Clear edge for new cluster, findAndSelectEdge will pick
            setLastReviewedEdgeId(null); // Reset for a new cluster
            initialClusterSelection = true;
          } else {
            setSelectedClusterId(null); // No clusters on this page
            setSelectedEdgeId(null);
            currentTargetClusterId = null;
          }
        }
        
        // Scenario 2: Current cluster is fully reviewed (progress is 100%), try to advance.
        // This relies on reviewProgress being up-to-date.
        if (currentTargetClusterId && reviewProgress[currentTargetClusterId] === 100 && !initialClusterSelection) {
          const currentIndex = fetchedClusters.findIndex((c: Cluster) => c.id === currentTargetClusterId);
          if (currentIndex >= 0) {
            if (currentIndex < fetchedClusters.length - 1) { // Advance to next cluster on the same page
              const nextCluster = fetchedClusters[currentIndex + 1];
              currentTargetClusterId = nextCluster.id;
              setSelectedClusterId(nextCluster.id);
              // setSelectedEdgeId(null);
              setLastReviewedEdgeId(null);
              didClusterChangeDueToCompletion = true;
            } else if (page < Math.ceil(fetchedTotal / limit)) { // Advance to next page
              setPage(prevPage => prevPage + 1);
              // loadClustersAndManageSelection will re-trigger due to page change; exit early.
              if (isMounted) setLoading(false);
              return;
            } else {
              // Last cluster on the last page is fully reviewed. No more clusters to advance to.
            }
          }
        }

        // Scenario 3: A cluster is active. Find and select an appropriate edge.
        if (currentTargetClusterId) {
          // If cluster changed (initial, or due to completion), we want the first unreviewed.
          // If refreshTriggered for the same cluster (lastReviewedEdgeId is set), we want the one after lastReviewedEdgeId.
          await findAndSelectEdge(currentTargetClusterId, (initialClusterSelection || didClusterChangeDueToCompletion) ? null : lastReviewedEdgeId);
        } else {
          setSelectedEdgeId(null); // No active cluster, ensure no edge is selected.
        }

      } catch (error) {
        console.error("Failed to load clusters:", error);
      } finally {
        if (isMounted) setLoading(false);
      }
    }

    loadClustersAndManageSelection();
    return () => { isMounted = false; };
  }, [page, refreshTrigger, reviewProgress, selectedClusterId, setSelectedClusterId, setSelectedEdgeId, findAndSelectEdge, lastReviewedEdgeId, setLastReviewedEdgeId]);
  // `selectedClusterId` is added as a dependency to re-run if it's changed externally (e.g. cleared).
  // `reviewProgress` is a key dependency for auto-advancing clusters.
  // `lastReviewedEdgeId` is a dependency to react to a specific edge being reviewed.

  // Handler for manual cluster selection from the list
  const handleClusterSelection = useCallback(async (clusterId: string) => {
    if (selectedClusterId !== clusterId) {
        setSelectedClusterId(clusterId);
        setSelectedEdgeId(null); // Clear previous edge selection immediately
        setLastReviewedEdgeId(null); // Reset for new cluster
        await findAndSelectEdge(clusterId, null); // Select first unreviewed in new cluster
    } else if (!selectedEdgeId) { 
        // Same cluster clicked again, but no edge is active (e.g., was cleared)
        // Attempt to select the first unreviewed edge again.
        setLastReviewedEdgeId(null); // Ensure we look from the start
        await findAndSelectEdge(clusterId, null);
    }
  }, [selectedClusterId, selectedEdgeId, setSelectedClusterId, setSelectedEdgeId, findAndSelectEdge, setLastReviewedEdgeId]);

  const handlePageChange = (newPage: number) => {
    if (newPage >= 1 && newPage <= Math.ceil(total / limit)) {
      // selectedClusterId will be re-evaluated by the main useEffect when page changes
      // setSelectedClusterId(null); // Optionally clear selection, but effect handles it
      // setSelectedEdgeId(null);
      // setLastReviewedEdgeId(null);
      setPage(newPage);
    }
  };

  const getCoherenceColor = (score: number | null) => {
    if (score === null) return "bg-gray-300";
    if (score < 0.4) return "bg-red-500";
    if (score < 0.7) return "bg-yellow-500";
    return "bg-green-500";
  };

  const getProgressForCluster = (clusterId: string) => {
    return reviewProgress[clusterId] || 0;
  };

  return (
    <div className="space-y-4 h-full flex flex-col bg-card p-3 rounded-lg shadow">
      <h3 className="text-lg font-semibold text-card-foreground border-b pb-2">Clusters for Review</h3>
      {loading && clusters.length === 0 ? (
        <div className="flex justify-center items-center flex-grow">
          <Loader2 className="h-8 w-8 animate-spin text-primary" />
        </div>
      ) : (
        <>
          <div className="space-y-3 flex-grow overflow-auto pr-1 custom-scrollbar">
            {clusters.length === 0 && !loading && (
                <div className="text-center text-muted-foreground py-10">No clusters found.</div>
            )}
            {clusters.map((cluster) => (
              <Card
                key={cluster.id}
                className={`cursor-pointer transition-all hover:shadow-lg border-l-4 ${
                  selectedClusterId === cluster.id
                    ? "ring-2 ring-primary border-primary shadow-md"
                    : "border-transparent hover:border-muted-foreground/30"
                }`}
                onClick={() => handleClusterSelection(cluster.id)}
              >
                <CardContent className="p-3">
                  <div className="flex justify-between items-start mb-1.5">
                    <div className="font-semibold text-sm text-card-foreground truncate" title={cluster.name || `Cluster ${cluster.id.substring(0,8)}...`}>
                        {cluster.name || `Cluster ${cluster.id.substring(0,8)}...`}
                    </div>
                    {(isAutoSelectingEdge && selectedClusterId === cluster.id) && <Loader2 className="h-4 w-4 animate-spin text-muted-foreground" />}
                  </div>

                  <div className="grid grid-cols-2 gap-2 text-xs text-muted-foreground mb-1.5">
                    <div>Entities: <span className="font-medium text-card-foreground">{cluster.entity_count}</span></div>
                    <div>Groups: <span className="font-medium text-card-foreground">{cluster.group_count}</span></div>
                  </div>

                  <div className="flex items-center gap-1.5 mb-2 text-xs">
                    <div className="text-muted-foreground">Coherence:</div>
                    <div className={`h-2.5 w-2.5 rounded-full ${getCoherenceColor(cluster.average_coherence_score)}`}></div>
                    <div className="font-medium text-card-foreground">
                      {cluster.average_coherence_score !== null
                        ? (cluster.average_coherence_score * 100).toFixed(0) + "%"
                        : "N/A"}
                    </div>
                  </div>

                  <div className="mt-1">
                    <div className="flex justify-between text-xs mb-0.5 text-muted-foreground">
                      <span>Review Progress</span>
                      <span className="font-medium text-card-foreground">{getProgressForCluster(cluster.id)}%</span>
                    </div>
                    <Progress value={getProgressForCluster(cluster.id)} className="h-1.5" />
                  </div>

                  {getProgressForCluster(cluster.id) === 100 && (
                    <div className="flex items-center mt-1.5 text-green-600 text-xs font-medium">
                      <CheckCircle className="h-3.5 w-3.5 mr-1" />
                      Review Complete
                    </div>
                  )}
                </CardContent>
              </Card>
            ))}
          </div>

          {total > limit && (
            <div className="flex justify-between items-center pt-3 border-t mt-auto">
              <Button variant="outline" size="sm" onClick={() => handlePageChange(page - 1)} disabled={page === 1 || loading}>
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

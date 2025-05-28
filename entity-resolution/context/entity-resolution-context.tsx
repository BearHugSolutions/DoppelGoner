"use client"

import { VisualizationDataResponse } from "@/types/entity-resolution";
import { createContext, useCallback, useContext, useState, type ReactNode } from "react"

export interface EntityResolutionContextType {
  selectedClusterId: string | null;
  setSelectedClusterId: (id: string | null) => void;
  selectedEdgeId: string | null;
  setSelectedEdgeId: (id: string | null) => void;
  reviewerId: string; // This might come from an auth context in a real app
  setReviewerId: (id: string) => void;
  reviewProgress: Record<string, number>;
  updateReviewProgress: (clusterId: string, progress: number) => void;
  refreshTrigger: number;
  triggerRefresh: () => void;
  lastReviewedEdgeId: string | null;
  setLastReviewedEdgeId: (id: string | null) => void;
  currentClusterVisualizationData: VisualizationDataResponse | null;
  setCurrentClusterVisualizationData: (data: VisualizationDataResponse | null) => void;
}

const EntityResolutionContext = createContext<EntityResolutionContextType | undefined>(undefined);

export function EntityResolutionProvider({ children }: { children: ReactNode }) {
  const [selectedClusterId, setSelectedClusterIdState] = useState<string | null>(null);
  const [selectedEdgeId, setSelectedEdgeIdState] = useState<string | null>(null);
  const [reviewerId, setReviewerId] = useState<string>("default-reviewer"); // Example default
  const [reviewProgress, setReviewProgress] = useState<Record<string, number>>({});
  const [refreshTrigger, setRefreshTrigger] = useState<number>(0);
  const [lastReviewedEdgeId, setLastReviewedEdgeId] = useState<string | null>(null);
  const [
    currentClusterVisualizationData,
    setCurrentClusterVisualizationDataState
  ] = useState<VisualizationDataResponse | null>(null);

  // Memoized update function for review progress
  const updateReviewProgress = useCallback((clusterId: string, progress: number) => {
    setReviewProgress((prevProgress) => {
      // Only update if the progress value has actually changed for the given cluster
      if (prevProgress[clusterId] === progress) {
        return prevProgress;
      }
      return {
        ...prevProgress,
        [clusterId]: progress,
      };
    });
  }, []);

  // Memoized function to trigger a refresh action
  const triggerRefresh = useCallback(() => {
    setRefreshTrigger((prevTrigger) => prevTrigger + 1);
  }, []);

  // Custom setter for selectedClusterId to handle side effects
  const setSelectedClusterId = useCallback((id: string | null) => {
    setSelectedClusterIdState(id);
    // When a cluster is deselected or changed, clear related states
    if (selectedClusterId !== id) { // only if the cluster ID actually changes
        setSelectedEdgeIdState(null);
        setLastReviewedEdgeId(null);
        setCurrentClusterVisualizationDataState(null); // Clear cached viz data for the old cluster
    }
  }, [selectedClusterId]); // Dependency on selectedClusterId to compare old vs new

  // Custom setter for selectedEdgeId (can be simple if no complex side-effects)
  const setSelectedEdgeId = useCallback((id: string | null) => {
    setSelectedEdgeIdState(id);
  }, []);

  // Custom setter for visualization data
  const setCurrentClusterVisualizationData = useCallback((data: VisualizationDataResponse | null) => {
    setCurrentClusterVisualizationDataState(data);
  }, []);


  return (
    <EntityResolutionContext.Provider
      value={{
        selectedClusterId,
        setSelectedClusterId,
        selectedEdgeId,
        setSelectedEdgeId,
        reviewerId,
        setReviewerId, // Direct setter is fine here
        reviewProgress,
        updateReviewProgress,
        refreshTrigger,
        triggerRefresh,
        lastReviewedEdgeId,
        setLastReviewedEdgeId, // Direct setter is fine
        currentClusterVisualizationData,
        setCurrentClusterVisualizationData,
      }}
    >
      {children}
    </EntityResolutionContext.Provider>
  );
}

export function useEntityResolution(): EntityResolutionContextType {
  const context = useContext(EntityResolutionContext);
  if (context === undefined) {
    throw new Error("useEntityResolution must be used within an EntityResolutionProvider");
  }
  return context;
}

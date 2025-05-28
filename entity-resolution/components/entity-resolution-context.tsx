"use client"

import { createContext, useCallback, useContext, useState, type ReactNode } from "react"

interface EntityResolutionContextType {
  selectedClusterId: string | null
  setSelectedClusterId: (id: string | null) => void
  selectedEdgeId: string | null
  setSelectedEdgeId: (id: string | null) => void
  reviewerId: string
  setReviewerId: (id: string) => void
  reviewProgress: Record<string, number>
  updateReviewProgress: (clusterId: string, progress: number) => void
  refreshTrigger: number
  triggerRefresh: () => void
}

const EntityResolutionContext = createContext<EntityResolutionContextType | undefined>(undefined)

export function EntityResolutionProvider({ children }: { children: ReactNode }) {
  const [selectedClusterId, setSelectedClusterId] = useState<string | null>(null)
  const [selectedEdgeId, setSelectedEdgeId] = useState<string | null>(null)
  const [reviewerId, setReviewerId] = useState<string>("default-reviewer")
  const [reviewProgress, setReviewProgress] = useState<Record<string, number>>({})
  const [refreshTrigger, setRefreshTrigger] = useState<number>(0)

  // Memoize the updateReviewProgress function to prevent unnecessary re-renders
  const updateReviewProgress = useCallback((clusterId: string, progress: number) => {
    setReviewProgress((prev) => {
      // Only update if the progress has changed
      if (prev[clusterId] === progress) {
        return prev
      }
      return {
        ...prev,
        [clusterId]: progress,
      }
    })
  }, [])

  const triggerRefresh = useCallback(() => {
    setRefreshTrigger((prev) => prev + 1)
  }, [])

  return (
    <EntityResolutionContext.Provider
      value={{
        selectedClusterId,
        setSelectedClusterId,
        selectedEdgeId,
        setSelectedEdgeId,
        reviewerId,
        setReviewerId,
        reviewProgress,
        updateReviewProgress,
        refreshTrigger,
        triggerRefresh,
      }}
    >
      {children}
    </EntityResolutionContext.Provider>
  )
}

export function useEntityResolution() {
  const context = useContext(EntityResolutionContext)
  if (context === undefined) {
    throw new Error("useEntityResolution must be used within an EntityResolutionProvider")
  }
  return context
}

import { Suspense } from "react"
import { EntityResolutionProvider } from "@/context/entity-resolution-context"
import ClusterSelector from "@/components/cluster-selector"
import GraphVisualizer from "@/components/graph-visualizer"
import ConnectionReviewTools from "@/components/connection-review-tools"
import { Skeleton } from "@/components/ui/skeleton"

export default function EntityResolutionPage() {
  return (
    <EntityResolutionProvider>
      <div className="flex flex-col h-[calc(100vh-4rem)] max-h-[calc(100vh-4rem)]">
        <div className="container mx-auto p-4 flex-1 flex flex-col overflow-hidden">
          <h1 className="text-3xl font-bold mb-4">Entity Resolution Review</h1>
          <div className="grid grid-cols-1 lg:grid-cols-4 gap-4 flex-1 min-h-0">
            <div className="lg:col-span-1 flex flex-col h-full">
              <h2 className="text-xl font-semibold mb-3">Clusters</h2>
              <div className="flex-1 overflow-auto">
                <Suspense fallback={<Skeleton className="h-full w-full" />}>
                  <ClusterSelector />
                </Suspense>
              </div>
            </div>
            <div className="lg:col-span-3 flex flex-col h-full min-h-0 gap-4">
              <div className="bg-white rounded-lg border shadow-sm p-4 flex flex-col min-h-0" style={{ flex: '2 1 0%' }}>
                <h2 className="text-xl font-semibold mb-2">Cluster Visualization</h2>
                <div className="flex-1 min-h-0">
                  <Suspense fallback={<Skeleton className="h-full w-full" />}>
                    <GraphVisualizer />
                  </Suspense>
                </div>
              </div>
              <div className="bg-white rounded-lg border shadow-sm p-4 flex flex-col overflow-auto" style={{ flex: '3 1 0%' }}>
                <Suspense fallback={<Skeleton className="h-full w-full" />}>
                  <ConnectionReviewTools />
                </Suspense>
              </div>
            </div>
          </div>
        </div>
      </div>
    </EntityResolutionProvider>
  )
}

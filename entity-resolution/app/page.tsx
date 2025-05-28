import { Suspense } from "react"
import { EntityResolutionProvider } from "@/context/entity-resolution-context"
import ClusterSelector from "@/components/cluster-selector"
import GraphVisualizer from "@/components/graph-visualizer"
import ConnectionReviewTools from "@/components/connection-review-tools"
import { Skeleton } from "@/components/ui/skeleton"

export default function EntityResolutionPage() {
  return (
    <EntityResolutionProvider>
      <div className="container mx-auto p-4 h-screen flex flex-col">
        <h1 className="text-3xl font-bold mb-4">Entity Resolution Review</h1>
        <div className="grid grid-cols-1 lg:grid-cols-4 gap-4 flex-grow overflow-hidden">
          <div className="lg:col-span-1 overflow-auto flex flex-col h-full">
            <h2 className="text-xl font-semibold mb-3">Clusters</h2>
            <div className="flex-grow overflow-auto">
              <Suspense fallback={<Skeleton className="h-full w-full" />}>
                <ClusterSelector />
              </Suspense>
            </div>
          </div>
          <div className="lg:col-span-3 flex flex-col h-full overflow-hidden">
            <div className="flex-grow min-h-0 bg-white rounded-lg border shadow-sm p-4 mb-4 overflow-hidden">
              <h2 className="text-xl font-semibold mb-2">Cluster Visualization</h2>
              <div className="h-[calc(100%-2rem)]">
                <Suspense fallback={<Skeleton className="h-full w-full" />}>
                  <GraphVisualizer />
                </Suspense>
              </div>
            </div>
            <div className="bg-white rounded-lg border shadow-sm p-4 h-auto max-h-[40vh] overflow-auto">
              <Suspense fallback={<Skeleton className="h-[200px] w-full" />}>
                <ConnectionReviewTools />
              </Suspense>
            </div>
          </div>
        </div>
      </div>
    </EntityResolutionProvider>
  )
}

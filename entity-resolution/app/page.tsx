"use client";
// app/page.tsx
import { Suspense } from "react";
import {
  EntityResolutionProvider,
  useEntityResolution,
} from "@/context/entity-resolution-context";
import ClusterSelector from "@/components/cluster-selector";
import GraphVisualizer from "@/components/graph-visualizer";
import ConnectionReviewTools from "@/components/connection-review-tools/connection-review-tools";
import ResolutionModeSwitcher from "@/components/resolution-mode-switcher";
import { Skeleton } from "@/components/ui/skeleton";

/**
 * A new child component to access the context for layout decisions.
 */
function ResolutionLayout() {
  const { isReviewToolsMaximized } = useEntityResolution();

  return (
    <div className="flex flex-col h-full">
      <div className="container mx-auto p-4 flex-1 flex flex-col min-h-0">
        <div className="flex justify-between items-center mb-4 flex-shrink-0">
          <h1 className="text-3xl font-bold">Resolution Review</h1>
          <ResolutionModeSwitcher />
        </div>
        <div className="grid grid-cols-1 lg:grid-cols-4 gap-4 flex-1 min-h-0">
          {/* Clusters Sidebar */}
          <div className="lg:col-span-1 flex flex-col min-h-0">
            <h2 className="text-xl font-semibold mb-3 flex-shrink-0">
              Clusters
            </h2>
            <div className="flex-1 min-h-0 overflow-hidden">
              <Suspense fallback={<Skeleton className="h-full w-full" />}>
                <ClusterSelector />
              </Suspense>
            </div>
          </div>

          {/* Main Content Area */}
          <div className="lg:col-span-3 flex flex-col min-h-0 gap-4">
            {/* Graph Visualization - Conditionally rendered */}
            {!isReviewToolsMaximized && (
              <div
                className="bg-white rounded-lg border shadow-sm p-4 flex flex-col min-h-0"
                style={{ flex: "1 1 0%" }}
              >
                <h2 className="text-xl font-semibold mb-2 flex-shrink-0">
                  Cluster Visualization
                </h2>
                <div className="flex-1 min-h-0">
                  <Suspense fallback={<Skeleton className="h-full w-full" />}>
                    <GraphVisualizer />
                  </Suspense>
                </div>
              </div>
            )}

            {/* Connection Review Tools - Flex behavior is adjusted */}
            <div
              className="bg-white rounded-lg border shadow-sm p-4 flex flex-col min-h-0"
              style={{ flex: isReviewToolsMaximized ? "1 1 auto" : "2 1 0%" }}
            >
              <div className="flex-1 min-h-0 overflow-hidden">
                <Suspense fallback={<Skeleton className="h-full w-full" />}>
                  <ConnectionReviewTools />
                </Suspense>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

export default function EntityResolutionPage() {
  return (
    <EntityResolutionProvider>
      <ResolutionLayout />
    </EntityResolutionProvider>
  );
}

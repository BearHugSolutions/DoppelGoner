// app/page.tsx
"use client";

import { Suspense } from "react";
import {
  EntityResolutionProvider,
  useEntityResolution,
} from "@/context/entity-resolution-context";
import ClusterSelector from "@/components/cluster-selector";
import GraphVisualizer from "@/components/graph-visualizer";
import ConnectionReviewTools from "@/components/connection-review-tools/connection-review-tools";
import { Skeleton } from "@/components/ui/skeleton";
// ✨ NEW: Import the overall progress bar
import { OverallProgressBar } from "@/components/progress-displays";

/**
 * A new child component to access the context for layout decisions.
 */
function ResolutionLayout() {
  const { isReviewToolsMaximized } = useEntityResolution();

  return (
    // ✨ UPDATED: Added a pb-24 padding-bottom to the main container to make space for the fixed progress bar
    <div className="flex flex-col h-full pb-24">
      <div className="py-4 flex-1 flex flex-col p-4 min-h-0">
        <div className="grid grid-cols-1 lg:grid-cols-4 gap-4 flex-1 min-h-0">
          {/* Clusters Sidebar */}
          <div className="lg:col-span-1 flex flex-col min-h-0">
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
                className="bg-white rounded-lg border shadow-sm p-2 flex flex-col min-h-0"
                style={{ flex: "1 1 0%" }}
              >
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
      <OverallProgressBar />
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

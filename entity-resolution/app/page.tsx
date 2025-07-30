// app/page.tsx - ENHANCED: Complete Audit Mode Integration with Optimized Layout
"use client";

import { Suspense, useEffect } from "react";
import {
  EntityResolutionProvider,
  useEntityResolution,
} from "@/context/entity-resolution-context";
import ClusterSelector from "@/components/cluster-selector";
import GraphVisualizer from "@/components/graph-visualizer";
import ConnectionReviewTools from "@/components/connection-review-tools/connection-review-tools";
import { Skeleton } from "@/components/ui/skeleton";
import { OverallProgressBar } from "@/components/progress-displays";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import {
  Info,
  Bot,
  Users,
  Eye,
  AlertTriangle,
  CheckCircle,
  Clock,
} from "lucide-react";
import { useAuth } from "@/context/auth-context";

/**
 * A child component to access context for layout decisions and audit mode.
 */
function ResolutionLayout() {
  const {
    isReviewToolsMaximized,
    auditMode,
    postProcessingFilter,
    selectedClusterId,
    clustersWithAuditData,
    currentVisualizationData,
    queries,
    actions,
  } = useEntityResolution();

  const isInAuditMode = queries.isInAuditMode();
  const auditFilterDisplayName = queries.getAuditFilterDisplayName();
  const auditCounts = clustersWithAuditData?.data?.auditCounts;

  if (isInAuditMode) {
    return (
      <div className="h-full flex flex-col bg-slate-50">
        <div className="flex-1 grid grid-cols-1 xl:grid-cols-5 gap-4 px-4 pb-4 min-h-0">
          {/* Left sidebar - Cluster selector */}
          <div className="xl:col-span-1 flex flex-col gap-4 min-h-0">
            <Suspense fallback={<Skeleton className="h-full w-full" />}>
              <ClusterSelector />
            </Suspense>
          </div>

          {/* Main content area - Stacked vertically */}
          <div className="xl:col-span-4 flex flex-col gap-4 min-h-0">
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
    );
  }

  // Normal/Manual Review Mode - Original layout with progress bar
  return (
    <div className="flex flex-col h-full pb-20 bg-slate-50">
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

/**
 * Main Page Component with Header and Mode Switching Logic
 */
export default function EntityResolutionPage() {
  return (
    <EntityResolutionProvider>
      <PageContent />
    </EntityResolutionProvider>
  );
}

/**
 * Renders the page content, including header and main layout.
 * This component lives inside the provider to access all contexts.
 */
function PageContent() {
  const { user, selectedOpinion } = useAuth();
  const { auditMode, clustersWithAuditData, queries, actions } =
    useEntityResolution();

  const isInAuditMode = queries.isInAuditMode();
  const auditCounts = clustersWithAuditData?.data?.auditCounts;

  // Effect to suggest switching to audit mode when there are unreviewed decisions
  useEffect(() => {
    if (
      auditCounts?.totalUnreviewed &&
      auditCounts.totalUnreviewed > 0 &&
      auditMode === "normal"
    ) {
      console.log(
        `[MainPage] ${auditCounts.totalUnreviewed} unreviewed automated decisions available.`
      );
    }
  }, [auditCounts, auditMode]);

  // Effect to automatically switch back to normal mode when audit is complete
  useEffect(() => {
    if (
      isInAuditMode &&
      auditCounts?.totalUnreviewed === 0 &&
      auditCounts?.totalReviewed > 0
    ) {
      console.log(
        "[MainPage] Audit mode complete, suggesting switch back to manual review"
      );
      // Don't auto-switch, but the user will see the completion message in the status info
    }
  }, [isInAuditMode, auditCounts]);

  if (!user || !selectedOpinion) {
    return (
      <div className="min-h-full bg-slate-50 flex items-center justify-center">
        <Card className="w-full max-w-md">
          <CardHeader className="text-center">
            <CardTitle className="text-2xl">Entity Resolution System</CardTitle>
          </CardHeader>
          <CardContent className="text-center space-y-4">
            <p className="text-muted-foreground">
              {!user
                ? "Please log in to continue."
                : "Please select an opinion to continue."}
            </p>

            {!user && (
              <Alert>
                <Info className="h-4 w-4" />
                <AlertDescription>
                  Authentication is required to access the entity resolution
                  workflow.
                </AlertDescription>
              </Alert>
            )}

            {user && !selectedOpinion && (
              <Alert>
                <Info className="h-4 w-4" />
                <AlertDescription>
                  Please select an opinion context to begin reviewing entity
                  relationships.
                </AlertDescription>
              </Alert>
            )}
          </CardContent>
        </Card>
      </div>
    );
  }

  return (
    <div className="h-full flex flex-col">
      <main className="flex-1 min-h-0">
        <ResolutionLayout />
      </main>
    </div>
  );
}

// components/audit/audit-loading-states.tsx - NEW: Loading states for audit workflow
"use client";

import React from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { Progress } from "@/components/ui/progress";
import {
  Bot,
  Loader2,
  CheckCircle,
  Clock,
  Database,
  Network,
  Users,
} from "lucide-react";

interface AuditLoadingStatesProps {
  loadingPhase: 'initial' | 'clusters' | 'visualization' | 'connections' | 'complete';
  progress?: number;
  message?: string;
  clustersLoaded?: number;
  totalClusters?: number;
  decisionsLoaded?: number;
  totalDecisions?: number;
}

export default function AuditLoadingStates({
  loadingPhase,
  progress = 0,
  message,
  clustersLoaded = 0,
  totalClusters = 0,
  decisionsLoaded = 0,
  totalDecisions = 0,
}: AuditLoadingStatesProps) {
  const getPhaseInfo = (phase: string) => {
    switch (phase) {
      case 'initial':
        return {
          icon: <Database className="h-5 w-5 text-blue-600" />,
          title: "Initializing Audit Mode",
          description: "Loading automated decisions and cluster information...",
          color: "bg-blue-50 border-blue-200"
        };
      case 'clusters':
        return {
          icon: <Users className="h-5 w-5 text-purple-600" />,
          title: "Loading Audit Clusters",
          description: "Fetching clusters with automated decisions...",
          color: "bg-purple-50 border-purple-200"
        };
      case 'visualization':
        return {
          icon: <Network className="h-5 w-5 text-green-600" />,
          title: "Building Visualization",
          description: "Loading rich data for selected cluster...",
          color: "bg-green-50 border-green-200"
        };
      case 'connections':
        return {
          icon: <Bot className="h-5 w-5 text-orange-600" />,
          title: "Processing Connections",
          description: "Loading connection details and entity information...",
          color: "bg-orange-50 border-orange-200"
        };
      case 'complete':
        return {
          icon: <CheckCircle className="h-5 w-5 text-green-600" />,
          title: "Audit Mode Ready",
          description: "All data loaded successfully. You can now review automated decisions.",
          color: "bg-green-50 border-green-200"
        };
      default:
        return {
          icon: <Loader2 className="h-5 w-5 text-gray-600 animate-spin" />,
          title: "Loading...",
          description: "Preparing audit interface...",
          color: "bg-gray-50 border-gray-200"
        };
    }
  };

  const phaseInfo = getPhaseInfo(loadingPhase);

  return (
    <Card className={`${phaseInfo.color} shadow-lg`}>
      <CardHeader className="pb-3">
        <CardTitle className="text-lg flex items-center gap-3">
          {phaseInfo.icon}
          <div>
            <div>{phaseInfo.title}</div>
            <div className="text-sm font-normal text-muted-foreground">
              {phaseInfo.description}
            </div>
          </div>
        </CardTitle>
      </CardHeader>
      
      <CardContent className="space-y-4">
        {/* Progress bar */}
        {progress > 0 && progress < 100 && (
          <div className="space-y-2">
            <div className="flex justify-between text-sm">
              <span>Progress</span>
              <span>{Math.round(progress)}%</span>
            </div>
            <Progress value={progress} className="h-2" />
          </div>
        )}

        {/* Custom message */}
        {message && (
          <div className="text-sm text-muted-foreground bg-white/50 p-2 rounded">
            {message}
          </div>
        )}

        {/* Loading statistics */}
        <div className="grid grid-cols-2 gap-4 text-sm">
          {totalClusters > 0 && (
            <div className="flex items-center gap-2">
              <Users className="h-4 w-4 text-muted-foreground" />
              <div>
                <div className="font-medium">{clustersLoaded} / {totalClusters}</div>
                <div className="text-xs text-muted-foreground">Clusters</div>
              </div>
            </div>
          )}
          
          {totalDecisions > 0 && (
            <div className="flex items-center gap-2">
              <Bot className="h-4 w-4 text-muted-foreground" />
              <div>
                <div className="font-medium">{decisionsLoaded} / {totalDecisions}</div>
                <div className="text-xs text-muted-foreground">Decisions</div>
              </div>
            </div>
          )}
        </div>

        {/* Loading skeleton for incomplete phases */}
        {loadingPhase !== 'complete' && (
          <div className="space-y-3">
            <div className="space-y-2">
              <Skeleton className="h-4 w-3/4" />
              <Skeleton className="h-4 w-1/2" />
            </div>
            <div className="flex gap-2">
              <Skeleton className="h-8 w-16" />
              <Skeleton className="h-8 w-20" />
              <Skeleton className="h-8 w-12" />
            </div>
          </div>
        )}

        {/* Time indicator */}
        <div className="flex items-center gap-2 text-xs text-muted-foreground pt-2 border-t">
          <Clock className="h-3 w-3" />
          <span>
            {loadingPhase === 'complete' 
              ? 'Ready for review' 
              : 'This may take a few moments...'
            }
          </span>
        </div>
      </CardContent>
    </Card>
  );
}

/**
 * Simplified loading indicator for smaller spaces
 */
export function AuditLoadingIndicator({ 
  phase, 
  showText = true 
}: { 
  phase: string; 
  showText?: boolean; 
}) {
  return (
    <div className="flex items-center gap-2 text-sm text-muted-foreground">
      <Loader2 className="h-4 w-4 animate-spin text-blue-600" />
      {showText && (
        <span>
          {phase === 'initial' && 'Initializing...'}
          {phase === 'clusters' && 'Loading clusters...'}
          {phase === 'visualization' && 'Building graph...'}
          {phase === 'connections' && 'Loading details...'}
          {phase === 'complete' && 'Ready!'}
        </span>
      )}
    </div>
  );
}

/**
 * Audit mode transition indicator
 */
export function AuditModeTransition({
  isEntering = true,
  onComplete
}: {
  isEntering?: boolean;
  onComplete?: () => void;
}) {
  React.useEffect(() => {
    const timer = setTimeout(() => {
      onComplete?.();
    }, 1500);
    
    return () => clearTimeout(timer);
  }, [onComplete]);

  return (
    <div className="fixed inset-0 bg-black/20 flex items-center justify-center z-50">
      <Card className="w-96 border-blue-200 bg-blue-50">
        <CardContent className="p-6 text-center">
          <div className="mb-4">
            <Bot className="h-12 w-12 text-blue-600 mx-auto mb-2" />
            <h3 className="text-lg font-semibold">
              {isEntering ? 'Entering Audit Mode' : 'Exiting Audit Mode'}
            </h3>
            <p className="text-sm text-muted-foreground">
              {isEntering 
                ? 'Switching to automated decision review workflow...' 
                : 'Returning to manual review workflow...'
              }
            </p>
          </div>
          
          <div className="flex items-center justify-center gap-2">
            <Loader2 className="h-5 w-5 animate-spin text-blue-600" />
            <span className="text-sm">Please wait...</span>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
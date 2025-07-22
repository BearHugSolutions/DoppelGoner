// components/progress-displays.tsx - ENHANCED with Server-Side Data Context
"use client";

import React from 'react';
import { Progress } from '@/components/ui/progress';
import { Badge } from '@/components/ui/badge';
import { Filter, BarChart3, Info, GitBranch } from 'lucide-react';
import { useEntityResolution } from '@/context/entity-resolution-context';
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from '@/components/ui/tooltip';

// ✨ NEW: Progress context component for displaying detailed progress information
export const ProgressContextDisplay = ({ clusterId }: { clusterId: string }) => {
  const { queries, workflowFilter } = useEntityResolution();
  
  const currentProgress = queries.getClusterProgress(clusterId);
  const totalProgress = queries.getClusterProgressUnfiltered(clusterId);
  const crossSourceProgress = queries.getClusterProgressCrossSource(clusterId);
  
  const isFiltered = workflowFilter === "cross-source-only";
  const hasServerData = currentProgress.totalEdges !== -1;
  
  if (!hasServerData) {
    return (
      <div className="text-xs text-muted-foreground">
        Progress data loading...
      </div>
    );
  }
  
  return (
    <div className="space-y-2">
      {/* Current view progress */}
      <div className="flex justify-between items-center">
        <div className="flex items-center gap-2">
          <span className="text-sm">
            {isFiltered ? "Cross-source progress:" : "Overall progress:"}
          </span>
          {isFiltered && (
            <Badge variant="outline" className="text-xs bg-blue-50 text-blue-700 border-blue-200">
              <GitBranch className="h-3 w-3 mr-1" />
              Filtered
            </Badge>
          )}
        </div>
        <span className="text-sm font-medium">
          {currentProgress.reviewedEdges} / {currentProgress.totalEdges} 
          ({currentProgress.progressPercentage}%)
        </span>
      </div>
      
      <Progress 
        value={currentProgress.progressPercentage} 
        className="h-2"
      />
      
      {/* Context information when filter is applied */}
      {isFiltered && totalProgress.totalEdges > currentProgress.totalEdges && (
        <div className="flex justify-between text-xs text-muted-foreground">
          <span>Total (all connections):</span>
          <span>
            {totalProgress.reviewedEdges} / {totalProgress.totalEdges} 
            ({totalProgress.progressPercentage}%)
          </span>
        </div>
      )}
      
      {/* Show cross-source availability when filter is not applied */}
      {!isFiltered && crossSourceProgress.totalEdges > 0 && (
        <div className="flex justify-between text-xs text-muted-foreground">
          <span>Cross-source available:</span>
          <span>
            {crossSourceProgress.reviewedEdges} / {crossSourceProgress.totalEdges}
            ({crossSourceProgress.progressPercentage}%)
          </span>
        </div>
      )}
      
      {/* Decision breakdown */}
      <div className="flex justify-between text-xs text-muted-foreground">
        <span>Decisions:</span>
        <span>
          {currentProgress.confirmedMatches}M / {currentProgress.confirmedNonMatches}NM / {currentProgress.pendingEdges}P
        </span>
      </div>
    </div>
  );
};

// ✨ ENHANCED: Overall progress bar component for bottom of screen with context information
export const OverallProgressBar = () => {
  const { overallProgress, workflowFilter } = useEntityResolution();

  if (!overallProgress || (overallProgress.totalPendingDecisions === 0 && overallProgress.totalCompletedDecisions === 0)) {
    return null;
  }

  const isFiltered = workflowFilter === "cross-source-only";
  
  // ✨ NEW: Generate progress text with context
  const getProgressText = () => {
    if (isFiltered) {
      return `${overallProgress.totalCompletedDecisions} of ${overallProgress.totalPendingDecisions + overallProgress.totalCompletedDecisions} cross-source decisions complete`;
    } else {
      return `${overallProgress.totalCompletedDecisions} of ${overallProgress.totalPendingDecisions + overallProgress.totalCompletedDecisions} decisions complete`;
    }
  };

  return (
    <div className="fixed bottom-0 left-0 right-0 bg-white/95 backdrop-blur-sm border-t shadow-lg p-3 z-50">
      <div className="max-w-7xl mx-auto">
        <div className="flex items-center justify-between mb-2">
          <div className="flex items-center gap-3">
            <BarChart3 className="h-4 w-4 text-primary" />
            <span className="font-medium text-sm">Overall Progress</span>
            {isFiltered && (
              <TooltipProvider>
                <Tooltip>
                  <TooltipTrigger>
                    <Badge variant="outline" className="text-xs bg-blue-50 text-blue-700 border-blue-200">
                      <Filter className="h-3 w-3 mr-1" />
                      Cross-Source Filter Active
                    </Badge>
                  </TooltipTrigger>
                  <TooltipContent>
                    <p>Only showing cross-source connection progress</p>
                  </TooltipContent>
                </Tooltip>
              </TooltipProvider>
            )}
          </div>
          
          <div className="flex items-center gap-4 text-sm">
            <div className="text-muted-foreground">
              {overallProgress.clustersComplete} / {overallProgress.totalClusters} clusters complete
            </div>
            <div className="font-medium">
              {overallProgress.totalPendingDecisions} decisions remaining
            </div>
          </div>
        </div>
        
        <Progress 
          value={overallProgress.overallProgressPercentage} 
          className="h-2"
        />
        
        <div className="flex justify-between text-xs text-muted-foreground mt-1">
          <span>
            {getProgressText()}
          </span>
          <span>
            {overallProgress.overallProgressPercentage}% complete
          </span>
        </div>
        
        {/* ✨ NEW: Additional context for filtered views */}
        {isFiltered && (
          <div className="mt-1 pt-1 border-t border-gray-200">
            <div className="text-xs text-blue-700 bg-blue-50 px-2 py-1 rounded">
              <strong>Filter Active:</strong> Progress shown is for cross-source connections only. 
              Switch to "All Connections" to see complete workflow progress.
            </div>
          </div>
        )}
      </div>
    </div>
  );
};

// ✨ NEW: Compact progress indicator for cluster items
export const ClusterProgressIndicator = ({ 
  clusterId, 
  showDetails = false 
}: { 
  clusterId: string; 
  showDetails?: boolean; 
}) => {
  const { queries, workflowFilter } = useEntityResolution();
  
  const progress = queries.getClusterProgress(clusterId);
  const hasServerData = progress.totalEdges !== -1;
  
  if (!hasServerData) {
    return (
      <div className="flex items-center gap-1 text-xs text-muted-foreground">
        <div className="w-2 h-2 bg-gray-300 rounded-full animate-pulse" />
        <span>Loading...</span>
      </div>
    );
  }
  
  const isComplete = progress.isComplete;
  const progressColor = isComplete 
    ? "bg-green-500" 
    : progress.progressPercentage > 50 
      ? "bg-blue-500" 
      : "bg-gray-400";
  
  if (showDetails) {
    return (
      <div className="space-y-1">
        <div className="flex justify-between text-xs">
          <span className="text-muted-foreground">Progress:</span>
          <span className="font-medium">
            {progress.reviewedEdges} / {progress.totalEdges}
          </span>
        </div>
        <Progress value={progress.progressPercentage} className="h-1" />
        {workflowFilter === "cross-source-only" && (
          <div className="text-xs text-blue-600">
            Cross-source view active
          </div>
        )}
      </div>
    );
  }
  
  return (
    <TooltipProvider>
      <Tooltip>
        <TooltipTrigger>
          <div className="flex items-center gap-1">
            <div className={`w-2 h-2 rounded-full ${progressColor}`} />
            <span className="text-xs font-medium">
              {progress.progressPercentage}%
            </span>
          </div>
        </TooltipTrigger>
        <TooltipContent>
          <div className="text-xs">
            <div>{progress.reviewedEdges} / {progress.totalEdges} reviewed</div>
            <div>{progress.confirmedMatches} matches, {progress.confirmedNonMatches} non-matches</div>
            {workflowFilter === "cross-source-only" && (
              <div className="text-blue-600 mt-1">Cross-source filter active</div>
            )}
          </div>
        </TooltipContent>
      </Tooltip>
    </TooltipProvider>
  );
};

// ✨ NEW: Progress summary card for detailed views
export const ProgressSummaryCard = ({ clusterId }: { clusterId: string }) => {
  const { queries, workflowFilter } = useEntityResolution();
  
  const currentProgress = queries.getClusterProgress(clusterId);
  const totalProgress = queries.getClusterProgressUnfiltered(clusterId);
  const crossSourceProgress = queries.getClusterProgressCrossSource(clusterId);
  
  const hasServerData = currentProgress.totalEdges !== -1;
  
  if (!hasServerData) {
    return (
      <div className="bg-gray-50 p-3 rounded-lg">
        <div className="text-sm text-muted-foreground">Loading progress data...</div>
      </div>
    );
  }
  
  return (
    <div className="bg-white border rounded-lg p-4 space-y-3">
      <h4 className="font-medium text-sm">Progress Summary</h4>
      
      {/* Current view progress */}
      <div className="space-y-2">
        <div className="flex justify-between items-center">
          <span className="text-sm">
            {workflowFilter === "cross-source-only" ? "Cross-source" : "All connections"}:
          </span>
          <span className="text-sm font-medium">
            {currentProgress.progressPercentage}%
          </span>
        </div>
        <Progress value={currentProgress.progressPercentage} className="h-2" />
        <div className="flex justify-between text-xs text-muted-foreground">
          <span>{currentProgress.reviewedEdges} reviewed</span>
          <span>{currentProgress.pendingEdges} pending</span>
        </div>
      </div>
      
      {/* Context information */}
      {workflowFilter === "cross-source-only" && crossSourceProgress.totalEdges !== totalProgress.totalEdges && (
        <div className="pt-2 border-t">
          <div className="flex justify-between text-xs text-muted-foreground">
            <span>Total connections:</span>
            <span>{totalProgress.reviewedEdges} / {totalProgress.totalEdges}</span>
          </div>
        </div>
      )}
      
      {workflowFilter === "all" && crossSourceProgress.totalEdges > 0 && (
        <div className="pt-2 border-t">
          <div className="flex justify-between text-xs text-muted-foreground">
            <span>Cross-source subset:</span>
            <span>{crossSourceProgress.reviewedEdges} / {crossSourceProgress.totalEdges}</span>
          </div>
        </div>
      )}
      
      {/* Decision breakdown */}
      <div className="pt-2 border-t">
        <div className="grid grid-cols-3 gap-2 text-xs">
          <div className="text-center">
            <div className="font-medium text-green-600">{currentProgress.confirmedMatches}</div>
            <div className="text-muted-foreground">Matches</div>
          </div>
          <div className="text-center">
            <div className="font-medium text-red-600">{currentProgress.confirmedNonMatches}</div>
            <div className="text-muted-foreground">Non-matches</div>
          </div>
          <div className="text-center">
            <div className="font-medium text-gray-600">{currentProgress.pendingEdges}</div>
            <div className="text-muted-foreground">Pending</div>
          </div>
        </div>
      </div>
    </div>
  );
};
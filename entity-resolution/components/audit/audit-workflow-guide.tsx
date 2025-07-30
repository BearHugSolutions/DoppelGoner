// components/audit/audit-workflow-guide.tsx - NEW: User guidance for audit workflow
"use client";

import React, { useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import {
  Bot,
  Eye,
  CheckCircle,
  ArrowRight,
  Users,
  HelpCircle,
  X,
  Lightbulb,
  Target,
  Zap,
} from "lucide-react";
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@/components/ui/collapsible";
import { Alert, AlertDescription } from "@/components/ui/alert";

interface AuditWorkflowGuideProps {
  isVisible: boolean;
  onDismiss: () => void;
  totalUnreviewed: number;
  currentFilter: string | null;
}

export default function AuditWorkflowGuide({
  isVisible,
  onDismiss,
  totalUnreviewed,
  currentFilter,
}: AuditWorkflowGuideProps) {
  const [isExpanded, setIsExpanded] = useState(false);

  if (!isVisible) return null;

  const getFilterDisplayName = (filter: string | null) => {
    if (!filter) return "All Automated Decisions";
    switch (filter) {
      case "disconnectDependentServices":
        return "Disconnect Dependent Services";
      default:
        return filter;
    }
  };

  return (
    <Card className="border-blue-200 bg-blue-50 mb-4">
      <CardHeader className="pb-3">
        <div className="flex items-center justify-between">
          <CardTitle className="text-base flex items-center gap-2">
            <Bot className="h-4 w-4 text-blue-600" />
            Audit Mode Guide
            <Badge variant="outline" className="bg-blue-100 text-blue-800">
              {totalUnreviewed} Pending
            </Badge>
          </CardTitle>
          <div className="flex items-center gap-2">
            <Collapsible open={isExpanded} onOpenChange={setIsExpanded}>
              <CollapsibleTrigger asChild>
                <Button variant="ghost" size="sm">
                  <HelpCircle className="h-4 w-4" />
                </Button>
              </CollapsibleTrigger>
            </Collapsible>
            <Button variant="ghost" size="sm" onClick={onDismiss}>
              <X className="h-4 w-4" />
            </Button>
          </div>
        </div>
      </CardHeader>
      
      <CardContent className="space-y-3">
        <Alert className="border-blue-200 bg-blue-100">
          <Target className="h-4 w-4 text-blue-600" />
          <AlertDescription className="text-blue-800">
            <strong>Current Focus:</strong> Reviewing {getFilterDisplayName(currentFilter)} decisions.
            You can manually override any automated decision by clicking the review buttons.
          </AlertDescription>
        </Alert>

        <div className="grid grid-cols-1 md:grid-cols-3 gap-3 text-sm">
          <div className="flex items-start gap-2">
            <div className="w-6 h-6 rounded-full bg-blue-600 text-white flex items-center justify-center text-xs font-bold">
              1
            </div>
            <div>
              <div className="font-medium">Select Cluster</div>
              <div className="text-xs text-muted-foreground">
                Choose a cluster with unreviewed automated decisions
              </div>
            </div>
          </div>
          
          <div className="flex items-start gap-2">
            <div className="w-6 h-6 rounded-full bg-blue-600 text-white flex items-center justify-center text-xs font-bold">
              2
            </div>
            <div>
              <div className="font-medium">Review Connection</div>
              <div className="text-xs text-muted-foreground">
                Click edges to see automated decisions and entity details
              </div>
            </div>
          </div>
          
          <div className="flex items-start gap-2">
            <div className="w-6 h-6 rounded-full bg-green-600 text-white flex items-center justify-center text-xs font-bold">
              3
            </div>
            <div>
              <div className="font-medium">Manual Override</div>
              <div className="text-xs text-muted-foreground">
                Use "Confirm Match" or "Not a Match" to override decisions
              </div>
            </div>
          </div>
        </div>

        <Collapsible open={isExpanded} onOpenChange={setIsExpanded}>
          <CollapsibleContent className="space-y-3">
            <div className="border-t pt-3">
              <h4 className="font-medium text-sm mb-2 flex items-center gap-2">
                <Lightbulb className="h-4 w-4 text-amber-500" />
                Key Features in Audit Mode
              </h4>
              
              <div className="space-y-2 text-xs">
                <div className="flex items-center gap-2">
                  <Zap className="h-3 w-3 text-blue-600" />
                  <span><strong>Dual-Action Reviews:</strong> Manual reviews automatically mark related automated decisions as reviewed</span>
                </div>
                
                <div className="flex items-center gap-2">
                  <Eye className="h-3 w-3 text-green-600" />
                  <span><strong>Rich Context:</strong> See full entity details and connection information just like manual review mode</span>
                </div>
                
                <div className="flex items-center gap-2">
                  <Bot className="h-3 w-3 text-purple-600" />
                  <span><strong>Visual Indicators:</strong> Edges with automated decisions are highlighted in the graph</span>
                </div>
                
                <div className="flex items-center gap-2">
                  <CheckCircle className="h-3 w-3 text-amber-600" />
                  <span><strong>Individual Review:</strong> Mark specific automated decisions as reviewed without changing connection status</span>
                </div>
              </div>
            </div>

            <div className="border-t pt-3">
              <h4 className="font-medium text-sm mb-2">What Happens When You Review?</h4>
              <div className="bg-white rounded p-3 text-xs space-y-2">
                <div className="flex items-center gap-2">
                  <ArrowRight className="h-3 w-3 text-blue-600" />
                  <span>Traditional edge review is submitted (same as manual mode)</span>
                </div>
                <div className="flex items-center gap-2">
                  <ArrowRight className="h-3 w-3 text-blue-600" />
                  <span>All related automated decisions are automatically marked as reviewed</span>
                </div>
                <div className="flex items-center gap-2">
                  <ArrowRight className="h-3 w-3 text-blue-600" />
                  <span>Decisions are removed from the audit queue</span>
                </div>
                <div className="flex items-center gap-2">
                  <ArrowRight className="h-3 w-3 text-blue-600" />
                  <span>Progress automatically advances to the next unreviewed item</span>
                </div>
              </div>
            </div>

            <div className="border-t pt-3">
              <h4 className="font-medium text-sm mb-2">Need to Switch Modes?</h4>
              <div className="flex items-center gap-2 text-xs">
                <Users className="h-3 w-3 text-gray-600" />
                <span>Click the "Manual Review" button in the top-right to return to standard entity resolution workflow</span>
              </div>
            </div>
          </CollapsibleContent>
        </Collapsible>
      </CardContent>
    </Card>
  );
}
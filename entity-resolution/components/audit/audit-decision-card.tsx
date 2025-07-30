// components/audit/audit-decision-card.tsx - NEW: Component for displaying audit decisions
"use client";

import { useState } from "react";
import { Card, CardContent } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import {
  Check,
  X,
  Clock,
  Bot,
  CheckCircle,
  XCircle,
  Loader2,
} from "lucide-react";
import type { PostProcessingDecision } from "@/types/post-processing";
import { formatDistanceToNow } from "date-fns";

interface AuditDecisionCardProps {
  decision: PostProcessingDecision;
  onMarkReviewed: (decisionId: string) => Promise<void>;
  isMarking?: boolean;
}

export default function AuditDecisionCard({
  decision,
  onMarkReviewed,
  isMarking = false,
}: AuditDecisionCardProps) {
  const [isLocallyMarking, setIsLocallyMarking] = useState(false);

  const handleMarkReviewed = async () => {
    if (isLocallyMarking || isMarking) return;
    
    setIsLocallyMarking(true);
    try {
      await onMarkReviewed(decision.id);
    } catch (error) {
      console.error("Failed to mark decision as reviewed:", error);
    } finally {
      setIsLocallyMarking(false);
    }
  };

  const getDecisionIcon = () => {
    if (decision.newStatus === "CONFIRMED_MATCH") {
      return <CheckCircle className="h-4 w-4 text-green-600" />;
    } else if (decision.newStatus === "CONFIRMED_NON_MATCH") {
      return <XCircle className="h-4 w-4 text-red-600" />;
    }
    return <Bot className="h-4 w-4 text-blue-600" />;
  };

  const getDecisionText = () => {
    if (decision.newStatus === "CONFIRMED_MATCH") {
      return "Automated Match";
    } else if (decision.newStatus === "CONFIRMED_NON_MATCH") {
      return "Automated Non-Match";
    }
    return "Automated Decision";
  };

  const getStatusBadge = () => {
    if (decision.reviewedByHuman) {
      return (
        <Badge variant="outline" className="bg-green-50 text-green-700 border-green-200">
          <Check className="h-3 w-3 mr-1" />
          Reviewed
        </Badge>
      );
    }
    return (
      <Badge variant="outline" className="bg-amber-50 text-amber-700 border-amber-200">
        <Clock className="h-3 w-3 mr-1" />
        Pending Review
      </Badge>
    );
  };

  const getFilterDisplayName = (filter: string) => {
    switch (filter) {
      case "disconnectDependentServices":
        return "Disconnect Dependent Services";
      default:
        return filter.replace(/([A-Z])/g, " $1").trim();
    }
  };

  const formatTimeAgo = (dateString: string) => {
    try {
      return formatDistanceToNow(new Date(dateString), { addSuffix: true });
    } catch {
      return "Unknown time";
    }
  };

  const isLoading = isLocallyMarking || isMarking;

  return (
    <Card className={`border-l-4 ${
      decision.reviewedByHuman 
        ? "border-l-green-500 bg-green-50/30" 
        : "border-l-blue-500 bg-blue-50/30"
    }`}>
      <CardContent className="p-3">
        <div className="flex items-center justify-between mb-2">
          <div className="flex items-center gap-2">
            {getDecisionIcon()}
            <span className="font-medium text-sm">{getDecisionText()}</span>
            {getStatusBadge()}
          </div>
          <div className="text-xs text-muted-foreground">
            {formatTimeAgo(decision.createdAt)}
          </div>
        </div>

        <div className="space-y-2 text-xs">
          <div className="grid grid-cols-2 gap-2">
            <div>
              <span className="font-medium">Filter:</span>{" "}
              <span className="text-muted-foreground">
                {getFilterDisplayName(decision.postProcFilter)}
              </span>
            </div>
            <div>
              <span className="font-medium">Original Status:</span>{" "}
              <span className="text-muted-foreground">
                {decision.originalStatus || "None"}
              </span>
            </div>
          </div>
          
          <div>
            <span className="font-medium">New Status:</span>{" "}
            <Badge 
              variant={decision.newStatus === "CONFIRMED_MATCH" ? "default" : "secondary"}
              className={`text-xs ${
                decision.newStatus === "CONFIRMED_MATCH"
                  ? "bg-green-100 text-green-800 border-green-300"
                  : decision.newStatus === "CONFIRMED_NON_MATCH"
                  ? "bg-red-100 text-red-800 border-red-300"
                  : ""
              }`}
            >
              {decision.newStatus.replace(/_/g, " ")}
            </Badge>
          </div>

          {decision.reviewedAt && (
            <div>
              <span className="font-medium">Reviewed:</span>{" "}
              <span className="text-muted-foreground">
                {formatTimeAgo(decision.reviewedAt)}
              </span>
            </div>
          )}
        </div>

        {!decision.reviewedByHuman && (
          <div className="mt-3 pt-2 border-t">
            <Button
              variant="outline"
              size="sm"
              onClick={handleMarkReviewed}
              disabled={isLoading}
              className="w-full text-xs"
            >
              {isLoading ? (
                <>
                  <Loader2 className="h-3 w-3 mr-1 animate-spin" />
                  Marking as Reviewed...
                </>
              ) : (
                <>
                  <Check className="h-3 w-3 mr-1" />
                  Mark as Reviewed
                </>
              )}
            </Button>
          </div>
        )}
      </CardContent>
    </Card>
  );
}
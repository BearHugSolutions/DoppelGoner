// components/connection-review-tools.tsx

"use client"

import { useEffect, useState, useCallback } from "react";
import { useEntityResolution } from "@/context/entity-resolution-context";
// postEntityGroupFeedback and triggerClusterFinalization are now called by the context
// import {
//   postEntityGroupFeedback,
//   triggerClusterFinalization
// } from "@/utils/api-client";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
import { Badge } from "@/components/ui/badge";
import { Check, ChevronDown, ChevronUp, X, AlertCircle, Info, Loader2, AlertTriangle, RefreshCw, CheckCircle, XCircle, SkipForward, RotateCcw } from "lucide-react";
import type {
  VisualizationEntityEdge,
  EntityGroup,
  MatchDecisionDetails,
  Entity,
  EntityGroupReviewDecision,
  // EntityGroupReviewApiPayload, // No longer directly used here
  QueuedReviewBatch, // Added for explicit typing
} from "@/types/entity-resolution";
// useAuth is used by the context now
// import { useAuth } from "@/context/auth-context";
import { useToast } from "@/hooks/use-toast";


// Helper function to extract contact information (remains the same)
const getContactInfo = (entityGroups: EntityGroup[], entityId: string) => {
  const contactInfo: {
    email?: string;
    phone?: string;
    address?: string;
    url?: string;
  } = {};

  for (const group of entityGroups) {
    const values = group.match_values.values;
    const isEntity1 = group.entity_id_1 === entityId;
    const isEntity2 = group.entity_id_2 === entityId;
    if (!isEntity1 && !isEntity2) continue;

    switch (group.match_values.type?.toLowerCase()) {
      case 'email':
        contactInfo.email = isEntity1 ? values.original_email1 : values.original_email2;
        break;
      case 'phone':
        contactInfo.phone = isEntity1 ? values.original_phone1 : values.original_phone2;
        break;
      case 'address':
        contactInfo.address = isEntity1 ? values.original_address1 : values.original_address2;
        break;
      case 'url':
        contactInfo.url = isEntity1 ? values.original_url1 : values.original_url2;
        break;
    }
  }
  return contactInfo;
};

// Helper function to get suggested actions (remains the same)
const getSuggestedActions = (entityGroups: EntityGroup[]) => {
  const actions: { action_type: string }[] = [];
  const confirmedGroups = entityGroups.filter(g => g.confirmed_status === 'CONFIRMED_MATCH');
  const deniedGroups = entityGroups.filter(g => g.confirmed_status === 'CONFIRMED_NON_MATCH');
  if (confirmedGroups.length > 0 && deniedGroups.length > 0) {
    actions.push({ action_type: 'conflicting_decisions' });
  }
  return actions;
};


export default function ConnectionReviewTools() {
  const {
    selectedEdgeId,
    // selectedClusterId, // Not directly needed here for submission
    currentConnectionData,
    actions,
    queries,
    reviewQueue, // Directly access reviewQueue here
  } = useEntityResolution();
  // const { currentUser } = useAuth(); // Context handles user auth for submissions
  const { toast } = useToast();

  const [isSubmitting, setIsSubmitting] = useState(false); // Local submitting state for button feedback
  const [isExpanded, setIsExpanded] = useState(true);
  // Feedback message is now handled by global toasts via context for API errors
  // const [feedbackMessage, setFeedbackMessage] = useState<{type: 'error' | 'success' | 'info', text: string} | null>(null);

  const edgeStatus = selectedEdgeId ? queries.getEdgeStatus(selectedEdgeId) : null;
  const isEdgeReviewed = selectedEdgeId ? queries.isEdgeReviewed(selectedEdgeId) : false;
  const queueStatus = selectedEdgeId ? queries.getQueueItemStatus(selectedEdgeId) : null;

  useEffect(() => {
    if (selectedEdgeId && !currentConnectionData && !queries.isConnectionDataLoading(selectedEdgeId)) {
      actions.loadConnectionData(selectedEdgeId);
    }
  }, [selectedEdgeId, currentConnectionData, queries, actions]);

  useEffect(() => {
    // Reset local submitting state when edge changes
    setIsSubmitting(false);
    setIsExpanded(true);
  }, [selectedEdgeId]);

  const handleReviewDecision = async (decision: EntityGroupReviewDecision) => {
    if (!selectedEdgeId) {
      toast({ title: "Error", description: "No connection selected.", variant: "destructive" });
      return;
    }
    // Prevent re-review if already reviewed and not failed in queue
    if (isEdgeReviewed && queueStatus !== 'failed') {
      // FIX: Removed `type: 'info'` as it's not a valid variant.
      toast({ title: "Already Reviewed", description: "This connection has already been reviewed." });
      return;
    }
    if (queueStatus === 'processing' || queueStatus === 'pending') {
      // FIX: Removed `type: 'info'`
      toast({ title: "In Progress", description: "This connection review is already being processed." });
      return;
    }


    setIsSubmitting(true);
    try {
      await actions.submitEdgeReview(selectedEdgeId, decision);
      // UI updates optimistically via context. Toast for success could be added here if desired,
      // but context handles failure toasts.
      // Example: toast({ title: "Review Queued", description: "Your decision has been recorded and will be synced." });
    } catch (error) {
      // This catch is for errors during the optimistic update/queuing process itself,
      // not for API submission errors (which context handles).
      toast({ title: "Submission Error", description: (error as Error).message, variant: "destructive" });
    } finally {
      setIsSubmitting(false);
    }
  };

  const handleRetryLoad = useCallback(() => {
    if (selectedEdgeId) {
      actions.invalidateConnectionData(selectedEdgeId); // Force a fresh load
      actions.loadConnectionData(selectedEdgeId);
    }
  }, [selectedEdgeId, actions]);

  const handleSkipToNext = useCallback(() => {
    if (selectedEdgeId) {
        actions.selectNextUnreviewedEdge(selectedEdgeId);
    } else {
        actions.selectNextUnreviewedEdge();
    }
  }, [actions, selectedEdgeId]);

  const handleRetryQueueItem = () => {
    if (selectedEdgeId && queueStatus === 'failed') {
      // FIX: Access reviewQueue directly, not via actions
      // FIX: Explicitly type 'b' as QueuedReviewBatch for clarity, though TS should infer it now.
      const batchToRetry = reviewQueue.find((b: QueuedReviewBatch) => b.edgeId === selectedEdgeId && b.isTerminalFailure);
      if (batchToRetry) {
        actions.retryFailedBatch(batchToRetry.batchId);
        toast({title: "Retrying Submission", description: `Retrying review for connection ${selectedEdgeId}.`});
      } else {
        toast({title: "Retry Error", description: "Could not find failed batch to retry.", variant: "destructive"});
      }
    }
  };


  const isLoading = selectedEdgeId ? queries.isConnectionDataLoading(selectedEdgeId) : false;
  const error = selectedEdgeId ? queries.getConnectionError(selectedEdgeId) : null;

  if (!selectedEdgeId && !isLoading) {
    return (
      <div className="flex justify-center items-center h-[100px] text-muted-foreground p-4 border rounded-md bg-card shadow">
        Select a connection from the graph to review its details.
      </div>
    );
  }

  if (isLoading && !currentConnectionData) { // Show loader only if no data is present yet
    return (
      <div className="flex justify-center items-center h-[100px] border rounded-md bg-card shadow">
        <Loader2 className="h-6 w-6 animate-spin text-primary" />
      </div>
    );
  }

  if (error && !currentConnectionData) { // Show error only if no data is present
    return (
      <Card className="h-full flex flex-col items-center justify-center">
        <CardHeader>
          <CardTitle className="flex items-center"><AlertCircle className="h-6 w-6 mr-2 text-destructive" /> Error Loading Details</CardTitle>
        </CardHeader>
        <CardContent className="text-center">
          <p className="text-destructive mb-2">Could not load details for the selected connection.</p>
          {error && <p className="text-xs mt-1 text-muted-foreground">{error}</p>}
          <div className="flex gap-2 mt-3">
            <Button variant="outline" size="sm" onClick={handleRetryLoad}>
              <RefreshCw className="h-4 w-4 mr-1" /> Retry
            </Button>
            <Button variant="outline" size="sm" onClick={() => actions.setSelectedEdgeId(null)}>
              Clear Selection
            </Button>
          </div>
        </CardContent>
      </Card>
    );
  }

  if (!currentConnectionData) return null; // Should be covered by loading/error states if edge is selected

  const { edge, entityGroups, entity1, entity2 } = currentConnectionData;

  const currentEdgeEntityGroups = entityGroups.filter(group =>
    (group.entity_id_1 === edge.entity_id_1 && group.entity_id_2 === edge.entity_id_2) ||
    (group.entity_id_1 === edge.entity_id_2 && group.entity_id_2 === edge.entity_id_1)
  );

  const suggestedActions = getSuggestedActions(currentEdgeEntityGroups);
  const entity1ContactInfo = getContactInfo(currentEdgeEntityGroups, edge.entity_id_1);
  const entity2ContactInfo = getContactInfo(currentEdgeEntityGroups, edge.entity_id_2);

  const showReviewButtons = !isEdgeReviewed || queueStatus === 'failed';


  return (
    <Collapsible
      open={isExpanded}
      onOpenChange={setIsExpanded}
      className="h-full flex flex-col"
    >
      <div className="flex justify-between items-center flex-shrink-0 pb-2 border-b mb-3">
        <div className="flex items-center gap-2">
          <h3 className="text-lg font-medium">Connection Review</h3>
          {queueStatus === 'processing' && <Badge variant="outline" className="bg-blue-50 text-blue-700 border-blue-300"><Loader2 className="h-3 w-3 mr-1 animate-spin"/>Processing</Badge>}
          {queueStatus === 'pending' && <Badge variant="outline" className="bg-yellow-50 text-yellow-700 border-yellow-300"><Info className="h-3 w-3 mr-1"/>Queued</Badge>}
          {queueStatus === 'failed' && <Badge variant="destructive"><AlertTriangle className="h-3 w-3 mr-1"/>Failed</Badge>}

          {isEdgeReviewed && !queueStatus && ( // Only show if not in queue
            <Badge
              variant={edgeStatus === 'CONFIRMED_MATCH' ? 'default' : 'secondary'}
              className={`${edgeStatus === 'CONFIRMED_MATCH' ? 'bg-green-100 text-green-800 border-green-300' : 'bg-red-100 text-red-800 border-red-300'}`}
            >
              {edgeStatus === 'CONFIRMED_MATCH' ? <CheckCircle className="h-3 w-3 mr-1" /> : <XCircle className="h-3 w-3 mr-1" />}
              {edgeStatus === 'CONFIRMED_MATCH' ? 'Match' : 'Non-Match'}
            </Badge>
          )}
          {suggestedActions.length > 0 && showReviewButtons && (
            <Badge variant="outline" className="bg-yellow-50 text-yellow-800 border-yellow-300">
              <AlertTriangle className="h-3 w-3 mr-1" />
              {suggestedActions[0].action_type.replace(/_/g, " ")}
            </Badge>
          )}
        </div>
        <CollapsibleTrigger asChild>
          <Button variant="ghost" size="sm">
            {isExpanded ? <ChevronUp className="h-4 w-4" /> : <ChevronDown className="h-4 w-4" />}
            <span className="sr-only">Toggle review panel</span>
          </Button>
        </CollapsibleTrigger>
      </div>

      <CollapsibleContent className="flex-1 min-h-0">
        <div className="h-full overflow-y-auto space-y-3 pr-2 custom-scrollbar">
          {showReviewButtons ? (
            <div className="space-y-3 p-1">
              <p className="text-sm text-muted-foreground">
                Do these records represent the same real-world entity?
              </p>
              <div className="flex flex-col sm:flex-row justify-between gap-2">
                <Button
                  variant="outline"
                  className="border-red-500 text-red-600 hover:bg-red-50 hover:text-red-700 flex-1"
                  size="sm"
                  onClick={() => handleReviewDecision('REJECTED')}
                  disabled={isSubmitting || queueStatus === 'processing' || queueStatus === 'pending'}
                >
                  <X className="h-4 w-4 mr-1" />
                  Not a Match
                </Button>
                <Button
                  variant="default"
                  className="bg-green-600 hover:bg-green-700 flex-1"
                  size="sm"
                  onClick={() => handleReviewDecision('ACCEPTED')}
                  disabled={isSubmitting || queueStatus === 'processing' || queueStatus === 'pending'}
                >
                  <Check className="h-4 w-4 mr-1" />
                  Confirm Match
                </Button>
              </div>
               {queueStatus === 'failed' && (
                <Button
                    variant="outline"
                    size="sm"
                    onClick={handleRetryQueueItem}
                    className="w-full mt-2 border-amber-500 text-amber-600 hover:bg-amber-50"
                    disabled={isSubmitting}
                >
                    <RotateCcw className="h-4 w-4 mr-1" />
                    Retry Failed Submission
                </Button>
              )}
            </div>
          ) : (
            <div className="space-y-3 p-1">
              <Card className={`border-2 ${edgeStatus === 'CONFIRMED_MATCH' ? 'border-green-200 bg-green-50' : 'border-red-200 bg-red-50'}`}>
                <CardContent className="p-3">
                  <div className="flex items-center gap-3">
                    {edgeStatus === 'CONFIRMED_MATCH' ? <CheckCircle className="h-6 w-6 text-green-600" /> : <XCircle className="h-6 w-6 text-red-600" />}
                    <div>
                      <h4 className="font-semibold text-sm">
                        {edgeStatus === 'CONFIRMED_MATCH' ? 'Match Confirmed' : 'Non-Match Confirmed'}
                      </h4>
                      <p className="text-xs text-muted-foreground">This connection has been reviewed.</p>
                    </div>
                  </div>
                </CardContent>
              </Card>
              <Button variant="outline" size="sm" onClick={handleSkipToNext} className="w-full">
                <SkipForward className="h-4 w-4 mr-1" /> Continue to Next
              </Button>
            </div>
          )}

          <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
            <Card>
              <CardHeader className="p-3">
                <CardTitle className="text-sm">Entity 1</CardTitle>
              </CardHeader>
              <CardContent className="p-3 pt-0">
                {entity1 ? (
                  <div className="space-y-1 text-xs">
                    <div><span className="font-medium">Name:</span> {entity1.name || 'N/A'}</div>
                    <div><span className="font-medium">Source:</span> {entity1.source_system || 'N/A'} {entity1.source_id && `(${entity1.source_id})`}</div>
                    {entity1ContactInfo.address && <div><span className="font-medium">Address:</span> {entity1ContactInfo.address}</div>}
                    {entity1ContactInfo.phone && <div><span className="font-medium">Phone:</span> {entity1ContactInfo.phone}</div>}
                    {entity1ContactInfo.email && <div><span className="font-medium">Email:</span> {entity1ContactInfo.email}</div>}
                    {entity1ContactInfo.url && <div><span className="font-medium">URL:</span> <a href={entity1ContactInfo.url} target="_blank" rel="noopener noreferrer" className="text-blue-600 hover:underline break-all">{entity1ContactInfo.url}</a></div>}
                  </div>
                ) : <p className="text-xs text-muted-foreground">Details not available.</p>}
              </CardContent>
            </Card>

            <Card>
              <CardHeader className="p-3">
                <CardTitle className="text-sm">Entity 2</CardTitle>
              </CardHeader>
              <CardContent className="p-3 pt-0">
                {entity2 ? (
                  <div className="space-y-1 text-xs">
                    <div><span className="font-medium">Name:</span> {entity2.name || 'N/A'}</div>
                    <div><span className="font-medium">Source:</span> {entity2.source_system || 'N/A'} {entity2.source_id && `(${entity2.source_id})`}</div>
                    {entity2ContactInfo.address && <div><span className="font-medium">Address:</span> {entity2ContactInfo.address}</div>}
                    {entity2ContactInfo.phone && <div><span className="font-medium">Phone:</span> {entity2ContactInfo.phone}</div>}
                    {entity2ContactInfo.email && <div><span className="font-medium">Email:</span> {entity2ContactInfo.email}</div>}
                    {entity2ContactInfo.url && <div><span className="font-medium">URL:</span> <a href={entity2ContactInfo.url} target="_blank" rel="noopener noreferrer" className="text-blue-600 hover:underline break-all">{entity2ContactInfo.url}</a></div>}
                  </div>
                ) : <p className="text-xs text-muted-foreground">Details not available.</p>}
              </CardContent>
            </Card>
          </div>

          <Tabs defaultValue="matching-methods">
            <TabsList className="grid w-full grid-cols-1">
              <TabsTrigger value="matching-methods">Matching Methods</TabsTrigger>
            </TabsList>
            <TabsContent value="matching-methods" className="space-y-3">
              <Card>
                <CardHeader className="p-3">
                  <CardTitle className="text-sm flex justify-between items-center">
                    <span>Matching Methods</span>
                    <span className="text-xs font-normal">Overall Confidence: <span className="font-medium">{edge?.edge_weight?.toFixed(2) ?? 'N/A'}</span></span>
                  </CardTitle>
                </CardHeader>
                <CardContent className="p-3 pt-0">
                  {edge?.details?.methods && edge.details.methods.length > 0 ? (
                    <div className="space-y-1">
                      {edge.details.methods.map((method: any, index: number) => ( // Added type any for method temporarily
                        <div key={index} className="grid grid-cols-[1fr_auto_auto_auto] gap-2 items-center text-xs">
                          <div>{method.method_type?.replace(/_/g, " ") ?? 'Unknown Method'}</div>
                          <div className="text-right"><span className="text-xs text-muted-foreground">Pre-RL:</span> {method.pre_rl_confidence?.toFixed(2) ?? 'N/A'}</div>
                          <div className="text-right"><span className="text-xs text-muted-foreground">RL:</span> {method.rl_confidence?.toFixed(2) ?? 'N/A'}</div>
                          <div className="text-right font-medium">{method.combined_confidence?.toFixed(2) ?? 'N/A'}</div>
                        </div>
                      ))}
                    </div>
                  ) : <p className="text-xs text-muted-foreground">No specific matching methods detailed.</p>}
                </CardContent>
              </Card>

              {currentEdgeEntityGroups.length > 0 && (
                <Card>
                  <CardHeader className="p-3"><CardTitle className="text-sm">Underlying Group Details</CardTitle></CardHeader>
                  <CardContent className="p-3 pt-0 space-y-2">
                    {currentEdgeEntityGroups.map((group) => (
                      <div key={group.id} className="rounded-md border bg-muted/30 p-2">
                        <div className="flex justify-between items-center mb-1">
                          <span className="text-xs font-medium capitalize">{group.method_type.replace(/_/g, " ")} Match</span>
                          <Badge variant={group.confirmed_status === 'CONFIRMED_MATCH' ? 'default' : group.confirmed_status === 'CONFIRMED_NON_MATCH' ? 'destructive' : 'outline'} className="text-xs">
                            {group.confirmed_status?.replace(/_/g, " ") ?? 'Pending'}
                          </Badge>
                          <span className="text-xs text-muted-foreground">{group.confidence_score ? group.confidence_score.toFixed(3) : 'N/A'}</span>
                        </div>
                        <div className="text-xs text-muted-foreground space-y-0.5">
                          {/* Simplified display of match_values for brevity */}
                          {Object.entries(group.match_values.values).map(([key, val]) => (
                            <div key={key} className="truncate"><span className="font-medium">{key.replace(/original_|_1|_2/gi, '')}:</span> {String(val)}</div>
                          )).slice(0,2) /* Show first 2 match values */}
                        </div>
                      </div>
                    ))}
                  </CardContent>
                </Card>
              )}
            </TabsContent>
          </Tabs>
        </div>
      </CollapsibleContent>
    </Collapsible>
  );
}

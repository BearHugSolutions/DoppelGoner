// components/connection-review-tools.tsx

"use client"

import { useEffect, useState, useCallback } from "react";
import { useEntityResolution } from "@/context/entity-resolution-context";
import {
  postEntityGroupFeedback,
  triggerClusterFinalization
} from "@/utils/api-client";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
import { Badge } from "@/components/ui/badge";
import { Check, ChevronDown, ChevronUp, X, AlertCircle, Info, Loader2, AlertTriangle, RefreshCw, CheckCircle, XCircle, SkipForward } from "lucide-react";
import type {
  VisualizationEntityEdge,
  EntityGroup,
  MatchDecisionDetails,
  Entity,
  EntityGroupReviewDecision,
  EntityGroupReviewApiPayload,
} from "@/types/entity-resolution";
import { useAuth } from "@/context/auth-context";

// Helper function to extract contact information from match values
const getContactInfo = (entityGroups: EntityGroup[], entityId: string) => {
  const contactInfo: {
    email?: string;
    phone?: string;
    address?: string;
    url?: string;
  } = {};

  for (const group of entityGroups) {
    const values = group.match_values.values;
    
    // Check if this entity is involved in this group
    const isEntity1 = group.entity_id_1 === entityId;
    const isEntity2 = group.entity_id_2 === entityId;
    
    if (!isEntity1 && !isEntity2) continue;

    // Extract information based on match type
    switch (group.match_values.type?.toLowerCase()) {
      case 'email':
        if (isEntity1 && values.original_email1) {
          contactInfo.email = values.original_email1;
        } else if (isEntity2 && values.original_email2) {
          contactInfo.email = values.original_email2;
        }
        break;
      
      case 'phone':
        if (isEntity1 && values.original_phone1) {
          contactInfo.phone = values.original_phone1;
        } else if (isEntity2 && values.original_phone2) {
          contactInfo.phone = values.original_phone2;
        }
        break;
      
      case 'address':
        if (isEntity1 && values.original_address1) {
          contactInfo.address = values.original_address1;
        } else if (isEntity2 && values.original_address2) {
          contactInfo.address = values.original_address2;
        }
        break;
      
      case 'url':
        if (isEntity1 && values.original_url1) {
          contactInfo.url = values.original_url1;
        } else if (isEntity2 && values.original_url2) {
          contactInfo.url = values.original_url2;
        }
        break;
    }
  }

  return contactInfo;
};

// Helper function to get suggested actions (if any critical issues)
const getSuggestedActions = (entityGroups: EntityGroup[]) => {
  const actions: { action_type: string }[] = [];
  
  // Example: if there are conflicting confirmations
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
    selectedClusterId, 
    currentConnectionData,
    actions,
    queries,
  } = useEntityResolution();
  const { currentUser } = useAuth();

  const [submitting, setSubmitting] = useState(false);
  const [isExpanded, setIsExpanded] = useState(true);
  const [feedbackMessage, setFeedbackMessage] = useState<{type: 'error' | 'success' | 'info', text: string} | null>(null);

  // Get edge review status directly
  const edgeStatus = selectedEdgeId ? queries.getEdgeStatus(selectedEdgeId) : null;
  const isEdgeReviewed = selectedEdgeId ? queries.isEdgeReviewed(selectedEdgeId) : false;

  // Load connection data when edge changes
  useEffect(() => {
    if (selectedEdgeId && !currentConnectionData && !queries.isConnectionDataLoading(selectedEdgeId)) {
      actions.loadConnectionData(selectedEdgeId);
    }
  }, [selectedEdgeId, currentConnectionData, queries, actions]);

  // Reset UI state when edge changes
  useEffect(() => {
    setFeedbackMessage(null);
    setIsExpanded(true);
  }, [selectedEdgeId]);

  const handleSubmitFeedback = async (decision: EntityGroupReviewDecision) => {
    if (!selectedEdgeId || !currentConnectionData?.edge || !currentUser?.id || !selectedClusterId) {
      setFeedbackMessage({type: 'error', text: "Cannot submit feedback: Missing critical data (edge, user, or cluster)."});
      return;
    }
    
    // Prevent re-review of already reviewed edges
    if (isEdgeReviewed) {
      setFeedbackMessage({type: 'info', text: "This connection has already been reviewed and cannot be changed."});
      return;
    }
    
    const { edge, entityGroups } = currentConnectionData;
    
    const relevantEntityGroups = entityGroups.filter(group =>
        (group.entity_id_1 === edge.entity_id_1 && group.entity_id_2 === edge.entity_id_2) ||
        (group.entity_id_1 === edge.entity_id_2 && group.entity_id_2 === edge.entity_id_1)
    );

    if (relevantEntityGroups.length === 0) {
        setFeedbackMessage({type: 'info', text: "No underlying match groups found for this specific connection. Marking as reviewed and advancing."});
        actions.setLastReviewedEdgeId(selectedEdgeId);
        actions.selectNextUnreviewedEdge();
        return;
    }

    setSubmitting(true);
    setFeedbackMessage(null);

    try {
      for (const group of relevantEntityGroups) {
        const payload: EntityGroupReviewApiPayload = {
          decision,
          reviewerId: currentUser.id,
        };
        await postEntityGroupFeedback(group.id, payload);
      }
      
      setFeedbackMessage({type: 'success', text: "Feedback submitted. Finalizing cluster review..."});

      // Invalidate data to force refresh with new edge status
      actions.invalidateConnectionData(selectedEdgeId);
      actions.invalidateVisualizationData(selectedClusterId);

      // Attempt to finalize the cluster
      try {
        const finalizationResponse = await triggerClusterFinalization(selectedClusterId);
        console.log('Cluster finalization status:', finalizationResponse);
        
        let finalMessage = `Cluster finalization: ${finalizationResponse.message}`;
        if (finalizationResponse.status === 'COMPLETED_SPLIT_OCCURRED' && finalizationResponse.newClusterIds?.length) {
          finalMessage += ` Original cluster ${finalizationResponse.originalClusterId} was split. New clusters: ${finalizationResponse.newClusterIds.join(', ')}.`;
        }
        setFeedbackMessage({type: 'info', text: finalMessage});
      } catch (finalizationError) {
        console.log('Cluster finalization not ready yet:', finalizationError);
        setFeedbackMessage({type: 'success', text: "Feedback submitted successfully. Continue reviewing other connections in this cluster."});
      }

      // Mark this edge as reviewed and trigger refresh
      actions.setLastReviewedEdgeId(selectedEdgeId);
      actions.triggerRefresh(); // This will trigger data refresh

      // IMPORTANT: Only auto-advance after submitting feedback
      // This replaces the automatic effect that was in the context
      actions.checkAndAdvanceIfComplete();

    } catch (error) {
      console.error("Failed to submit feedback:", error);
      const errorMessage = error instanceof Error ? error.message : "An unknown error occurred.";
      setFeedbackMessage({type: 'error', text: `Error: ${errorMessage}`});
    } finally {
      setSubmitting(false);
    }
  };

  const handleRetryLoad = useCallback(() => {
    if (selectedEdgeId) {
      actions.invalidateConnectionData(selectedEdgeId);
      actions.loadConnectionData(selectedEdgeId);
    }
  }, [selectedEdgeId, actions]);

  const handleSkipToNext = useCallback(() => {
    actions.selectNextUnreviewedEdge();
  }, [actions]);

  // Get current loading/error state
  const isLoading = selectedEdgeId ? queries.isConnectionDataLoading(selectedEdgeId) : false;
  const error = selectedEdgeId ? queries.getConnectionError(selectedEdgeId) : null;

  if (!selectedEdgeId && !isLoading) {
    return (
      <div className="flex justify-center items-center h-[100px] text-muted-foreground p-4 border rounded-md bg-card shadow">
        Select a connection from the graph to review its details.
      </div>
    );
  }

  if (isLoading) {
    return (
      <div className="flex justify-center items-center h-[100px] border rounded-md bg-card shadow">
        <Loader2 className="h-6 w-6 animate-spin text-primary" />
      </div>
    );
  }
  
  if (error || (!currentConnectionData && !isLoading)) {
    return (
      <div className="flex flex-col justify-center items-center h-[150px] text-destructive p-4 border rounded-md bg-card shadow">
        <AlertCircle className="h-6 w-6 mb-2" />
        <p className="font-semibold">Could not load details for the selected connection.</p>
        {error && <p className="text-xs mt-1">{error}</p>}
        <div className="flex gap-2 mt-3">
          <Button variant="outline" size="sm" onClick={handleRetryLoad}>
            <RefreshCw className="h-4 w-4 mr-1" />
            Retry
          </Button>
          <Button variant="outline" size="sm" onClick={() => actions.setSelectedEdgeId(null)}>
            Clear Selection
          </Button>
        </div>
      </div>
    );
  }
  
  if (!currentConnectionData) return null;

  const { edge, entityGroups, entity1, entity2 } = currentConnectionData;

  const currentEdgeEntityGroups = entityGroups.filter(group =>
    (group.entity_id_1 === edge.entity_id_1 && group.entity_id_2 === edge.entity_id_2) ||
    (group.entity_id_1 === edge.entity_id_2 && group.entity_id_2 === edge.entity_id_1)
  );

  const suggestedActions = getSuggestedActions(currentEdgeEntityGroups);
  const entity1ContactInfo = getContactInfo(currentEdgeEntityGroups, edge.entity_id_1);
  const entity2ContactInfo = getContactInfo(currentEdgeEntityGroups, edge.entity_id_2);

  return (
    <Collapsible
      open={isExpanded}
      onOpenChange={setIsExpanded}
      className="h-full flex flex-col"
    >
      <div className="flex justify-between items-center flex-shrink-0 pb-2">
        <div className="flex items-center gap-2">
          <h3 className="text-lg font-medium">Connection Review</h3>
          {/* Show review status badge */}
          {isEdgeReviewed && (
            <Badge 
              variant={edgeStatus === 'CONFIRMED_MATCH' ? 'default' : 'secondary'}
              className={`
                ${edgeStatus === 'CONFIRMED_MATCH' 
                  ? 'bg-green-100 text-green-800 border-green-300' 
                  : 'bg-red-100 text-red-800 border-red-300'
                }
              `}
            >
              {edgeStatus === 'CONFIRMED_MATCH' ? (
                <>
                  <CheckCircle className="h-3 w-3 mr-1" />
                  Confirmed Match
                </>
              ) : (
                <>
                  <XCircle className="h-3 w-3 mr-1" />
                  Confirmed Non-Match
                </>
              )}
            </Badge>
          )}
          {suggestedActions.length > 0 && !isEdgeReviewed && (
            <Badge variant="outline" className="bg-yellow-50 text-yellow-800 border-yellow-300">
              <AlertTriangle className="h-3 w-3 mr-1" />
              {suggestedActions[0].action_type.replace(/_/g, " ")}
            </Badge>
          )}
        </div>
        <CollapsibleTrigger asChild>
          <Button variant="ghost" size="sm">
            {isExpanded ? <ChevronUp className="h-4 w-4" /> : <ChevronDown className="h-4 w-4" />}
          </Button>
        </CollapsibleTrigger>
      </div>

      <CollapsibleContent className="flex-1 min-h-0">
        <div className="h-full overflow-auto space-y-3 pr-2">
          {/* Show different UI based on review status */}
          {isEdgeReviewed ? (
            <div className="space-y-3">
              <Card className={`border-2 ${
                edgeStatus === 'CONFIRMED_MATCH' 
                  ? 'border-green-200 bg-green-50' 
                  : 'border-red-200 bg-red-50'
              }`}>
                <CardContent className="p-4">
                  <div className="flex items-center gap-3">
                    {edgeStatus === 'CONFIRMED_MATCH' ? (
                      <CheckCircle className="h-8 w-8 text-green-600" />
                    ) : (
                      <XCircle className="h-8 w-8 text-red-600" />
                    )}
                    <div>
                      <h4 className="font-semibold">
                        {edgeStatus === 'CONFIRMED_MATCH' ? 'Match Confirmed' : 'Non-Match Confirmed'}
                      </h4>
                      <p className="text-sm text-muted-foreground">
                        This connection has been reviewed and marked as a{' '}
                        {edgeStatus === 'CONFIRMED_MATCH' ? 'confirmed match' : 'confirmed non-match'}.
                      </p>
                    </div>
                  </div>
                </CardContent>
              </Card>
              
              <Button 
                variant="outline" 
                size="sm" 
                onClick={handleSkipToNext}
                className="w-full"
              >
                <SkipForward className="h-4 w-4 mr-1" />
                Continue to Next Unreviewed Connection
              </Button>
            </div>
          ) : (
            <div className="space-y-3">
              <div className="flex justify-between">
                <Button 
                  variant="destructive" 
                  size="sm" 
                  onClick={() => handleSubmitFeedback('REJECTED')} 
                  disabled={submitting}
                >
                  <X className="h-4 w-4 mr-1" />
                  Not a Match
                </Button>

                <Button
                  variant="default"
                  size="sm"
                  onClick={() => handleSubmitFeedback('ACCEPTED')}
                  disabled={submitting}
                  className="bg-green-600 hover:bg-green-700"
                >
                  <Check className="h-4 w-4 mr-1" />
                  Confirm Match
                </Button>
              </div>
              
              <p className="text-sm text-muted-foreground">
                Do these records represent the same real-world entity?
              </p>
            </div>
          )}
          
          {/* Entity details cards */}
          <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
            <Card>
              <CardContent className="p-3">
                <h4 className="font-medium mb-1 text-sm">Entity 1</h4>
                {entity1 && (
                  <div className="space-y-1 text-xs">
                    <div>
                      <span className="font-medium">Name:</span> {entity1.name || 'Unknown'}
                    </div>
                    <div>
                      <span className="font-medium">Source:</span> {entity1.source_system || 'Unknown'} 
                      {entity1.source_id && ` (ID: ${entity1.source_id})`}
                    </div>
                    {entity1ContactInfo.address && (
                      <div>
                        <span className="font-medium">Address:</span> {entity1ContactInfo.address}
                      </div>
                    )}
                    {entity1ContactInfo.phone && (
                      <div>
                        <span className="font-medium">Phone:</span> {entity1ContactInfo.phone}
                      </div>
                    )}
                    {entity1ContactInfo.email && (
                      <div>
                        <span className="font-medium">Email:</span> {entity1ContactInfo.email}
                      </div>
                    )}
                    {entity1ContactInfo.url && (
                      <div>
                        <span className="font-medium">URL:</span> 
                        <a href={entity1ContactInfo.url} target="_blank" rel="noopener noreferrer" className="text-blue-600 hover:underline ml-1">
                          {entity1ContactInfo.url}
                        </a>
                      </div>
                    )}
                  </div>
                )}
              </CardContent>
            </Card>
            
            <Card>
              <CardContent className="p-3">
                <h4 className="font-medium mb-1 text-sm">Entity 2</h4>
                {entity2 && (
                  <div className="space-y-1 text-xs">
                    <div>
                      <span className="font-medium">Name:</span> {entity2.name || 'Unknown'}
                    </div>
                    <div>
                      <span className="font-medium">Source:</span> {entity2.source_system || 'Unknown'}
                      {entity2.source_id && ` (ID: ${entity2.source_id})`}
                    </div>
                    {entity2ContactInfo.address && (
                      <div>
                        <span className="font-medium">Address:</span> {entity2ContactInfo.address}
                      </div>
                    )}
                    {entity2ContactInfo.phone && (
                      <div>
                        <span className="font-medium">Phone:</span> {entity2ContactInfo.phone}
                      </div>
                    )}
                    {entity2ContactInfo.email && (
                      <div>
                        <span className="font-medium">Email:</span> {entity2ContactInfo.email}
                      </div>
                    )}
                    {entity2ContactInfo.url && (
                      <div>
                        <span className="font-medium">URL:</span> 
                        <a href={entity2ContactInfo.url} target="_blank" rel="noopener noreferrer" className="text-blue-600 hover:underline ml-1">
                          {entity2ContactInfo.url}
                        </a>
                      </div>
                    )}
                  </div>
                )}
              </CardContent>
            </Card>
          </div>

          <Tabs defaultValue="matching-methods">
            <TabsList className="grid w-full grid-cols-1">
              <TabsTrigger value="matching-methods">Matching Methods</TabsTrigger>
            </TabsList>

            <TabsContent value="matching-methods" className="space-y-3">
              <div className="rounded-md border">
                <div className="bg-muted px-3 py-1 rounded-t-md flex justify-between items-center">
                  <h4 className="font-medium text-xs">Matching Methods</h4>
                  <div className="text-xs">
                    Overall Confidence: <span className="font-medium">{edge?.edge_weight.toFixed(2)}</span>
                  </div>
                </div>
                <div className="p-2">
                  <div className="space-y-1">
                    {edge?.details.methods.map((method, index) => (
                      <div key={index} className="grid grid-cols-[1fr_auto_auto_auto] gap-2 items-center text-xs">
                        <div>{method.method_type.replace(/_/g, " ")}</div>
                        <div className="text-right">
                          <span className="text-xs text-muted-foreground">Pre-RL:</span>{" "}
                          {method.pre_rl_confidence.toFixed(2)}
                        </div>
                        <div className="text-right">
                          <span className="text-xs text-muted-foreground">RL:</span> {method.rl_confidence.toFixed(2)}
                        </div>
                        <div className="text-right font-medium">{method.combined_confidence.toFixed(2)}</div>
                      </div>
                    ))}
                  </div>
                </div>
              </div>

              {/* Detailed Entity Groups Information */}
              {currentEdgeEntityGroups.length > 0 && (
                <div className="space-y-2">
                  <h5 className="text-xs font-medium text-muted-foreground">
                    {isEdgeReviewed ? 'Match Details (Reference)' : 'Detailed Match Information'}
                  </h5>
                  {currentEdgeEntityGroups.map((group) => (
                    <div key={group.id} className="rounded-md border bg-muted/20 p-2">
                      <div className="flex justify-between items-center mb-1">
                        <span className="text-xs font-medium capitalize">
                          {group.method_type.replace(/_/g, " ")} Match
                        </span>
                        <span className="text-xs text-muted-foreground">
                          {group.confidence_score ? group.confidence_score.toFixed(3) : 'N/A'}
                        </span>
                      </div>
                      <div className="text-xs text-muted-foreground space-y-0.5">
                        {group.match_values.type === 'Email' && (
                          <>
                            <div>Email 1: {group.match_values.values.original_email1}</div>
                            <div>Email 2: {group.match_values.values.original_email2}</div>
                          </>
                        )}
                        {group.match_values.type === 'Phone' && (
                          <>
                            <div>Phone 1: {group.match_values.values.original_phone1}</div>
                            <div>Phone 2: {group.match_values.values.original_phone2}</div>
                          </>
                        )}
                        {group.match_values.type === 'Address' && (
                          <>
                            <div>Address 1: {group.match_values.values.original_address1}</div>
                            <div>Address 2: {group.match_values.values.original_address2}</div>
                          </>
                        )}
                        {group.match_values.type === 'Url' && (
                          <>
                            <div>URL 1: {group.match_values.values.original_url1}</div>
                            <div>URL 2: {group.match_values.values.original_url2}</div>
                          </>
                        )}
                        {group.match_values.type === 'Name' && (
                          <>
                            <div>Name 1: {group.match_values.values.original_name1}</div>
                            <div>Name 2: {group.match_values.values.original_name2}</div>
                          </>
                        )}
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </TabsContent>
          </Tabs>

          {/* Feedback Messages */}
          {feedbackMessage && (
            <div className={`text-xs p-2 rounded-md flex items-start ${
              feedbackMessage.type === 'error' ? 'bg-destructive/10 text-destructive' : 
              feedbackMessage.type === 'success' ? 'bg-green-600/10 text-green-700' : 
              'bg-blue-600/10 text-blue-700'
            }`}>
              {feedbackMessage.type === 'error' && <AlertCircle className="h-4 w-4 mr-2 flex-shrink-0 mt-0.5"/>}
              {feedbackMessage.type === 'success' && <Check className="h-4 w-4 mr-2 flex-shrink-0 mt-0.5"/>}
              {feedbackMessage.type === 'info' && <Info className="h-4 w-4 mr-2 flex-shrink-0 mt-0.5"/>}
              <span>{feedbackMessage.text}</span>
            </div>
          )}
        </div>
      </CollapsibleContent>
    </Collapsible>
  );
}
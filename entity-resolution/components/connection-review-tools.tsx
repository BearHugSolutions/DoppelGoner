// components/connection-review-tools.tsx
"use client";

import { useEffect, useState, useCallback } from "react";
import { useEntityResolution } from "@/context/entity-resolution-context";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
import { Badge } from "@/components/ui/badge";
import { Check, ChevronDown, ChevronUp, X, AlertCircle, Info, Loader2, AlertTriangle, RefreshCw, CheckCircle, XCircle, SkipForward, RotateCcw } from "lucide-react";
import type {
  VisualizationEntityEdge,
  EntityGroup,
  Entity,
  GroupReviewDecision,
  QueuedReviewBatch,
  ServiceGroup, // New
  VisualizationServiceEdge, // New
  Service, // New
  EntityConnectionDataResponse, // Specific
  ServiceConnectionDataResponse, // Specific
  BaseNode, // For generic entity/service display
} from "@/types/entity-resolution";
import { useToast } from "@/hooks/use-toast";

// Helper function to extract contact information
const getContactInfo = (
    groups: Array<EntityGroup | ServiceGroup>,
    nodeId: string,
    mode: 'entity' | 'service'
  ) => {
  const contactInfo: { email?: string; phone?: string; address?: string; url?: string; } = {};

  for (const group of groups) {
    const values = group.match_values.values;
    let isNode1 = false;
    let isNode2 = false;

    if (mode === 'entity') {
      const eg = group as EntityGroup;
      isNode1 = eg.entity_id_1 === nodeId;
      isNode2 = eg.entity_id_2 === nodeId;
    } else {
      const sg = group as ServiceGroup;
      isNode1 = sg.service_id_1 === nodeId;
      isNode2 = sg.service_id_2 === nodeId;
    }

    if (!isNode1 && !isNode2) continue;

    // Assuming match_values structure is consistent enough
    // Use more robust access to avoid errors if keys are missing
    switch (group.match_values.type?.toLowerCase()) {
      case 'email':
        contactInfo.email = isNode1 ? (values.original_email1 || values.email1) : (values.original_email2 || values.email2);
        break;
      case 'phone':
        contactInfo.phone = isNode1 ? (values.original_phone1 || values.phone1) : (values.original_phone2 || values.phone2);
        break;
      case 'address':
        contactInfo.address = isNode1 ? (values.original_address1 || values.address1) : (values.original_address2 || values.address2);
        break;
      case 'url':
        contactInfo.url = isNode1 ? (values.original_url1 || values.url1) : (values.original_url2 || values.url2);
        break;
    }
  }
  return contactInfo;
};


export default function ConnectionReviewTools() {
  const {
    resolutionMode, // Get current mode
    selectedEdgeId,
    currentConnectionData, // This is now EntityConnectionDataResponse | ServiceConnectionDataResponse | null
    actions,
    queries,
    reviewQueue,
  } = useEntityResolution();
  const { toast } = useToast();

  const [isSubmitting, setIsSubmitting] = useState(false);
  const [isExpanded, setIsExpanded] = useState(true);

  const edgeStatus = selectedEdgeId ? queries.getEdgeStatus(selectedEdgeId) : null;
  const isEdgeReviewed = selectedEdgeId ? queries.isEdgeReviewed(selectedEdgeId) : false;
  const queueStatus = selectedEdgeId ? queries.getQueueItemStatus(selectedEdgeId) : null;

  useEffect(() => {
    if (selectedEdgeId && !currentConnectionData && !queries.isConnectionDataLoading(selectedEdgeId)) {
      actions.loadConnectionData(selectedEdgeId); // loadConnectionData is mode-aware
    }
  }, [selectedEdgeId, currentConnectionData, queries, actions]);

  useEffect(() => {
    setIsSubmitting(false);
    setIsExpanded(true);
  }, [selectedEdgeId]);

  const handleReviewDecision = async (decision: GroupReviewDecision) => {
    if (!selectedEdgeId) {
      toast({ title: "Error", description: "No connection selected.", variant: "destructive" });
      return;
    }
    if (isEdgeReviewed && queueStatus !== 'failed') {
      toast({ title: "Already Reviewed", description: "This connection has already been reviewed." });
      return;
    }
    if (queueStatus === 'processing' || queueStatus === 'pending') {
      toast({ title: "In Progress", description: "This connection review is already being processed." });
      return;
    }

    setIsSubmitting(true);
    try {
      // submitEdgeReview is now mode-aware internally or calls the correct feedback function
      await actions.submitEdgeReview(selectedEdgeId, decision);
    } catch (error) {
      toast({ title: "Submission Error", description: (error as Error).message, variant: "destructive" });
    } finally {
      setIsSubmitting(false);
    }
  };

  const handleRetryLoad = useCallback(() => {
    if (selectedEdgeId) {
      actions.invalidateConnectionData(selectedEdgeId);
      actions.loadConnectionData(selectedEdgeId);
    }
  }, [selectedEdgeId, actions]);

  const handleSkipToNext = useCallback(() => {
    actions.selectNextUnreviewedEdge(selectedEdgeId || undefined);
  }, [actions, selectedEdgeId]);

  const handleRetryQueueItem = () => {
    if (selectedEdgeId && queueStatus === 'failed') {
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

  const nodeLabel = resolutionMode === 'entity' ? 'Entity' : 'Service';

  if (!selectedEdgeId && !isLoading) {
    return (
      <div className="flex justify-center items-center h-[100px] text-muted-foreground p-4 border rounded-md bg-card shadow">
        Select a connection from the graph to review its details.
      </div>
    );
  }
  if (isLoading && !currentConnectionData) {
    return <div className="flex justify-center items-center h-[100px] border rounded-md bg-card shadow"><Loader2 className="h-6 w-6 animate-spin text-primary" /></div>;
  }
  if (error && !currentConnectionData) {
    return (
      <Card className="h-full flex flex-col items-center justify-center">
        <CardHeader><CardTitle className="flex items-center"><AlertCircle className="h-6 w-6 mr-2 text-destructive" /> Error Loading Details</CardTitle></CardHeader>
        <CardContent className="text-center">
          <p className="text-destructive mb-2">Could not load details for the selected connection.</p>
          {error && <p className="text-xs mt-1 text-muted-foreground">{error}</p>}
          <div className="flex gap-2 mt-3">
            <Button variant="outline" size="sm" onClick={handleRetryLoad}><RefreshCw className="h-4 w-4 mr-1" /> Retry</Button>
            <Button variant="outline" size="sm" onClick={() => actions.setSelectedEdgeId(null)}>Clear Selection</Button>
          </div>
        </CardContent>
      </Card>
    );
  }

  if (!currentConnectionData) return null;

  // Type-safe access to connection data based on mode
  let edgeDetails: VisualizationEntityEdge | VisualizationServiceEdge;
  let groupsForEdge: Array<EntityGroup | ServiceGroup>;
  let node1: Entity | Service;
  let node2: Entity | Service;

  if (resolutionMode === 'entity') {
    const data = currentConnectionData as EntityConnectionDataResponse;
    edgeDetails = data.edge;
    groupsForEdge = data.entityGroups;
    node1 = data.entity1;
    node2 = data.entity2;
  } else {
    const data = currentConnectionData as ServiceConnectionDataResponse;
    edgeDetails = data.edge;
    groupsForEdge = data.entityGroups as ServiceGroup[]; // Cast because ConnectionDataResponse is generic here
    node1 = data.entity1;
    node2 = data.entity2;
  }

  const node1ContactInfo = getContactInfo(groupsForEdge, node1.id, resolutionMode);
  const node2ContactInfo = getContactInfo(groupsForEdge, node2.id, resolutionMode);

  const showReviewButtons = !isEdgeReviewed || queueStatus === 'failed';

  return (
    <Collapsible open={isExpanded} onOpenChange={setIsExpanded} className="h-full flex flex-col">
      <div className="flex justify-between items-center flex-shrink-0 pb-2 border-b mb-3">
        <div className="flex items-center gap-2">
          <h3 className="text-lg font-medium">{nodeLabel} Connection Review</h3>
          {queueStatus === 'processing' && <Badge variant="outline" className="bg-blue-50 text-blue-700 border-blue-300"><Loader2 className="h-3 w-3 mr-1 animate-spin"/>Processing</Badge>}
          {queueStatus === 'pending' && <Badge variant="outline" className="bg-yellow-50 text-yellow-700 border-yellow-300"><Info className="h-3 w-3 mr-1"/>Queued</Badge>}
          {queueStatus === 'failed' && <Badge variant="destructive"><AlertTriangle className="h-3 w-3 mr-1"/>Failed</Badge>}
          {isEdgeReviewed && !queueStatus && (
            <Badge variant={edgeStatus === 'CONFIRMED_MATCH' ? 'default' : 'secondary'} className={`${edgeStatus === 'CONFIRMED_MATCH' ? 'bg-green-100 text-green-800 border-green-300' : 'bg-red-100 text-red-800 border-red-300'}`}>
              {edgeStatus === 'CONFIRMED_MATCH' ? <CheckCircle className="h-3 w-3 mr-1" /> : <XCircle className="h-3 w-3 mr-1" />}
              {edgeStatus === 'CONFIRMED_MATCH' ? 'Match' : 'Non-Match'}
            </Badge>
          )}
          {/* Suggested actions might need to be mode-aware if logic differs */}
        </div>
        <CollapsibleTrigger asChild><Button variant="ghost" size="sm">{isExpanded ? <ChevronUp className="h-4 w-4" /> : <ChevronDown className="h-4 w-4" />}<span className="sr-only">Toggle review panel</span></Button></CollapsibleTrigger>
      </div>

      <CollapsibleContent className="flex-1 min-h-0">
        <div className="h-full overflow-y-auto space-y-3 pr-2 custom-scrollbar">
          {showReviewButtons ? (
            <div className="space-y-3 p-1">
              <p className="text-sm text-muted-foreground">Do these records represent the same real-world {resolutionMode}?</p>
              <div className="flex flex-col sm:flex-row justify-between gap-2">
                <Button variant="outline" className="border-red-500 text-red-600 hover:bg-red-50 hover:text-red-700 flex-1" size="sm" onClick={() => handleReviewDecision('REJECTED')} disabled={isSubmitting || queueStatus === 'processing' || queueStatus === 'pending'}><X className="h-4 w-4 mr-1" /> Not a Match</Button>
                <Button variant="default" className="bg-green-600 hover:bg-green-700 flex-1" size="sm" onClick={() => handleReviewDecision('ACCEPTED')} disabled={isSubmitting || queueStatus === 'processing' || queueStatus === 'pending'}><Check className="h-4 w-4 mr-1" /> Confirm Match</Button>
              </div>
              {queueStatus === 'failed' && (<Button variant="outline" size="sm" onClick={handleRetryQueueItem} className="w-full mt-2 border-amber-500 text-amber-600 hover:bg-amber-50" disabled={isSubmitting}><RotateCcw className="h-4 w-4 mr-1" /> Retry Failed Submission</Button>)}
            </div>
          ) : (
            <div className="space-y-3 p-1">
              <Card className={`border-2 ${edgeStatus === 'CONFIRMED_MATCH' ? 'border-green-200 bg-green-50' : 'border-red-200 bg-red-50'}`}>
                <CardContent className="p-3"><div className="flex items-center gap-3">{edgeStatus === 'CONFIRMED_MATCH' ? <CheckCircle className="h-6 w-6 text-green-600" /> : <XCircle className="h-6 w-6 text-red-600" />}<div><h4 className="font-semibold text-sm">{edgeStatus === 'CONFIRMED_MATCH' ? 'Match Confirmed' : 'Non-Match Confirmed'}</h4><p className="text-xs text-muted-foreground">This connection has been reviewed.</p></div></div></CardContent>
              </Card>
              <Button variant="outline" size="sm" onClick={handleSkipToNext} className="w-full"><SkipForward className="h-4 w-4 mr-1" /> Continue to Next</Button>
            </div>
          )}

          <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
            <Card>
              <CardHeader className="p-3"><CardTitle className="text-sm">{nodeLabel} 1</CardTitle></CardHeader>
              <CardContent className="p-3 pt-0">
                {node1 ? (<div className="space-y-1 text-xs">
                    <div><span className="font-medium">Name:</span> {node1.name || 'N/A'}</div>
                    <div><span className="font-medium">Source:</span> {node1.source_system || 'N/A'} {node1.source_id && `(${node1.source_id})`}</div>
                    {node1ContactInfo.address && <div><span className="font-medium">Address:</span> {node1ContactInfo.address}</div>}
                    {node1ContactInfo.phone && <div><span className="font-medium">Phone:</span> {node1ContactInfo.phone}</div>}
                    {node1ContactInfo.email && <div><span className="font-medium">Email:</span> {node1ContactInfo.email}</div>}
                    {node1ContactInfo.url && <div><span className="font-medium">URL:</span> <a href={node1ContactInfo.url} target="_blank" rel="noopener noreferrer" className="text-blue-600 hover:underline break-all">{node1ContactInfo.url}</a></div>}
                  </div>) : <p className="text-xs text-muted-foreground">Details not available.</p>}
              </CardContent>
            </Card>
            <Card>
              <CardHeader className="p-3"><CardTitle className="text-sm">{nodeLabel} 2</CardTitle></CardHeader>
              <CardContent className="p-3 pt-0">
                 {node2 ? (<div className="space-y-1 text-xs">
                    <div><span className="font-medium">Name:</span> {node2.name || 'N/A'}</div>
                    <div><span className="font-medium">Source:</span> {node2.source_system || 'N/A'} {node2.source_id && `(${node2.source_id})`}</div>
                    {node2ContactInfo.address && <div><span className="font-medium">Address:</span> {node2ContactInfo.address}</div>}
                    {node2ContactInfo.phone && <div><span className="font-medium">Phone:</span> {node2ContactInfo.phone}</div>}
                    {node2ContactInfo.email && <div><span className="font-medium">Email:</span> {node2ContactInfo.email}</div>}
                    {node2ContactInfo.url && <div><span className="font-medium">URL:</span> <a href={node2ContactInfo.url} target="_blank" rel="noopener noreferrer" className="text-blue-600 hover:underline break-all">{node2ContactInfo.url}</a></div>}
                  </div>) : <p className="text-xs text-muted-foreground">Details not available.</p>}
              </CardContent>
            </Card>
          </div>

          <Tabs defaultValue="matching-methods">
            <TabsList className="grid w-full grid-cols-1"><TabsTrigger value="matching-methods">Matching Methods</TabsTrigger></TabsList>
            <TabsContent value="matching-methods" className="space-y-3">
              <Card>
                <CardHeader className="p-3"><CardTitle className="text-sm flex justify-between items-center"><span>Matching Methods</span><span className="text-xs font-normal">Overall Confidence: <span className="font-medium">{edgeDetails?.edge_weight?.toFixed(2) ?? 'N/A'}</span></span></CardTitle></CardHeader>
                <CardContent className="p-3 pt-0">
                  {/* Details structure might differ for service edges */}
                  {edgeDetails?.details && 'methods' in edgeDetails.details && Array.isArray(edgeDetails.details.methods) && edgeDetails.details.methods.length > 0 ? (
                    <div className="space-y-1">
                      {(edgeDetails.details.methods as Array<any>).map((method: any, index: number) => (
                        <div key={index} className="grid grid-cols-[1fr_auto_auto_auto] gap-2 items-center text-xs">
                          <div>{method.method_type?.replace(/_/g, " ") ?? 'Unknown Method'}</div>
                          <div className="text-right"><span className="text-xs text-muted-foreground">Pre-RL:</span> {method.pre_rl_confidence?.toFixed(2) ?? 'N/A'}</div>
                          <div className="text-right"><span className="text-xs text-muted-foreground">RL:</span> {method.rl_confidence?.toFixed(2) ?? 'N/A'}</div>
                          <div className="text-right font-medium">{method.combined_confidence?.toFixed(2) ?? 'N/A'}</div>
                        </div>
                      ))}
                    </div>
                  ) : edgeDetails?.details ? ( // Fallback for generic JSON details for services
                     <pre className="text-xs bg-muted p-2 rounded-md overflow-x-auto">{JSON.stringify(edgeDetails.details, null, 2)}</pre>
                  ) : <p className="text-xs text-muted-foreground">No specific matching methods detailed.</p>}
                </CardContent>
              </Card>

              {groupsForEdge.length > 0 && (
                <Card>
                  <CardHeader className="p-3"><CardTitle className="text-sm">Underlying Group Details</CardTitle></CardHeader>
                  <CardContent className="p-3 pt-0 space-y-2">
                    {groupsForEdge.map((group) => (
                      <div key={group.id} className="rounded-md border bg-muted/30 p-2">
                        <div className="flex justify-between items-center mb-1">
                          <span className="text-xs font-medium capitalize">{group.method_type.replace(/_/g, " ")} Match</span>
                          <Badge variant={group.confirmed_status === 'CONFIRMED_MATCH' ? 'default' : group.confirmed_status === 'CONFIRMED_NON_MATCH' ? 'destructive' : 'outline'} className="text-xs">{group.confirmed_status?.replace(/_/g, " ") ?? 'Pending'}</Badge>
                          <span className="text-xs text-muted-foreground">{group.confidence_score ? group.confidence_score.toFixed(3) : 'N/A'}</span>
                        </div>
                        <div className="text-xs text-muted-foreground space-y-0.5">
                          {Object.entries(group.match_values.values).map(([key, val]) => (<div key={key} className="truncate"><span className="font-medium">{key.replace(/original_|_1|_2/gi, '')}:</span> {String(val)}</div>)).slice(0,2)}
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

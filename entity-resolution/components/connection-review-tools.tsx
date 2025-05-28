"use client"

import { useEffect, useState } from "react"
import { useEntityResolution } from "@/context/entity-resolution-context"
import { getConnectionData, submitFeedback, updateEntityGroupStatus } from "@/utils/api-client"
import { Button } from "@/components/ui/button"
import { Card, CardContent } from "@/components/ui/card"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible"
import { Check, ChevronDown, ChevronUp, X } from "lucide-react"
import { VisualizationEntityEdge, EntityGroup, MatchDecisionDetails, Entity } from "@/types/entity-resolution"

// Helper function to extract email from match values based on entity ID
const getEmailFromMatchValues = (entityGroups: EntityGroup[], entityId: string): string | null => {
  const emailGroup = entityGroups.find(group => group.method_type === 'email')
  
  if (!emailGroup?.match_values?.values) return null
  
  // Based on the API response structure for email type
  const values = emailGroup.match_values.values
  if (values.original_email1 && values.original_email2) {
    // For now, we'll need to determine which email belongs to which entity
    // This might require additional logic or API changes
    return values.original_email1 || values.original_email2
  }
  
  return null
}

// Helper function to extract phone from match values
const getPhoneFromMatchValues = (entityGroups: EntityGroup[], entityId: string): string | null => {
  const phoneGroup = entityGroups.find(group => group.method_type === 'phone')
  
  if (!phoneGroup?.match_values?.values) return null
  
  const values = phoneGroup.match_values.values
  return values.original_phone1 || values.original_phone2 || null
}

// Helper function to extract address from match values
const getAddressFromMatchValues = (entityGroups: EntityGroup[], entityId: string): any | null => {
  const addressGroup = entityGroups.find(group => group.method_type === 'address')
  
  if (!addressGroup?.match_values?.values) return null
  
  return addressGroup.match_values.values
}

// Helper function to extract URL from match values
const getUrlFromMatchValues = (entityGroups: EntityGroup[], entityId: string): string | null => {
  const urlGroup = entityGroups.find(group => group.method_type === 'url')
  
  if (!urlGroup?.match_values?.values) return null
  
  const values = urlGroup.match_values.values
  return values.original_url1 || values.original_url2 || null
}

export default function ConnectionReviewTools() {
  const { selectedEdgeId, setSelectedEdgeId, reviewerId, triggerRefresh } = useEntityResolution()

  const [loading, setLoading] = useState(false)
  const [edge, setEdge] = useState<VisualizationEntityEdge | null>(null)
  const [entityGroups, setEntityGroups] = useState<EntityGroup[]>([])
  const [matchDecisions, setMatchDecisions] = useState<MatchDecisionDetails[]>([])
  const [entity1, setEntity1] = useState<Entity | null>(null)
  const [entity2, setEntity2] = useState<Entity | null>(null)
  const [submitting, setSubmitting] = useState(false)
  const [isExpanded, setIsExpanded] = useState(true)

  useEffect(() => {
    if (!selectedEdgeId) return

    async function loadConnectionData() {
      setLoading(true)
      try {
        if (!selectedEdgeId) return
        
        const connectionData = await getConnectionData(selectedEdgeId)
        setEdge(connectionData.edge)
        setEntityGroups(connectionData.entityGroups)
        setMatchDecisions(connectionData.matchDecisions || [])
        setEntity1(connectionData.entity1)
        setEntity2(connectionData.entity2)

        setIsExpanded(true)
      } catch (error) {
        console.error("Failed to load connection data:", error)
      } finally {
        setLoading(false)
      }
    }

    loadConnectionData()
    return () => {
      setEdge(null)
      setEntityGroups([])
      setMatchDecisions([])
      setEntity1(null)
      setEntity2(null)
    }
  }, [selectedEdgeId])

  const handleSubmitFeedback = async (isMatch: boolean) => {
    if (!selectedEdgeId || !edge || entityGroups.length === 0) return

    setSubmitting(true)
    try {
      // Update all entity groups for this edge
      for (const group of entityGroups) {
        // Update entity group status
        await updateEntityGroupStatus(group.id, isMatch ? "ACCEPTED" : "REJECTED", reviewerId)
      }
      
      // Submit feedback for the edge
      await submitFeedback(selectedEdgeId, isMatch, reviewerId, '')

      // Trigger refresh of data
      triggerRefresh()

    } catch (error) {
      console.error("Failed to submit feedback:", error)
    } finally {
      setSubmitting(false)
    }
  }

  if (!selectedEdgeId) {
    return (
      <div className="flex justify-center items-center h-[100px] text-muted-foreground">
        Select a connection to review
      </div>
    )
  }

  if (loading) {
    return (
      <div className="flex justify-center items-center h-[100px]">
        <div className="animate-spin rounded-full h-6 w-6 border-b-2 border-gray-900"></div>
      </div>
    )
  }

  return (
    <Collapsible
      open={isExpanded}
      onOpenChange={setIsExpanded}
      className="space-y-2 max-h-[calc(40vh-2rem)] overflow-auto"
    >
      <div className="flex justify-between items-center sticky top-0 bg-white z-10 py-1">
        <div className="flex items-center gap-2">
          <h3 className="text-lg font-medium">Connection Review</h3>
        </div>
        <CollapsibleTrigger asChild>
          <Button variant="ghost" size="sm">
            {isExpanded ? <ChevronUp className="h-4 w-4" /> : <ChevronDown className="h-4 w-4" />}
          </Button>
        </CollapsibleTrigger>
      </div>

      <CollapsibleContent>
        <div className="space-y-3 pt-1">
          <div className="flex justify-between">
            <Button variant="destructive" size="sm" onClick={() => handleSubmitFeedback(false)} disabled={submitting}>
              <X className="h-4 w-4 mr-1" />
              Not a Match
            </Button>

            <Button
              variant="default"
              size="sm"
              onClick={() => handleSubmitFeedback(true)}
              disabled={submitting}
              className="bg-green-600 hover:bg-green-700"
            >
              <Check className="h-4 w-4 mr-1" />
              Confirm Match
            </Button>
          </div>
          <p className="text-sm text-muted-foreground">Do these records represent the same real-world entity?</p>
          
          <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
            <Card>
              <CardContent className="p-3">
                <h4 className="font-medium mb-1 text-sm">Entity 1</h4>
                {entity1 && (
                  <div className="space-y-1 text-xs">
                    <div>
                      <span className="font-medium">Name:</span> {entity1.name}
                    </div>
                    {entity1.source_system && (
                      <div>
                        <span className="font-medium">Source:</span> {entity1.source_system}
                      </div>
                    )}
                    {(() => {
                      const email = getEmailFromMatchValues(entityGroups, entity1.id);
                      return email && (
                        <div>
                          <span className="font-medium">Email:</span> {email}
                        </div>
                      );
                    })()}
                    {(() => {
                      const phone = getPhoneFromMatchValues(entityGroups, entity1.id);
                      return phone && (
                        <div>
                          <span className="font-medium">Phone:</span> {phone}
                        </div>
                      );
                    })()}
                    {(() => {
                      const url = getUrlFromMatchValues(entityGroups, entity1.id);
                      return url && (
                        <div>
                          <span className="font-medium">URL:</span> {url}
                        </div>
                      );
                    })()}
                    {(() => {
                      const address = getAddressFromMatchValues(entityGroups, entity1.id);
                      return address && (
                        <div>
                          <span className="font-medium">Address:</span> {address.address_1 || ''}{address.city ? `, ${address.city}` : ''}{address.state_province ? `, ${address.state_province}` : ''} {address.postal_code || ''}
                        </div>
                      );
                    })()}
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
                      <span className="font-medium">Name:</span> {entity2.name}
                    </div>
                    {entity2.source_system && (
                      <div>
                        <span className="font-medium">Source:</span> {entity2.source_system}
                      </div>
                    )}
                    {(() => {
                      const email = getEmailFromMatchValues(entityGroups, entity2.id);
                      return email && (
                        <div>
                          <span className="font-medium">Email:</span> {email}
                        </div>
                      );
                    })()}
                    {(() => {
                      const phone = getPhoneFromMatchValues(entityGroups, entity2.id);
                      return phone && (
                        <div>
                          <span className="font-medium">Phone:</span> {phone}
                        </div>
                      );
                    })()}
                    {(() => {
                      const url = getUrlFromMatchValues(entityGroups, entity2.id);
                      return url && (
                        <div>
                          <span className="font-medium">URL:</span> {url}
                        </div>
                      );
                    })()}
                    {(() => {
                      const address = getAddressFromMatchValues(entityGroups, entity2.id);
                      return address && (
                        <div>
                          <span className="font-medium">Address:</span> {address.address_1 || ''}{address.city ? `, ${address.city}` : ''}{address.state_province ? `, ${address.state_province}` : ''} {address.postal_code || ''}
                        </div>
                      );
                    })()}
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

              {/* Show detailed match values */}
              <div className="space-y-2">
                {entityGroups.map((group) => (
                  <div key={group.id} className="rounded-md border">
                    <div className="bg-muted px-3 py-1 rounded-t-md">
                      <h4 className="font-medium text-xs capitalize">{group.method_type} Match Details</h4>
                    </div>
                    <div className="p-2 text-xs">
                      <div className="space-y-1">
                        <div>
                          <span className="font-medium">Confidence:</span> {group.confidence_score.toFixed(2)}
                        </div>
                        <div>
                          <span className="font-medium">Status:</span> {group.confirmed_status}
                        </div>
                        {group.match_values.type === 'Email' && (
                          <div className="space-y-1">
                            <div>
                              <span className="font-medium">Email 1:</span> {group.match_values.values.original_email1}
                            </div>
                            <div>
                              <span className="font-medium">Email 2:</span> {group.match_values.values.original_email2}
                            </div>
                            <div>
                              <span className="font-medium">Normalized:</span> {group.match_values.values.normalized_shared_email}
                            </div>
                          </div>
                        )}
                        {group.match_values.type === 'Name' && (
                          <div className="space-y-1">
                            <div>
                              <span className="font-medium">Name 1:</span> {group.match_values.values.original_name1}
                            </div>
                            <div>
                              <span className="font-medium">Name 2:</span> {group.match_values.values.original_name2}
                            </div>
                            <div>
                              <span className="font-medium">Match Type:</span> {group.match_values.values.pre_rl_match_type}
                            </div>
                          </div>
                        )}
                        {group.match_values.type === 'Url' && (
                          <div className="space-y-1">
                            <div>
                              <span className="font-medium">URL 1:</span> {group.match_values.values.original_url1}
                            </div>
                            <div>
                              <span className="font-medium">URL 2:</span> {group.match_values.values.original_url2}
                            </div>
                            <div>
                              <span className="font-medium">Shared Domain:</span> {group.match_values.values.normalized_shared_domain}
                            </div>
                          </div>
                        )}
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            </TabsContent>
          </Tabs>
        </div>
      </CollapsibleContent>
    </Collapsible>
  )
}
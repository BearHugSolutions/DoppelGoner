"use client"

import { useEffect, useRef, useState, useCallback, useMemo } from "react"
import { useEntityResolution } from "@/context/entity-resolution-context"
import { getVisualizationData } from "@/utils/api-client"
import type { EntityNode, EntityLink, EntityGroup } from "@/types/entity-resolution"
import * as d3 from "d3"
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover"
import { Info, ZoomIn, ZoomOut, Maximize, Minimize, RefreshCw, Move, RotateCcw } from "lucide-react"
import { Button } from "@/components/ui/button"
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip"
import { useResizeObserver } from "@/hooks/use-resize-observer"

export default function GraphVisualizer() {
  const { selectedClusterId, setSelectedEdgeId, selectedEdgeId, updateReviewProgress, refreshTrigger } =
    useEntityResolution()

  const containerRef = useRef<HTMLDivElement>(null)
  const svgRef = useRef<SVGSVGElement>(null)
  const gRef = useRef<d3.Selection<SVGGElement, unknown, null, undefined> | null>(null)
  const zoomRef = useRef<d3.ZoomBehavior<Element, unknown> | null>(null)
  const [nodes, setNodes] = useState<EntityNode[]>([])
  const [links, setLinks] = useState<EntityLink[]>([])
  const [loading, setLoading] = useState(false)
  const [hoverNode, setHoverNode] = useState<string | null>(null)
  const [hoverLink, setHoverLink] = useState<string | null>(null)
  const [nodeDetails, setNodeDetails] = useState<Record<string, any>>({})
  const [zoomLevel, setZoomLevel] = useState(1)
  const [rawDimensions, setRawDimensions] = useState({ width: 0, height: 0 })

  // Memoize dimensions to prevent unnecessary recreations
  const dimensions = useMemo(() => ({
    width: rawDimensions.width,
    height: rawDimensions.height
  }), [rawDimensions.width, rawDimensions.height])

  // Track reviewed edges - use the actual edge IDs
  const [reviewedEdges, setReviewedEdges] = useState<Record<string, "CONFIRMED_MATCH" | "CONFIRMED_NON_MATCH">>({})

  // D3 simulation reference
  const simulationRef = useRef<d3.Simulation<d3.SimulationNodeDatum, undefined> | null>(null)
  
  // Flag to prevent state updates during programmatic zoom changes
  const isProgrammaticZoomRef = useRef(false)

  // Use resize observer to track container dimensions
  useResizeObserver(containerRef, (entry) => {
    if (entry) {
      const { width, height } = entry.contentRect
      // Only update if dimensions actually changed
      setRawDimensions(prev => {
        if (prev.width !== width || prev.height !== height) {
          return { width, height }
        }
        return prev
      })
    }
  })

  // Memoized stable setters to prevent recreation
  const stableSetSelectedEdgeId = useCallback((id: string) => {
    setSelectedEdgeId(id)
  }, [setSelectedEdgeId])

  // Load node details - memoized with stable dependencies
  const loadNodeDetails = useCallback(async (nodeId: string): Promise<void> => {
    if (nodeDetails[nodeId]) return;

    try {
      setNodeDetails((prev) => ({
        ...prev,
        [nodeId]: { 
          id: nodeId, 
          name: `Node ${nodeId.slice(0, 6)}`,
          organization: {
            name: nodes.find(n => n.id === nodeId)?.name || `Node ${nodeId.slice(0, 6)}`,
            description: null
          },
          addresses: [],
          phones: []
        },
      }));
    } catch (error) {
      console.error("Failed to load node details:", error);
    }
  }, [nodeDetails, nodes]);

  // Helper function to safely apply zoom transform
  const safeZoomTo = useCallback((transform: d3.ZoomTransform) => {
    if (!zoomRef.current || !svgRef.current) return
    
    // Validate transform values to prevent NaN
    const safeTransform = d3.zoomIdentity
      .translate(
        isFinite(transform.x) ? transform.x : 0,
        isFinite(transform.y) ? transform.y : 0
      )
      .scale(isFinite(transform.k) && transform.k > 0 ? transform.k : 1)
    
    d3.select(svgRef.current)
      .transition()
      .duration(500)
      .call(zoomRef.current.transform as any, safeTransform)
  }, [])

  // Zoom control functions
  const zoomIn = useCallback(() => {
    if (!zoomRef.current || !svgRef.current) return
    d3.select(svgRef.current)
      .transition()
      .duration(300)
      .call(zoomRef.current.scaleBy as any, 1.5)
  }, [])

  const zoomOut = useCallback(() => {
    if (!zoomRef.current || !svgRef.current) return
    d3.select(svgRef.current)
      .transition()
      .duration(300)
      .call(zoomRef.current.scaleBy as any, 1 / 1.5)
  }, [])

  const resetZoom = useCallback(() => {
    console.log('[resetZoom] Reset zoom triggered');
    console.log('[resetZoom] Refs available - zoomRef:', !!zoomRef.current, 'svgRef:', !!svgRef.current, 'gRef:', !!gRef.current);
    
    if (!zoomRef.current || !svgRef.current || !gRef.current) {
      console.error('[resetZoom] Missing required refs');
      return;
    }
    
    const { width, height } = dimensions;
    console.log('[resetZoom] Dimensions:', { width, height });
    
    if (width === 0 || height === 0) {
      console.warn('[resetZoom] Invalid dimensions');
      return;
    }

    try {
      // Get the bounds of the graph content
      const graphBounds = gRef.current.node()?.getBBox();
      console.log('[resetZoom] Graph bounds:', graphBounds);
      
      if (!graphBounds || graphBounds.width === 0 || graphBounds.height === 0) {
        // Fallback to simple center if no bounds available
        console.log('[resetZoom] No valid bounds, using fallback center');
        safeZoomTo(d3.zoomIdentity.translate(width / 2, height / 2).scale(1));
        return;
      }

      // Calculate scale to fit the graph with some padding
      const padding = 50;
      const scaleX = (width - padding * 2) / graphBounds.width;
      const scaleY = (height - padding * 2) / graphBounds.height;
      const scale = Math.min(scaleX, scaleY, 2); // Cap at 2x zoom

      // Calculate translation to center the graph
      const centerX = graphBounds.x + graphBounds.width / 2;
      const centerY = graphBounds.y + graphBounds.height / 2;
      const translateX = width / 2 - scale * centerX;
      const translateY = height / 2 - scale * centerY;

      const transform = d3.zoomIdentity
        .translate(translateX, translateY)
        .scale(scale);

      console.log('[resetZoom] Calculated transform:', {
        scale,
        translateX,
        translateY,
        centerX,
        centerY
      });

      safeZoomTo(transform);
    } catch (error) {
      console.warn('[resetZoom] Error in resetZoom, falling back to simple center:', error);
      safeZoomTo(d3.zoomIdentity.translate(width / 2, height / 2).scale(1));
    }
  }, [dimensions, safeZoomTo])

  const centerGraph = useCallback(() => {
    console.log('[centerGraph] Manual center requested');
    resetZoom();
  }, [resetZoom])

  async function loadVisualizationData() {
    if (!selectedClusterId) return
    setLoading(true)
    try {
      const { nodes, links, entityGroups } = await getVisualizationData(selectedClusterId)
      setNodes(nodes)
      setLinks(links)
      
      // Initialize reviewed edges based on entity groups
      const reviewedEdgesMap: Record<string, 'CONFIRMED_MATCH' | 'CONFIRMED_NON_MATCH'> = {}
      
      entityGroups.forEach((group: EntityGroup) => {
        if (group.confirmed_status === 'CONFIRMED_MATCH' || group.confirmed_status === 'CONFIRMED_NON_MATCH') {
          // Map entity groups to edges - this may need adjustment based on your data structure
        }
      })
      
      setReviewedEdges(reviewedEdgesMap)
      
      // Calculate review progress based on edge status from the API response
      const totalEdges = links.length
      const reviewedCount = links.filter(link => 
        link.status === 'CONFIRMED_MATCH' || link.status === 'CONFIRMED_NON_MATCH'
      ).length
      const progress = totalEdges > 0 ? Math.round((reviewedCount / totalEdges) * 100) : 0
      
      if (selectedClusterId) {
        updateReviewProgress(selectedClusterId, progress)
      }

      // Auto-select the first unreviewed connection
      if (links.length > 0) {
        const firstUnreviewedEdge = links.find((link) => 
          !link.status || link.status === 'PENDING_REVIEW'
        )
        if (firstUnreviewedEdge) {
          setSelectedEdgeId(firstUnreviewedEdge.id)
        }
      }
    } catch (error) {
      console.error("Failed to load visualization data:", error)
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    console.log("selectedClusterId:" + selectedClusterId)
    if (!selectedClusterId) return
    loadVisualizationData()
  }, [selectedClusterId, updateReviewProgress, setSelectedEdgeId, refreshTrigger])

  // Initialize and update D3 visualization - now with proper dependencies
  const updateVisualization = useCallback(() => {
    console.log('[updateVisualization] Starting visualization update');
    console.log('[updateVisualization] Conditions:', JSON.stringify({
      svgRef: !!svgRef.current,
      nodesLength: nodes.length,
      linksLength: links.length,
      dimensions: dimensions
    }));
    
    if (
      !svgRef.current ||
      nodes.length === 0 ||
      links.length === 0 ||
      dimensions.width === 0 ||
      dimensions.height === 0
    ) {
      console.log('[updateVisualization] Skipping update due to missing requirements');
      return;
    }

    // Store current transform before clearing
    const currentTransform = svgRef.current ? d3.zoomTransform(svgRef.current) : d3.zoomIdentity;
    console.log('[updateVisualization] Preserving current transform:', JSON.stringify({
      x: currentTransform.x,
      y: currentTransform.y,
      k: currentTransform.k
    }));

    // Clear previous visualization
    d3.select(svgRef.current).selectAll("*").remove();
    console.log('[updateVisualization] Cleared previous visualization');

    const width = dimensions.width;
    const height = dimensions.height;

    // Create SVG container
    const svg = d3.select(svgRef.current)
      .attr("width", width)
      .attr("height", height);

    // Create main group for zooming and panning
    const g = svg.append("g");
    gRef.current = g;

    // Initialize zoom behavior
    zoomRef.current = d3
      .zoom<Element, unknown>()
      .scaleExtent([0.1, 4])
      .on("zoom", (event) => {
        const transform = event.transform;
        console.log('[updateVisualization] Zoom event:', {
          x: transform.x,
          y: transform.y,
          k: transform.k,
          isProgrammatic: isProgrammaticZoomRef.current
        });
        // Validate transform before applying
        if (isFinite(transform.x) && isFinite(transform.y) && isFinite(transform.k) && transform.k > 0) {
          g.attr("transform", transform.toString());
          // Only update zoom level state for user-initiated zooms, not programmatic ones
          if (!isProgrammaticZoomRef.current) {
            setZoomLevel(transform.k);
          }
        }
      });

    // Apply zoom behavior to the SVG
    svg.call(zoomRef.current as any);
    console.log('[updateVisualization] Applied zoom behavior');

    // Create links
    const link = g
      .append("g")
      .selectAll("line")
      .data(links)
      .enter()
      .append("line")
      .attr("stroke-width", (d) => 1 + d.weight * 3)
      .attr("stroke", (d) => {
        if (d.status === "CONFIRMED_MATCH") return "#000"
        if (d.status === "CONFIRMED_NON_MATCH") return "transparent"
        return d3.interpolateRgb("#f87171", "#60a5fa")(d.weight)
      })
      .attr("stroke-opacity", (d) => {
        if (d.status === "CONFIRMED_NON_MATCH") return 0
        return 0.6
      })
      .attr("id", (d) => `link-${d.id}`)
      .on("mouseover", (event, d) => {
        setHoverLink(d.id);
        d3.select(`#link-${d.id}`)
          .attr("stroke-width", 1 + d.weight * 5)
          .attr("stroke-opacity", 0.8);
      })
      .on("mouseout", (event, d) => {
        setHoverLink(null);
        d3.select(`#link-${d.id}`)
          .attr("stroke-width", 1 + d.weight * 3)
          .attr("stroke-opacity", d.status === "CONFIRMED_NON_MATCH" ? 0 : 0.6);
      })
      .on("click", (event, d: EntityLink) => {
        stableSetSelectedEdgeId(d.id)
        event.stopPropagation()
      })

    // Create nodes
    const node = g
      .append("g")
      .selectAll("circle")
      .data(nodes)
      .enter()
      .append("circle")
      .attr("r", 8)
      .attr("fill", "#e5e7eb")
      .attr("stroke", "#9ca3af")
      .attr("stroke-width", 1)
      .attr("id", (d) => `node-${d.id}`)
      .on("mouseover", (event, d) => {
        setHoverNode(d.id);
        d3.select(`#node-${d.id}`).attr("fill", "#d1d5db").attr("r", 10);
        loadNodeDetails(d.id);
      })
      .on("mouseout", (event, d) => {
        setHoverNode(null);
        d3.select(`#node-${d.id}`).attr("fill", "#e5e7eb").attr("r", 8);
      })
      .call(d3.drag().on("start", dragstarted).on("drag", dragged).on("end", dragended) as any)

    // Add node labels
    const label = g
      .append("g")
      .selectAll("text")
      .data(nodes)
      .enter()
      .append("text")
      .text((d) => d.name || "Unnamed")
      .attr("font-size", "8px")
      .attr("dx", 10)
      .attr("dy", 3)
      .attr("fill", "#4b5563")

    console.log('[updateVisualization] Created DOM elements');

    // Create simulation
    if (simulationRef.current) {
      console.log('[updateVisualization] Stopping previous simulation');
      simulationRef.current.stop()
    }

    const simulation = d3
      .forceSimulation(nodes as d3.SimulationNodeDatum[])
      .force(
        "link",
        d3
          .forceLink(links)
          .id((d) => (d as EntityNode).id)
          .distance(100)
          .strength(0.1),
      )
      .force("charge", d3.forceManyBody().strength(-200))
      .force("center", d3.forceCenter(width / 2, height / 2))
      .force("collision", d3.forceCollide().radius(20))
      .on("tick", () => {
        link
          .attr("x1", (d) => (d.source as any).x)
          .attr("y1", (d) => (d.source as any).y)
          .attr("x2", (d) => (d.target as any).x)
          .attr("y2", (d) => (d.target as any).y)

        node.attr("cx", (d) => (d as any).x).attr("cy", (d) => (d as any).y)

        label.attr("x", (d) => (d as any).x).attr("y", (d) => (d as any).y)
      })
      .on("end", () => {
        console.log('[updateVisualization] Simulation ended');
        // DON'T auto-reset zoom here - this was overriding user zoom actions!
        // The user can manually reset if they want to center the graph
      });

    simulationRef.current = simulation;
    console.log('[updateVisualization] Created new simulation');

    // Restore the previous transform if it was meaningful (not identity)
    if (currentTransform.k !== 1 || currentTransform.x !== 0 || currentTransform.y !== 0) {
      console.log('[updateVisualization] Restoring previous transform');
      setTimeout(() => {
        if (zoomRef.current && svgRef.current) {
          isProgrammaticZoomRef.current = true;
          svg.call(zoomRef.current.transform as any, currentTransform);
          // Reset flag after restore
          setTimeout(() => {
            isProgrammaticZoomRef.current = false;
          }, 100);
        }
      }, 100);
    }

    // Highlight selected edge
    if (selectedEdgeId) {
      d3.select(`#link-${selectedEdgeId}`)
        .attr("stroke-width", 1 + (links.find((l) => l.id === selectedEdgeId)?.weight || 0) * 5)
        .attr("stroke-opacity", 0.8)
    }

    function dragstarted(event: any) {
      if (!event.active) simulation.alphaTarget(0.3).restart()
      event.subject.fx = event.subject.x
      event.subject.fy = event.subject.y
    }

    function dragged(event: any) {
      event.subject.fx = event.x
      event.subject.fy = event.y
    }

    function dragended(event: any) {
      if (!event.active) simulation.alphaTarget(0)
      event.subject.fx = null
      event.subject.fy = null
    }
  }, [dimensions, nodes, links, selectedEdgeId, stableSetSelectedEdgeId, loadNodeDetails])

  // Update visualization when data or dimensions change
  useEffect(() => {
    console.log('[useEffect] Triggering updateVisualization due to dependency change');
    console.log('[useEffect] Dependencies changed:', JSON.stringify({
      hasDimensions: dimensions.width > 0 && dimensions.height > 0,
      hasNodes: nodes.length > 0,
      hasLinks: links.length > 0,
      selectedEdgeId
    }));
    updateVisualization()
  }, [updateVisualization])

  // Handle edge status updates from external sources
  useEffect(() => {
    const handleEdgeStatusUpdate = (event: CustomEvent) => {
      const { edgeId, status } = event.detail
      setReviewedEdges(prev => ({
        ...prev,
        [edgeId]: status
      }))
      
      // Update link appearance
      const linkElement = d3.select(`#link-${edgeId}`)
      if (status === "CONFIRMED_MATCH") {
        linkElement.attr("stroke", "#000").attr("stroke-opacity", 0.8)
      } else if (status === "CONFIRMED_NON_MATCH") {
        linkElement.attr("stroke", "transparent").attr("stroke-opacity", 0)
      }

      // Calculate and update progress
      const totalEdges = links.length
      const reviewedCount = Object.keys(reviewedEdges).length + 1
      const progress = totalEdges > 0 ? Math.round((reviewedCount / totalEdges) * 100) : 0

      if (selectedClusterId) {
        updateReviewProgress(selectedClusterId, progress)
      }

      // Auto-advance to next unreviewed edge
      const nextUnreviewedEdge = links.find((link) => 
        !reviewedEdges[link.id] && link.id !== edgeId && 
        (!link.status || link.status === 'PENDING_REVIEW')
      )
      if (nextUnreviewedEdge) {
        setSelectedEdgeId(nextUnreviewedEdge.id)
      } else if (progress >= 100) {
        setSelectedEdgeId("")
      }
    }

    window.addEventListener('edgeStatusUpdate', handleEdgeStatusUpdate as EventListener)
    return () => {
      window.removeEventListener('edgeStatusUpdate', handleEdgeStatusUpdate as EventListener)
    }
  }, [links, reviewedEdges, selectedClusterId, updateReviewProgress, setSelectedEdgeId])

  // Auto-select the first unreviewed connection when links change
  useEffect(() => {
    if (links.length > 0 && !selectedEdgeId) {
      const firstUnreviewedEdge = links.find((link) => 
        !reviewedEdges[link.id] && (!link.status || link.status === 'PENDING_REVIEW')
      )
      if (firstUnreviewedEdge) {
        setSelectedEdgeId(firstUnreviewedEdge.id)
      }
    }
  }, [links, reviewedEdges, selectedEdgeId, setSelectedEdgeId])

  // Cleanup function for the component
  useEffect(() => {
    return () => {
      if (simulationRef.current) {
        simulationRef.current.stop()
      }
    }
  }, [])

  return (
    <div
      ref={containerRef}
      className={`relative h-full flex flex-col`}
    >
      {loading ? (
        <div className="flex justify-center items-center h-full">
          <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-gray-900"></div>
        </div>
      ) : !selectedClusterId ? (
        <div className="flex justify-center items-center h-full text-muted-foreground">
          Select a cluster to visualize
        </div>
      ) : (
        <>
          {/* Zoom Toolbar */}
          <div className="absolute top-4 right-4 z-30 flex flex-col gap-1 bg-white/90 backdrop-blur-sm rounded-lg p-2 shadow-lg border">
            <TooltipProvider>
              <div className="flex flex-col gap-1">
                <Tooltip>
                  <TooltipTrigger asChild>
                    <Button
                      size="sm"
                      variant="ghost"
                      onClick={zoomIn}
                      className="h-8 w-8 p-0"
                    >
                      <ZoomIn className="h-4 w-4" />
                    </Button>
                  </TooltipTrigger>
                  <TooltipContent side="left">
                    <p>Zoom In</p>
                  </TooltipContent>
                </Tooltip>

                <Tooltip>
                  <TooltipTrigger asChild>
                    <Button
                      size="sm"
                      variant="ghost"
                      onClick={zoomOut}
                      className="h-8 w-8 p-0"
                    >
                      <ZoomOut className="h-4 w-4" />
                    </Button>
                  </TooltipTrigger>
                  <TooltipContent side="left">
                    <p>Zoom Out</p>
                  </TooltipContent>
                </Tooltip>

                <Tooltip>
                  <TooltipTrigger asChild>
                    <Button
                      size="sm"
                      variant="ghost"
                      onClick={centerGraph}
                      className="h-8 w-8 p-0"
                    >
                      <RotateCcw className="h-4 w-4" />
                    </Button>
                  </TooltipTrigger>
                  <TooltipContent side="left">
                    <p>Reset & Center</p>
                  </TooltipContent>
                </Tooltip>
              </div>
            </TooltipProvider>

            {/* Zoom Level Indicator */}
            <div className="text-xs text-center text-gray-500 mt-1 min-w-0">
              {Math.round(zoomLevel * 100)}%
            </div>
          </div>

          {/* Node Hover Tooltip */}
          {hoverNode && nodeDetails[hoverNode] && (
            <div
              className="absolute bg-white p-3 rounded-md shadow-md border text-sm z-20 pointer-events-none"
              style={{
                left: 20,
                top: 20
              }}
            >
              <div className="font-medium">{nodeDetails[hoverNode].organization.name}</div>
              {nodeDetails[hoverNode].organization.description && (
                <div className="text-xs text-muted-foreground mt-1">
                  {nodeDetails[hoverNode].organization.description}
                </div>
              )}
              {nodeDetails[hoverNode].addresses.length > 0 && (
                <div className="text-xs mt-1">
                  {nodeDetails[hoverNode].addresses[0].address_1}, {nodeDetails[hoverNode].addresses[0].city}
                </div>
              )}
              {nodeDetails[hoverNode].phones.length > 0 && (
                <div className="text-xs mt-1">{nodeDetails[hoverNode].phones[0].number}</div>
              )}
            </div>
          )}

          {/* Link Hover Tooltip */}
          {hoverLink && !selectedEdgeId && (
            <div
              className="absolute bg-white p-3 rounded-md shadow-md border text-sm z-20 pointer-events-none"
              style={{
                left: Math.min(
                  dimensions.width - 200,
                  ((d3.select(`#link-${hoverLink}`).node() as any)?.x1.baseVal.value +
                    (d3.select(`#link-${hoverLink}`).node() as any)?.x2.baseVal.value) /
                    2 || 0,
                ),
                top: Math.min(
                  dimensions.height - 100,
                  ((d3.select(`#link-${hoverLink}`).node() as any)?.y1.baseVal.value +
                    (d3.select(`#link-${hoverLink}`).node() as any)?.y2.baseVal.value) /
                    2 || 0,
                ),
              }}
            >
              <div className="font-medium">Connection Details</div>
              <div className="text-xs mt-1">
                Confidence: {(links.find((l) => l.id === hoverLink)?.weight || 0).toFixed(2)}
              </div>
              <div
                className="text-xs text-blue-600 mt-1 cursor-pointer"
                onClick={(e) => {
                  e.stopPropagation()
                  setSelectedEdgeId(hoverLink)
                }}
              >
                Click to review
              </div>
            </div>
          )}

          {/* Main SVG */}
          <svg
            ref={svgRef}
            className="w-full h-full border rounded-md bg-white"
            onClick={() => setSelectedEdgeId("")}
          ></svg>
        </>
      )}
    </div>
  )
}
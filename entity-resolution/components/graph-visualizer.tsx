"use client"

import { useEffect, useRef, useState, useCallback } from "react"
import { useEntityResolution } from "@/context/entity-resolution-context"
import { getVisualizationData } from "@/utils/api-client"
import type { EntityNode, EntityLink, EntityGroup } from "@/types/entity-resolution"
import * as d3 from "d3"
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover"
import { Info, ZoomIn, ZoomOut, Maximize, Minimize, RefreshCw } from "lucide-react"
import { Button } from "@/components/ui/button"
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip"
import { useResizeObserver } from "@/hooks/use-resize-observer"

export default function GraphVisualizer() {
  const { selectedClusterId, setSelectedEdgeId, selectedEdgeId, updateReviewProgress, refreshTrigger } =
    useEntityResolution()

  const containerRef = useRef<HTMLDivElement>(null)
  const svgRef = useRef<SVGSVGElement>(null)
  const zoomRef = useRef<d3.ZoomBehavior<Element, unknown> | null>(null)
  const [nodes, setNodes] = useState<EntityNode[]>([])
  const [links, setLinks] = useState<EntityLink[]>([])
  const [loading, setLoading] = useState(false)
  const [hoverNode, setHoverNode] = useState<string | null>(null)
  const [hoverLink, setHoverLink] = useState<string | null>(null)
  const [nodeDetails, setNodeDetails] = useState<Record<string, any>>({})
  const [zoomLevel, setZoomLevel] = useState(1)
  const [fullscreen, setFullscreen] = useState(false)
  const [dimensions, setDimensions] = useState({ width: 0, height: 0 })

  // Track reviewed edges - use the actual edge IDs
  const [reviewedEdges, setReviewedEdges] = useState<Record<string, "ACCEPTED" | "REJECTED">>({})

  // D3 simulation reference
  const simulationRef = useRef<d3.Simulation<d3.SimulationNodeDatum, undefined> | null>(null)

  // Use resize observer to track container dimensions
  useResizeObserver(containerRef, (entry) => {
    if (entry) {
      const { width, height } = entry.contentRect
      setDimensions({ width, height })
    }
  })

  async function loadVisualizationData() {
    setLoading(true)
    try {
      const { nodes, links, entityGroups } = await getVisualizationData(selectedClusterId)
      setNodes(nodes)
      setLinks(links)
      
      // Initialize reviewed edges based on entity groups
      // We need to map entity groups to their corresponding edges
      const reviewedEdgesMap: Record<string, 'ACCEPTED' | 'REJECTED'> = {}
      
      // Group entity groups by their entities to find corresponding edges
      entityGroups.forEach((group: EntityGroup) => {
        if (group.confirmed_status === 'ACCEPTED' || group.confirmed_status === 'REJECTED') {
          // Find the edge that connects the entities in this group
          // Since entity groups don't directly reference edges, we need to infer this
          // This is a simplified approach - you may need to adjust based on your data relationships
          
          // For now, we'll track by group ID and map it later when we have edge selection
          // A more robust solution would require the API to provide edge-to-group mappings
        }
      })
      
      setReviewedEdges(reviewedEdgesMap)
      
      // Calculate review progress based on edge status from the API response
      const totalEdges = links.length
      const reviewedCount = links.filter(link => 
        link.status === 'ACCEPTED' || link.status === 'REJECTED'
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

  // Load node details
  const loadNodeDetails = useCallback(async (nodeId: string): Promise<void> => {
    if (nodeDetails[nodeId]) return

    try {
      // TODO: Implement node details loading if needed
      // For now, we'll just set a placeholder
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
      }))
    } catch (error) {
      console.error("Failed to load node details:", error)
    }
  }, [nodeDetails, nodes])

  // Initialize and update D3 visualization
  const updateVisualization = useCallback(() => {
    if (
      !svgRef.current ||
      nodes.length === 0 ||
      links.length === 0 ||
      dimensions.width === 0 ||
      dimensions.height === 0
    )
      return

    // Clear previous visualization
    d3.select(svgRef.current).selectAll("*").remove()

    const width = dimensions.width
    const height = dimensions.height

    // Create SVG container with zoom behavior
    const svg = d3.select(svgRef.current).attr("width", width).attr("height", height)

    const g = svg.append("g")

    // Initialize or reuse zoom behavior
    if (!zoomRef.current) {
      zoomRef.current = d3
        .zoom()
        .scaleExtent([0.1, 4])
        .on("zoom", (event) => {
          g.attr("transform", event.transform)
          setZoomLevel(event.transform.k)
        })
    }

    // Apply zoom behavior to the SVG
    svg.call(zoomRef.current as any)

    // Create links
    const link = g
      .append("g")
      .selectAll("line")
      .data(links)
      .enter()
      .append("line")
      .attr("stroke-width", (d) => 1 + d.weight * 3)
      .attr("stroke", (d) => {
        // Use the status from the link itself (from API response)
        if (d.status === "ACCEPTED") return "#000"
        if (d.status === "REJECTED") return "transparent"

        // Color spectrum based on confidence for pending review
        return d3.interpolateRgb("#f87171", "#60a5fa")(d.weight)
      })
      .attr("stroke-opacity", (d) => {
        if (d.status === "REJECTED") return 0
        return 0.6
      })
      .attr("id", (d) => `link-${d.id}`)
      .on("mouseover", (event, d) => {
        // Clear any existing hover state first
        setHoverLink(null)
        // Set after a small delay to prevent flickering
        setTimeout(() => {
          setHoverLink(d.id)
          d3.select(`#link-${d.id}`)
            .attr("stroke-width", 1 + d.weight * 5)
            .attr("stroke-opacity", 0.8)
        }, 50)
      })
      .on("mouseout", (event, d) => {
        setTimeout(() => {
          if (hoverLink === d.id) {
            setHoverLink(null)
            d3.select(`#link-${d.id}`)
              .attr("stroke-width", 1 + d.weight * 3)
              .attr("stroke-opacity", d.status === "REJECTED" ? 0 : 0.6)
          }
        }, 100)
      })
      .on("click", (event, d: EntityLink) => {
        setSelectedEdgeId(d.id)
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
        // Clear any existing hover state first
        setHoverNode(null)
        // Set after a small delay to prevent flickering
        setTimeout(() => {
          setHoverNode(d.id)
          d3.select(`#node-${d.id}`).attr("fill", "#d1d5db").attr("r", 10)

          loadNodeDetails(d.id)
        }, 50)
      })
      .on("mouseout", (event, d) => {
        setTimeout(() => {
          if (hoverNode === d.id) {
            setHoverNode(null)
            d3.select(`#node-${d.id}`).attr("fill", "#e5e7eb").attr("r", 8)
          }
        }, 100)
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

    // Create simulation
    if (simulationRef.current) {
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

    simulationRef.current = simulation

    // Highlight selected edge
    if (selectedEdgeId) {
      d3.select(`#link-${selectedEdgeId}`)
        .attr("stroke-width", 1 + (links.find((l) => l.id === selectedEdgeId)?.weight || 0) * 5)
        .attr("stroke-opacity", 0.8)
    }

    // Auto-fit the graph to the viewport
    setTimeout(() => {
      const bounds = g.node()?.getBBox()
      if (bounds) {
        const dx = bounds.width
        const dy = bounds.height
        const x = bounds.x + dx / 2
        const y = bounds.y + dy / 2

        // Calculate the scale to fit the graph
        const scale = 0.9 / Math.max(dx / width, dy / height)
        const translate = [width / 2 - scale * x, height / 2 - scale * y]

        // Apply the transform using the shared zoom reference
        if (zoomRef.current) {
          const transform = d3.zoomIdentity
            .translate(translate[0], translate[1])
            .scale(scale);
          
          svg
            .transition()
            .duration(500)
            .call(zoomRef.current.transform as any, transform);
        }
        
        setZoomLevel(scale)
      }
    }, 500) // Wait for simulation to stabilize

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
  }, [dimensions, nodes, links, selectedEdgeId, nodeDetails, setSelectedEdgeId, loadNodeDetails])

  // Update visualization when data or dimensions change
  useEffect(() => {
    updateVisualization()
  }, [updateVisualization])

  // Handle edge status updates from external sources (after review submission)
  useEffect(() => {
    // Listen for edge status updates
    const handleEdgeStatusUpdate = (event: CustomEvent) => {
      const { edgeId, status } = event.detail
      setReviewedEdges(prev => ({
        ...prev,
        [edgeId]: status
      }))
      
      // Update link appearance
      const linkElement = d3.select(`#link-${edgeId}`)
      if (status === "ACCEPTED") {
        linkElement.attr("stroke", "#000").attr("stroke-opacity", 0.8)
      } else if (status === "REJECTED") {
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
        // If all edges are reviewed, clear selection
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
      className={`relative ${fullscreen ? "fixed inset-0 z-50 bg-white p-4 flex flex-col" : "h-full flex flex-col"}`}
      style={{
        height: fullscreen ? "100vh" : "100%",
      }}
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

          {hoverLink && !selectedEdgeId && (
            <div
              className="absolute bg-white p-3 rounded-md shadow-md border text-sm z-20 pointer-events-none"
              style={{
                left: Math.min(
                  dimensions.width - 200, // Prevent overflow to the right
                  ((d3.select(`#link-${hoverLink}`).node() as any)?.x1.baseVal.value +
                    (d3.select(`#link-${hoverLink}`).node() as any)?.x2.baseVal.value) /
                    2 || 0,
                ),
                top: Math.min(
                  dimensions.height - 100, // Prevent overflow to the bottom
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
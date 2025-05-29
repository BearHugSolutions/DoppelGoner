// components/graph-visualizer.tsx

"use client";

import { useEffect, useRef, useState, useCallback, useMemo } from "react";
import { useEntityResolution } from "@/context/entity-resolution-context";
import type {
  EntityNode,
  EntityLink,
  EntityGroup,
} from "@/types/entity-resolution";
import * as d3 from "d3";
// Removed Eye, EyeOff, Info, Maximize, Minimize, RefreshCw, Move as they are no longer used or were not used
import { ZoomIn, ZoomOut, RotateCcw, AlertCircle } from "lucide-react";
import { Button } from "@/components/ui/button";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import { useResizeObserver } from "@/hooks/use-resize-observer";

export default function GraphVisualizer() {
  const {
    selectedClusterId,
    selectedEdgeId,
    currentVisualizationData,
    actions,
    queries,
  } = useEntityResolution();

  const containerRef = useRef<HTMLDivElement>(null);
  const svgRef = useRef<SVGSVGElement>(null);
  const gRef = useRef<d3.Selection<
    SVGGElement,
    unknown,
    null,
    undefined
  > | null>(null);
  const zoomRef = useRef<d3.ZoomBehavior<Element, unknown> | null>(null);
  const [nodes, setNodes] = useState<EntityNode[]>([]);
  const [links, setLinks] = useState<EntityLink[]>([]);
  const [entityGroups, setEntityGroups] = useState<EntityGroup[]>([]);
  const [hoverNode, setHoverNode] = useState<string | null>(null);
  const [hoverLink, setHoverLink] = useState<string | null>(null);
  const [nodeContactInfo, setNodeContactInfo] = useState<Record<string, any>>(
    {}
  );
  const [zoomLevel, setZoomLevel] = useState(1);
  const [rawDimensions, setRawDimensions] = useState({ width: 0, height: 0 });

  // Removed state for showing/hiding connections
  // const [showConfirmedMatches, setShowConfirmedMatches] = useState(true)
  // const [showConfirmedNonMatches, setShowConfirmedNonMatches] = useState(true)
  // const [showPendingReview, setShowPendingReview] = useState(true)

  const dimensions = useMemo(
    () => ({
      width: rawDimensions.width,
      height: rawDimensions.height,
    }),
    [rawDimensions.width, rawDimensions.height]
  );

  const simulationRef = useRef<d3.Simulation<
    d3.SimulationNodeDatum,
    undefined
  > | null>(null);

  const isProgrammaticZoomRef = useRef(false);

  const computeAllNodeContactInfo = useCallback(() => {
    const allContactInfo: Record<string, any> = {};

    for (const node of nodes) {
      const contactInfo: {
        email?: string;
        phone?: string;
        address?: string;
        url?: string;
      } = {};

      for (const group of entityGroups) {
        const values = group.match_values.values;

        const isEntity1 = group.entity_id_1 === node.id;
        const isEntity2 = group.entity_id_2 === node.id;

        if (!isEntity1 && !isEntity2) continue;

        switch (group.match_values.type?.toLowerCase()) {
          case "email":
            if (isEntity1 && values.original_email1) {
              contactInfo.email = values.original_email1;
            } else if (isEntity2 && values.original_email2) {
              contactInfo.email = values.original_email2;
            }
            break;

          case "phone":
            if (isEntity1 && values.original_phone1) {
              contactInfo.phone = values.original_phone1;
            } else if (isEntity2 && values.original_phone2) {
              contactInfo.phone = values.original_phone2;
            }
            break;

          case "address":
            if (isEntity1 && values.original_address1) {
              contactInfo.address = values.original_address1;
            } else if (isEntity2 && values.original_address2) {
              contactInfo.address = values.original_address2;
            }
            break;

          case "url":
            if (isEntity1 && values.original_url1) {
              contactInfo.url = values.original_url1;
            } else if (isEntity2 && values.original_url2) {
              contactInfo.url = values.original_url2;
            }
            break;
        }
      }

      allContactInfo[node.id] = {
        ...node,
        contactInfo,
      };
    }

    setNodeContactInfo(allContactInfo);
  }, [nodes, entityGroups]);

  useEffect(() => {
    if (nodes.length > 0 && entityGroups.length > 0) {
      computeAllNodeContactInfo();
    }
  }, [computeAllNodeContactInfo]);

  useResizeObserver(containerRef, (entry) => {
    if (entry) {
      const { width, height } = entry.contentRect;
      setRawDimensions((prev) => {
        if (prev.width !== width || prev.height !== height) {
          return { width, height };
        }
        return prev;
      });
    }
  });

  const safeZoomTo = useCallback((transform: d3.ZoomTransform) => {
    if (!zoomRef.current || !svgRef.current) return;

    const safeTransform = d3.zoomIdentity
      .translate(
        isFinite(transform.x) ? transform.x : 0,
        isFinite(transform.y) ? transform.y : 0
      )
      .scale(isFinite(transform.k) && transform.k > 0 ? transform.k : 1);

    d3.select(svgRef.current)
      .transition()
      .duration(500)
      .call(zoomRef.current.transform as any, safeTransform);
  }, []);

  const zoomIn = useCallback(() => {
    if (!zoomRef.current || !svgRef.current) return;
    d3.select(svgRef.current)
      .transition()
      .duration(300)
      .call(zoomRef.current.scaleBy as any, 1.5);
  }, []);

  const zoomOut = useCallback(() => {
    if (!zoomRef.current || !svgRef.current) return;
    d3.select(svgRef.current)
      .transition()
      .duration(300)
      .call(zoomRef.current.scaleBy as any, 1 / 1.5);
  }, []);

  const resetZoom = useCallback(() => {
    if (!zoomRef.current || !svgRef.current || !gRef.current) return;

    const { width, height } = dimensions;
    if (width === 0 || height === 0) return;

    try {
      const graphBounds = gRef.current.node()?.getBBox();

      if (!graphBounds || graphBounds.width === 0 || graphBounds.height === 0) {
        safeZoomTo(d3.zoomIdentity.translate(width / 2, height / 2).scale(1));
        return;
      }

      const padding = 50;
      const scaleX = (width - padding * 2) / graphBounds.width;
      const scaleY = (height - padding * 2) / graphBounds.height;
      const scale = Math.min(scaleX, scaleY, 2);

      const centerX = graphBounds.x + graphBounds.width / 2;
      const centerY = graphBounds.y + graphBounds.height / 2;
      const translateX = width / 2 - scale * centerX;
      const translateY = height / 2 - scale * centerY;

      const transform = d3.zoomIdentity
        .translate(translateX, translateY)
        .scale(scale);

      safeZoomTo(transform);
    } catch (error) {
      console.warn("Error in resetZoom, falling back to simple center:", error);
      safeZoomTo(d3.zoomIdentity.translate(width / 2, height / 2).scale(1));
    }
  }, [dimensions, safeZoomTo]);

  const centerGraph = useCallback(() => {
    resetZoom();
  }, [resetZoom]);

  // visibleLinks now includes all links, as the filtering states have been removed.
  const visibleLinks = useMemo(() => {
    return links;
  }, [links]);

  useEffect(() => {
    if (!currentVisualizationData) {
      setNodes([]);
      setLinks([]);
      setEntityGroups([]);
      return;
    }

    const { nodes, links, entityGroups } = currentVisualizationData;
    setNodes(nodes);
    setLinks(links);
    setEntityGroups(entityGroups);
  }, [currentVisualizationData]);

  const updateVisualization = useCallback(() => {
    if (
      !svgRef.current ||
      nodes.length === 0 ||
      // visibleLinks (now all links) can be empty initially, but we might still want to draw nodes
      dimensions.width === 0 ||
      dimensions.height === 0
    ) {
      // Clear previous visualization if dimensions are zero or no nodes
      if (svgRef.current) d3.select(svgRef.current).selectAll("*").remove();
      return;
    }

    const currentTransform = svgRef.current
      ? d3.zoomTransform(svgRef.current)
      : d3.zoomIdentity;
    d3.select(svgRef.current).selectAll("*").remove();

    const width = dimensions.width;
    const height = dimensions.height;

    const svg = d3
      .select(svgRef.current)
      .attr("width", width)
      .attr("height", height);

    const g = svg.append("g");
    gRef.current = g;

    zoomRef.current = d3
      .zoom<Element, unknown>()
      .scaleExtent([0.1, 4])
      .on("zoom", (event) => {
        const transform = event.transform;
        if (
          isFinite(transform.x) &&
          isFinite(transform.y) &&
          isFinite(transform.k) &&
          transform.k > 0
        ) {
          g.attr("transform", transform.toString());
          if (!isProgrammaticZoomRef.current) {
            setZoomLevel(transform.k);
          }
        }
      });

    svg.call(zoomRef.current as any);

    const linkElements = g
      .append("g")
      .selectAll("line")
      .data(visibleLinks) // Using visibleLinks which now contains all links
      .enter()
      .append("line")
      .attr("stroke-width", (d) => {
        if (d.status === "CONFIRMED_MATCH") return 4;
        if (d.status === "CONFIRMED_NON_MATCH") return 2;
        return 1 + d.weight * 3;
      })
      .attr("stroke", (d) => {
        if (d.status === "CONFIRMED_MATCH") return "#16a34a";
        if (d.status === "CONFIRMED_NON_MATCH") return "#dc2626";
        return d3.interpolateRgb("#f87171", "#3b82f6")(d.weight);
      })
      .attr("stroke-opacity", (d) => {
        if (d.status === "CONFIRMED_NON_MATCH") return 0.4;
        if (d.status === "CONFIRMED_MATCH") return 0.9;
        return 0.7;
      })
      .attr("stroke-dasharray", (d) => {
        return d.status === "CONFIRMED_NON_MATCH" ? "5,5" : "none";
      })
      .attr("id", (d) => `link-${d.id}`)
      .on("mouseover", (event, d) => {
        setHoverLink(d.id);
        if (selectedEdgeId && selectedEdgeId === d.id) return; // Don't apply hover effect if selected
        d3.select(`#link-${d.id}`)
          .attr(
            "stroke-width",
            d.status === "CONFIRMED_MATCH"
              ? 6
              : d.status === "CONFIRMED_NON_MATCH"
              ? 4
              : 1 + d.weight * 5
          )
          .attr("stroke-opacity", 0.9);
      })
      .on("mouseout", (event, d) => {
        setHoverLink(null);
        if (selectedEdgeId && selectedEdgeId === d.id) return; // Don't reset if selected
        d3.select(`#link-${d.id}`)
          .attr(
            "stroke-width",
            d.status === "CONFIRMED_MATCH"
              ? 4
              : d.status === "CONFIRMED_NON_MATCH"
              ? 2
              : 1 + d.weight * 3
          )
          .attr(
            "stroke-opacity",
            d.status === "CONFIRMED_NON_MATCH"
              ? 0.4
              : d.status === "CONFIRMED_MATCH"
              ? 0.9
              : 0.7
          );
      })
      .on("click", (event, d: EntityLink) => {
        actions.setSelectedEdgeId(d.id);
        event.stopPropagation();
      });

    const nodeElements = g
      .append("g")
      .selectAll("circle")
      .data(nodes)
      .enter()
      .append("circle")
      .attr("r", (d) => {
        const hasConfirmedConnections = visibleLinks.some(
          (link) =>
            (link.source === d.id || link.target === d.id) &&
            link.status === "CONFIRMED_MATCH"
        );
        return hasConfirmedConnections ? 10 : 8;
      })
      .attr("fill", (d) => {
        const confirmedConnections = visibleLinks.filter(
          (link) =>
            (link.source === d.id || link.target === d.id) &&
            link.status === "CONFIRMED_MATCH"
        ).length;
        const pendingConnections = visibleLinks.filter(
          (link) =>
            (link.source === d.id || link.target === d.id) &&
            link.status === "PENDING_REVIEW"
        ).length;

        if (confirmedConnections > 0) return "#dcfce7";
        if (pendingConnections > 0) return "#fef3c7";
        return "#f3f4f6";
      })
      .attr("stroke", (d) => {
        const confirmedConnections = visibleLinks.filter(
          (link) =>
            (link.source === d.id || link.target === d.id) &&
            link.status === "CONFIRMED_MATCH"
        ).length;
        const pendingConnections = visibleLinks.filter(
          (link) =>
            (link.source === d.id || link.target === d.id) &&
            link.status === "PENDING_REVIEW"
        ).length;

        if (confirmedConnections > 0) return "#16a34a";
        if (pendingConnections > 0) return "#d97706";
        return "#9ca3af";
      })
      .attr("stroke-width", 2)
      .attr("id", (d) => `node-${d.id}`)
      .on("mouseover", (event, d) => {
        setHoverNode(d.id);
        d3.select(`#node-${d.id}`).attr("r", 12);
      })
      .on("mouseout", (event, d) => {
        setHoverNode(null);
        const hasConfirmedConnections = visibleLinks.some(
          (link) =>
            (link.source === d.id || link.target === d.id) &&
            link.status === "CONFIRMED_MATCH"
        );
        d3.select(`#node-${d.id}`).attr("r", hasConfirmedConnections ? 10 : 8);
      })
      .call(
        d3
          .drag()
          .on("start", dragstarted)
          .on("drag", dragged)
          .on("end", dragended) as any
      );

    const labelElements = g
      .append("g")
      .selectAll("text")
      .data(nodes)
      .enter()
      .append("text")
      .text((d) => d.name || "Unnamed")
      .attr("font-size", "9px")
      .attr("dx", 12)
      .attr("dy", 3)
      .attr("fill", "#374151")
      .attr("font-weight", "500");

    const nodeTooltipElements = g
      .append("g")
      .selectAll("foreignObject")
      .data(nodes)
      .enter()
      .append("foreignObject")
      .attr("width", 200)
      .attr("height", 120) // Fixed height for node tooltips
      .style("pointer-events", "none")
      .style("opacity", 0)
      .attr("id", (d) => `tooltip-${d.id}`)
      .html((d) => {
        const nodeInfo = nodeContactInfo[d.id];
        if (!nodeInfo) return "";

        const contactInfo = nodeInfo.contactInfo;
        return `
          <div style="background: white; padding: 8px; border-radius: 6px; box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06); border: 1px solid #e5e7eb; font-size: 11px; max-width: 200px; height: 100%; overflow: hidden;">
            <div style="font-weight: 500; margin-bottom: 4px;">${
              nodeInfo.name || "Unknown"
            }</div>
            <div style="margin-bottom: 2px;"><span style="font-weight: 500;">Source:</span> ${
              nodeInfo.source_system || "Unknown"
            }${nodeInfo.source_id ? ` (${nodeInfo.source_id})` : ""}</div>
            ${
              contactInfo.address
                ? `<div style="margin-bottom: 2px;"><span style="font-weight: 500;">Address:</span> ${contactInfo.address}</div>`
                : ""
            }
            ${
              contactInfo.phone
                ? `<div style="margin-bottom: 2px;"><span style="font-weight: 500;">Phone:</span> ${contactInfo.phone}</div>`
                : ""
            }
            ${
              contactInfo.email
                ? `<div style="margin-bottom: 2px;"><span style="font-weight: 500;">Email:</span> ${contactInfo.email}</div>`
                : ""
            }
            ${
              contactInfo.url
                ? `<div style="margin-bottom: 2px;"><span style="font-weight: 500;">URL:</span> <span style="color: #2563eb; word-break: break-all;">${contactInfo.url}</span></div>`
                : ""
            }
          </div>
        `;
      });

    const linkTooltipElements = g
      .append("g")
      .selectAll("foreignObject")
      .data(visibleLinks) // Using visibleLinks which now contains all links
      .enter()
      .append("foreignObject")
      .attr("width", 270)
      .attr("height", 280)
      .style("pointer-events", "none")
      .style("opacity", 0)
      .attr("id", (d) => `link-tooltip-${d.id}`)
      .html((d_link: EntityLink) => {
        const linkEntityGroups = entityGroups.filter(
          (group) =>
            (group.entity_id_1 === d_link.source &&
              group.entity_id_2 === d_link.target) ||
            (group.entity_id_1 === d_link.target &&
              group.entity_id_2 === d_link.source)
        );

        let statusHtml = "";
        if (d_link.status === "CONFIRMED_MATCH") {
          statusHtml = `<div style="color: #16a34a; margin-bottom: 8px; font-weight: 500; display: flex; align-items: center; gap: 4px; font-size: 11px;">
                          <div style="width: 8px; height: 8px; background-color: #16a34a; border-radius: 50%;"></div>
                          Confirmed Match
                        </div>`;
        } else if (d_link.status === "CONFIRMED_NON_MATCH") {
          statusHtml = `<div style="color: #dc2626; margin-bottom: 8px; font-weight: 500; display: flex; align-items: center; gap: 4px; font-size: 11px;">
                          <div style="width: 8px; height: 8px; background-color: #dc2626; border-radius: 50%;"></div>
                          Confirmed Non-Match
                        </div>`;
        } else if (d_link.status === "PENDING_REVIEW") {
          statusHtml = `<div style="color: #2563eb; margin-bottom: 8px; font-weight: 500; display: flex; align-items: center; gap: 4px; font-size: 11px;">
                          <div style="width: 8px; height: 8px; background-color: #2563eb; border-radius: 50%;"></div>
                          Pending Review
                        </div>`;
        }

        let entityGroupsHtml = "";
        if (linkEntityGroups.length > 0) {
          entityGroupsHtml = `
            <div style="margin-top: 8px;">
              <div style="font-size: 10px; font-weight: 500; color: #6b7280; margin-bottom: 4px;">Match Methods:</div>
              <div style="max-height: 150px; overflow-y: auto; space-y: 4px; padding-right: 5px;">
              ${linkEntityGroups
                .map(
                  (group) => `
                <div style="border-radius: 4px; border: 1px solid #e5e7eb; background-color: rgba(243, 244, 246, 0.5); padding: 6px;">
                  <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 2px;">
                    <span style="font-size: 10px; font-weight: 500; text-transform: capitalize;">
                      ${group.method_type.replace(/_/g, " ")}
                    </span>
                    <span style="font-size: 10px; color: #6b7280;">
                      ${
                        group.confidence_score
                          ? group.confidence_score.toFixed(3)
                          : "N/A"
                      }
                    </span>
                  </div>
                  <div style="font-size: 10px; color: #6b7280; space-y: 2px;">
                    ${
                      group.match_values.type === "Email"
                        ? `
                      <div>${
                        group.match_values.values.original_email1 || "N/A"
                      }</div>
                      <div>${
                        group.match_values.values.original_email2 || "N/A"
                      }</div>
                    `
                        : ""
                    }
                    ${
                      group.match_values.type === "Phone"
                        ? `
                      <div>${
                        group.match_values.values.original_phone1 || "N/A"
                      }</div>
                      <div>${
                        group.match_values.values.original_phone2 || "N/A"
                      }</div>
                    `
                        : ""
                    }
                    ${
                      group.match_values.type === "Address"
                        ? `
                      <div>${
                        group.match_values.values.original_address1 || "N/A"
                      }</div>
                      <div>${
                        group.match_values.values.original_address2 || "N/A"
                      }</div>
                    `
                        : ""
                    }
                    ${
                      group.match_values.type === "Url"
                        ? `
                      <div style="word-break: break-all;">${
                        group.match_values.values.original_url1 || "N/A"
                      }</div>
                      <div style="word-break: break-all;">${
                        group.match_values.values.original_url2 || "N/A"
                      }</div>
                    `
                        : ""
                    }
                    ${
                      group.match_values.type === "Name"
                        ? `
                      <div>${
                        group.match_values.values.original_name1 || "N/A"
                      }</div>
                      <div>${
                        group.match_values.values.original_name2 || "N/A"
                      }</div>
                    `
                        : ""
                    }
                  </div>
                </div>
              `
                )
                .join("")}
              </div>
            </div>
          `;
        }

        const clickToReviewHtml =
          d_link.status === "PENDING_REVIEW"
            ? `
          <div style="font-size: 10px; color: #2563eb; margin-top: 6px; cursor: pointer;">
            Click edge to review →
          </div>
        `
            : "";

        return `
          <div style="background: white; padding: 8px; border-radius: 6px; box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06); border: 1px solid #e5e7eb; font-size: 11px; max-width: 270px; height: 100%; overflow: hidden; display: flex; flex-direction: column;">
            <div style="font-weight: 500; font-size: 12px; margin-bottom: 4px;">Connection Details</div>
            <div style="font-size: 11px; margin-bottom: 8px;">
              Confidence: ${d_link.weight.toFixed(2)}
            </div>
            ${statusHtml}
            ${entityGroupsHtml}
            ${clickToReviewHtml}
          </div>
        `;
      });

    if (simulationRef.current) {
      simulationRef.current.stop();
    }

    const simulation = d3
      .forceSimulation(nodes as d3.SimulationNodeDatum[])
      .force(
        "link",
        d3
          .forceLink(visibleLinks) // Using visibleLinks which now contains all links
          .id((d) => (d as EntityNode).id)
          .distance((d) => {
            const linkData = d as EntityLink;
            if (linkData.status === "CONFIRMED_MATCH") return 80;
            if (linkData.status === "CONFIRMED_NON_MATCH") return 120;
            return 100;
          })
          .strength(0.1)
      )
      .force("charge", d3.forceManyBody().strength(-300))
      .force("center", d3.forceCenter(width / 2, height / 2))
      .force("collision", d3.forceCollide().radius(25))
      .on("tick", () => {
        linkElements
          .attr("x1", (d) => (d.source as any).x)
          .attr("y1", (d) => (d.source as any).y)
          .attr("x2", (d) => (d.target as any).x)
          .attr("y2", (d) => (d.target as any).y);

        nodeElements
          .attr("cx", (d) => (d as any).x)
          .attr("cy", (d) => (d as any).y);

        labelElements
          .attr("x", (d) => (d as any).x)
          .attr("y", (d) => (d as any).y);

        nodeTooltipElements
          .attr("x", (d) => (d as any).x + 15)
          .attr("y", (d) => (d as any).y - 10);

        linkTooltipElements
          .attr(
            "x",
            (d) => ((d.source as any).x + (d.target as any).x) / 2 + 20
          )
          .attr(
            "y",
            (d) => ((d.source as any).y + (d.target as any).y) / 2 - 280 / 2
          );
      });

    simulationRef.current = simulation;

    if (
      currentTransform.k !== 1 ||
      currentTransform.x !== 0 ||
      currentTransform.y !== 0
    ) {
      setTimeout(() => {
        if (zoomRef.current && svgRef.current) {
          isProgrammaticZoomRef.current = true;
          svg.call(zoomRef.current.transform as any, currentTransform);
          setTimeout(() => {
            isProgrammaticZoomRef.current = false;
          }, 100);
        }
      }, 100);
    }

    if (selectedEdgeId) {
      const selectedLinkData = visibleLinks.find(
        (l) => l.id === selectedEdgeId
      ); // Using visibleLinks
      if (selectedLinkData) {
        d3.select(`#link-${selectedEdgeId}`)
          .attr(
            "stroke-width",
            selectedLinkData.status === "CONFIRMED_MATCH"
              ? 6
              : selectedLinkData.status === "CONFIRMED_NON_MATCH"
              ? 5
              : 1 + (selectedLinkData.weight || 0) * 5
          )
          .attr("stroke-opacity", 1.0)
          .attr("stroke", "#000000");
      }
    }

    function dragstarted(event: any) {
      if (!event.active) simulation.alphaTarget(0.3).restart();
      event.subject.fx = event.subject.x;
      event.subject.fy = event.subject.y;
    }

    function dragged(event: any) {
      event.subject.fx = event.x;
      event.subject.fy = event.y;
    }

    function dragended(event: any) {
      if (!event.active) simulation.alphaTarget(0);
      event.subject.fx = null;
      event.subject.fy = null;
    }
  }, [
    dimensions,
    nodes,
    visibleLinks,
    entityGroups,
    selectedEdgeId,
    actions.setSelectedEdgeId,
    nodeContactInfo,
  ]);

  useEffect(() => {
    if (!svgRef.current) return;

    const svg = d3.select(svgRef.current);

    svg
      .selectAll("foreignObject[id^='tooltip-']")
      .transition()
      .duration(150)
      .style("opacity", 0);

    if (hoverNode) {
      svg
        .select(`#tooltip-${hoverNode}`)
        .transition()
        .duration(150)
        .style("opacity", 1);
    }
  }, [hoverNode]);

  useEffect(() => {
    if (!svgRef.current) return;

    const svg = d3.select(svgRef.current);

    svg
      .selectAll("foreignObject[id^='link-tooltip-']")
      .transition()
      .duration(150)
      .style("opacity", 0);

    if (hoverLink && !selectedEdgeId) {
      svg
        .select(`#link-tooltip-${hoverLink}`)
        .transition()
        .duration(150)
        .style("opacity", 1);
    }
  }, [hoverLink, selectedEdgeId]);

  useEffect(() => {
    updateVisualization();
  }, [updateVisualization]);

  useEffect(() => {
    return () => {
      if (simulationRef.current) {
        simulationRef.current.stop();
      }
    };
  }, []);

  const isLoading = selectedClusterId
    ? queries.isVisualizationDataLoading(selectedClusterId)
    : false;
  const error = selectedClusterId
    ? queries.getVisualizationError(selectedClusterId)
    : null;
  const hasData = nodes.length > 0;

  const edgeCounts = useMemo(() => {
    const confirmed = links.filter(
      (l) => l.status === "CONFIRMED_MATCH"
    ).length;
    const nonMatch = links.filter(
      (l) => l.status === "CONFIRMED_NON_MATCH"
    ).length;
    const pending = links.filter((l) => l.status === "PENDING_REVIEW").length;
    return { confirmed, nonMatch, pending, total: links.length };
  }, [links]);

  return (
    <div ref={containerRef} className={`relative h-full flex flex-col`}>
      {isLoading ? (
        <div className="flex justify-center items-center h-full">
          <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-gray-900"></div>
        </div>
      ) : error ? (
        <div className="flex flex-col justify-center items-center h-full text-destructive">
          <AlertCircle className="h-8 w-8 mb-2" />
          <p className="font-semibold">Failed to load visualization</p>
          <p className="text-sm text-muted-foreground">{error}</p>
        </div>
      ) : !selectedClusterId ? (
        <div className="flex justify-center items-center h-full text-muted-foreground">
          Select a cluster to visualize
        </div>
      ) : !hasData ? (
        <div className="flex justify-center items-center h-full text-muted-foreground">
          No visualization data available for this cluster (no entities).
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
                      {" "}
                      <ZoomIn className="h-4 w-4" />{" "}
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
                      {" "}
                      <ZoomOut className="h-4 w-4" />{" "}
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
                      {" "}
                      <RotateCcw className="h-4 w-4" />{" "}
                    </Button>
                  </TooltipTrigger>
                  <TooltipContent side="left">
                    <p>Reset & Center</p>
                  </TooltipContent>
                </Tooltip>
              </div>
            </TooltipProvider>
            <div className="text-xs text-center text-gray-500 mt-1 min-w-0">
              {" "}
              {Math.round(zoomLevel * 100)}%{" "}
            </div>
          </div>

          {/* Visibility Controls Removed */}

          {/* Legend */}
          {/* The legend is now positioned to the top-left where the visibility controls used to be. */}
          {/* You might want to adjust its position (e.g., bottom-left as it was originally) if this new position is not desired. */}
          <div className="absolute top-4 left-4 z-30 bg-white/90 backdrop-blur-sm rounded-lg p-3 shadow-lg border">
            <h4 className="text-xs font-semibold mb-2">Connection Status</h4>
            <div className="space-y-1.5 text-xs">
              <div className="flex items-center gap-2">
                {" "}
                <div className="w-4 h-0.5 bg-green-600"></div>{" "}
                <span>Confirmed Match ({edgeCounts.confirmed})</span>{" "}
              </div>
              <div className="flex items-center gap-2">
                {" "}
                <div
                  className="w-4 h-0.5 bg-red-600 opacity-60"
                  style={{ borderTop: "1px dashed #dc2626" }}
                ></div>{" "}
                <span>Confirmed Non-Match ({edgeCounts.nonMatch})</span>{" "}
              </div>
              <div className="flex items-center gap-2">
                {" "}
                <div className="w-4 h-0.5 bg-blue-500"></div>{" "}
                <span>Pending Review ({edgeCounts.pending})</span>{" "}
              </div>
              <div className="text-xs text-muted-foreground mt-2 pt-2 border-t">
                {" "}
                Total: {edgeCounts.total} connections{" "}
              </div>
            </div>
          </div>

          <svg
            ref={svgRef}
            className="w-full h-full border rounded-md bg-white"
            onClick={() => {
              actions.setSelectedEdgeId("");
              setHoverLink(null);
            }}
          ></svg>
        </>
      )}
    </div>
  );
}

// components/graph-visualizer.tsx
"use client";

import { useEffect, useRef, useState, useCallback, useMemo } from "react";
import { useEntityResolution } from "@/context/entity-resolution-context";
import type {
  EntityNode,
  EntityLink,
  EntityGroup,
  ServiceNode, // New
  ServiceLink, // New
  ServiceGroup, // New
  BaseNode, // Generic (now includes source_system, source_id)
  BaseLink, // Generic
  EntityVisualizationDataResponse, // Specific
  ServiceVisualizationDataResponse, // Specific
} from "@/types/entity-resolution";
import * as d3 from "d3";
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
    resolutionMode, // Get current mode
    selectedClusterId,
    selectedEdgeId,
    currentVisualizationData, // This is now EntityVisualizationDataResponse | ServiceVisualizationDataResponse | null
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

  // Use generic BaseNode and BaseLink for D3 simulation
  // BaseNode now includes source_system and source_id from entity-resolution.ts
  const [nodes, setNodes] = useState<BaseNode[]>([]);
  const [links, setLinks] = useState<BaseLink[]>([]);
  // Store groups based on mode for tooltips
  const [displayGroups, setDisplayGroups] = useState<
    EntityGroup[] | ServiceGroup[]
  >([]);

  const [hoverNode, setHoverNode] = useState<string | null>(null);
  const [hoverLink, setHoverLink] = useState<string | null>(null);
  // nodeContactInfo will store BaseNode properties (id, name, source_system, source_id) plus contactInfo object
  const [nodeContactInfo, setNodeContactInfo] = useState<
    Record<string, BaseNode & { contactInfo: Record<string, any> }>
  >({});
  const [zoomLevel, setZoomLevel] = useState(1);
  const [rawDimensions, setRawDimensions] = useState({ width: 0, height: 0 });

  const dimensions = useMemo(
    () => ({ width: rawDimensions.width, height: rawDimensions.height }),
    [rawDimensions.width, rawDimensions.height]
  );

  const simulationRef = useRef<d3.Simulation<
    d3.SimulationNodeDatum,
    undefined
  > | null>(null);
  const isProgrammaticZoomRef = useRef(false);

  const computeAllNodeContactInfo = useCallback(() => {
    const allContactInfo: Record<
      string,
      BaseNode & { contactInfo: Record<string, any> }
    > = {};
    if (!currentVisualizationData) return; // Still needed for displayGroups

    // The 'nodes' state variable is an array of BaseNode.
    // BaseNode type (from entity-resolution.ts) now includes source_system and source_id.
    for (const node of nodes) {
      // 'node' is of type BaseNode
      const contactInfo: {
        email?: string;
        phone?: string;
        address?: string;
        url?: string;
      } = {};

      // This part iterates through displayGroups (EntityGroup[] or ServiceGroup[])
      // to find contact details (email, phone, address, url) from match_values.
      // This logic remains the same.
      for (const group of displayGroups) {
        const values = group.match_values.values;
        let isEntity1 = false;
        let isEntity2 = false;

        if (resolutionMode === "entity") {
          const eg = group as EntityGroup;
          isEntity1 = eg.entity_id_1 === node.id;
          isEntity2 = eg.entity_id_2 === node.id;
        } else {
          // service mode
          const sg = group as ServiceGroup;
          isEntity1 = sg.service_id_1 === node.id;
          isEntity2 = sg.service_id_2 === node.id;
        }

        if (!isEntity1 && !isEntity2) continue;

        switch (group.match_values.type?.toLowerCase()) {
          case "email":
            contactInfo.email = isEntity1
              ? values.original_email1 || values.email1
              : values.original_email2 || values.email2;
            break;
          case "phone":
            contactInfo.phone = isEntity1
              ? values.original_phone1 || values.phone1
              : values.original_phone2 || values.phone2;
            break;
          case "address":
            contactInfo.address = isEntity1
              ? values.original_address1 || values.address1
              : values.original_address2 || values.address2;
            break;
          case "url":
            contactInfo.url = isEntity1
              ? values.original_url1 || values.url1
              : values.original_url2 || values.url2;
            break;
        }
      }

      // 'node' itself (type BaseNode) now contains id, name, source_system, and source_id.
      // We spread these properties and add the computed contactInfo.
      allContactInfo[node.id] = {
        ...node, // Spreads id, name, source_system, source_id from BaseNode
        contactInfo, // Adds the contact details extracted from groups
      };
    }
    setNodeContactInfo(allContactInfo);
  }, [nodes, displayGroups, resolutionMode, currentVisualizationData]);

  useEffect(() => {
    if (nodes.length > 0 && displayGroups.length > 0) {
      computeAllNodeContactInfo();
    }
  }, [computeAllNodeContactInfo]);

  useResizeObserver(containerRef, (entry) => {
    if (entry) {
      const { width, height } = entry.contentRect;
      setRawDimensions((prev) =>
        prev.width !== width || prev.height !== height
          ? { width, height }
          : prev
      );
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

  const centerGraph = useCallback(() => resetZoom(), [resetZoom]);

  useEffect(() => {
    if (!currentVisualizationData) {
      setNodes([]);
      setLinks([]);
      setDisplayGroups([]);
      return;
    }
    // currentVisualizationData.nodes are EntityNode[] or ServiceNode[]
    // EntityNode and ServiceNode are now BaseNode, which includes source_system and source_id.
    // The cast to BaseNode[] is correct.
    setNodes(currentVisualizationData.nodes as BaseNode[]);
    setLinks(currentVisualizationData.links as BaseLink[]);
    if (resolutionMode === "entity") {
      setDisplayGroups(
        (currentVisualizationData as EntityVisualizationDataResponse)
          .entityGroups
      );
    } else {
      setDisplayGroups(
        (currentVisualizationData as ServiceVisualizationDataResponse)
          .entityGroups as ServiceGroup[]
      );
    }
  }, [currentVisualizationData, resolutionMode]);

  const updateVisualization = useCallback(() => {
    if (
      !svgRef.current ||
      nodes.length === 0 ||
      dimensions.width === 0 ||
      dimensions.height === 0
    ) {
      if (svgRef.current) d3.select(svgRef.current).selectAll("*").remove();
      return;
    }

    const currentTransform = svgRef.current
      ? d3.zoomTransform(svgRef.current)
      : d3.zoomIdentity;
    d3.select(svgRef.current).selectAll("*").remove();

    const { width, height } = dimensions;
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
          if (!isProgrammaticZoomRef.current) setZoomLevel(transform.k);
        }
      });
    svg.call(zoomRef.current as any);

    const linkElements = g
      .append("g")
      .selectAll("line")
      .data(links)
      .enter()
      .append("line")
      .attr("stroke-width", (d) =>
        d.status === "CONFIRMED_MATCH"
          ? 4
          : d.status === "CONFIRMED_NON_MATCH"
          ? 2
          : 1 + d.weight * 3
      )
      .attr("stroke", (d) =>
        d.status === "CONFIRMED_MATCH"
          ? "#16a34a"
          : d.status === "CONFIRMED_NON_MATCH"
          ? "#dc2626"
          : d3.interpolateRgb("#f87171", "#3b82f6")(d.weight)
      )
      .attr("stroke-opacity", (d) =>
        d.status === "CONFIRMED_NON_MATCH"
          ? 0.4
          : d.status === "CONFIRMED_MATCH"
          ? 0.9
          : 0.7
      )
      .attr("stroke-dasharray", (d) =>
        d.status === "CONFIRMED_NON_MATCH" ? "5,5" : "none"
      )
      .attr("id", (d) => `link-${d.id}`)
      .on("mouseover", (event, d) => {
        setHoverLink(d.id);
        if (selectedEdgeId === d.id) return;
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
        if (selectedEdgeId === d.id) return;
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
      .on("click", (event, d: BaseLink) => {
        actions.setSelectedEdgeId(d.id);
        event.stopPropagation();
      });

    const nodeElements = g
      .append("g")
      .selectAll("circle")
      .data(nodes)
      .enter()
      .append("circle")
      .attr("r", (d) =>
        links.some(
          (link) =>
            (link.source === d.id || link.target === d.id) &&
            link.status === "CONFIRMED_MATCH"
        )
          ? 10
          : 8
      )
      .attr("fill", (d) => {
        const confirmed = links.filter(
          (l) =>
            (l.source === d.id || l.target === d.id) &&
            l.status === "CONFIRMED_MATCH"
        ).length;
        const pending = links.filter(
          (l) =>
            (l.source === d.id || l.target === d.id) &&
            l.status === "PENDING_REVIEW"
        ).length;
        if (confirmed > 0) return "#dcfce7";
        if (pending > 0) return "#fef3c7";
        return "#f3f4f6";
      })
      .attr("stroke", (d) => {
        const confirmed = links.filter(
          (l) =>
            (l.source === d.id || l.target === d.id) &&
            l.status === "CONFIRMED_MATCH"
        ).length;
        const pending = links.filter(
          (l) =>
            (l.source === d.id || l.target === d.id) &&
            l.status === "PENDING_REVIEW"
        ).length;
        if (confirmed > 0) return "#16a34a";
        if (pending > 0) return "#d97706";
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
        d3.select(`#node-${d.id}`).attr(
          "r",
          links.some(
            (l) =>
              (l.source === d.id || l.target === d.id) &&
              l.status === "CONFIRMED_MATCH"
          )
            ? 10
            : 8
        );
      })
      .call(
        d3
          .drag()
          .on("start", dragstarted)
          .on("drag", dragged)
          .on("end", dragended) as any
      );

    // Node 'd' is BaseNode, which now includes name (string | null).
    // The `|| "Unnamed"` handles the null case for name.
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

    // Node Tooltip: 'd' is BaseNode. nodeContactInfo[d.id] will have name, source_system, source_id.
    g.append("g")
      .selectAll("foreignObject")
      .data(nodes)
      .enter()
      .append("foreignObject")
      .attr("width", 200)
      .attr("height", 120)
      .style("pointer-events", "none")
      .style("opacity", 0)
      .attr("id", (d) => `tooltip-${d.id}`)
      .html((d) => {
        const nodeInfo = nodeContactInfo[d.id]; // nodeInfo is BaseNode & { contactInfo: ... }
        if (!nodeInfo) return "";
        const contactInfo = nodeInfo.contactInfo; // This is the { email, phone, address, url } object
        return `
          <div style="background: white; padding: 8px; border-radius: 6px; box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06); border: 1px solid #e5e7eb; font-size: 11px; max-width: 200px; height: 100%; overflow: hidden;">
            <div style="font-weight: 500; margin-bottom: 4px;">${
              nodeInfo.name || "Unknown"
            } (${resolutionMode})</div>
            <div style="margin-bottom: 2px;"><span style="font-weight: 500;">Source:</span> ${
              nodeInfo.contactInfo.source_system || "Unknown"
            }${nodeInfo.contactInfo.source_id ? ` (${nodeInfo.contactInfo.source_id})` : ""}</div>
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
          </div>`;
      });

    // Link Tooltip (remains the same)
    g.append("g")
      .selectAll("foreignObject")
      .data(links)
      .enter()
      .append("foreignObject")
      .attr("width", 270)
      .attr("height", 280)
      .style("pointer-events", "none")
      .style("opacity", 0)
      .attr("id", (d) => `link-tooltip-${d.id}`)
      .html((d_link: BaseLink) => {
        const linkSpecificGroups = displayGroups.filter((group) => {
          if (resolutionMode === "entity") {
            const eg = group as EntityGroup;
            return (
              (eg.entity_id_1 === d_link.source &&
                eg.entity_id_2 === d_link.target) ||
              (eg.entity_id_1 === d_link.target &&
                eg.entity_id_2 === d_link.source)
            );
          } else {
            const sg = group as ServiceGroup;
            return (
              (sg.service_id_1 === d_link.source &&
                sg.service_id_2 === d_link.target) ||
              (sg.service_id_1 === d_link.target &&
                sg.service_id_2 === d_link.source)
            );
          }
        });

        let statusHtml = "";
        if (d_link.status === "CONFIRMED_MATCH")
          statusHtml = `<div style="color: #16a34a; margin-bottom: 8px; font-weight: 500; display: flex; align-items: center; gap: 4px; font-size: 11px;"><div style="width: 8px; height: 8px; background-color: #16a34a; border-radius: 50%;"></div>Confirmed Match</div>`;
        else if (d_link.status === "CONFIRMED_NON_MATCH")
          statusHtml = `<div style="color: #dc2626; margin-bottom: 8px; font-weight: 500; display: flex; align-items: center; gap: 4px; font-size: 11px;"><div style="width: 8px; height: 8px; background-color: #dc2626; border-radius: 50%;"></div>Confirmed Non-Match</div>`;
        else if (d_link.status === "PENDING_REVIEW")
          statusHtml = `<div style="color: #2563eb; margin-bottom: 8px; font-weight: 500; display: flex; align-items: center; gap: 4px; font-size: 11px;"><div style="width: 8px; height: 8px; background-color: #2563eb; border-radius: 50%;"></div>Pending Review</div>`;

        let groupsHtml = "";
        if (linkSpecificGroups.length > 0) {
          groupsHtml = `<div style="margin-top: 8px;"><div style="font-size: 10px; font-weight: 500; color: #6b7280; margin-bottom: 4px;">Match Methods:</div><div style="max-height: 150px; overflow-y: auto; space-y: 4px; padding-right: 5px;">${linkSpecificGroups
            .map((group) => {
              const confidence = group.confidence_score
                ? group.confidence_score.toFixed(3)
                : "N/A";
              const matchValuesDisplay = Object.entries(
                group.match_values.values
              )
                .slice(0, 2)
                .map(
                  ([key, val]) =>
                    `<div>${key.replace(/original_|_1|_2/gi, "")}: ${String(
                      val
                    ).substring(0, 30)}${
                      String(val).length > 30 ? "..." : ""
                    }</div>`
                )
                .join("");
              return `<div style="border-radius: 4px; border: 1px solid #e5e7eb; background-color: rgba(243, 244, 246, 0.5); padding: 6px;"><div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 2px;"><span style="font-size: 10px; font-weight: 500; text-transform: capitalize;">${group.method_type.replace(
                /_/g,
                " "
              )}</span><span style="font-size: 10px; color: #6b7280;">${confidence}</span></div><div style="font-size: 10px; color: #6b7280; space-y: 2px;">${matchValuesDisplay}</div></div>`;
            })
            .join("")}</div></div>`;
        }
        const clickToReviewHtml =
          d_link.status === "PENDING_REVIEW"
            ? `<div style="font-size: 10px; color: #2563eb; margin-top: 6px; cursor: pointer;">Click edge to review →</div>`
            : "";
        return `<div style="background: white; padding: 8px; border-radius: 6px; box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06); border: 1px solid #e5e7eb; font-size: 11px; max-width: 270px; height: 100%; overflow: hidden; display: flex; flex-direction: column;"><div style="font-weight: 500; font-size: 12px; margin-bottom: 4px;">Connection Details (${resolutionMode})</div><div style="font-size: 11px; margin-bottom: 8px;">Confidence: ${d_link.weight.toFixed(
          2
        )}</div>${statusHtml}${groupsHtml}${clickToReviewHtml}</div>`;
      });

    if (simulationRef.current) simulationRef.current.stop();
    const simulation = d3
      .forceSimulation(nodes as d3.SimulationNodeDatum[])
      .force(
        "link",
        d3
          .forceLink(links)
          .id((d) => (d as BaseNode).id)
          .distance((d) =>
            (d as BaseLink).status === "CONFIRMED_MATCH"
              ? 80
              : (d as BaseLink).status === "CONFIRMED_NON_MATCH"
              ? 120
              : 100
          )
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
        g.selectAll("foreignObject[id^='tooltip-']")
          .attr("x", (d) => (d as any).x + 15)
          .attr("y", (d) => (d as any).y - 10);
        g.selectAll("foreignObject[id^='link-tooltip-']")
          .attr(
            "x",
            (d) => ((d as any).source.x + (d as any).target.x) / 2 + 20
          )
          .attr(
            "y",
            (d) => ((d as any).source.y + (d as any).target.y) / 2 - 280 / 2
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
      const selectedLinkData = links.find((l) => l.id === selectedEdgeId);
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
    links,
    displayGroups,
    selectedEdgeId,
    actions.setSelectedEdgeId,
    nodeContactInfo,
    resolutionMode,
  ]);

  useEffect(() => {
    if (!svgRef.current) return;
    const svg = d3.select(svgRef.current);
    svg
      .selectAll("foreignObject[id^='tooltip-']")
      .transition()
      .duration(150)
      .style("opacity", 0);
    if (hoverNode)
      svg
        .select(`#tooltip-${hoverNode}`)
        .transition()
        .duration(150)
        .style("opacity", 1);
  }, [hoverNode]);

  useEffect(() => {
    if (!svgRef.current) return;
    const svg = d3.select(svgRef.current);
    svg
      .selectAll("foreignObject[id^='link-tooltip-']")
      .transition()
      .duration(150)
      .style("opacity", 0);
    if (hoverLink && !selectedEdgeId)
      svg
        .select(`#link-tooltip-${hoverLink}`)
        .transition()
        .duration(150)
        .style("opacity", 1);
  }, [hoverLink, selectedEdgeId]);

  useEffect(updateVisualization, [updateVisualization]);
  useEffect(
    () => () => {
      if (simulationRef.current) simulationRef.current.stop();
    },
    []
  );

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
          No visualization data for this {resolutionMode} cluster.
        </div>
      ) : (
        <>
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
            <div className="text-xs text-center text-gray-500 mt-1 min-w-0">
              {Math.round(zoomLevel * 100)}%
            </div>
          </div>
          <div className="absolute top-4 left-4 z-30 bg-white/90 backdrop-blur-sm rounded-lg p-3 shadow-lg border">
            <h4 className="text-xs font-semibold mb-2">
              Connection Status ({resolutionMode})
            </h4>
            <div className="space-y-1.5 text-xs">
              <div className="flex items-center gap-2">
                <div className="w-4 h-0.5 bg-green-600"></div>
                <span>Confirmed Match ({edgeCounts.confirmed})</span>
              </div>
              <div className="flex items-center gap-2">
                <div
                  className="w-4 h-0.5 bg-red-600 opacity-60"
                  style={{ borderTop: "1px dashed #dc2626" }}
                ></div>
                <span>Confirmed Non-Match ({edgeCounts.nonMatch})</span>
              </div>
              <div className="flex items-center gap-2">
                <div className="w-4 h-0.5 bg-blue-500"></div>
                <span>Pending Review ({edgeCounts.pending})</span>
              </div>
              <div className="text-xs text-muted-foreground mt-2 pt-2 border-t">
                Total: {edgeCounts.total} connections
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

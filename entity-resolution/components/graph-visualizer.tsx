// components/graph-visualizer.tsx
"use client";

import { useEffect, useRef, useState, useCallback, useMemo } from "react";
import { useEntityResolution } from "@/context/entity-resolution-context";
import type {
  EntityGroup,
  BaseNode,
  BaseLink,
  EntityVisualizationDataResponse,
} from "@/types/entity-resolution";
import * as d3 from "d3";
import {
  ZoomIn,
  ZoomOut,
  RotateCcw,
  AlertCircle,
  AlertTriangle,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import { useResizeObserver } from "@/hooks/use-resize-observer";

export const LARGE_CLUSTER_THRESHOLD = 100; // Threshold for warning

export default function GraphVisualizer() {
  const {
    resolutionMode,
    selectedClusterId,
    selectedEdgeId,
    currentVisualizationData, // This is EntityVisualizationDataResponse | null
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

  const [nodes, setNodes] = useState<BaseNode[]>([]);
  const [links, setLinks] = useState<BaseLink[]>([]);
  // REFACTOR: Simplified state to only handle EntityGroup, as ServiceGroup is deprecated.
  const [displayGroups, setDisplayGroups] = useState<EntityGroup[]>([]);

  const [hoverNode, setHoverNode] = useState<string | null>(null);
  const [hoverLink, setHoverLink] = useState<string | null>(null);
  const [nodeContactInfo, setNodeContactInfo] = useState<
    Record<string, BaseNode & { contactInfo: Record<string, any> }>
  >({});
  const [zoomLevel, setZoomLevel] = useState(1);
  const [rawDimensions, setRawDimensions] = useState({ width: 0, height: 0 });

  const [showLargeGraphWarning, setShowLargeGraphWarning] = useState(false);
  const [forceRenderLargeGraph, setForceRenderLargeGraph] = useState(false);

  const dimensions = useMemo(
    () => ({ width: rawDimensions.width, height: rawDimensions.height }),
    [rawDimensions.width, rawDimensions.height]
  );

  const simulationRef = useRef<d3.Simulation<
    d3.SimulationNodeDatum,
    undefined
  > | null>(null);
  const isProgrammaticZoomRef = useRef(false);

  // REFACTOR: Simplified data processing logic.
  // The backend now returns EntityGroup for both resolution modes, so the conditional logic is removed.
  const processVisualizationData = useCallback(() => {
    const data = currentVisualizationData;

    if (!data) {
      setNodes([]);
      setLinks([]);
      setDisplayGroups([]);
      return;
    }

    // BUG FIX: Create deep copies of nodes and links.
    // D3's force simulation mutates the objects passed to it by adding
    // properties like 'x', 'y', 'vx', 'vy', and 'index'. If the objects
    // from the context are immutable (e.g., from Immer), this will throw
    // a "Cannot assign to read only property" error.
    // By creating a new array of new, shallow-copied objects, we ensure
    // that the data used by D3 is mutable.
    const mutableNodes = (data.nodes || []).map((n) => ({ ...n }));
    const mutableLinks = (data.links || []).map((l) => ({ ...l }));

    setNodes(mutableNodes);
    setLinks(mutableLinks);

    const allGroups = data.groups || [];

    if (Array.isArray(allGroups)) {
      // Filter for valid EntityGroup objects, regardless of resolutionMode.
      const validGroups = allGroups.filter(
        (g): g is EntityGroup =>
          typeof g === "object" &&
          g !== null &&
          "entityId1" in g &&
          "entityId2" in g
      );
      setDisplayGroups(validGroups);
    } else {
      console.warn("Visualization groups data is not an array:", allGroups);
      setDisplayGroups([]);
    }
  }, [currentVisualizationData]);

  // REFACTOR: Simplified contact info computation.
  // Removed conditional logic for ServiceGroup as it's no longer a distinct type.
  const computeAllNodeContactInfo = useCallback(() => {
    if (!currentVisualizationData) return;

    const allContactInfo: Record<
      string,
      BaseNode & { contactInfo: Record<string, any> }
    > = {};

    for (const node of nodes) {
      const contactInfo: {
        email?: string;
        phone?: string;
        address?: string;
        url?: string;
      } = {};

      for (const group of displayGroups) {
        const values = group.matchValues?.values;
        const isNodeInGroup =
          group.entityId1 === node.id || group.entityId2 === node.id;

        if (!isNodeInGroup) continue;

        // Determine which set of values to use (_1 or _2) based on the node's position in the group.
        const useSuffix1 = group.entityId1 === node.id;

        switch (group.matchValues?.type?.toLowerCase()) {
          case "email":
            contactInfo.email = useSuffix1
              ? values?.original_email1 || values?.email1
              : values?.original_email2 || values?.email2;
            break;
          case "phone":
            contactInfo.phone = useSuffix1
              ? values?.original_phone1 || values?.phone1
              : values?.original_phone2 || values?.phone2;
            break;
          case "address":
            contactInfo.address = useSuffix1
              ? values?.original_address1 || values?.address1
              : values?.original_address2 || values?.address2;
            break;
          case "url":
            contactInfo.url = useSuffix1
              ? values?.original_url1 || values?.url1
              : values?.original_url2 || values?.url2;
            break;
        }
      }
      allContactInfo[node.id] = {
        ...node,
        contactInfo,
      };
    }
    setNodeContactInfo(allContactInfo);
  }, [nodes, displayGroups, currentVisualizationData]);

  useEffect(() => {
    if (nodes.length > 0 && displayGroups.length > 0) {
      computeAllNodeContactInfo();
    }
  }, [computeAllNodeContactInfo, nodes, displayGroups]);

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

  useEffect(() => {
    setForceRenderLargeGraph(false);
    setShowLargeGraphWarning(false);
    setNodes([]);
    setLinks([]);
    setDisplayGroups([]);
  }, [selectedClusterId]);

  useEffect(() => {
    const data = currentVisualizationData;
    if (!data) {
      setNodes([]);
      setLinks([]);
      setDisplayGroups([]);
      setShowLargeGraphWarning(false);
      return;
    }

    const recordCount = data.nodes?.length || 0;
    const isLarge = recordCount > LARGE_CLUSTER_THRESHOLD;

    if (isLarge && !forceRenderLargeGraph) {
      setShowLargeGraphWarning(true);
      setNodes([]);
      setLinks([]);
      setDisplayGroups([]);
    } else {
      processVisualizationData();
      setShowLargeGraphWarning(false);
    }
  }, [
    currentVisualizationData,
    forceRenderLargeGraph,
    processVisualizationData,
  ]);

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
      if (
        !graphBounds ||
        graphBounds.width === 0 ||
        graphBounds.height === 0 ||
        nodes.length === 0
      ) {
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
  }, [dimensions, safeZoomTo, nodes]);

  const centerGraph = useCallback(() => resetZoom(), [resetZoom]);

  const updateVisualization = useCallback(() => {
    if (!svgRef.current || dimensions.width === 0 || dimensions.height === 0) {
      if (svgRef.current) d3.select(svgRef.current).selectAll("*").remove();
      return;
    }

    if (nodes.length === 0) {
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

    const nodeIds = new Set(nodes.map((n) => n.id));
    const filteredLinks = links.filter((l) => {
      const sourceId =
        typeof l.source === "string" ? l.source : (l.source as BaseNode).id;
      const targetId =
        typeof l.target === "string" ? l.target : (l.target as BaseNode).id;
      const sourceExists = nodeIds.has(sourceId);
      const targetExists = nodeIds.has(targetId);
      if (!sourceExists || !targetExists) {
        console.warn(
          `Filtering out link ${l.id} due to missing node(s). Source ID: ${sourceId} (exists: ${sourceExists}), Target ID: ${targetId} (exists: ${targetExists}). Available node IDs count: ${nodeIds.size}`
        );
      }
      return sourceExists && targetExists;
    });

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
      .data(filteredLinks)
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
        filteredLinks.some(
          (link) =>
            ((typeof link.source === "string"
              ? link.source
              : (link.source as BaseNode).id) === d.id ||
              (typeof link.target === "string"
                ? link.target
                : (link.target as BaseNode).id) === d.id) &&
            link.status === "CONFIRMED_MATCH"
        )
          ? 10
          : 8
      )
      .attr("fill", (d) => {
        const confirmed = filteredLinks.filter(
          (l) =>
            ((typeof l.source === "string"
              ? l.source
              : (l.source as BaseNode).id) === d.id ||
              (typeof l.target === "string"
                ? l.target
                : (l.target as BaseNode).id) === d.id) &&
            l.status === "CONFIRMED_MATCH"
        ).length;
        const pending = filteredLinks.filter(
          (l) =>
            ((typeof l.source === "string"
              ? l.source
              : (l.source as BaseNode).id) === d.id ||
              (typeof l.target === "string"
                ? l.target
                : (l.target as BaseNode).id) === d.id) &&
            l.status === "PENDING_REVIEW"
        ).length;
        if (confirmed > 0) return "#dcfce7";
        if (pending > 0) return "#fef3c7";
        return "#f3f4f6";
      })
      .attr("stroke", (d) => {
        const confirmed = filteredLinks.filter(
          (l) =>
            ((typeof l.source === "string"
              ? l.source
              : (l.source as BaseNode).id) === d.id ||
              (typeof l.target === "string"
                ? l.target
                : (l.target as BaseNode).id) === d.id) &&
            l.status === "CONFIRMED_MATCH"
        ).length;
        const pending = filteredLinks.filter(
          (l) =>
            ((typeof l.source === "string"
              ? l.source
              : (l.source as BaseNode).id) === d.id ||
              (typeof l.target === "string"
                ? l.target
                : (l.target as BaseNode).id) === d.id) &&
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
          filteredLinks.some(
            (l) =>
              ((typeof l.source === "string"
                ? l.source
                : (l.source as BaseNode).id) === d.id ||
                (typeof l.target === "string"
                  ? l.target
                  : (l.target as BaseNode).id) === d.id) &&
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
        const nodeInfo = nodeContactInfo[d.id];
        if (!nodeInfo) return "";
        const contactInfo = nodeInfo.contactInfo;
        return `
          <div style="background: white; padding: 8px; border-radius: 6px; box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06); border: 1px solid #e5e7eb; font-size: 11px; max-width: 200px; height: 100%; overflow: hidden;">
            <div style="font-weight: 500; margin-bottom: 4px;">${
              nodeInfo.name || "Unknown"
            } (${resolutionMode})</div>
            <div style="margin-bottom: 2px;"><span style="font-weight: 500;">Source:</span> ${
              nodeInfo.sourceSystem || "Unknown"
            }${nodeInfo.sourceId ? ` (${nodeInfo.sourceId})` : ""}</div>
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

    g.append("g")
      .selectAll("foreignObject")
      .data(filteredLinks)
      .enter()
      .append("foreignObject")
      .attr("width", 270)
      .attr("height", 280)
      .style("pointer-events", "none")
      .style("opacity", 0)
      .attr("id", (d) => `link-tooltip-${d.id}`)
      .html((d_link: BaseLink) => {
        const linkSourceId =
          typeof d_link.source === "string"
            ? d_link.source
            : (d_link.source as BaseNode).id;
        const linkTargetId =
          typeof d_link.target === "string"
            ? d_link.target
            : (d_link.target as BaseNode).id;

        // REFACTOR: Simplified group filtering logic, as all groups are now EntityGroup.
        const linkSpecificGroups = displayGroups.filter(
          (group) =>
            (group.entityId1 === linkSourceId &&
              group.entityId2 === linkTargetId) ||
            (group.entityId1 === linkTargetId &&
              group.entityId2 === linkSourceId)
        );

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
              // REFACTOR: No need to cast to a union type. 'group' is EntityGroup.
              const confidence = group.confidenceScore
                ? group.confidenceScore.toFixed(3)
                : "N/A";
              const matchValuesDisplay = Object.entries(
                group.matchValues?.values || {}
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
              return `<div style="border-radius: 4px; border: 1px solid #e5e7eb; background-color: rgba(243, 244, 246, 0.5); padding: 6px;"><div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 2px;"><span style="font-size: 10px; font-weight: 500; text-transform: capitalize;">${group.methodType.replace(
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
          .forceLink(filteredLinks)
          .id((d) => (d as BaseNode).id)
          .distance((d) =>
            (d as BaseLink).status === "CONFIRMED_MATCH"
              ? 40
              : (d as BaseLink).status === "CONFIRMED_NON_MATCH"
              ? 80
              : 60
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
      const selectedLinkData = filteredLinks.find(
        (l) => l.id === selectedEdgeId
      );
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

  if (isLoading) {
    return (
      <div className="flex justify-center items-center h-full">
        <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-gray-900"></div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="flex flex-col justify-center items-center h-full text-destructive">
        <AlertCircle className="h-8 w-8 mb-2" />
        <p className="font-semibold">Failed to load visualization</p>
        <p className="text-sm text-muted-foreground">{error}</p>
      </div>
    );
  }

  if (!selectedClusterId) {
    return (
      <div className="flex justify-center items-center h-full text-muted-foreground">
        Select a cluster to visualize
      </div>
    );
  }

  if (showLargeGraphWarning && !forceRenderLargeGraph) {
    const data = currentVisualizationData;
    return (
      <div className="flex flex-col justify-center items-center h-full text-orange-500 p-4">
        <AlertTriangle className="h-12 w-12 mb-4" />
        <h2 className="text-xl font-semibold mb-2 text-gray-800 dark:text-gray-200">
          Large Cluster Warning
        </h2>
        <p className="text-sm text-center text-gray-600 dark:text-gray-400 mb-6 max-w-md">
          This cluster contains <strong>{data?.nodes?.length || "many"}</strong>{" "}
          records. Visualizing it may cause performance issues or browser lag.
        </p>
        <Button
          onClick={() => {
            setForceRenderLargeGraph(true);
          }}
          variant="outline"
          className="bg-orange-500 hover:bg-orange-600 text-white dark:bg-orange-600 dark:hover:bg-orange-700"
        >
          Render Anyway
        </Button>
      </div>
    );
  }

  if (!hasData) {
    return (
      <div className="flex justify-center items-center h-full text-muted-foreground">
        No visualization data to display for this {resolutionMode} cluster.
      </div>
    );
  }

  return (
    <div ref={containerRef} className={`relative h-full flex flex-col`}>
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
    </div>
  );
}

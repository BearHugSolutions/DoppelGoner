// app/api/bulk-visualizations/route.ts

import { NextRequest, NextResponse } from "next/server";
import { fetchFromGateway, handleGatewayError } from '@/utils/gateway-client';
import { requireTeamContext } from '@/utils/team-context';
import type { BulkVisualizationsRequest, BulkVisualizationsResponse } from "@/types/entity-resolution";

export async function POST(request: NextRequest) {
  const response = NextResponse.next();
  const authResult = await requireTeamContext(request, response);
  if (authResult instanceof NextResponse) return authResult;
  const { teamContext, user } = authResult;

  const opinionName = request.headers.get('X-Opinion-Name');
   
  console.log("Bulk Visualizations API: Request with opinion header:", opinionName);

  let payload: BulkVisualizationsRequest;
  try {
    payload = await request.json();
  } catch (error) {
    return NextResponse.json({ error: "Invalid request body." }, { status: 400 });
  }

  // Validate the payload
  if (!payload || !Array.isArray(payload.items) || payload.items.length === 0) {
    return NextResponse.json({ error: "Request payload must be an object with a non-empty 'items' array." }, { status: 400 });
  }

  // Validate each item in the payload
  for (const item of payload.items) {
    if (!item.clusterId || typeof item.clusterId !== 'string') {
      return NextResponse.json({ error: "Each item must have a valid 'clusterId'." }, { status: 400 });
    }
    if (!item.itemType || (item.itemType !== 'entity' && item.itemType !== 'service')) {
      return NextResponse.json({ error: "Each item must have a valid 'itemType' ('entity' or 'service')." }, { status: 400 });
    }
    // NEW: Validate optional pagination and filter parameters
    if (item.limit !== undefined && (typeof item.limit !== 'number' || !Number.isInteger(item.limit) || item.limit <= 0)) {
        return NextResponse.json({ error: "If provided, 'limit' must be a positive integer." }, { status: 400 });
    }
    if (item.cursor !== undefined && typeof item.cursor !== 'string') {
        return NextResponse.json({ error: "If provided, 'cursor' must be a string." }, { status: 400 });
    }
    if (item.crossSystemOnly !== undefined && typeof item.crossSystemOnly !== 'boolean') {
        return NextResponse.json({ error: "If provided, 'crossSystemOnly' must be a boolean." }, { status: 400 });
    }
  }

  console.log(`Fetching bulk visualization data for ${payload.items.length} items with opinion:`, opinionName || "default");

  try {
    // The payload is passed directly to the gateway, which now understands the new fields.
    const gatewayResponse = await fetchFromGateway<BulkVisualizationsResponse>(
      `/bulk-visualizations`,
      {
        method: 'POST',
        body: JSON.stringify(payload),
        headers: {
          'Content-Type': 'application/json',
        },
      },
      teamContext,
      opinionName
    );

    return NextResponse.json(gatewayResponse);

  } catch (error: any) {
    console.error(`Error fetching bulk visualization data from gateway:`, error);
    return handleGatewayError(error, `Failed to fetch bulk visualization data`);
  }
}
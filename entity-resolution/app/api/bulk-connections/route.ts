// app/api/bulk-connections/route.ts
import { NextRequest, NextResponse } from "next/server";
import { fetchFromGateway, handleGatewayError } from '@/utils/gateway-client';
import { requireTeamContext } from '@/utils/team-context';
import type { BulkConnectionsRequest, PaginatedBulkConnectionsResponse } from "@/types/entity-resolution";

export async function POST(request: NextRequest) {
  const response = NextResponse.next();
  const authResult = await requireTeamContext(request, response);
  if (authResult instanceof NextResponse) return authResult;
  const { teamContext, user } = authResult;

  const opinionName = request.headers.get('X-Opinion-Name');
  
  console.log("Bulk Connections API: Request with opinion header:", opinionName);

  let payload: BulkConnectionsRequest;
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
    if (!item.edgeId || typeof item.edgeId !== 'string') {
      return NextResponse.json({ error: "Each item must have a valid 'edgeId'." }, { status: 400 });
    }
    if (!item.itemType || (item.itemType !== 'entity' && item.itemType !== 'service')) {
      return NextResponse.json({ error: "Each item must have a valid 'itemType' ('entity' or 'service')." }, { status: 400 });
    }
  }

  // NEW: Validate pagination parameters
  if (payload.limit !== undefined && (typeof payload.limit !== 'number' || !Number.isInteger(payload.limit) || payload.limit <= 0)) {
    return NextResponse.json({ error: "If provided, 'limit' must be a positive integer." }, { status: 400 });
  }
  if (payload.cursor !== undefined && typeof payload.cursor !== 'string') {
    return NextResponse.json({ error: "If provided, 'cursor' must be a string." }, { status: 400 });
  }
  if (payload.crossSystemOnly !== undefined && typeof payload.crossSystemOnly !== 'boolean') {
    return NextResponse.json({ error: "If provided, 'crossSystemOnly' must be a boolean." }, { status: 400 });
  }

  console.log(`Fetching bulk connection data for ${payload.items.length} items with opinion:`, opinionName || "default");
  console.log("Payload:", JSON.stringify(payload, null, 2));

  try {
    // ✅ FIX: Use the correct paginated response type
    const gatewayResponse = await fetchFromGateway<PaginatedBulkConnectionsResponse>(
      `/bulk-connections`,
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
    
    console.log("Gateway response:", gatewayResponse);
    return NextResponse.json(gatewayResponse);

  } catch (error: any) {
    console.error(`Error fetching bulk connection data from gateway:`, error);
    return handleGatewayError(error, `Failed to fetch bulk connection data`);
  }
}
// app/api/bulk-connections/route.ts
import { NextRequest, NextResponse } from "next/server";
import { fetchFromGateway, handleGatewayError } from '@/utils/gateway-client';
import { requireTeamContext } from '@/utils/team-context';
import type { BulkConnectionsRequest, BulkConnectionsResponse } from "@/types/entity-resolution";

export async function POST(request: NextRequest) {
  const response = NextResponse.next();
  const authResult = await requireTeamContext(request, response);
  if (authResult instanceof NextResponse) return authResult;
  const { teamContext, user } = authResult;

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

  try {
    const gatewayResponse = await fetchFromGateway<BulkConnectionsResponse>(
      `/bulk-connections`,
      {
        method: 'POST',
        body: JSON.stringify(payload),
        headers: {
          'Content-Type': 'application/json',
        },
      },
      teamContext // Pass team context
    );
    
    return NextResponse.json(gatewayResponse);

  } catch (error: any) {
    console.error(`Error fetching bulk connection data from gateway:`, error);
    return handleGatewayError(error, `Failed to fetch bulk connection data`);
  }
}

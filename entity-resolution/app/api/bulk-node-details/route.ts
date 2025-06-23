// app/api/bulk-node-details/route.ts
import { NextResponse, NextRequest } from "next/server";
import { fetchFromGateway, handleGatewayError } from "@/utils/gateway-client";
import { requireTeamContext } from '@/utils/team-context';
import { NodeDetailResponse, BulkNodeDetailsRequest } from "@/types/entity-resolution";

export async function POST(request: NextRequest) {
  console.log("--- Entering Next.js API route: /api/bulk-node-details ---");
  
  const response = NextResponse.next();
  const authResult = await requireTeamContext(request, response);
  if (authResult instanceof NextResponse) return authResult;
  const { teamContext, user } = authResult;

  let payload: BulkNodeDetailsRequest;
  try {
    payload = await request.json();
    console.log(`Received bulk-node-details request for ${payload.items?.length || 0} items.`);
  } catch (error) {
    console.error("Error parsing JSON payload for bulk-node-details:", error);
    return NextResponse.json(
      { error: "Invalid JSON payload" },
      { status: 400 }
    );
  }

  if (!payload || !Array.isArray(payload.items) || payload.items.length === 0) {
    console.warn("bulk-node-details: Empty or invalid items array in payload.");
    return NextResponse.json([], { status: 200 });
  }
  
  try {
    const gatewayResponse = await fetchFromGateway<NodeDetailResponse[]>(
      "/bulk-node-details",
      {
        method: "POST",
        body: JSON.stringify(payload),
        headers: {
          "Content-Type": "application/json",
        },
      },
      teamContext // Pass team context
    );

    console.log(`Successfully fetched bulk data for ${gatewayResponse?.length || 0} nodes from gateway.`);
    return NextResponse.json(gatewayResponse);
  } catch (error) {
    console.error("Error in Next.js API route for /api/bulk-node-details:", error);
    return handleGatewayError(
      error,
      "Internal server error while fetching bulk node data"
    );
  } finally {
    console.log("--- Exiting Next.js API route: /api/bulk-node-details ---");
  }
}
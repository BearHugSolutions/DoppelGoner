// app/api/nodeData/[nodeId]/route.ts
import { NextResponse, NextRequest } from "next/server";
import { fetchFromGateway, handleGatewayError } from "@/utils/gateway-client";
import { requireTeamContext } from '@/utils/team-context';
import { NodeDetailResponse } from "@/types/entity-resolution";

export async function GET(
  request: NextRequest,
  { params }: { params: { nodeId: string } }
) {
  console.log("--- Entering Next.js API route: /api/nodeData/[nodeId] ---");
  
  const response = NextResponse.next();
  const authResult = await requireTeamContext(request, response);
  if (authResult instanceof NextResponse) return authResult;
  const { teamContext, user } = authResult;

  const { nodeId } = await params;
  const { searchParams } = new URL(request.url);
  const type = searchParams.get("type");

  console.log(`Received nodeId: ${nodeId}, type: ${type}`);

  if (!nodeId || !type || (type !== "entity" && type !== "service")) {
    console.error(`Validation failed: nodeId=${nodeId}, type=${type}`);
    return NextResponse.json(
      { error: "Missing nodeId or invalid type parameter" },
      { status: 400 }
    );
  }

  try {
    const gatewayResponse = await fetchFromGateway<NodeDetailResponse>(
      `/nodeData/${nodeId}`,
      {
        method: "GET",
        params: { type },
      },
      teamContext // Pass team context for consistency
    );

    console.log(`Successfully fetched data for node: ${nodeId}`);
    return NextResponse.json(gatewayResponse);
  } catch (error) {
    console.error(`Error in Next.js API route for nodeData/${nodeId}:`, error);
    return handleGatewayError(
      error,
      "Internal server error while fetching node data"
    );
  } finally {
    console.log("--- Exiting Next.js API route: /api/nodeData/[nodeId] ---");
  }
}
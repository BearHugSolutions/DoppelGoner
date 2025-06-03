// app/api/nodeData/[nodeId]/route.ts
import { NextResponse, NextRequest } from "next/server";
// import { getUserSchemaFromSession } from '@/utils/auth-db'; // Not needed for this route based on main.rs
import { fetchFromGateway, handleGatewayError } from "@/utils/gateway-client"; // Import new utility
import { NodeDetailResponse } from "@/types/entity-resolution"; // Keep this for type safety

export async function GET(
  request: NextRequest, // Use NextRequest for easier access to URL
  { params }: { params: { nodeId: string } }
) {
  console.log("--- Entering Next.js API route: /api/nodeData/[nodeId] ---");
  const { nodeId } = await params; // Get nodeId directly from params
  const { searchParams } = new URL(request.url);
  const type = searchParams.get("type"); // 'entity' or 'service'

  console.log(`Received nodeId: ${nodeId}, type: ${type}`);

  if (!nodeId || !type || (type !== "entity" && type !== "service")) {
    console.error(`Validation failed: nodeId=${nodeId}, type=${type}`);
    return NextResponse.json(
      { error: "Missing nodeId or invalid type parameter" },
      { status: 400 }
    );
  }

  try {
    // This route does not require userSchema based on your main.rs setup,
    // so we pass null for userSchema.
    const gatewayResponse = await fetchFromGateway<NodeDetailResponse>(
      `/nodeData/${nodeId}`,
      {
        method: "GET",
        params: { type }, // Pass 'type' as a query parameter
      },
      null // No userSchema needed for this specific backend route
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

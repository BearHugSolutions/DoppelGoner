// app/api/connections/[edgeId]/route.ts
import { NextRequest, NextResponse } from "next/server";
import { getUserSchemaFromSession } from "@/utils/auth-db"; // Still needed
import { fetchFromGateway, handleGatewayError } from '@/utils/gateway-client'; // Import new utility

export async function GET(
  request: NextRequest,
  { params }: { params: { edgeId: string } }
) {
  const userSchema = await getUserSchemaFromSession(request);
  if (!userSchema) {
    return NextResponse.json(
      { error: "Unauthorized: User session not found or invalid." },
      { status: 401 }
    );
  }

  const { searchParams } = new URL(request.url);
  const type = searchParams.get("type") || "entity"; // Default to 'entity'
  const { edgeId } = await params; // Correctly get edgeId from params

  if (!edgeId) {
    return NextResponse.json({ error: "Edge ID is required." }, { status: 400 });
  }

  try {
    // Call the Rust gateway
    const gatewayResponse = await fetchFromGateway(
      `/connections/${edgeId}`,
      {
        method: 'GET',
        params: { type },
      },
      userSchema // Pass userSchema as a header
    );

    // The gateway response is assumed to match the structure previously returned
    return NextResponse.json(gatewayResponse);

  } catch (error: any) {
    console.error(`Error fetching ${type} connection data for edge ${edgeId} from gateway:`, error);
    return handleGatewayError(error, `Failed to fetch ${type} connection data`);
  }
}

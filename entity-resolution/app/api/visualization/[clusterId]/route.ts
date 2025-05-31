// app/api/visualization/[clusterId]/route.ts
import { NextResponse, NextRequest } from 'next/server';
import { getUserSchemaFromSession } from '@/utils/auth-db'; // Still needed
import { fetchFromGateway, handleGatewayError } from '@/utils/gateway-client'; // Import new utility

export async function GET(
  request: NextRequest,
  { params }: { params: { clusterId: string } }
) {
  const userSchema = await getUserSchemaFromSession(request);
  if (!userSchema) {
    return NextResponse.json(
      { error: 'Unauthorized: User session not found or invalid.' },
      { status: 401 }
    );
  }

  const { searchParams } = new URL(request.url);
  const type = searchParams.get('type') || 'entity'; // Default to 'entity'
  const { clusterId } = await params; // Correctly get clusterId from params

  if (!clusterId) {
    return NextResponse.json({ error: "Cluster ID is required." }, { status: 400 });
  }

  try {
    // Call the Rust gateway
    const gatewayResponse = await fetchFromGateway(
      `/visualization/${clusterId}`,
      {
        method: 'GET',
        params: { type },
      },
      userSchema // Pass userSchema as a header
    );

    // The gateway response is assumed to match the structure previously returned
    return NextResponse.json(gatewayResponse);
    
  } catch (error: any) {
    console.error(`Error fetching ${type} visualization data for cluster ${clusterId} from gateway:`, error);
    return handleGatewayError(error, `Failed to fetch ${type} visualization data`);
  }
}

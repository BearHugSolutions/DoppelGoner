// app/api/clusters/route.ts
import { NextResponse, NextRequest } from 'next/server';
import { getUserSchemaFromSession } from '@/utils/auth-db'; // Still needed to get user schema
import { fetchFromGateway, handleGatewayError } from '@/utils/gateway-client'; // Import new utility

export async function GET(request: NextRequest) {
  const userSchema = await getUserSchemaFromSession(request);

  if (!userSchema) {
    return NextResponse.json(
      { error: 'Unauthorized: User session not found or invalid.' },
      { status: 401 }
    );
  }

  const { searchParams } = new URL(request.url);
  const page = searchParams.get('page') || '1';
  const limit = searchParams.get('limit') || '10';
  const type = searchParams.get('type') || 'entity'; // Default to 'entity'

  try {
    // Call the Rust gateway
    // The gateway is expected to handle pagination and type filtering based on query params
    const gatewayResponse = await fetchFromGateway(
      '/clusters',
      {
        method: 'GET',
        params: {
          page,
          limit,
          type,
          // userSchema: userSchema // Pass userSchema if your gateway expects it as a query param
        },
      },
      userSchema // Pass userSchema as a header if your gateway expects it that way
    );

    // The gateway response is assumed to match the structure previously returned
    return NextResponse.json(gatewayResponse);

  } catch (error: any) {
    console.error(`Error fetching ${type} clusters from gateway for schema ${userSchema}:`, error);
    return handleGatewayError(error, `Failed to fetch ${type} clusters`);
  }
}

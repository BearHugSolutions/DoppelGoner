// app/api/bulk-visualizations/route.ts
import { NextRequest, NextResponse } from "next/server";
import { getUserSchemaFromSession } from "@/utils/auth-db";
import { fetchFromGateway, handleGatewayError } from '@/utils/gateway-client';
import type { BulkVisualizationsRequest, BulkVisualizationsResponse } from "@/types/entity-resolution";

export async function POST(request: NextRequest) {
  // Authenticate the user and get their schema
  const userSchema = await getUserSchemaFromSession(request);
  if (!userSchema) {
    return NextResponse.json(
      { error: "Unauthorized: User session not found or invalid." },
      { status: 401 }
    );
  }

  let payload: BulkVisualizationsRequest;
  try {
    // Parse the request body
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
  }

  try {
    // Call the Rust gateway's bulk visualizations endpoint
    const gatewayResponse = await fetchFromGateway<BulkVisualizationsResponse>(
      `/bulk-visualizations`, // Path to the Rust backend endpoint
      {
        method: 'POST',
        body: JSON.stringify(payload), // Send the received payload directly to the gateway
        headers: {
          'Content-Type': 'application/json',
        },
      },
      userSchema // Pass userSchema as a header
    );

    // console.log("Bulk visualizations gateway response:", JSON.stringify(gatewayResponse[0]));
    // Return the gateway's response
    return NextResponse.json(gatewayResponse);

  } catch (error: any) {
    // Log the error and return a standardized error response
    console.error(`Error fetching bulk visualization data from gateway:`, error);
    return handleGatewayError(error, `Failed to fetch bulk visualization data`);
  }
}

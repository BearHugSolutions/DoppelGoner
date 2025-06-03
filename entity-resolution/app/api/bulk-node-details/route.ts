// app/api/bulk-node-details/route.ts
import { NextResponse, NextRequest } from "next/server";
import { fetchFromGateway, handleGatewayError } from "@/utils/gateway-client";
import { NodeDetailResponse, BulkNodeDetailsRequest } from "@/types/entity-resolution";

export async function POST(request: NextRequest) {
  console.log("--- Entering Next.js API route: /api/bulk-node-details ---");

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
    return NextResponse.json(
      // Return empty array for empty request, or an error, depending on desired behavior.
      // For now, let's return empty array, as the backend also handles empty array.
      [], 
      { status: 200 } 
      // Alternatively, to return an error:
      // { error: "Missing or empty items array in payload" },
      // { status: 400 }
    );
  }

  // Extract user schema from headers, if present and needed by the gateway/Rust backend
  // The schema_extractor_middleware in Rust is applied to the /v1/* routes,
  // so the X-User-Schema header should be forwarded by fetchFromGateway.
  const userSchema = request.headers.get("X-User-Schema"); 
  
  if (!userSchema) {
    console.warn("X-User-Schema header is missing from the request to /api/bulk-node-details");
    // Depending on your Rust backend's schema_extractor_middleware strictness,
    // you might want to return an error here if the schema is absolutely required.
    // For now, we'll let fetchFromGateway proceed, and the Rust backend will handle it if it's an issue.
  } else {
    console.log("Forwarding with X-User-Schema:", userSchema);
  }

  try {
    const gatewayResponse = await fetchFromGateway<NodeDetailResponse[]>(
      "/bulk-node-details", // Path to the Rust backend endpoint
      {
        method: "POST",
        body: JSON.stringify(payload), // Forward the original payload
        headers: {
          "Content-Type": "application/json",
          // X-User-Schema will be added by fetchFromGateway if userSchema is provided
        },
      },
      userSchema // Pass the extracted userSchema to fetchFromGateway
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

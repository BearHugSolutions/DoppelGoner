// app/api/post-processing-audit/route.ts
// FIXED: Properly pass query parameters to the Rust backend

import { fetchFromGateway, handleGatewayError } from "@/utils/gateway-client";
import { requireTeamContext } from "@/utils/team-context";
import { NextRequest, NextResponse } from "next/server";

export async function GET(request: NextRequest) {
  const response = NextResponse.next();
  const authResult = await requireTeamContext(request, response);
  if (authResult instanceof NextResponse) return authResult;
  const { teamContext } = authResult;

  // Extract the opinion name from the request header
  const opinionName = request.headers.get('X-Opinion-Name');
  
  console.log("Post-Processing Audit API: Request with opinion header:", opinionName);

  // Extract query parameters
  const { searchParams } = new URL(request.url);
  const queryParams = {
    entity_type: searchParams.get('entity_type'),
    post_proc_filter: searchParams.get('post_proc_filter'),
    reviewed_by_human: searchParams.get('reviewed_by_human'),
    cluster_id: searchParams.get('cluster_id'),
    page: searchParams.get('page'),
    limit: searchParams.get('limit'),
  };

  // Filter out null values using 'reduce' to ensure correct type inference.
  const filteredParams = Object.entries(queryParams).reduce((acc, [key, value]) => {
    if (value !== null) {
      acc[key] = value;
    }
    return acc;
  }, {} as Record<string, string>);

  console.log("[API_CLIENT] Requesting post-processing audit data");
  console.log("[API_CLIENT] Query params:", filteredParams);
  console.log("[API_CLIENT] Using opinion:", opinionName || "default");

  // ✅ ENHANCEMENT: Log when no entity_type is specified
  if (!queryParams.entity_type) {
    console.log("[API_CLIENT] No entity_type specified - will return audit data for all entity types");
  }

  try {
    // 🔧 FIX: Build the URL with query parameters manually
    let gatewayUrl = '/post-processing-audit';
    if (Object.keys(filteredParams).length > 0) {
      const queryString = new URLSearchParams(filteredParams).toString();
      gatewayUrl = `${gatewayUrl}?${queryString}`;
    }

    console.log("[API_CLIENT] Gateway URL:", gatewayUrl);

    const gatewayResponse = await fetchFromGateway(
      gatewayUrl,  // ← Now includes query parameters in the URL
      {
        method: 'GET',
        headers: {
          'Content-Type': 'application/json',
        },
        // Removed the searchParams property - not needed anymore
      },
      teamContext,
      opinionName
    );

    // ✅ ENHANCEMENT: Log the response to help debug
    console.log("[API_CLIENT] Post-processing audit response:", {
      decisionsCount: gatewayResponse.decisions?.length || 0,
      total: gatewayResponse.total || 0,
      entityTypeRequested: queryParams.entity_type || 'all',
      entityTypesInResponse: gatewayResponse.decisions ? 
        [...new Set(gatewayResponse.decisions.map((d: any) => d.entityType))] : []
    });

    return NextResponse.json(gatewayResponse);
  } catch (error: any) {
    console.error(`Error fetching post-processing audit data via gateway:`, error);
    return handleGatewayError(error, 'Failed to fetch post-processing audit data');
  }
}
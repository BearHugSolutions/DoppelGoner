// Debug script to trace team context flow
// Add this to your route.ts to debug the header alignment

import { fetchFromGateway, handleGatewayError } from "@/utils/gateway-client";
import { requireTeamContext } from "@/utils/team-context";
import { NextRequest, NextResponse } from "next/server";

export async function GET(request: NextRequest) {
    console.log("=== DEBUGGING TEAM CONTEXT FLOW ===");
    
    const response = NextResponse.next();
    
    // Step 1: Check if requireTeamContext works
    console.log("Step 1: Calling requireTeamContext...");
    const authResult = await requireTeamContext(request, response);
    
    if (authResult instanceof NextResponse) {
      console.error("❌ requireTeamContext failed:", authResult);
      return authResult;
    }
    
    const { teamContext, user } = authResult;
    console.log("✅ Team context extracted successfully:");
    console.log("  - teamSchema:", teamContext.teamSchema);
    console.log("  - userPrefix:", teamContext.userPrefix);
    console.log("  - user.username:", user.username);
    console.log("  - user.teamName:", user.teamName);
  
    const { searchParams } = new URL(request.url);
    const page = searchParams.get("page") || "1";
    const limit = searchParams.get("limit") || "10";
    const type = searchParams.get("type") || "entity";
    const review_status = searchParams.get("review_status") || "unreviewed";
  
    console.log("Step 2: Query parameters:");
    console.log("  - page:", page);
    console.log("  - limit:", limit);
    console.log("  - type:", type);
    console.log("  - review_status:", review_status);
  
    try {
      console.log("Step 3: Calling fetchFromGateway...");
      console.log("  - URL: /clusters");
      console.log("  - Team context being passed:");
      console.log("    - teamSchema:", teamContext.teamSchema);
      console.log("    - userPrefix:", teamContext.userPrefix);
      
      const gatewayResponse = await fetchFromGateway(
        "/clusters",
        {
          method: "GET",
          params: {
            page,
            limit,
            type,
            review_status,
          },
        },
        teamContext // Pass team context
      );
  
      console.log("✅ Gateway response received successfully");
      console.log("=== END DEBUG ===");
      return NextResponse.json(gatewayResponse);
    } catch (error: any) {
      console.error("❌ Gateway call failed:");
      console.error("  - Error message:", error.message);
      console.error("  - Error status:", error.status);
      console.error("  - Error responseBody:", error.responseBody);
      console.error("=== END DEBUG ===");
      return handleGatewayError(error, `Failed to fetch ${type} clusters`);
    }
  }
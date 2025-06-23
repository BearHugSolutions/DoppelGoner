// utils/team-context.ts
import { NextRequest, NextResponse } from "next/server";
import { getIronSession } from "iron-session";
import { sessionOptions } from "@/lib/session";
import { TeamContext } from "./gateway-client";

// Session data interface (should match the login route)
export interface UserSessionData {
  userId: string;
  sessionId: string;
  username: string;
  teamId: string;
  teamName: string;
  teamSchema: string;
  userPrefix: string;
  isLoggedIn: true;
}

/**
 * Extracts team context from the user's session.
 * This function should be called by API routes that need to make team-aware requests to the gateway.
 */
// Debug version of team context extraction
// Add this to your team-context.ts

export async function getTeamContextFromSession(
  request: NextRequest,
  response: NextResponse
): Promise<{ teamContext: TeamContext; user: UserSessionData } | null> {
  console.log("=== TEAM CONTEXT EXTRACTION DEBUG ===");

  try {
    console.log("Step 1: Getting iron session...");
    const session = await getIronSession<UserSessionData>(
      request,
      response,
      sessionOptions
    );

    console.log("Step 2: Session data:");
    console.log("  - isLoggedIn:", session.isLoggedIn);
    console.log("  - userId:", session.userId);
    console.log("  - username:", session.username);
    console.log("  - teamId:", session.teamId);
    console.log("  - teamName:", session.teamName);
    console.log("  - teamSchema:", session.teamSchema);
    console.log("  - userPrefix:", session.userPrefix);

    if (!session.isLoggedIn || !session.userId) {
      console.log("❌ User not logged in or missing userId");
      return null;
    }

    // Validate that we have all required team information
    if (!session.teamSchema || !session.userPrefix) {
      console.error("❌ Session missing team context:");
      console.error("  - userId:", session.userId);
      console.error("  - username:", session.username);
      console.error("  - hasTeamSchema:", !!session.teamSchema);
      console.error("  - hasUserPrefix:", !!session.userPrefix);
      console.error("  - teamSchema value:", session.teamSchema);
      console.error("  - userPrefix value:", session.userPrefix);
      return null;
    }

    const teamContext: TeamContext = {
      teamSchema: session.teamSchema,
      userPrefix: session.userPrefix,
    };

    console.log("✅ Team context created successfully:");
    console.log("  - teamSchema:", teamContext.teamSchema);
    console.log("  - userPrefix:", teamContext.userPrefix);
    console.log("=== END TEAM CONTEXT EXTRACTION DEBUG ===");

    return {
      teamContext,
      user: session,
    };
  } catch (error) {
    console.error("❌ Error extracting team context from session:", error);
    console.log("=== END TEAM CONTEXT EXTRACTION DEBUG ===");
    return null;
  }
}

/**
 * Middleware-like function to validate team context and return appropriate error responses.
 * This can be used at the beginning of API routes to ensure the user is authenticated
 * and has valid team context.
 */

// Also debug the requireTeamContext function
export async function requireTeamContext(
  request: NextRequest,
  response: NextResponse
): Promise<{ teamContext: TeamContext; user: UserSessionData } | NextResponse> {
  console.log("=== REQUIRE TEAM CONTEXT DEBUG ===");

  const sessionData = await getTeamContextFromSession(request, response);

  if (!sessionData) {
    console.log("❌ No session data - returning 401");
    console.log("=== END REQUIRE TEAM CONTEXT DEBUG ===");
    return NextResponse.json(
      { error: "Authentication required. Please log in." },
      { status: 401 }
    );
  }

  console.log("✅ Team context validation successful");
  console.log("=== END REQUIRE TEAM CONTEXT DEBUG ===");
  return sessionData;
}

/**
 * Helper function to create consistent error responses for authentication/authorization issues.
 */
export function createAuthErrorResponse(
  message: string,
  status: number = 401
): NextResponse {
  return NextResponse.json({ error: message }, { status });
}

/**
 * Helper function to validate that a user has access to specific functionality.
 * This can be extended in the future for more granular permissions.
 */
export function validateUserPermissions(
  user: UserSessionData,
  requiredPermissions?: string[]
): boolean {
  // For now, all authenticated users with team context have access
  // This can be extended later for role-based access control
  return !!user.isLoggedIn && !!user.teamSchema && !!user.userPrefix;
}

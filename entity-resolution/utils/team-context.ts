// utils/team-context.ts
import { NextRequest, NextResponse } from "next/server";
import { getIronSession } from "iron-session";
import { sessionOptions } from "@/lib/session";

// Simple opinion interface that matches auth-context.tsx
export interface OpinionInfo {
  name: string;
  isDefault: boolean;
}

// Session data interface (should match iron-session augmentation)
// Represents the user session data stored on the server
export interface UserSessionData {
  userId: string;
  sessionId: string;
  username: string;
  email?: string;
  teamId: string;
  teamName: string;
  teamSchema: string;
  userPrefix: string;
  isLoggedIn: true;
  opinions: OpinionInfo[]; // Store all available opinions in the session
}

export interface TeamContext {
  teamSchema: string;
  userPrefix: string;
}

interface AuthResult {
  teamContext: TeamContext;
  user: UserSessionData;
}

/**
 * ✨ UPDATED: Validates if a session is complete and compatible.
 * No longer lenient with missing opinions - forces re-authentication.
 */
function validateSessionData(session: Partial<UserSessionData>): session is UserSessionData {
  // Check basic authentication
  if (!session.isLoggedIn || !session.userId) {
    return false;
  }

  // Check required fields
  if (!session.username || !session.teamId || !session.teamName || 
      !session.teamSchema || !session.userPrefix) {
    return false;
  }

  // ✨ STRICT: Opinions must exist and be a valid array
  if (!session.opinions || !Array.isArray(session.opinions)) {
    console.warn("⚠️ Session missing opinions array - pre-refactor session detected", {
      userId: session.userId,
      username: session.username
    });
    return false;
  }

  // ✨ STRICT: Must have at least one opinion
  if (session.opinions.length === 0) {
    console.warn("⚠️ Session has empty opinions array", {
      userId: session.userId,
      username: session.username
    });
    return false;
  }

  // Validate opinion structure
  for (const opinion of session.opinions) {
    if (!opinion.name || typeof opinion.isDefault !== 'boolean') {
      console.warn("⚠️ Session has malformed opinion data", {
        userId: session.userId,
        username: session.username,
        malformedOpinion: opinion
      });
      return false;
    }
  }

  return true;
}

/**
 * ✨ UPDATED: Ensures that the user is logged in and has the necessary team context.
 * Now strictly validates session compatibility and rejects pre-refactor sessions.
 *
 * @param request The incoming NextRequest.
 * @param response The outgoing NextResponse.
 * @returns A promise that resolves with the user and team context, or a NextResponse for redirection/error.
 */
export async function requireTeamContext(
  request: NextRequest,
  response: NextResponse
): Promise<AuthResult | NextResponse> {
  try {
    // Use Partial<UserSessionData> to avoid type issues with incomplete sessions
    const session = await getIronSession<Partial<UserSessionData>>(request, response, sessionOptions);

    // ✨ STRICT VALIDATION: Use the new validation function
    if (!validateSessionData(session)) {
      console.warn("Invalid or incompatible session in requireTeamContext, rejecting request");
      
      // Clear the invalid session
      try {
        // Type assertion since we know this is a session object with destroy method
        (session as any).destroy();
        const cookieName = sessionOptions.cookieName || 'iron-session';
        response.cookies.set(cookieName, '', {
          httpOnly: true,
          secure: process.env.NODE_ENV === 'production',
          sameSite: 'lax',
          path: '/',
          expires: new Date(0),
          maxAge: 0
        });
      } catch (clearError) {
        console.error("Error clearing invalid session:", clearError);
      }
      
      return NextResponse.json({ 
        error: "Invalid session. Please log in again.",
        sessionCleared: true 
      }, { status: 401 });
    }

    // At this point, we know session is valid, so cast it to the full type
    const validSession = session as UserSessionData;
    return {
      teamContext: { 
        teamSchema: validSession.teamSchema, 
        userPrefix: validSession.userPrefix 
      },
      user: validSession,
    };

  } catch (error) {
    console.error("Error in requireTeamContext:", error);
    return NextResponse.json({ 
      error: "Session validation failed. Please log in again." 
    }, { status: 401 });
  }
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
  return !!user.isLoggedIn && !!user.teamSchema && !!user.userPrefix && 
         !!user.opinions && user.opinions.length > 0;
}

/**
 * Helper function to get the default opinion for a user.
 * Returns the opinion marked as default, or the first opinion if none is marked as default.
 */
export function getDefaultOpinionName(opinions: OpinionInfo[]): string | null {
  if (!opinions || opinions.length === 0) {
    return null;
  }
  
  const defaultOpinion = opinions.find(opinion => opinion.isDefault);
  return defaultOpinion ? defaultOpinion.name : opinions[0].name;
}

/**
 * Helper function to validate that an opinion name is valid for the user.
 */
export function isValidOpinionName(opinions: OpinionInfo[], opinionName: string | null): boolean {
  if (!opinionName) {
    return true; // null/undefined is valid (will use default)
  }
  
  return opinions.some(opinion => opinion.name === opinionName);
}

/**
 * ✨ NEW: Helper function to check if a session needs to be upgraded/migrated.
 * This can be used in middleware or other places to detect old sessions.
 */
export function isLegacySession(session: Partial<UserSessionData>): boolean {
  return session.isLoggedIn === true && 
         !!session.userId && 
         (!session.opinions || !Array.isArray(session.opinions) || session.opinions.length === 0);
}
// utils/session-migration.ts
// ✨ NEW: Utilities for handling session migration and legacy session detection

import { NextRequest, NextResponse } from "next/server";
import { getIronSession } from "iron-session";
import { sessionOptions } from "@/lib/session";
import { UserSessionData, isLegacySession } from "@/utils/team-context";

/**
 * Middleware-style function to detect and handle legacy sessions.
 * Can be used in API routes or middleware to automatically clear old sessions.
 */
export async function handleLegacySession(
  request: NextRequest
): Promise<{ shouldClearSession: boolean; response?: NextResponse }> {
  try {
    const response = NextResponse.next();
    const session = await getIronSession<Partial<UserSessionData>>(request, response, sessionOptions);
    
    // Check if this is a legacy session that needs clearing
    if (isLegacySession(session)) {
      console.log("Legacy session detected, preparing to clear", {
        userId: session.userId,
        username: session.username,
        hasOpinions: !!session.opinions,
        opinionsType: typeof session.opinions
      });
      
      // Clear the session
      session.destroy();
      
      // Clear the cookie
      const cookieName = sessionOptions.cookieName || 'iron-session';
      response.cookies.set(cookieName, '', {
        httpOnly: true,
        secure: process.env.NODE_ENV === 'production',
        sameSite: 'lax',
        path: '/',
        expires: new Date(0),
        maxAge: 0
      });
      
      return { 
        shouldClearSession: true, 
        response 
      };
    }
    
    return { shouldClearSession: false };
    
  } catch (error) {
    console.error("Error in handleLegacySession:", error);
    return { shouldClearSession: false };
  }
}

/**
 * Helper function to create a "session cleared" response.
 * Useful for API endpoints that detect legacy sessions.
 */
export function createSessionClearedResponse(message: string = "Session expired. Please log in again."): NextResponse {
  return NextResponse.json({
    error: message,
    sessionCleared: true,
    requiresReauth: true
  }, { status: 401 });
}

/**
 * Utility to check if a request has a potentially legacy session
 * without modifying it. Useful for logging/monitoring.
 */
export async function detectLegacySession(request: NextRequest): Promise<boolean> {
  try {
    const response = NextResponse.next();
    const session = await getIronSession<Partial<UserSessionData>>(request, response, sessionOptions);
    return isLegacySession(session);
  } catch (error) {
    console.error("Error detecting legacy session:", error);
    return false;
  }
}

/**
 * Enhanced session validation that includes migration logic.
 * Returns detailed information about session state.
 */
export async function validateSession(request: NextRequest): Promise<{
  isValid: boolean;
  isLegacy: boolean;
  needsClearing: boolean;
  session?: UserSessionData;
  error?: string;
}> {
  try {
    const response = NextResponse.next();
    const session = await getIronSession<Partial<UserSessionData>>(request, response, sessionOptions);
    
    // Check if session exists at all
    if (!session.isLoggedIn || !session.userId) {
      return {
        isValid: false,
        isLegacy: false,
        needsClearing: false
      };
    }
    
    // Check if it's a legacy session
    if (isLegacySession(session)) {
      return {
        isValid: false,
        isLegacy: true,
        needsClearing: true,
        error: "Legacy session detected - missing opinions data"
      };
    }
    
    // Validate complete session structure
    if (!session.username || !session.teamId || !session.teamName || 
        !session.teamSchema || !session.userPrefix) {
      return {
        isValid: false,
        isLegacy: false,
        needsClearing: true,
        error: "Incomplete session data"
      };
    }
    
    // Validate opinions
    if (!session.opinions || !Array.isArray(session.opinions) || session.opinions.length === 0) {
      return {
        isValid: false,
        isLegacy: false,
        needsClearing: true,
        error: "Invalid or missing opinions data"
      };
    }
    
    // Session is valid
    return {
      isValid: true,
      isLegacy: false,
      needsClearing: false,
      session: session as UserSessionData
    };
    
  } catch (error) {
    return {
      isValid: false,
      isLegacy: false,
      needsClearing: true,
      error: `Session validation error: ${error instanceof Error ? error.message : 'Unknown error'}`
    };
  }
}

/**
 * Utility function for API routes to quickly handle session validation
 * and return appropriate responses.
 */
export async function requireValidSession(
  request: NextRequest
): Promise<{ session: UserSessionData } | NextResponse> {
  const validation = await validateSession(request);
  
  if (!validation.isValid) {
    if (validation.needsClearing) {
      // Clear the invalid session
      await handleLegacySession(request);
      return createSessionClearedResponse(validation.error || "Invalid session detected");
    }
    
    return NextResponse.json({ 
      error: "Authentication required" 
    }, { status: 401 });
  }
  
  return { session: validation.session! };
}
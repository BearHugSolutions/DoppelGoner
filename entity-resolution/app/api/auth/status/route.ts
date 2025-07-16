// app/api/auth/status/route.ts
import { NextRequest, NextResponse } from 'next/server';
import { getIronSession } from 'iron-session';
import { sessionOptions } from '@/lib/session';
import { UserSessionData, OpinionInfo } from '@/utils/team-context';

// User interface that matches auth-context.tsx
export interface User {
  id: string;
  username: string;
  email?: string;
  teamId: string;
  teamName: string;
  teamSchema: string;
  userPrefix: string;
  opinions: OpinionInfo[];
}

/**
 * Validates if a session is compatible with the current application version.
 * Returns true if the session is valid and compatible, false otherwise.
 */
function isValidSession(session: Partial<UserSessionData>): session is UserSessionData {
  // Check if user is logged in
  if (!session.isLoggedIn || !session.userId) {
    return false;
  }

  // Check for required basic fields
  if (!session.username || !session.teamId || !session.teamName || 
      !session.teamSchema || !session.userPrefix) {
    console.warn("Session missing required basic fields", {
      username: !!session.username,
      teamId: !!session.teamId,
      teamName: !!session.teamName,
      teamSchema: !!session.teamSchema,
      userPrefix: !!session.userPrefix
    });
    return false;
  }

  // ✨ CRITICAL: Check for opinions array (this detects pre-refactor sessions)
  if (!session.opinions || !Array.isArray(session.opinions)) {
    console.warn("Session missing opinions array - this is a pre-refactor session that needs re-authentication", {
      userId: session.userId,
      username: session.username,
      hasOpinions: !!session.opinions,
      opinionsType: typeof session.opinions
    });
    return false;
  }

  // Validate opinions array structure
  if (session.opinions.length === 0) {
    console.warn("Session has empty opinions array - invalid session state", {
      userId: session.userId,
      username: session.username
    });
    return false;
  }

  // Validate each opinion has required fields
  for (const opinion of session.opinions) {
    if (!opinion.name || typeof opinion.isDefault !== 'boolean') {
      console.warn("Session has malformed opinion data", {
        userId: session.userId,
        username: session.username,
        opinion: opinion
      });
      return false;
    }
  }

  return true;
}

/**
 * Clears an invalid session by destroying it and clearing the cookie.
 */
async function clearInvalidSession(request: NextRequest, response: NextResponse) {
  try {
    const session = await getIronSession<UserSessionData>(request, response, sessionOptions);
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
    
    console.log("Cleared invalid session and cookie");
  } catch (error) {
    console.error("Error clearing invalid session:", error);
  }
}

/**
 * GET /api/auth/status
 * 
 * Returns the current user's authentication status and session data.
 * This is used by the AuthProvider to re-establish state on page load.
 * 
 * ✨ UPDATED: Now strictly validates session compatibility and forces
 * re-authentication for pre-refactor sessions.
 */
export async function GET(request: NextRequest) {
  const response = NextResponse.next();
  
  try {
    const session = await getIronSession<UserSessionData>(request, response, sessionOptions);

    // ✨ STRICT VALIDATION: Use the new validation function
    if (!isValidSession(session)) {
      console.log("Invalid or incompatible session detected, forcing logout");
      
      // Clear the invalid session
      await clearInvalidSession(request, response);
      
      return NextResponse.json({
        isLoggedIn: false,
        user: null,
        sessionCleared: true, // Optional flag to help with debugging
      });
    }

    // At this point, TypeScript knows session is a valid UserSessionData
    const userPayload: User = {
      id: session.userId,
      username: session.username,
      email: session.email,
      teamId: session.teamId,
      teamName: session.teamName,
      teamSchema: session.teamSchema,
      userPrefix: session.userPrefix,
      opinions: session.opinions,
    };

    console.log(`Auth status check: User ${session.username} is logged in with ${session.opinions.length} opinions`, {
      hasEmail: !!session.email,
      email: session.email ? session.email.substring(0, 3) + '***' : 'none',
      opinionsCount: session.opinions.length,
      defaultOpinion: session.opinions.find(op => op.isDefault)?.name || 'none'
    });

    return NextResponse.json({
      isLoggedIn: true,
      user: userPayload,
    });

  } catch (error) {
    console.error("Error checking auth status:", error);
    
    // Clear any potentially corrupted session
    await clearInvalidSession(request, response);
    
    return NextResponse.json({
      isLoggedIn: false,
      user: null,
      error: "Session validation failed"
    });
  }
}
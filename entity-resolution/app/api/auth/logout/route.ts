// app/api/auth/logout/route.ts
import { NextRequest, NextResponse } from 'next/server';
import { getIronSession } from 'iron-session';
import { sessionOptions } from '@/lib/session';
import { UserSessionData } from '@/utils/team-context';

/**
 * POST /api/auth/logout
 * 
 * Logs out the current user by destroying their session and clearing cookies.
 */
export async function POST(request: NextRequest) {
  try {
    const response = NextResponse.json({
      message: "Logged out successfully",
    }, { status: 200 });
    
    const session = await getIronSession<UserSessionData>(request, response, sessionOptions);
    
    const username = session.username;
    
    // Destroy the session data
    session.destroy();
    
    // Explicitly clear the session cookie by setting it to expire immediately
    // This ensures the cookie is properly removed from the client
    const cookieName = sessionOptions.cookieName || 'iron-session';
    response.cookies.set(cookieName, '', {
      httpOnly: true,
      secure: process.env.NODE_ENV === 'production',
      sameSite: 'lax',
      path: '/',
      expires: new Date(0), // Set to past date to delete
      maxAge: 0
    });
    
    console.log(`User ${username || 'unknown'} logged out successfully`);
    
    return response;
    
  } catch (error) {
    console.error("Error during logout:", error);
    
    // Even if there's an error, we should try to clear the session cookie
    const response = NextResponse.json({
      message: "Logged out",
    }, { status: 200 });
    
    const cookieName = sessionOptions.cookieName || 'iron-session';
    response.cookies.set(cookieName, '', {
      httpOnly: true,
      secure: process.env.NODE_ENV === 'production',
      sameSite: 'lax',
      path: '/',
      expires: new Date(0),
      maxAge: 0
    });
    
    return response;
  }
}
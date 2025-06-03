// app/api/auth/login/route.ts
import { NextResponse, NextRequest } from 'next/server';
import { getIronSession } from 'iron-session';
import { v4 as uuidv4 } from 'uuid';
import { sessionOptions } from '@/lib/session';
import { fetchFromGateway, handleGatewayError } from '@/utils/gateway-client'; // Import gateway client

// Define the shape of the data stored in the session
export interface UserSessionData {
  userId: string;
  sessionId: string;
  username: string;
  userSchema: string;
  isLoggedIn: true;
}

// Expected response structure from the gateway's /auth/login endpoint
interface GatewayLoginResponse {
  message: string;
  user: {
    id: string;
    username: string;
    email?: string;
    userSchemaName: string; // Note: field name from Rust model is userSchemaName
    // Include other fields if your gateway returns them and they are needed
  };
}

export async function POST(request: NextRequest) {
  console.log("Login request received");
  console.log("Request body:", await request.json());
  console.log("Environment variables:", process.env);
  try {
    const { username, password } = await request.json();

    if (!username || typeof username !== 'string' || username.trim() === '') {
      return NextResponse.json({ error: 'Username is required.' }, { status: 400 });
    }
    if (!password || typeof password !== 'string') {
      return NextResponse.json({ error: 'Password is required.' }, { status: 400 });
    }

    const trimmedUsername = username.trim();

    // Call the Rust gateway's login endpoint
    const gatewayResponse = await fetchFromGateway<GatewayLoginResponse>(
      '/auth/login', // Path to the gateway's login endpoint
      {
        method: 'POST',
        headers: { // Explicitly set Content-Type to application/json
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ username: trimmedUsername, password }),
      }
    );
    
    // The gateway performs user lookup, password verification, and activity check.

    const gatewayUser = gatewayResponse.user;

    // 4. Prepare Response Payload for Next.js AuthContext (if still used in the same way)
    const userResponsePayload = {
      id: gatewayUser.id,
      username: gatewayUser.username,
      email: gatewayUser.email,
      userSchema: gatewayUser.userSchemaName, // Use userSchemaName from gateway
    };

    const response = NextResponse.json({
      message: gatewayResponse.message, // Use message from gateway
      user: userResponsePayload,
    }, { status: 200 });

    // Iron Session Debugging (can be kept if useful)
    console.log("--- Iron Session Debugging ---");
    // ... (existing session debugging logs)
    console.log("--- End Iron Session Debugging ---");

    // 5. Create/Update Iron Session
    const session = await getIronSession<UserSessionData>(request, response, sessionOptions);
    const newSessionId = uuidv4();

    session.userId = gatewayUser.id;
    session.sessionId = newSessionId;
    session.username = gatewayUser.username;
    session.userSchema = gatewayUser.userSchemaName; // Use userSchemaName from gateway
    session.isLoggedIn = true;

    await session.save();

    // 6. Update last login (The gateway's login handler should now be responsible for this)
    // No direct call to updateUserLastLogin from Next.js needed anymore.

    return response;

  } catch (error: any) {
    console.error('Login error:', error);
     if (error.message?.includes("Password must be at least 32 characters long")) {
        console.error("Iron-session specific error: Ensure SECRET_COOKIE_PASSWORD is correctly set.");
        return NextResponse.json({ error: 'Session configuration error. Please contact support.' }, { status: 500 });
    }
    // Use handleGatewayError for errors originating from the gateway call
    return handleGatewayError(error, 'Login failed. Please try again later.');
  }
}

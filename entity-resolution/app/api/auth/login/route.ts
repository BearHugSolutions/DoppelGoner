// app/api/auth/login/route.ts
import { NextResponse, NextRequest } from "next/server";
import { getIronSession } from "iron-session";
import { v4 as uuidv4 } from "uuid";
import { sessionOptions } from "@/lib/session";
import { fetchFromGateway, handleGatewayError } from "@/utils/gateway-client";

// Updated session data for team-based system
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

// Updated to match actual backend response structure
interface GatewayLoginResponse {
  message: string;
  user: {
    id: string;
    username: string;
    email?: string;
    teamId: string;
    teamName: string;
    teamSchemaName: string;
    userOpinionPrefix: string;
    userSchemaName: string;
    isActive: boolean;
    createdAt: string;
    updatedAt: string;
    lastLoginAt?: string;
  };
}

export async function POST(request: NextRequest) {
  try {
    const { username, password } = await request.json();

    if (!username || typeof username !== "string" || username.trim() === "") {
      return NextResponse.json(
        { error: "Username is required." },
        { status: 400 }
      );
    }
    if (!password || typeof password !== "string") {
      return NextResponse.json(
        { error: "Password is required." },
        { status: 400 }
      );
    }

    const trimmedUsername = username.trim();

    // Call the Rust gateway's login endpoint
    const gatewayResponse = await fetchFromGateway<GatewayLoginResponse>(
      "/auth/login",
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ username: trimmedUsername, password }),
      }
      // No team context needed for login itself
    );

    const gatewayUser = gatewayResponse.user;

    // Updated validation to check correct field names from backend
    if (
      !gatewayUser.teamId ||
      !gatewayUser.teamName ||
      !gatewayUser.teamSchemaName ||
      !gatewayUser.userOpinionPrefix
    ) {
      console.error(
        "Gateway login response missing team information:",
        gatewayUser
      );
      return NextResponse.json(
        {
          error: "Invalid user data received. Missing team information.",
        },
        { status: 500 }
      );
    }

    // Prepare response payload for Next.js AuthContext - map backend fields to expected frontend fields
    const userResponsePayload = {
      id: gatewayUser.id,
      username: gatewayUser.username,
      email: gatewayUser.email,
      teamId: gatewayUser.teamId,
      teamName: gatewayUser.teamName,
      teamSchema: gatewayUser.teamSchemaName, // Map to expected frontend field name
      userPrefix: gatewayUser.userOpinionPrefix, // Map to expected frontend field name
    };

    const response = NextResponse.json(
      {
        message: gatewayResponse.message,
        user: userResponsePayload,
      },
      { status: 200 }
    );

    // Create/Update Iron Session with team information
    const session = await getIronSession<UserSessionData>(
      request,
      response,
      sessionOptions
    );
    const newSessionId = uuidv4();

    session.userId = gatewayUser.id;
    session.sessionId = newSessionId;
    session.username = gatewayUser.username;
    session.teamId = gatewayUser.teamId;
    session.teamName = gatewayUser.teamName;
    session.teamSchema = gatewayUser.teamSchemaName; // Use actual backend field name
    session.userPrefix = gatewayUser.userOpinionPrefix; // Use actual backend field name
    session.isLoggedIn = true;

    await session.save();

    console.log("User logged in with team context:", {
      username: gatewayUser.username,
      teamName: gatewayUser.teamName,
      teamSchema: gatewayUser.teamSchemaName, // Updated field name
      userPrefix: gatewayUser.userOpinionPrefix, // Updated field name
    });

    return response;
  } catch (error: any) {
    console.error("Login error:", error);
    if (
      error.message?.includes("Password must be at least 32 characters long")
    ) {
      console.error(
        "Iron-session specific error: Ensure SECRET_COOKIE_PASSWORD is correctly set."
      );
      return NextResponse.json(
        {
          error: "Session configuration error. Please contact support.",
        },
        { status: 500 }
      );
    }
    return handleGatewayError(error, "Login failed. Please try again later.");
  }
}

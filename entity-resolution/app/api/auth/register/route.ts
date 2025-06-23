// app/api/auth/register/route.ts
import { NextResponse, NextRequest } from "next/server";
import { fetchFromGateway, handleGatewayError } from "@/utils/gateway-client";

// Updated to match actual backend response structure
interface GatewayRegisterResponse {
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
  };
}

export async function POST(request: NextRequest) {
  try {
    const { username, password, email, teamName, datasets } =
      await request.json();

    // Validate required fields
    if (!username || typeof username !== "string" || username.trim() === "") {
      return NextResponse.json(
        { error: "Username is required." },
        { status: 400 }
      );
    }
    if (!password || typeof password !== "string" || password.length < 6) {
      return NextResponse.json(
        {
          error: "Password is required and must be at least 6 characters.",
        },
        { status: 400 }
      );
    }
    if (!email || typeof email !== "string" || email.trim() === "") {
      return NextResponse.json(
        { error: "Email is required." },
        { status: 400 }
      );
    }
    if (!teamName || typeof teamName !== "string" || teamName.trim() === "") {
      return NextResponse.json(
        { error: "Team name is required." },
        { status: 400 }
      );
    }

    const trimmedUsername = username.trim();
    const trimmedEmail = email.trim();
    const trimmedTeamName = teamName.trim();

    // Prepare the registration payload
    const registrationPayload: any = {
      username: trimmedUsername,
      password,
      email: trimmedEmail,
      teamName: trimmedTeamName,
      datasets: datasets || [], // Ensure datasets is always an array, even if empty
    };

    // Include datasets if provided (for new team creation)
    if (datasets && Array.isArray(datasets) && datasets.length > 0) {
      registrationPayload.datasets = datasets;
    }

    // Call the Rust gateway's register endpoint
    const gatewayResponse = await fetchFromGateway<GatewayRegisterResponse>(
      "/auth/register",
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(registrationPayload),
      }
      // No team context needed for registration itself
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
        "Gateway register response missing team information:",
        gatewayUser
      );
      return NextResponse.json(
        {
          error:
            "Registration completed but user data is incomplete. Please contact support.",
        },
        { status: 500 }
      );
    }

    console.log("User registered with team context:", {
      username: gatewayUser.username,
      teamName: gatewayUser.teamName,
      teamSchemaName: gatewayUser.teamSchemaName, // Updated field name
      userOpinionPrefix: gatewayUser.userOpinionPrefix, // Updated field name
    });

    // Return response with field names that match what your frontend expects
    return NextResponse.json(
      {
        message: gatewayResponse.message,
        user: {
          id: gatewayUser.id,
          username: gatewayUser.username,
          email: gatewayUser.email,
          teamId: gatewayUser.teamId,
          teamName: gatewayUser.teamName,
          teamSchema: gatewayUser.teamSchemaName, // Map to expected frontend field name
          userPrefix: gatewayUser.userOpinionPrefix, // Map to expected frontend field name
        },
      },
      { status: 201 }
    );
  } catch (error: any) {
    console.error("Registration error:", error);

    // Handle specific error cases from the gateway
    if (error.responseBody?.message) {
      const errorMessage = error.responseBody.message.toLowerCase();

      if (
        errorMessage.includes("username already taken") ||
        errorMessage.includes("username already exists")
      ) {
        return NextResponse.json(
          {
            error: "Username already taken. Please choose another.",
          },
          { status: 409 }
        );
      }

      if (
        errorMessage.includes("email address already registered") ||
        errorMessage.includes("email already exists")
      ) {
        return NextResponse.json(
          {
            error: "Email address already registered.",
          },
          { status: 409 }
        );
      }

      if (
        errorMessage.includes("team name already exists") ||
        errorMessage.includes("team already exists")
      ) {
        return NextResponse.json(
          {
            error:
              "Team name already exists. Please choose another or join the existing team.",
          },
          { status: 409 }
        );
      }

      if (
        errorMessage.includes("team not found") ||
        errorMessage.includes("team does not exist")
      ) {
        return NextResponse.json(
          {
            error:
              "Selected team does not exist. Please choose a different team or create a new one.",
          },
          { status: 404 }
        );
      }

      if (
        errorMessage.includes("email not whitelisted") ||
        errorMessage.includes("email not authorized")
      ) {
        return NextResponse.json(
          {
            error:
              "Email address is not authorized for registration. Please contact an administrator.",
          },
          { status: 403 }
        );
      }
    }

    return handleGatewayError(
      error,
      "Registration failed. Please try again later."
    );
  }
}

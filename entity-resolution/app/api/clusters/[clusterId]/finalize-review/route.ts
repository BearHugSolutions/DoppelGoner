// app/api/clusters/[clusterId]/finalize-review/route.ts
import { NextRequest, NextResponse } from "next/server";
import { getIronSession } from "iron-session";
import { sessionOptions } from "@/lib/session";
import { fetchFromGateway, handleGatewayError } from "@/utils/gateway-client";
import type { UserSessionData } from "@/app/api/auth/login/route";
import type { ClusterFinalizationStatusResponse } from "@/types/entity-resolution";

export async function POST(
  request: NextRequest,
  { params }: { params: { clusterId: string } }
) {
  const { clusterId } = await params;
  const searchParams = request.nextUrl.searchParams;
  const type = searchParams.get("type");

  // 1. Initial Parameter Validation
  if (!clusterId) {
    return NextResponse.json(
      { error: "Cluster ID is required." },
      { status: 400 }
    );
  }
  if (type !== "entity" && type !== "service") {
    return NextResponse.json(
      {
        error:
          "Invalid or missing 'type' parameter. Must be 'entity' or 'service'.",
      },
      { status: 400 }
    );
  }

  let userSchema: string | undefined;
  let sessionUserId: string | undefined;
  let sessionId: string | undefined;

  // 2. Authentication and Session Handling
  try {
    const tempResponse = new NextResponse(); // Required for iron-session in Route Handlers
    const session = await getIronSession<UserSessionData>(
      request,
      tempResponse,
      sessionOptions
    );
    if (
      !session.isLoggedIn ||
      !session.userSchema ||
      !session.userId ||
      !session.sessionId
    ) {
      console.warn(
        `[FINALIZE_CLUSTER] Unauthorized attempt for clusterId: ${clusterId}.`
      );
      return NextResponse.json(
        { error: "Unauthorized: User session not found or invalid." },
        { status: 401 }
      );
    }
    userSchema = session.userSchema;
    sessionUserId = session.userId;
    sessionId = session.sessionId;
  } catch (error) {
    console.error("[FINALIZE_CLUSTER] Session validation error:", error);
    return NextResponse.json(
      { error: "An error occurred during session validation." },
      { status: 500 }
    );
  }

  // 3. Gateway Communication
  try {
    console.log(
      `[GATEWAY_CALL] Calling finalize-review for cluster ${clusterId}`
    );

    // The Rust handler expects 'type_param', 'session_user_id', and 'session_id' as query parameters.
    // The fetchFromGateway utility handles adding these from the `params` object to the URL.
    const gatewayResponse =
      await fetchFromGateway<ClusterFinalizationStatusResponse>(
        `/clusters/${clusterId}/finalize-review`,
        {
          method: "POST",
          // The Rust handler uses `Query(FinalizeReviewQueryPayload)`, so we pass data in the URL.
          params: {
            type_param: type,
            session_user_id: sessionUserId,
            session_id: sessionId,
          },
          // No body is needed for this POST request as per the Rust handler's signature.
        },
        userSchema // The user's schema is passed as a header for tenancy.
      );

    // The gateway's response is forwarded directly to the client.
    return NextResponse.json(gatewayResponse);
  } catch (error: any) {
    console.error(
      `[GATEWAY_ERROR] Failed to finalize cluster review for ${clusterId}:`,
      error
    );
    // Use the standardized error handler to return a consistent error format.
    return handleGatewayError(error, "Failed to finalize cluster review.");
  }
}

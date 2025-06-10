// app/api/clusters/route.ts
import { NextResponse, NextRequest } from "next/server";
import { getUserSchemaFromSession } from "@/utils/auth-db";
import { fetchFromGateway, handleGatewayError } from "@/utils/gateway-client";

export async function GET(request: NextRequest) {
  const userSchema = await getUserSchemaFromSession(request);

  if (!userSchema) {
    return NextResponse.json(
      { error: "Unauthorized: User session not found or invalid." },
      { status: 401 }
    );
  }

  const { searchParams } = new URL(request.url);
  const page = searchParams.get("page") || "1";
  const limit = searchParams.get("limit") || "10";
  const type = searchParams.get("type") || "entity";
  const review_status = searchParams.get("review_status") || "unreviewed";

  console.log("Fetching clusters for type: ", type);
  console.log("Fetching clusters for review_status: ", review_status);

  try {
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
      userSchema
    );

    return NextResponse.json(gatewayResponse);
  } catch (error: any) {
    console.error(
      `Error fetching ${type} clusters from gateway for schema ${userSchema}:`,
      error
    );
    return handleGatewayError(error, `Failed to fetch ${type} clusters`);
  }
}

// app/api/clusters/route.ts
import { NextResponse, NextRequest } from "next/server";
import { fetchFromGateway, handleGatewayError } from "@/utils/gateway-client";
import { requireTeamContext } from '@/utils/team-context';

export async function GET(request: NextRequest) {
  const response = NextResponse.next();
  const authResult = await requireTeamContext(request, response);
  if (authResult instanceof NextResponse) return authResult;
  const { teamContext, user } = authResult;

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
      teamContext // Pass team context
    );

    return NextResponse.json(gatewayResponse);
  } catch (error: any) {
    console.error(
      `Error fetching ${type} clusters from gateway:`,
      error
    );
    return handleGatewayError(error, `Failed to fetch ${type} clusters`);
  }
}

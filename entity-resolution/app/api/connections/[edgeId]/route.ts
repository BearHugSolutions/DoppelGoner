// app/api/connections/[edgeId]/route.ts
import { NextRequest, NextResponse } from "next/server";
import { fetchFromGateway, handleGatewayError } from '@/utils/gateway-client';
import { requireTeamContext } from '@/utils/team-context';

export async function GET(
  request: NextRequest,
  { params }: { params: { edgeId: string } }
) {
  const response = NextResponse.next();
  const authResult = await requireTeamContext(request, response);
  if (authResult instanceof NextResponse) return authResult;
  const { teamContext, user } = authResult;

  // ✨ Extract the opinion name from the request header
  const opinionName = request.headers.get('X-Opinion-Name');
  
  console.log("Connections API: Request with opinion header:", opinionName);

  const { searchParams } = new URL(request.url);
  const type = searchParams.get("type") || "entity";
  const { edgeId } = await params;

  if (!edgeId) {
    return NextResponse.json({ error: "Edge ID is required." }, { status: 400 });
  }

  console.log(`Fetching ${type} connection data for edge ${edgeId} with opinion:`, opinionName || "default");

  try {
    const gatewayResponse = await fetchFromGateway(
      `/connections/${edgeId}`,
      {
        method: 'GET',
        params: { type },
      },
      teamContext, // Pass team context
      opinionName // ✨ Pass opinion name to gateway client
    );

    return NextResponse.json(gatewayResponse);

  } catch (error: any) {
    console.error(`Error fetching ${type} connection data for edge ${edgeId} from gateway:`, error);
    return handleGatewayError(error, `Failed to fetch ${type} connection data`);
  }
}
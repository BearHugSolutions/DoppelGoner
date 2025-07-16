// app/api/visualization/[clusterId]/route.ts
import { NextResponse, NextRequest } from 'next/server';
import { fetchFromGateway, handleGatewayError } from '@/utils/gateway-client';
import { requireTeamContext } from '@/utils/team-context';

export async function GET(
  request: NextRequest,
  { params }: { params: { clusterId: string } }
) {
  const response = NextResponse.next();
  const authResult = await requireTeamContext(request, response);
  if (authResult instanceof NextResponse) return authResult;
  const { teamContext, user } = authResult;

  // ✨ Extract the opinion name from the request header
  const opinionName = request.headers.get('X-Opinion-Name');
  
  console.log("Visualization API: Request with opinion header:", opinionName);

  const { searchParams } = new URL(request.url);
  const type = searchParams.get('type') || 'entity';
  const { clusterId } = await params;

  if (!clusterId) {
    return NextResponse.json({ error: "Cluster ID is required." }, { status: 400 });
  }

  console.log(`Fetching ${type} visualization data for cluster ${clusterId} with opinion:`, opinionName || "default");

  try {
    const gatewayResponse = await fetchFromGateway(
      `/visualization/${clusterId}`,
      {
        method: 'GET',
        params: { type },
      },
      teamContext, // Pass team context
      opinionName // ✨ Pass opinion name to gateway client
    );

    return NextResponse.json(gatewayResponse);
    
  } catch (error: any) {
    console.error(`Error fetching ${type} visualization data for cluster ${clusterId} from gateway:`, error);
    return handleGatewayError(error, `Failed to fetch ${type} visualization data`);
  }
}
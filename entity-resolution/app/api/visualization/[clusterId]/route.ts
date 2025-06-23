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

  const { searchParams } = new URL(request.url);
  const type = searchParams.get('type') || 'entity';
  const { clusterId } = await params;

  if (!clusterId) {
    return NextResponse.json({ error: "Cluster ID is required." }, { status: 400 });
  }

  try {
    const gatewayResponse = await fetchFromGateway(
      `/visualization/${clusterId}`,
      {
        method: 'GET',
        params: { type },
      },
      teamContext // Pass team context
    );

    return NextResponse.json(gatewayResponse);
    
  } catch (error: any) {
    console.error(`Error fetching ${type} visualization data for cluster ${clusterId} from gateway:`, error);
    return handleGatewayError(error, `Failed to fetch ${type} visualization data`);
  }
}

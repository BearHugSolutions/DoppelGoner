// app/api/cluster-progress/route.ts
import { NextRequest, NextResponse } from 'next/server';
import { requireTeamContext } from '@/utils/team-context';
import { fetchFromGateway, handleGatewayError } from '@/utils/gateway-client';

export async function GET(request: NextRequest) {
  const response = NextResponse.next();
  // Use requireTeamContext to handle auth and get context in one step
  const authResult = await requireTeamContext(request, response);
  if (authResult instanceof NextResponse) return authResult;
  const { teamContext } = authResult;

  // Extract query parameters from the request URL
  const { searchParams } = new URL(request.url);
  const page = searchParams.get('page') || '1';
  const limit = searchParams.get('limit') || '10';
  const type = searchParams.get('type') || 'entity';
  const review_status = searchParams.get('review_status') || 'unreviewed';
  const workflow_filter = searchParams.get('workflow_filter') || 'all';

  // Extract the opinion name from the request header
  const opinionName = request.headers.get('X-Opinion-Name');

  console.log(`[CLUSTER_PROGRESS_API] Request: page=${page}, limit=${limit}, type=${type}, review_status=${review_status}, workflow_filter=${workflow_filter}, opinion=${opinionName || 'default'}`);

  try {
    // Call the gateway with parameters passed in the options object.
    // The client will construct the query string and set the necessary headers.
    const gatewayResponse = await fetchFromGateway(
      "/cluster-progress",
      {
        method: "GET",
        params: {
          page,
          limit,
          type,
          review_status,
          workflow_filter,
        },
      },
      teamContext, // Pass team context
      opinionName  // Pass opinion name
    );

    console.log(`[CLUSTER_PROGRESS_API] Success: ${gatewayResponse.clusters?.length || 0} clusters, overall progress: ${gatewayResponse.overallProgress?.currentView?.overallProgressPercentage || 'N/A'}%`);

    return NextResponse.json(gatewayResponse);

  } catch (error: any) {
    // Use the centralized error handler for consistent error responses
    console.error('[CLUSTER_PROGRESS_API] Error fetching cluster progress from gateway:', error);
    return handleGatewayError(error, 'Failed to fetch cluster progress');
  }
}

// app/api/post-processing-audit/clusters/route.ts
import { NextRequest, NextResponse } from 'next/server';
import { fetchFromGateway, handleGatewayError } from '@/utils/gateway-client';
import { requireTeamContext } from '@/utils/team-context';

export async function GET(request: NextRequest) {
  const response = NextResponse.next();
  const authResult = await requireTeamContext(request, response);
  if (authResult instanceof NextResponse) return authResult;
  const { teamContext } = authResult;

  // Extract the opinion name from the request header
  const opinionName = request.headers.get('X-Opinion-Name');
  
  console.log("Post-Processing Audit Clusters API: Request with opinion header:", opinionName);

  // Extract query parameters
  const { searchParams } = new URL(request.url);
  const queryParams: Record<string, string> = {};
  
  // Build query parameters object
  const entityType = searchParams.get('entity_type');
  const postProcFilter = searchParams.get('post_proc_filter');
  const reviewedByHuman = searchParams.get('reviewed_by_human');
  const page = searchParams.get('page');
  const limit = searchParams.get('limit');

  if (entityType) queryParams.entity_type = entityType;
  if (postProcFilter) queryParams.post_proc_filter = postProcFilter;
  if (reviewedByHuman) queryParams.reviewed_by_human = reviewedByHuman;
  if (page) queryParams.page = page;
  if (limit) queryParams.limit = limit;

  console.log("[API_CLIENT] Requesting post-processing audit clusters data");
  console.log("[API_CLIENT] Query params:", queryParams);
  console.log("[API_CLIENT] Using opinion:", opinionName || "default");

  try {
    // Build the URL with query parameters
    const baseUrl = '/post-processing-audit/clusters';
    const queryString = new URLSearchParams(queryParams).toString();
    const fullUrl = queryString ? `${baseUrl}?${queryString}` : baseUrl;

    const gatewayResponse = await fetchFromGateway(
      fullUrl,
      {
        method: 'GET',
        headers: {
          'Content-Type': 'application/json',
        },
      },
      teamContext, // Pass team context
      opinionName // Pass opinion name to gateway client
    );

    return NextResponse.json(gatewayResponse);
  } catch (error: any) {
    console.error(`Error fetching post-processing audit clusters data via gateway:`, error);
    return handleGatewayError(error, 'Failed to fetch post-processing audit clusters data');
  }
}
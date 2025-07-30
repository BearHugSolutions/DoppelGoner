// app/api/post-processing-audit/bulk-mark-reviewed/route.ts
import { NextRequest, NextResponse } from 'next/server';
import { fetchFromGateway, handleGatewayError } from '@/utils/gateway-client';
import { requireTeamContext } from '@/utils/team-context';
import type { BulkMarkReviewedRequest } from '@/types/post-processing';

export async function POST(request: NextRequest) {
  const response = NextResponse.next();
  const authResult = await requireTeamContext(request, response);
  if (authResult instanceof NextResponse) return authResult;
  const { teamContext } = authResult;

  // Extract the opinion name from the request header
  const opinionName = request.headers.get('X-Opinion-Name');
  
  console.log("Bulk Mark Reviewed API: Request with opinion header:", opinionName);

  let payload: BulkMarkReviewedRequest;
  try {
    payload = await request.json();
  } catch (e) {
    console.error('Error parsing JSON body for bulk mark reviewed:', e);
    return NextResponse.json({ error: 'Invalid JSON body' }, { status: 400 });
  }

  const { decisionIds, reviewerId } = payload;

  // Validate required fields
  if (!decisionIds || !Array.isArray(decisionIds) || decisionIds.length === 0) {
    return NextResponse.json(
      { error: 'decisionIds is required and must be a non-empty array' },
      { status: 400 }
    );
  }

  if (!reviewerId) {
    return NextResponse.json(
      { error: 'reviewerId is required' },
      { status: 400 }
    );
  }

  // Validate array size limit (max 1000 as mentioned in the docs)
  if (decisionIds.length > 1000) {
    return NextResponse.json(
      { error: 'Maximum 1000 decisions can be marked at once' },
      { status: 400 }
    );
  }

  // Validate that all decision IDs are strings
  if (!decisionIds.every(id => typeof id === 'string')) {
    return NextResponse.json(
      { error: 'All decision IDs must be strings' },
      { status: 400 }
    );
  }

  console.log("[API_CLIENT] Requesting bulk mark reviewed for decisions");
  console.log("[API_CLIENT] Decision count:", decisionIds.length);
  console.log("[API_CLIENT] Reviewer ID:", reviewerId);
  console.log("[API_CLIENT] Using opinion:", opinionName || "default");

  try {
    const gatewayResponse = await fetchFromGateway(
      `/post-processing-audit/bulk-mark-reviewed`,
      {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(payload),
      },
      teamContext, // Pass team context
      opinionName // Pass opinion name to gateway client
    );

    return NextResponse.json(gatewayResponse);
  } catch (error: any) {
    console.error(`Error submitting bulk mark reviewed via gateway:`, error);
    return handleGatewayError(error, 'Failed to bulk mark decisions as reviewed');
  }
}
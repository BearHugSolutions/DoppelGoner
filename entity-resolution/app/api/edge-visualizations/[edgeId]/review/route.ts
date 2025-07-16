// app/api/edge-visualizations/[edgeId]/review/route.ts
import { NextRequest, NextResponse } from 'next/server';
import { fetchFromGateway, handleGatewayError } from '@/utils/gateway-client';
import { requireTeamContext } from '@/utils/team-context';
import type { EdgeReviewApiPayload } from '@/types/entity-resolution';

export async function POST(
  request: NextRequest,
  { params }: { params: { edgeId: string } }
) {
  const response = NextResponse.next();
  const authResult = await requireTeamContext(request, response);
  if (authResult instanceof NextResponse) return authResult;
  const { teamContext, user } = authResult;

  // ✨ Extract the opinion name from the request header
  const opinionName = request.headers.get('X-Opinion-Name');
  
  console.log("Edge Review API: Request with opinion header:", opinionName);

  const { edgeId } = await params;
  if (!edgeId) {
    return NextResponse.json({ error: 'Edge ID is required.' }, { status: 400 });
  }

  let payload: EdgeReviewApiPayload;
  try {
    payload = await request.json();
  } catch (e) {
    console.error('Error parsing JSON body for edge review:', e);
    return NextResponse.json({ error: 'Invalid JSON body' }, { status: 400 });
  }

  const { decision, reviewerId, notes, type, disconnectDependentServices } = payload;

  if (!decision || !reviewerId || !type) {
    return NextResponse.json(
      { error: 'Missing decision, reviewerId, or type in request body' },
      { status: 400 }
    );
  }

  if (decision !== 'ACCEPTED' && decision !== 'REJECTED') {
    return NextResponse.json(
      { error: 'Invalid decision value. Must be "ACCEPTED" or "REJECTED".' },
      { status: 400 }
    );
  }

  if (type !== 'entity' && type !== 'service') {
    return NextResponse.json(
      { error: 'Invalid type value. Must be "entity" or "service".' },
      { status: 400 }
    );
  }

  // Validate disconnectDependentServices if provided
  if (disconnectDependentServices !== undefined && typeof disconnectDependentServices !== 'boolean') {
    return NextResponse.json(
      { error: 'Invalid disconnectDependentServices value. Must be a boolean.' },
      { status: 400 }
    );
  }

  console.log("[API_CLIENT] Requesting edge review for edge", edgeId);
  console.log("[API_CLIENT] Payload:", payload);
  console.log("[API_CLIENT] Using opinion:", opinionName || "default");

  try {
    const gatewayResponse = await fetchFromGateway(
      `/edge-visualizations/${edgeId}/review`,
      {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(payload),
      },
      teamContext, // Pass team context
      opinionName // ✨ Pass opinion name to gateway client
    );

    return NextResponse.json(gatewayResponse);
  } catch (error: any) {
    console.error(`Error submitting edge review for edge ${edgeId} via gateway:`, error);
    return handleGatewayError(error, 'Failed to submit edge review');
  }
}
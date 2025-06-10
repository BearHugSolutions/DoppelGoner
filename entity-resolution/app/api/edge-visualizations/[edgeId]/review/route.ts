// app/api/edge-visualizations/[edgeId]/review/route.ts
import { NextRequest, NextResponse } from 'next/server';
import { getUserSchemaFromSession } from '@/utils/auth-db';
import { fetchFromGateway, handleGatewayError } from '@/utils/gateway-client';
import type { EdgeReviewApiPayload } from '@/types/entity-resolution';

export async function POST(
  request: NextRequest,
  { params }: { params: { edgeId: string } }
) {
  const userSchema = await getUserSchemaFromSession(request);
  if (!userSchema) {
    return NextResponse.json(
      { error: 'Unauthorized: User session not found or invalid.' },
      { status: 401 }
    );
  }

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

  const { decision, reviewerId, notes, type } = payload;

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

  console.log("[API_CLIENT] Requesting edge review for edge", edgeId);
  console.log("[API_CLIENT] Payload:", payload);

  try {
    // Proxy the request to the new Rust gateway endpoint
    const gatewayResponse = await fetchFromGateway(
      // The path now uses the new, unified endpoint structure
      `/edge-visualizations/${edgeId}/review`,
      {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        // The entire payload received by this route is forwarded
        body: JSON.stringify(payload),
      },
      userSchema // Pass the user's schema for the gateway to use
    );

    return NextResponse.json(gatewayResponse);
  } catch (error: any) {
    console.error(`Error submitting edge review for edge ${edgeId} via gateway:`, error);
    return handleGatewayError(error, 'Failed to submit edge review');
  }
}

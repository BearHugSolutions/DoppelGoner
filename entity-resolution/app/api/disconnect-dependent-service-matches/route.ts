// app/api/disconnect-dependent-service-matches/route.ts
import { NextRequest, NextResponse } from 'next/server';
import { fetchFromGateway, handleGatewayError } from '@/utils/gateway-client';
import { requireTeamContext } from '@/utils/team-context';
import type { DisconnectDependentServicesRequest } from '@/types/entity-resolution';

export async function POST(request: NextRequest) {
  const response = NextResponse.next();
  const authResult = await requireTeamContext(request, response);
  if (authResult instanceof NextResponse) return authResult;
  const { teamContext } = authResult;

  let payload: DisconnectDependentServicesRequest;
  try {
    payload = await request.json();
  } catch (e) {
    console.error('Error parsing JSON body for bulk disconnect:', e);
    return NextResponse.json({ error: 'Invalid JSON body' }, { status: 400 });
  }

  const { reviewerId, notes, asyncProcessing } = payload;

  if (!reviewerId) {
    return NextResponse.json(
      { error: 'Missing reviewerId in request body' },
      { status: 400 }
    );
  }

  console.log("[API_CLIENT] Requesting bulk disconnect of dependent services");
  console.log("[API_CLIENT] Payload:", payload);

  try {
    const gatewayResponse = await fetchFromGateway(
      `/disconnect-dependent-service-matches`,
      {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(payload),
      },
      teamContext // Pass team context
    );

    return NextResponse.json(gatewayResponse);
  } catch (error: any) {
    console.error(`Error submitting bulk disconnect via gateway:`, error);
    return handleGatewayError(error, 'Failed to submit bulk disconnect request');
  }
}
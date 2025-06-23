// app/api/entity-groups/[groupId]/review/route.ts
import { NextRequest, NextResponse } from 'next/server';
import { fetchFromGateway, handleGatewayError } from '@/utils/gateway-client';
import { requireTeamContext } from '@/utils/team-context';

export async function POST(
  request: NextRequest,
  { params }: { params: { groupId: string } }
) {
  const response = NextResponse.next();
  const authResult = await requireTeamContext(request, response);
  if (authResult instanceof NextResponse) return authResult;
  const { teamContext, user } = authResult;

  const { groupId } = await params;
  if (!groupId) {
    return NextResponse.json({ error: "Group ID is required." }, { status: 400 });
  }

  let requestBody;
  try {
    requestBody = await request.json();
  } catch (e) {
    console.error('Error parsing JSON body for entity group review:', e);
    return NextResponse.json({ error: 'Invalid JSON body' }, { status: 400 });
  }

  const { decision, reviewerId, notes } = requestBody;

  if (!decision || !reviewerId) {
    return NextResponse.json(
      { error: 'Missing decision or reviewerId in request body' },
      { status: 400 }
    );
  }

  if (decision !== 'ACCEPTED' && decision !== 'REJECTED') {
    return NextResponse.json(
      { error: 'Invalid decision value. Must be "ACCEPTED" or "REJECTED".' },
      { status: 400 }
    );
  }

  try {
    const gatewayResponse = await fetchFromGateway(
      `/entity-groups/${groupId}/review`,
      {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ decision, reviewerId, notes }),
      },
      teamContext // Pass team context
    );

    return NextResponse.json(gatewayResponse);

  } catch (error: any) {
    console.error(`Error submitting entity group review for group ${groupId} via gateway:`, error);
    return handleGatewayError(error, 'Failed to submit entity group review');
  }
}
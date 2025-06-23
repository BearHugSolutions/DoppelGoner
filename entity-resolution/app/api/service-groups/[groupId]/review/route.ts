// app/api/service-groups/[groupId]/review/route.ts
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
    console.error('Error parsing JSON body for service group review:', e);
    return NextResponse.json({ error: 'Invalid JSON body' }, { status: 400 });
  }

  const { decision, reviewerId, notes } = requestBody;

  if (!['ACCEPTED', 'REJECTED'].includes(decision)) {
    return NextResponse.json({ error: 'Invalid decision value. Must be "ACCEPTED" or "REJECTED".' }, { status: 400 });
  }
  if (!reviewerId) {
    return NextResponse.json({ error: 'Reviewer ID is required.' }, { status: 400 });
  }

  try {
    const gatewayResponse = await fetchFromGateway(
      `/service-groups/${groupId}/review`,
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
    console.error(`Error submitting service group review for group ${groupId} via gateway:`, error);
    return handleGatewayError(error, 'Failed to submit service group review');
  }
}


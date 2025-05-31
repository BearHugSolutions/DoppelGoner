// app/api/entity-groups/[groupId]/review/route.ts
import { NextRequest, NextResponse } from 'next/server';
import { getUserSchemaFromSession } from '@/utils/auth-db'; // Still needed
import { fetchFromGateway, handleGatewayError } from '@/utils/gateway-client'; // Import new utility

export async function POST(
  request: NextRequest,
  { params }: { params: { groupId: string } }
) {
  const userSchema = await getUserSchemaFromSession(request);
  if (!userSchema) {
    return NextResponse.json(
      { error: 'Unauthorized: User session not found or invalid.' },
      { status: 401 }
    );
  }

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
    // Call the Rust gateway
    const gatewayResponse = await fetchFromGateway(
      `/entity-groups/${groupId}/review`,
      {
        method: 'POST',
        headers: { // Explicitly set Content-Type to application/json
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ decision, reviewerId, notes }), // Pass the original body
      },
      userSchema // Pass userSchema as a header
    );

    // The gateway response is assumed to match the structure previously returned
    return NextResponse.json(gatewayResponse);

  } catch (error: any) {
    console.error(`Error submitting entity group review for group ${groupId} via gateway:`, error);
    return handleGatewayError(error, 'Failed to submit entity group review');
  }
}

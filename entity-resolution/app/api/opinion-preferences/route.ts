// app/api/opinion-preferences/route.ts
import { NextRequest, NextResponse } from 'next/server';
import { fetchFromGateway, handleGatewayError } from '@/utils/gateway-client';
import { requireTeamContext } from '@/utils/team-context';

export interface OpinionPreferences {
  disconnectDependentServices: boolean;
  // Add other preferences here as needed
}

export interface GetOpinionPreferencesResponse {
  preferences: OpinionPreferences;
  opinionName: string;
}

export interface UpdateOpinionPreferencesRequest {
  disconnectDependentServices?: boolean;
  // Add other preferences here as needed
}

export interface UpdateOpinionPreferencesResponse {
  message: string;
  preferences: OpinionPreferences;
  opinionName: string;
}

/**
 * GET /api/opinion-preferences
 * Get the current opinion preferences for the user
 */
export async function GET(request: NextRequest) {
  const response = NextResponse.next();
  const authResult = await requireTeamContext(request, response);
  if (authResult instanceof NextResponse) return authResult;
  const { teamContext, user } = authResult;

  const opinionName = request.headers.get('X-Opinion-Name') || 'default';
  
  console.log("Opinion Preferences GET: Request for opinion:", opinionName);

  try {
    const gatewayResponse = await fetchFromGateway<GetOpinionPreferencesResponse>(
      `/opinion-preferences`,
      {
        method: 'GET',
        headers: {
          'Content-Type': 'application/json',
        },
      },
      teamContext,
      opinionName
    );

    return NextResponse.json(gatewayResponse);
  } catch (error: any) {
    console.error(`Error getting opinion preferences via gateway:`, error);
    return handleGatewayError(error, 'Failed to get opinion preferences');
  }
}

/**
 * POST /api/opinion-preferences
 * Update opinion preferences for the user
 */
export async function POST(request: NextRequest) {
  const response = NextResponse.next();
  const authResult = await requireTeamContext(request, response);
  if (authResult instanceof NextResponse) return authResult;
  const { teamContext, user } = authResult;

  const opinionName = request.headers.get('X-Opinion-Name') || 'default';
  
  let payload: UpdateOpinionPreferencesRequest;
  try {
    payload = await request.json();
  } catch (e) {
    console.error('Error parsing JSON body for opinion preferences update:', e);
    return NextResponse.json({ error: 'Invalid JSON body' }, { status: 400 });
  }

  console.log("Opinion Preferences POST: Request for opinion:", opinionName);
  console.log("Opinion Preferences POST: Payload:", payload);

  try {
    const gatewayResponse = await fetchFromGateway<UpdateOpinionPreferencesResponse>(
      `/opinion-preferences`,
      {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(payload),
      },
      teamContext,
      opinionName
    );

    return NextResponse.json(gatewayResponse);
  } catch (error: any) {
    console.error(`Error updating opinion preferences via gateway:`, error);
    return handleGatewayError(error, 'Failed to update opinion preferences');
  }
}
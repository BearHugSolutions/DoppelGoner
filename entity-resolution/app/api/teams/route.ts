// app/api/teams/route.ts
import { NextResponse, NextRequest } from 'next/server';
import { fetchFromGateway, handleGatewayError } from '@/utils/gateway-client';

interface Team {
  id: string;
  name: string;
  displayName?: string;
  description?: string;
  memberCount?: number;
}

interface GatewayTeamsResponse {
  teams: Team[];
}

interface GatewayCreateTeamResponse {
  message: string;
  team: Team;
}

// GET /api/teams - List available teams
export async function GET(request: NextRequest) {
  try {
    const gatewayResponse = await fetchFromGateway<GatewayTeamsResponse>(
      '/teams',
      {
        method: 'GET',
      }
      // No team context needed for listing teams
    );

    return NextResponse.json(gatewayResponse.teams, { status: 200 });

  } catch (error: any) {
    console.error('Error fetching teams:', error);
    return handleGatewayError(error, 'Failed to fetch teams.');
  }
}

// POST /api/teams - Create a new team (if needed for admin functionality)
export async function POST(request: NextRequest) {
  try {
    const { name, displayName, description, datasets } = await request.json();

    if (!name || typeof name !== 'string' || name.trim() === '') {
      return NextResponse.json({ error: 'Team name is required.' }, { status: 400 });
    }

    const createTeamPayload: any = {
      name: name.trim(),
    };

    if (displayName) {
      createTeamPayload.displayName = displayName.trim();
    }

    if (description) {
      createTeamPayload.description = description.trim();
    }

    if (datasets && Array.isArray(datasets)) {
      createTeamPayload.datasets = datasets;
    }

    const gatewayResponse = await fetchFromGateway<GatewayCreateTeamResponse>(
      '/teams',
      {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(createTeamPayload),
      }
      // No team context needed for creating teams
    );

    return NextResponse.json({
      message: gatewayResponse.message,
      team: gatewayResponse.team,
    }, { status: 201 });

  } catch (error: any) {
    console.error('Error creating team:', error);
    
    if (error.responseBody?.message) {
      const errorMessage = error.responseBody.message.toLowerCase();
      
      if (errorMessage.includes("team name already exists") || errorMessage.includes("team already exists")) {
        return NextResponse.json({ 
          error: 'Team name already exists. Please choose another.' 
        }, { status: 409 });
      }
    }
    
    return handleGatewayError(error, 'Failed to create team.');
  }
}
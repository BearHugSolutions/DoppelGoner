// app/api/datasets/route.ts
import { NextResponse, NextRequest } from 'next/server';
import { fetchFromGateway, handleGatewayError } from '@/utils/gateway-client';

interface DatasetInfo {
  name: string;
  displayName: string;
  description?: string;
  entityCount?: number;
  isActive: boolean;
}

interface DatasetsResponse {
  datasets: DatasetInfo[];
}

export async function GET(request: NextRequest) {
  try {
    // No team context needed for fetching available datasets
    const response = await fetchFromGateway<DatasetsResponse>(
      '/datasets',
      {
        method: 'GET',
      }
    );

    return NextResponse.json(response, { status: 200 });

  } catch (error: any) {
    console.error('Failed to fetch datasets:', error);
    return handleGatewayError(error, 'Failed to fetch available datasets.');
  }
}
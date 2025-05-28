// app/api/clusters/route.ts
import { query } from '@/types/db';
import { NextResponse } from 'next/server';

export async function GET(request: Request) {
  const { searchParams } = new URL(request.url);
  const page = parseInt(searchParams.get('page') || '1');
  const limit = parseInt(searchParams.get('limit') || '10');
  const offset = (page - 1) * limit;

  try {
    // Get paginated clusters
    const clusters = await query(`
      SELECT 
        ec.id,
        ec.name,
        ec.description,
        ec.entity_count,
        ec.group_count,
        ec.average_coherence_score,
        ec.created_at,
        ec.updated_at
      FROM entity_group_cluster ec
      ORDER BY ec.updated_at DESC
      LIMIT $1 OFFSET $2
    `, [limit, offset]);

    // Get total count for pagination
    const countResult = await query(
      'SELECT COUNT(*) FROM entity_group_cluster'
    );
    const total = parseInt(countResult[0].count, 10);

    return NextResponse.json({
      clusters,
      total,
      page,
      limit,
      totalPages: Math.ceil(total / limit)
    });
  } catch (error) {
    console.error('Error fetching clusters:', error);
    return NextResponse.json(
      { error: 'Failed to fetch clusters' },
      { status: 500 }
    );
  }
}
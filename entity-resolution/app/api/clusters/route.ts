// app/api/clusters/route.ts
import { query } from '@/types/db';
import { NextResponse, NextRequest } from 'next/server';
// Import the function to get user schema from session
import { getUserSchemaFromSession } from '@/utils/auth-db'; // Adjust path if your auth-db.ts is elsewhere

export async function GET(request: NextRequest) {
  // Retrieve the user's schema using the session
  const userSchema = await getUserSchemaFromSession(request);

  if (!userSchema) {
    // If no userSchema is found, the user is likely not authenticated or session is invalid
    return NextResponse.json(
      { error: 'Unauthorized: User session not found or invalid.' },
      { status: 401 }
    );
  }

  const { searchParams } = new URL(request.url);
  const page = parseInt(searchParams.get('page') || '1');
  const limit = parseInt(searchParams.get('limit') || '10');
  const offset = (page - 1) * limit;

  try {
    // Get paginated clusters from the user's schema
    const clustersSql = `
      SELECT 
        ec.id,
        ec.name,
        ec.description,
        ec.entity_count,
        ec.group_count,
        ec.average_coherence_score,
        ec.created_at,
        ec.updated_at
      FROM "${userSchema}".entity_group_cluster ec
      ORDER BY ec.updated_at DESC
      LIMIT $1 OFFSET $2
    `;
    const clusters = await query(clustersSql, [limit, offset]);

    // Get total count for pagination from the user's schema
    const countSql = `SELECT COUNT(*) FROM "${userSchema}".entity_group_cluster`;
    const countResult = await query(countSql);
    
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
    console.error(`Error occurred for schema: ${userSchema}`);
    return NextResponse.json(
      { error: 'Failed to fetch clusters' },
      { status: 500 }
    );
  }
}

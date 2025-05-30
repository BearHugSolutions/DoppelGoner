// app/api/clusters/route.ts
import { query } from '@/types/db';
import { NextResponse, NextRequest } from 'next/server';
import { getUserSchemaFromSession } from '@/utils/auth-db';

export async function GET(request: NextRequest) {
  const userSchema = await getUserSchemaFromSession(request);

  if (!userSchema) {
    return NextResponse.json(
      { error: 'Unauthorized: User session not found or invalid.' },
      { status: 401 }
    );
  }

  const { searchParams } = new URL(request.url);
  const page = parseInt(searchParams.get('page') || '1');
  const limit = parseInt(searchParams.get('limit') || '10');
  const type = searchParams.get('type') || 'entity'; // Default to 'entity'
  const offset = (page - 1) * limit;

  try {
    let clustersSql;
    let countSql;
    let clusters;
    let countResult;

    if (type === 'service') {
      // Fetch service clusters
      clustersSql = `
        SELECT 
          sc.id,
          sc.name,
          sc.description,
          sc.service_count,
          sc.service_group_count,
          sc.average_coherence_score,
          sc.created_at,
          sc.updated_at
        FROM "${userSchema}".service_group_cluster sc
        ORDER BY sc.updated_at DESC
        LIMIT $1 OFFSET $2
      `;
      clusters = await query(clustersSql, [limit, offset]);

      countSql = `SELECT COUNT(*) FROM "${userSchema}".service_group_cluster`;
      countResult = await query(countSql);

    } else {
      // Fetch entity clusters (existing logic)
      clustersSql = `
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
      clusters = await query(clustersSql, [limit, offset]);

      countSql = `SELECT COUNT(*) FROM "${userSchema}".entity_group_cluster`;
      countResult = await query(countSql);
    }
    
    const total = parseInt(countResult[0].count, 10);

    console.log('clusters: ', JSON.stringify(clusters))

    return NextResponse.json({
      clusters,
      total,
      page,
      limit,
      totalPages: Math.ceil(total / limit)
    });
  } catch (error) {
    console.error(`Error fetching ${type} clusters:`, error);
    console.error(`Error occurred for schema: ${userSchema}`);
    return NextResponse.json(
      { error: `Failed to fetch ${type} clusters` },
      { status: 500 }
    );
  }
}

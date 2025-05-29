import { query } from "@/types/db";
import { getUserSchemaFromSession } from "@/utils/auth-db";
import { NextRequest, NextResponse } from "next/server";

// app/api/connections/[edgeId]/route.ts
export async function GET(
  request: NextRequest,
  { params }: { params: { edgeId: string } }
) {
  const userSchema = await getUserSchemaFromSession(request);
  if (!userSchema) {
    return NextResponse.json(
      { error: 'Unauthorized: User session not found or invalid.' },
      { status: 401 }
    );
  }

  const { edgeId } = await params;

  try {
    // Get edge details with review status
    const edgeSql = `
      SELECT 
        eev.*,
        e1.name as entity1_name,
        e2.name as entity2_name
      FROM public.entity_edge_visualization eev
      JOIN public.entity e1 ON eev.entity_id_1 = e1.id
      JOIN public.entity e2 ON eev.entity_id_2 = e2.id
      WHERE eev.id = $1
    `;
    const edgeResult = await query(edgeSql, [edgeId]);

    if (edgeResult.length === 0) {
      return NextResponse.json({ error: 'Edge not found' }, { status: 404 });
    }

    const edge = edgeResult[0];

    // Get entity groups for detailed information
    const entityGroupsSql = `
      SELECT 
        eg.id,
        eg.entity_id_1,
        eg.entity_id_2,
        eg.confidence_score,
        eg.method_type,
        eg.match_values,
        eg.pre_rl_confidence_score,
        eg.confirmed_status,
        eg.reviewed_at,
        eg.reviewer_id
      FROM "${userSchema}".entity_group eg
      WHERE ((eg.entity_id_1 = $1 AND eg.entity_id_2 = $2)
         OR (eg.entity_id_1 = $2 AND eg.entity_id_2 = $1))
         AND eg.confirmed_status != 'DENIED'
    `;
    const entityGroups = await query(entityGroupsSql, [edge.entity_id_1, edge.entity_id_2]);

    // Get entity details
    const entity1Sql = `SELECT * FROM public.entity WHERE id = $1`;
    const entity2Sql = `SELECT * FROM public.entity WHERE id = $1`;
    
    const [entity1Result, entity2Result] = await Promise.all([
      query(entity1Sql, [edge.entity_id_1]),
      query(entity2Sql, [edge.entity_id_2])
    ]);
    
    const responseData = {
      edge: {
        id: edge.id,
        cluster_id: edge.cluster_id,
        entity_id_1: edge.entity_id_1,
        entity_id_2: edge.entity_id_2,
        edge_weight: edge.edge_weight,
        details: edge.details,
        pipeline_run_id: edge.pipeline_run_id,
        created_at: edge.created_at,
        entity1_name: edge.entity1_name,
        entity2_name: edge.entity2_name,
        status: edge.confirmed_status,  // Direct from edge table!
        display_weight: edge.confirmed_status === 'CONFIRMED_MATCH' ? 1.0 : edge.edge_weight,
        color: edge.confirmed_status === 'CONFIRMED_MATCH' ? '#000000' : null
      },
      entity1: entity1Result[0] || null,
      entity2: entity2Result[0] || null,
      entityGroups: entityGroups.map(eg => ({
        ...eg,
        confirmed_status: eg.confirmed_status || 'PENDING_REVIEW'
      }))
    };
    
    return NextResponse.json(responseData);
  } catch (error) {
    console.error('Error fetching connection data:', error);
    return NextResponse.json(
      { error: 'Failed to fetch connection data' },
      { status: 500 }
    );
  }
}
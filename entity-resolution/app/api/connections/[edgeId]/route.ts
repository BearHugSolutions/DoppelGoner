// app/api/connections/[edgeId]/route.ts
import { NextResponse, NextRequest } from 'next/server';
import { query } from '@/types/db';
// Import the function to get user schema from session
import { getUserSchemaFromSession } from '@/utils/auth-db'; // Adjust path if your auth-db.ts is elsewhere

export async function GET(
  request: NextRequest,
  { params }: { params: { edgeId: string } }
) {
  // Retrieve the user's schema using the session
  const userSchema = await getUserSchemaFromSession(request);

  if (!userSchema) {
    return NextResponse.json(
      { error: 'Unauthorized: User session not found or invalid.' },
      { status: 401 }
    );
  }

  const { edgeId } = await params; // Accessing edgeId from params

  try {
    // Get edge details.
    // 'entity_edge_visualization' is in the user's schema.
    // 'entity' table (aliased as e1 and e2) is in the 'public' schema.
    const edgeSql = `
      SELECT 
        eev.*,
        e1.name as entity1_name,
        e2.name as entity2_name
      FROM "${userSchema}".entity_edge_visualization eev
      JOIN public.entity e1 ON eev.entity_id_1 = e1.id
      JOIN public.entity e2 ON eev.entity_id_2 = e2.id
      WHERE eev.id = $1
    `;
    const edgeResult = await query(edgeSql, [edgeId]);

    if (edgeResult.length === 0) {
      return NextResponse.json(
        { error: 'Edge not found' },
        { status: 404 }
      );
    }

    const edge = edgeResult[0];

    // Get entity groups for this edge.
    // 'entity_group' table is in the user's schema.
    // FIXED: Added entity_id_1 and entity_id_2 to the SELECT statement
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

    const hasConfirmed = entityGroups.some(eg => eg.confirmed_status === 'CONFIRMED');
    const edgeStatus = hasConfirmed ? 'CONFIRMED' : 'PENDING_REVIEW';

    // Get entity details.
    // 'entity' table is in the 'public' schema.
    const entity1Sql = `SELECT * FROM public.entity WHERE id = $1`;
    const entity2Sql = `SELECT * FROM public.entity WHERE id = $1`;
    
    const [entity1Result, entity2Result] = await Promise.all([
      query(entity1Sql, [edge.entity_id_1]),
      query(entity2Sql, [edge.entity_id_2])
    ]);
    
    const entity1 = entity1Result[0] || null;
    const entity2 = entity2Result[0] || null;

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
        status: edgeStatus,
        display_weight: hasConfirmed ? 1.0 : edge.edge_weight,
        color: hasConfirmed ? '#000000' : null
      },
      entity1: entity1,
      entity2: entity2,
      entityGroups: entityGroups.map(eg => ({
        ...eg,
        confirmed_status: eg.confirmed_status || 'PENDING_REVIEW'
      })),
      matchDecisions: entityGroups
        .filter((eg: any) => eg.match_decision_id)
        .map((eg: any) => ({
          id: eg.match_decision_id,
          entity_group_id: eg.id,
          confidence_score: eg.match_confidence_score,
          decision: eg.match_decision,
          reviewed_at: eg.match_reviewed_at
        }))
    };
    
    // console.log('Connections API Response:', JSON.stringify(responseData, null, 2));
    return NextResponse.json(responseData);
  } catch (error) {
    console.error('Error fetching connection data:', error);
    console.error(`Error occurred for schema: ${userSchema}, edgeId: ${edgeId}`);
    return NextResponse.json(
      { error: 'Failed to fetch connection data' },
      { status: 500 }
    );
  }
}
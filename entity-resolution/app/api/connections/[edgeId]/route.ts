// app/api/connections/[edgeId]/route.ts
import { NextResponse } from 'next/server';
import { query } from '@/types/db';

export async function GET(
  request: Request,
  { params }: { params: { edgeId: string } }
) {
  const { edgeId } = params;

  try {
    // Get edge details
    const edgeResult = await query(`
      SELECT 
        eev.*,
        e1.name as entity1_name,
        e2.name as entity2_name
      FROM entity_edge_visualization eev
      JOIN entity e1 ON eev.entity_id_1 = e1.id
      JOIN entity e2 ON eev.entity_id_2 = e2.id
      WHERE eev.id = $1
    `, [edgeId]);

    if (edgeResult.length === 0) {
      return NextResponse.json(
        { error: 'Edge not found' },
        { status: 404 }
      );
    }


    const edge = edgeResult[0];

    // Get entity groups for this edge with their confirmed status
    const entityGroups = await query(`
      SELECT 
        eg.id,
        eg.confidence_score,
        eg.method_type,
        eg.match_values,
        eg.pre_rl_confidence_score,
        eg.confirmed_status,
        eg.reviewed_at,
        eg.reviewer_id
      FROM entity_group eg
      WHERE ((eg.entity_id_1 = $1 AND eg.entity_id_2 = $2)
         OR (eg.entity_id_1 = $2 AND eg.entity_id_2 = $1))
         AND eg.confirmed_status != 'DENIED'
    `, [edge.entity_id_1, edge.entity_id_2]);

    // Determine the highest priority status for the edge
    const hasConfirmed = entityGroups.some(eg => eg.confirmed_status === 'CONFIRMED');
    const edgeStatus = hasConfirmed ? 'CONFIRMED' : 'PENDING_REVIEW';

    // Get entity details
    const [entity1, entity2] = await Promise.all([
      query('SELECT * FROM entity WHERE id = $1', [edge.entity_id_1]),
      query('SELECT * FROM entity WHERE id = $1', [edge.entity_id_2])
    ]);

    return NextResponse.json({
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
        // Add status information for frontend styling
        status: edgeStatus,
        // For confirmed edges, use max weight (1.0), otherwise use the calculated edge weight
        display_weight: hasConfirmed ? 1.0 : edge.edge_weight,
        // For confirmed edges, use black, otherwise use null to let frontend determine color
        color: hasConfirmed ? '#000000' : null
      },
      entity1: entity1[0] || null,
      entity2: entity2[0] || null,
      entityGroups: entityGroups.map(eg => ({
        ...eg,
        // Ensure we have a consistent status value
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
    });
  } catch (error) {
    console.error('Error fetching connection data:', error);
    return NextResponse.json(
      { error: 'Failed to fetch connection data' },
      { status: 500 }
    );
  }
}
// NEW: app/api/entity-groups/[groupId]/review/route.ts
import { NextRequest, NextResponse } from 'next/server';
import { query, withTransaction } from '@/types/db';
import { getUserSchemaFromSession } from '@/utils/auth-db';

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
  const { decision, reviewerId, notes } = await request.json();

  try {
    const result = await withTransaction(async (client) => {
      // Update the entity group
      const updateGroupSql = `
        UPDATE "${userSchema}".entity_group 
        SET confirmed_status = $1, reviewer_id = $2, reviewed_at = NOW()
        WHERE id = $3
        RETURNING entity_id_1, entity_id_2
      `;
      const statusValue = decision === 'ACCEPTED' ? 'CONFIRMED_MATCH' : 'CONFIRMED_NON_MATCH';
      const groupResult = await client.query(updateGroupSql, [statusValue, reviewerId, groupId]);
      
      if (groupResult.rows.length === 0) {
        throw new Error('Entity group not found');
      }

      const { entity_id_1, entity_id_2 } = groupResult.rows[0];

      // Find the corresponding edge(s) and update their status
      const findEdgesSql = `
        SELECT id FROM public.entity_edge_visualization 
        WHERE (entity_id_1 = $1 AND entity_id_2 = $2) 
           OR (entity_id_1 = $2 AND entity_id_2 = $1)
      `;
      const edgeResults = await client.query(findEdgesSql, [entity_id_1, entity_id_2]);

      // Update each edge's status to match the review decision
      for (const edgeRow of edgeResults.rows) {
        const updateEdgeSql = `
          UPDATE public.entity_edge_visualization 
          SET confirmed_status = $1 
          WHERE id = $2
        `;
        await client.query(updateEdgeSql, [statusValue, edgeRow.id]);
      }

      return { 
        message: 'Review submitted successfully',
        updatedEdges: edgeResults.rows.length 
      };
    });

    return NextResponse.json(result);
  } catch (error) {
    console.error('Error submitting review:', error);
    return NextResponse.json(
      { error: 'Failed to submit review' },
      { status: 500 }
    );
  }
}
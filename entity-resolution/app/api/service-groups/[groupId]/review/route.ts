// app/api/service-groups/[groupId]/review/route.ts
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

  const { groupId } = params; // Correctly get groupId from params
  const { decision, reviewerId, notes } = await request.json(); // Notes might not be used yet but good to have

  if (!['ACCEPTED', 'REJECTED'].includes(decision)) {
    return NextResponse.json({ error: 'Invalid decision value.' }, { status: 400 });
  }
  if (!reviewerId) {
    return NextResponse.json({ error: 'Reviewer ID is required.' }, { status: 400 });
  }


  try {
    const result = await withTransaction(async (client) => {
      // Update the service group
      const updateGroupSql = `
        UPDATE "${userSchema}".service_group 
        SET confirmed_status = $1, reviewer_id = $2, reviewed_at = NOW(), updated_at = NOW()
        WHERE id = $3
        RETURNING service_id_1, service_id_2, group_cluster_id
      `;
      // Assuming 'CONFIRMED_NON_MATCH' is the equivalent of 'REJECTED' for services
      const statusValue = decision === 'ACCEPTED' ? 'CONFIRMED_MATCH' : 'CONFIRMED_NON_MATCH'; 
      const groupResult = await client.query(updateGroupSql, [statusValue, reviewerId, groupId]);
      
      if (groupResult.rows.length === 0) {
        throw new Error('Service group not found or no update occurred.');
      }

      const { service_id_1, service_id_2, group_cluster_id } = groupResult.rows[0];

      // The service_edge_visualization table in your schema does not have a 'confirmed_status' column.
      // So, we don't update it directly here. The visualization client will need to
      // derive the edge status based on the confirmed_status of the underlying service_group(s).
      // If, in the future, service_edge_visualization gets a status, this is where you'd update it:
      /*
      const findEdgesSql = `
        SELECT id FROM "${userSchema}".service_edge_visualization 
        WHERE service_group_cluster_id = $1 AND 
              ((service_id_1 = $2 AND service_id_2 = $3) OR (service_id_1 = $3 AND service_id_2 = $2))
      `;
      const edgeResults = await client.query(findEdgesSql, [group_cluster_id, service_id_1, service_id_2]);

      for (const edgeRow of edgeResults.rows) {
        // Hypothetical update if service_edge_visualization had confirmed_status
        // const updateEdgeSql = `
        //   UPDATE "${userSchema}".service_edge_visualization 
        //   SET confirmed_status = $1, updated_at = NOW() // Assuming an updated_at column
        //   WHERE id = $2
        // `;
        // await client.query(updateEdgeSql, [statusValue, edgeRow.id]);
      }
      */
      // For now, we just acknowledge the number of groups updated.
      // The client-side logic in `entity-resolution-context.tsx` already optimistically updates
      // the link status in the visualization based on the group review.

      return { 
        message: 'Service group review submitted successfully.',
        updatedGroupId: groupId,
        newStatus: statusValue
        // updatedEdges: edgeResults.rows.length // Would be relevant if edges were updated
      };
    });

    return NextResponse.json(result);
  } catch (error) {
    console.error('Error submitting service group review:', error);
    const errorMessage = error instanceof Error ? error.message : 'Failed to submit review';
    return NextResponse.json(
      { error: errorMessage },
      { status: 500 }
    );
  }
}

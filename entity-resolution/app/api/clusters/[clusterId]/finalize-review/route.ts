// app/api/clusters/[clusterId]/finalize-review/route.ts
import { NextRequest, NextResponse } from 'next/server';
import { getIronSession } from 'iron-session';
import { withTransaction } from '@/types/db'; // query, queryOne not used here
// import { getUserSchemaFromSession } from '@/utils/auth-db'; // DbUser not used here
import { sessionOptions } from '@/lib/session';
import { v4 as uuidv4 } from 'uuid';
import type { ClusterFinalizationStatusResponse } from '@/types/entity-resolution'; // Ensure this and other types are updated
import type { UserSessionData } from '@/app/api/auth/login/route';

// Interface for service groups or entity groups used in graph construction
interface GenericGroupForGraph {
  id: string; // group id
  item_id_1: string; // service_id_1 or entity_id_1
  item_id_2: string; // service_id_2 or entity_id_2
  group_cluster_id: string; // This is the original group_cluster.id
}

export async function POST(
  request: NextRequest,
  { params }: { params: { clusterId: string } }
) {
  const { clusterId: originalClusterId } = await params;

  if (!originalClusterId) {
    return NextResponse.json({
        status: 'ERROR',
        message: 'Cluster ID is required.',
        originalClusterId: ''
    } as ClusterFinalizationStatusResponse, { status: 400 });
  }

  const { searchParams } = new URL(request.url);
  const type = searchParams.get('type') || 'entity'; // Default to 'entity'

  if (type !== 'entity' && type !== 'service') {
    return NextResponse.json({
        status: 'ERROR',
        message: "Invalid type parameter. Must be 'entity' or 'service'.",
        originalClusterId: originalClusterId
    } as ClusterFinalizationStatusResponse, { status: 400 });
  }

  let userSchema: string | null;
  let sessionUserId: string | null;
  let sessionId: string | null;

  try {
    const tempResponse = new NextResponse();
    const session = await getIronSession<UserSessionData>(request, tempResponse, sessionOptions);

    if (!session.isLoggedIn || !session.userSchema || !session.userId || !session.sessionId) {
      return NextResponse.json({
          status: 'ERROR',
          message: 'Unauthorized: User session not found or invalid.',
          originalClusterId: originalClusterId
      } as ClusterFinalizationStatusResponse, { status: 401 });
    }
    userSchema = session.userSchema;
    sessionUserId = session.userId;
    sessionId = session.sessionId;
  } catch (error) {
    console.error('Session error:', error);
    return NextResponse.json({
        status: 'ERROR',
        message: 'Failed to retrieve user session.',
        originalClusterId: originalClusterId
    } as ClusterFinalizationStatusResponse, { status: 500 });
  }

  if (!userSchema || !sessionUserId) {
    return NextResponse.json({
        status: 'ERROR',
        message: 'User schema or session user ID could not be determined.',
        originalClusterId: originalClusterId
    } as ClusterFinalizationStatusResponse, { status: 500 });
  }

  // Table and column names based on type
  const tableNames = {
    cluster: type === 'entity' ? 'entity_group_cluster' : 'service_group_cluster',
    group: type === 'entity' ? 'entity_group' : 'service_group',
    edgeVisualization: type === 'entity' ? 'entity_edge_visualization' : 'service_edge_visualization',
    itemCountCol: type === 'entity' ? 'entity_count' : 'service_count',
    groupCountCol: type === 'entity' ? 'group_count' : 'service_group_count',
    itemId1Col: type === 'entity' ? 'entity_id_1' : 'service_id_1',
    itemId2Col: type === 'entity' ? 'entity_id_2' : 'service_id_2',
    visClusterIdCol: type === 'entity' ? 'cluster_id' : 'service_group_cluster_id', // For edge_visualization table
  };

  try {
    const result = await withTransaction(async (client) => {
      // 1. Check original cluster exists.
      const originalClusterCheckSql = `SELECT id FROM "${userSchema}".${tableNames.cluster} WHERE id = $1;`;
      const originalClusterCheckResult = await client.query(originalClusterCheckSql, [originalClusterId]);
      if (originalClusterCheckResult.rowCount === 0) {
          return { status: 'CLUSTER_NOT_FOUND', message: `${type.charAt(0).toUpperCase() + type.slice(1)} cluster ${originalClusterId} not found.`, originalClusterId: originalClusterId } as ClusterFinalizationStatusResponse;
      }

      // 2. Check if fully reviewed.
      const reviewStatusSql = `
          SELECT
          COUNT(*) AS total_groups,
          SUM(CASE WHEN confirmed_status IN ('CONFIRMED_MATCH', 'CONFIRMED_NON_MATCH', 'DENIED') THEN 1 ELSE 0 END) AS reviewed_groups
          FROM "${userSchema}".${tableNames.group}
          WHERE group_cluster_id = $1;
      `;
      const reviewStatusResult = await client.query(reviewStatusSql, [originalClusterId]);
      const { total_groups, reviewed_groups } = reviewStatusResult.rows[0];

      if (parseInt(reviewed_groups, 10) < parseInt(total_groups, 10)) {
          return {
              status: 'PENDING_FULL_REVIEW',
              message: `${type.charAt(0).toUpperCase() + type.slice(1)} cluster ${originalClusterId} is not fully reviewed. ${reviewed_groups}/${total_groups} groups reviewed.`,
              originalClusterId: originalClusterId
          } as ClusterFinalizationStatusResponse;
      }

      // 3. Fetch confirmed groups.
      const confirmedGroupsSql = `
          SELECT id, ${tableNames.itemId1Col} AS item_id_1, ${tableNames.itemId2Col} AS item_id_2, group_cluster_id
          FROM "${userSchema}".${tableNames.group}
          WHERE group_cluster_id = $1 AND confirmed_status = 'CONFIRMED_MATCH';
      `;
      const confirmedGroupsResult = await client.query(confirmedGroupsSql, [originalClusterId]);
      const confirmedGenericGroups: GenericGroupForGraph[] = confirmedGroupsResult.rows;

      // Handle empty cluster (no confirmed matches after review)
      if (confirmedGenericGroups.length === 0) {
          await client.query(
              `UPDATE "${userSchema}".${tableNames.cluster}
               SET ${tableNames.itemCountCol} = 0, ${tableNames.groupCountCol} = 0, description = COALESCE(description, '') || ' (Reviewed - Empty)', updated_at = CURRENT_TIMESTAMP, was_split = FALSE
               WHERE id = $1;`, // Explicitly set was_split to FALSE
              [originalClusterId]
          );
          return {
              status: 'COMPLETED_NO_SPLIT_NEEDED',
              message: `${type.charAt(0).toUpperCase() + type.slice(1)} cluster ${originalClusterId} has no confirmed groups after review. Marked as empty.`,
              originalClusterId: originalClusterId
          } as ClusterFinalizationStatusResponse;
      }

      // Build graph & Find components using item IDs
      const adj: Record<string, string[]> = {};
      const allItems = new Set<string>();
      confirmedGenericGroups.forEach(group => {
          adj[group.item_id_1] = (adj[group.item_id_1] || []).concat(group.item_id_2);
          adj[group.item_id_2] = (adj[group.item_id_2] || []).concat(group.item_id_1);
          allItems.add(group.item_id_1);
          allItems.add(group.item_id_2);
      });
      const visited = new Set<string>();
      const components: Array<Set<string>> = [];
      allItems.forEach(itemId => {
          if (!visited.has(itemId)) {
              const currentComponent = new Set<string>();
              const queue: string[] = [itemId];
              visited.add(itemId);
              currentComponent.add(itemId);
              while (queue.length > 0) {
                  const u = queue.shift()!;
                  (adj[u] || []).forEach(v => {
                      if (!visited.has(v)) {
                          visited.add(v);
                          currentComponent.add(v);
                          queue.push(v);
                      }
                  });
              }
              components.push(currentComponent);
          }
      });

      // Handle no-split
      if (components.length <= 1) {
          // Even if no split, we update the counts based on the main component of *confirmed* matches
          const mainComponentItems = components.length > 0 ? components[0] : new Set<string>();
          const groupsInMainComponent = confirmedGenericGroups.filter(g => mainComponentItems.has(g.item_id_1) && mainComponentItems.has(g.item_id_2));

          // If the single component is empty (e.g. all confirmed_match groups were isolated pairs that got denied, leaving no graph),
          // this would have been caught by the confirmedGenericGroups.length === 0 check earlier.
          // So, mainComponentItems.size and groupsInMainComponent.length should reflect the content of the finalized cluster.
          await client.query(
              `UPDATE "${userSchema}".${tableNames.cluster}
               SET ${tableNames.itemCountCol} = $1, ${tableNames.groupCountCol} = $2, description = COALESCE(description, '') || ' (Review Finalized)', updated_at = CURRENT_TIMESTAMP, was_split = FALSE
               WHERE id = $3;`, // Explicitly set was_split to FALSE
              [mainComponentItems.size, groupsInMainComponent.length, originalClusterId]
          );
          return {
              status: 'COMPLETED_NO_SPLIT_NEEDED',
              message: `${type.charAt(0).toUpperCase() + type.slice(1)} cluster ${originalClusterId} review finalized. No split occurred.`,
              originalClusterId: originalClusterId
          } as ClusterFinalizationStatusResponse;
      }

      // Handle split
      const newClusterIds: string[] = [];
      for (const component of components) {
          const newClusterId = uuidv4();
          newClusterIds.push(newClusterId);
          const componentItems = Array.from(component);
          const groupsInComponent = confirmedGenericGroups.filter(g => component.has(g.item_id_1) && component.has(g.item_id_2));
          const newClusterName = `User ${type.charAt(0).toUpperCase() + type.slice(1)} Split from ${originalClusterId.substring(0,8)} - ${newClusterId.substring(0,8)}`;
          const newClusterDescription = `User-defined ${type} cluster split from ${originalClusterId} by user ${sessionUserId} on ${new Date().toISOString()}. Contains ${componentItems.length} ${type}s and ${groupsInComponent.length} ${type} groups.`;

          await client.query(
              `INSERT INTO "${userSchema}".${tableNames.cluster} (id, name, description, ${tableNames.itemCountCol}, ${tableNames.groupCountCol}, created_at, updated_at, was_split)
               VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, FALSE);`, // New clusters are not "split" themselves
              [newClusterId, newClusterName, newClusterDescription, componentItems.length, groupsInComponent.length]
          );

          if (groupsInComponent.length > 0) {
              await client.query(
                  `UPDATE "${userSchema}".${tableNames.group} SET group_cluster_id = $1, updated_at = CURRENT_TIMESTAMP WHERE id = ANY($2::text[]);`,
                  [newClusterId, groupsInComponent.map(g => g.id)]
              );
          }

          if (componentItems.length >= 2) {
              const updateVisualizationSql = `
                  UPDATE "${userSchema}".${tableNames.edgeVisualization}
                  SET ${tableNames.visClusterIdCol} = $1
                  ${type === 'entity' ? ', updated_at = CURRENT_TIMESTAMP' : ''} -- service_edge_visualization might not have updated_at
                  WHERE ${tableNames.visClusterIdCol} = $2
                    AND ${tableNames.itemId1Col} = ANY($3::text[])
                    AND ${tableNames.itemId2Col} = ANY($3::text[]);
              `;
              await client.query(updateVisualizationSql, [newClusterId, originalClusterId, componentItems]);
          }
      }

      const originalClusterUpdateDesc = `Split into ${newClusterIds.length} new ${type} clusters by user ${sessionUserId} (Session: ${sessionId}) on ${new Date().toISOString()}. Original cluster had ${total_groups} total groups. Its pre-split counts are retained.`;
      // IMPORTANT CHANGE: Do NOT zero out counts for the original cluster. Set was_split = TRUE.
      await client.query(
          `UPDATE "${userSchema}".${tableNames.cluster}
           SET description = $1, was_split = TRUE, updated_at = CURRENT_TIMESTAMP
           WHERE id = $2;`,
          [originalClusterUpdateDesc, originalClusterId]
      );

      // Set group_cluster_id to NULL for DENIED/CONFIRMED_NON_MATCH groups from the original cluster
      await client.query(
          `UPDATE "${userSchema}".${tableNames.group} SET group_cluster_id = NULL, updated_at = CURRENT_TIMESTAMP WHERE group_cluster_id = $1 AND confirmed_status IN ('DENIED', 'CONFIRMED_NON_MATCH');`,
          [originalClusterId]
      );

      return {
          status: 'COMPLETED_SPLIT_OCCURRED',
          message: `${type.charAt(0).toUpperCase() + type.slice(1)} cluster ${originalClusterId} was split into ${components.length} new clusters. Original cluster record updated with was_split=true.`,
          newClusterIds,
          originalClusterId: originalClusterId
      } as ClusterFinalizationStatusResponse;
    });
    return NextResponse.json(result, { status: 200 });

  } catch (error: any) {
    console.error(`Error finalizing review for ${type} cluster ${originalClusterId}:`, error);
    return NextResponse.json({
        status: 'ERROR',
        message: `Failed to finalize ${type} review: ${error.message}`,
        originalClusterId: originalClusterId
    } as ClusterFinalizationStatusResponse, { status: 500 });
  }
}

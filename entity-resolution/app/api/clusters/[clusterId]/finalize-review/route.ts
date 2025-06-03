// app/api/clusters/[clusterId]/finalize-review/route.ts
import { NextRequest, NextResponse } from 'next/server';
import { getIronSession } from 'iron-session';
import { withTransaction } from '@/types/db'; // Assuming this is your DB helper
import { sessionOptions } from '@/lib/session'; // Assuming this is your session config
// import { v4 as uuidv4 } from 'uuid'; // No longer needed for new cluster IDs
import type { ClusterFinalizationStatusResponse } from '@/types/entity-resolution';
import type { UserSessionData } from '@/app/api/auth/login/route'; // Assuming this is your user session type

// Interface for service groups or entity groups used in graph construction
interface GenericGroupForGraph {
  id: string; // group id
  item_id_1: string; // service_id_1 or entity_id_1
  item_id_2: string; // service_id_2 or entity_id_2
  // group_cluster_id: string; // This is the original group_cluster.id - not strictly needed for component calculation
}

export async function POST(
  request: NextRequest,
  { params }: { params: { clusterId: string } }
) {
  const { clusterId: originalClusterId } = await params; // Renamed for clarity

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
    const tempResponse = new NextResponse(); // Required for iron-session in Route Handlers
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
    sessionId = session.sessionId; // Keep for logging/description
  } catch (error) {
    console.error('Session error:', error);
    return NextResponse.json({
        status: 'ERROR',
        message: 'Failed to retrieve user session.',
        originalClusterId: originalClusterId
    } as ClusterFinalizationStatusResponse, { status: 500 });
  }

  // Ensure userSchema and sessionUserId are available after session check
  if (!userSchema || !sessionUserId) {
    // This case should ideally be caught by the session check above,
    // but as a safeguard:
    return NextResponse.json({
        status: 'ERROR',
        message: 'User schema or session user ID could not be determined from session.',
        originalClusterId: originalClusterId
    } as ClusterFinalizationStatusResponse, { status: 500 });
  }


  // Table and column names based on type
  const tableNames = {
    cluster: type === 'entity' ? 'entity_group_cluster' : 'service_group_cluster',
    group: type === 'entity' ? 'entity_group' : 'service_group',
    // edgeVisualization: type === 'entity' ? 'entity_edge_visualization' : 'service_edge_visualization', // Not directly updated anymore
    itemCountCol: type === 'entity' ? 'entity_count' : 'service_count', // Used for updates
    groupCountCol: type === 'entity' ? 'group_count' : 'service_group_count', // Used for updates
    itemId1Col: type === 'entity' ? 'entity_id_1' : 'service_id_1',
    itemId2Col: type === 'entity' ? 'entity_id_2' : 'service_id_2',
    // visClusterIdCol: type === 'entity' ? 'cluster_id' : 'service_group_cluster_id', // Not directly updated anymore
  };

  try {
    const result = await withTransaction(async (client) => {
      // 1. Check original cluster exists.
      const originalClusterCheckSql = `SELECT id, ${tableNames.itemCountCol} AS original_item_count, ${tableNames.groupCountCol} AS original_group_count FROM "${userSchema}".${tableNames.cluster} WHERE id = $1;`;
      const originalClusterCheckResult = await client.query(originalClusterCheckSql, [originalClusterId]);
      if (originalClusterCheckResult.rowCount === 0) {
          return { status: 'CLUSTER_NOT_FOUND', message: `${type.charAt(0).toUpperCase() + type.slice(1)} cluster ${originalClusterId} not found.`, originalClusterId: originalClusterId } as ClusterFinalizationStatusResponse;
      }
      // const originalCounts = originalClusterCheckResult.rows[0]; // We might not need to explicitly use these if we don't zero them out

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

      // 3. Fetch confirmed groups to determine connectivity.
      const confirmedGroupsSql = `
          SELECT id, ${tableNames.itemId1Col} AS item_id_1, ${tableNames.itemId2Col} AS item_id_2
          FROM "${userSchema}".${tableNames.group}
          WHERE group_cluster_id = $1 AND confirmed_status = 'CONFIRMED_MATCH';
      `;
      const confirmedGroupsResult = await client.query(confirmedGroupsSql, [originalClusterId]);
      const confirmedGenericGroups: GenericGroupForGraph[] = confirmedGroupsResult.rows;

      if (confirmedGenericGroups.length === 0) {
          const descriptionSuffix = ` (Review Finalized - No Confirmed Matches)`;
          // Update description, ensure was_split is FALSE.
          // Item and group counts on the original cluster remain as they were,
          // as per the principle of not altering the original cluster structure visually.
          // The confirmed_status of individual groups already reflects their non-match status.
          await client.query(
              `UPDATE "${userSchema}".${tableNames.cluster}
               SET description = COALESCE(description, '') || $1, updated_at = CURRENT_TIMESTAMP, was_split = FALSE
               WHERE id = $2;`,
              [descriptionSuffix, originalClusterId]
          );
          // CRITICAL CHANGE: Do NOT nullify group_cluster_id for DENIED/CONFIRMED_NON_MATCH groups here.
          // Their confirmed_status already indicates they are not matches.
          // Keeping their group_cluster_id ensures they remain part of the original cluster's dataset
          // for visualization, allowing them to be styled as non-matches rather than disappearing.
          return {
              status: 'COMPLETED_NO_CONFIRMED_MATCHES', // Changed from COMPLETED_NO_SPLIT_NEEDED to be more specific
              message: `${type.charAt(0).toUpperCase() + type.slice(1)} cluster ${originalClusterId} has no confirmed groups after review. Marked as reviewed. Original structure preserved.`,
              originalClusterId: originalClusterId
          } as ClusterFinalizationStatusResponse;
      }

      // 4. Build graph & Find components using item IDs from 'CONFIRMED_MATCH' groups.
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
                  const u = queue.shift()!; // Bang operator is fine since queue.length > 0
                  (adj[u] || []).forEach(v => {
                      if (!visited.has(v)) {
                          visited.add(v);
                          currentComponent.add(v);
                          queue.push(v);
                      }
                  });
              }
              if (currentComponent.size > 0) { // Only add non-empty components
                components.push(currentComponent);
              }
          }
      });

      const wouldSplit = components.length > 1;
      let finalStatus: ClusterFinalizationStatusResponse['status'];
      let finalMessage: string;

      if (!wouldSplit) {
          // No split needed based on confirmed matches.
          // Update counts based on the single component of confirmed matches.
          const mainComponentItems = components.length > 0 ? components[0] : new Set<String>();
          const groupsInMainComponent = confirmedGenericGroups.filter(g =>
            mainComponentItems.has(g.item_id_1) && mainComponentItems.has(g.item_id_2)
          );

          const descriptionSuffix = ` (Review Finalized - No Split)`;
          await client.query(
              `UPDATE "${userSchema}".${tableNames.cluster}
               SET ${tableNames.itemCountCol} = $1, ${tableNames.groupCountCol} = $2, description = COALESCE(description, '') || $3, updated_at = CURRENT_TIMESTAMP, was_split = FALSE
               WHERE id = $4;`,
              [mainComponentItems.size, groupsInMainComponent.length, descriptionSuffix, originalClusterId]
          );
          finalStatus = 'COMPLETED_NO_SPLIT_NEEDED';
          finalMessage = `${type.charAt(0).toUpperCase() + type.slice(1)} cluster ${originalClusterId} review finalized. No split occurred. Counts updated based on confirmed matches. Original structure preserved.`;
      } else {
          // Split would have occurred based on confirmed matches. Mark was_split = TRUE.
          // Do NOT create new clusters. Do NOT reassign groups or edges.
          // Do NOT zero out counts on the original cluster.
          const descriptionSuffix = ` (Review Finalized - Marked as Split; Contained ${components.length} sub-components based on confirmed matches)`;
          await client.query(
              `UPDATE "${userSchema}".${tableNames.cluster}
               SET description = COALESCE(description, '') || $1, was_split = TRUE, updated_at = CURRENT_TIMESTAMP
               WHERE id = $2;`,
              [descriptionSuffix, originalClusterId]
          );
          finalStatus = 'COMPLETED_MARKED_AS_SPLIT'; // Changed status
          finalMessage = `${type.charAt(0).toUpperCase() + type.slice(1)} cluster ${originalClusterId} review finalized. A split into ${components.length} components (based on confirmed matches) was detected. Original cluster marked 'was_split = TRUE'. No new clusters created, original structure preserved.`;
      }

      return {
          status: finalStatus,
          message: finalMessage,
          originalClusterId: originalClusterId,
          newClusterIds: undefined, // Explicitly undefined as we are not creating new clusters
      } as ClusterFinalizationStatusResponse;
    });

    return NextResponse.json(result, { status: 200 });

  } catch (error: any) {
    console.error(`Error finalizing review for ${type} cluster ${originalClusterId}:`, error);
    return NextResponse.json({
        status: 'ERROR',
        message: `Failed to finalize ${type} review: ${error.message || 'Unknown server error'}`,
        originalClusterId: originalClusterId
    } as ClusterFinalizationStatusResponse, { status: 500 });
  }
}

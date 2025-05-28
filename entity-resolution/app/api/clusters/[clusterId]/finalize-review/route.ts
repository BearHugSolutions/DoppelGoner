// app/api/clusters/[clusterId]/finalize-review/route.ts

import { NextRequest, NextResponse } from 'next/server';
import { getIronSession, SessionOptions } from 'iron-session';
import { query, queryOne, withTransaction } from '@/types/db';
import { getUserSchemaFromSession, DbUser } from '@/utils/auth-db';
import { sessionOptions } from '@/lib/session';
import { v4 as uuidv4 } from 'uuid';
import type { ClusterFinalizationStatusResponse } from '@/types/entity-resolution';
import type { UserSessionData } from '@/app/api/auth/login/route'; // Import from login route

interface EntityGroupForGraph {
  id: string;
  entity_id_1: string;
  entity_id_2: string;
  group_cluster_id: string;
}

interface EntityGroupClusterRecord {
    id: string;
    name: string | null;
    description: string | null;
    entity_count: number;
    group_count: number;
    average_coherence_score: number | null;
    created_at: Date | null;
    updated_at: Date | null;
}

export async function POST(
  request: NextRequest,
  { params }: { params: { clusterId: string } }
) {
  const originalClusterId = await params.clusterId;

  if (!originalClusterId) {
    return NextResponse.json({ 
        status: 'ERROR', 
        message: 'Cluster ID is required.',
        originalClusterId: '' 
    } as ClusterFinalizationStatusResponse, { status: 400 });
  }

  let userSchema: string | null;
  let sessionUserId: string | null;
  let sessionId: string | null;

  try {
    const tempResponse = new NextResponse();
    const session = await getIronSession<UserSessionData>(request, tempResponse, sessionOptions);

    // Updated check for new session structure
    if (!session.isLoggedIn || !session.userSchema || !session.userId || !session.sessionId) {
      return NextResponse.json({ 
          status: 'ERROR', 
          message: 'Unauthorized: User session not found or invalid.',
          originalClusterId: originalClusterId 
      } as ClusterFinalizationStatusResponse, { status: 401 });
    }
    userSchema = session.userSchema;
    sessionUserId = session.userId; // Use session.userId
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
    return NextResponse.json({ status: 'ERROR', message: 'User schema or session user ID could not be determined.', originalClusterId: originalClusterId } as ClusterFinalizationStatusResponse, { status: 500 });
  }

  try {
    const result = await withTransaction(async (client) => {
        // --- Full Transaction Logic (updated to match actual schema) ---
        // 1. Check original cluster exists.
        const originalClusterCheckSql = `SELECT id FROM "${userSchema}".entity_group_cluster WHERE id = $1;`;
        const originalClusterCheckResult = await client.query(originalClusterCheckSql, [originalClusterId]);
        if (originalClusterCheckResult.rowCount === 0) {
            return { status: 'CLUSTER_NOT_FOUND', message: `Cluster ${originalClusterId} not found.`, originalClusterId: originalClusterId } as ClusterFinalizationStatusResponse;
        }

        // 2. Check if fully reviewed.
        const reviewStatusSql = `
            SELECT 
            COUNT(*) AS total_groups,
            SUM(CASE WHEN confirmed_status IN ('CONFIRMED', 'DENIED') THEN 1 ELSE 0 END) AS reviewed_groups
            FROM "${userSchema}".entity_group
            WHERE group_cluster_id = $1;
        `;
        const reviewStatusResult = await client.query(reviewStatusSql, [originalClusterId]);
        const { total_groups, reviewed_groups } = reviewStatusResult.rows[0];

        if (parseInt(reviewed_groups, 10) < parseInt(total_groups, 10)) {
            return { 
                status: 'PENDING_FULL_REVIEW', 
                message: `Cluster ${originalClusterId} is not fully reviewed. ${reviewed_groups}/${total_groups} groups reviewed.`,
                originalClusterId: originalClusterId
            } as ClusterFinalizationStatusResponse;
        }

        // 3. Fetch confirmed groups.
        const confirmedGroupsSql = `
            SELECT id, entity_id_1, entity_id_2, group_cluster_id
            FROM "${userSchema}".entity_group
            WHERE group_cluster_id = $1 AND confirmed_status = 'CONFIRMED';
        `;
        const confirmedGroupsResult = await client.query(confirmedGroupsSql, [originalClusterId]);
        const confirmedGroups: EntityGroupForGraph[] = confirmedGroupsResult.rows;

        // Handle empty cluster
        if (confirmedGroups.length === 0) {
            await client.query(
                `UPDATE "${userSchema}".entity_group_cluster 
                 SET entity_count = 0, group_count = 0, description = COALESCE(description, '') || ' (Reviewed - Empty)', updated_at = CURRENT_TIMESTAMP
                 WHERE id = $1;`,
                [originalClusterId]
            );
            return { 
                status: 'COMPLETED_NO_SPLIT_NEEDED', 
                message: `Cluster ${originalClusterId} has no confirmed groups after review. Marked as empty.`,
                originalClusterId: originalClusterId
            } as ClusterFinalizationStatusResponse;
        }


        // Build graph & Find components
        const adj: Record<string, string[]> = {};
        const allEntities = new Set<string>();
        confirmedGroups.forEach(group => { adj[group.entity_id_1] = (adj[group.entity_id_1] || []).concat(group.entity_id_2); adj[group.entity_id_2] = (adj[group.entity_id_2] || []).concat(group.entity_id_1); allEntities.add(group.entity_id_1); allEntities.add(group.entity_id_2); });
        const visited = new Set<string>();
        const components: Array<Set<string>> = [];
        allEntities.forEach(entityId => { if (!visited.has(entityId)) { const currentComponent = new Set<string>(); const queue: string[] = [entityId]; visited.add(entityId); currentComponent.add(entityId); while (queue.length > 0) { const u = queue.shift()!; (adj[u] || []).forEach(v => { if (!visited.has(v)) { visited.add(v); currentComponent.add(v); queue.push(v); } }); } components.push(currentComponent); } });


        // Handle no-split
        if (components.length <= 1) {
            const mainComponentEntities = components.length > 0 ? components[0] : new Set<string>();
            const groupsInMainComponent = confirmedGroups.filter(g => mainComponentEntities.has(g.entity_id_1) && mainComponentEntities.has(g.entity_id_2));
            await client.query(
                `UPDATE "${userSchema}".entity_group_cluster 
                 SET entity_count = $1, group_count = $2, description = COALESCE(description, '') || ' (Review Finalized)', updated_at = CURRENT_TIMESTAMP
                 WHERE id = $3;`,
                [mainComponentEntities.size, groupsInMainComponent.length, originalClusterId]
            );
            return { 
                status: 'COMPLETED_NO_SPLIT_NEEDED', 
                message: `Cluster ${originalClusterId} review finalized. No split occurred.`,
                originalClusterId: originalClusterId
            } as ClusterFinalizationStatusResponse;
        }

        // Handle split
        const newClusterIds: string[] = [];
        for (const component of components) {
            const newClusterId = uuidv4();
            newClusterIds.push(newClusterId);
            const componentEntities = Array.from(component);
            const groupsInComponent = confirmedGroups.filter(g => component.has(g.entity_id_1) && component.has(g.entity_id_2));
            const newClusterName = `User Split from ${originalClusterId.substring(0,8)} - ${newClusterId.substring(0,8)}`;
            const newClusterDescription = `User-defined cluster split from ${originalClusterId} by user ${sessionUserId} on ${new Date().toISOString()}. Contains ${componentEntities.length} entities and ${groupsInComponent.length} groups.`;
            
            // Insert new cluster (removed source_cluster_id and is_user_modified as they don't exist in the schema)
            await client.query(
                `INSERT INTO "${userSchema}".entity_group_cluster (id, name, description, entity_count, group_count, created_at, updated_at) 
                 VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);`,
                [newClusterId, newClusterName, newClusterDescription, componentEntities.length, groupsInComponent.length]
            );
            
            if (groupsInComponent.length > 0) {
                await client.query(`UPDATE "${userSchema}".entity_group SET group_cluster_id = $1, updated_at = CURRENT_TIMESTAMP WHERE id = ANY($2::text[]);`, [newClusterId, groupsInComponent.map(g => g.id)]);
            }
            
            // Update visualization edges if they exist
            if (componentEntities.length >= 2) {
                const updateVisualizationSql = `
                    UPDATE "${userSchema}".entity_edge_visualization 
                    SET cluster_id = $1, updated_at = CURRENT_TIMESTAMP 
                    WHERE cluster_id = $2 AND entity_id_1 = ANY($3::text[]) AND entity_id_2 = ANY($3::text[]);
                `;
                await client.query(updateVisualizationSql, [newClusterId, originalClusterId, componentEntities]);
            }
        }
        
        const originalClusterUpdateDesc = `Split into ${newClusterIds.length} new clusters by user ${sessionUserId} (Session: ${sessionId}) on ${new Date().toISOString()}. Original had ${total_groups} groups.`;
        await client.query(`UPDATE "${userSchema}".entity_group_cluster SET description = $1, entity_count = 0, group_count = 0, updated_at = CURRENT_TIMESTAMP WHERE id = $2;`, [originalClusterUpdateDesc, originalClusterId]);
        await client.query(`UPDATE "${userSchema}".entity_group SET group_cluster_id = NULL, updated_at = CURRENT_TIMESTAMP WHERE group_cluster_id = $1 AND confirmed_status = 'DENIED';`, [originalClusterId]);

        return { 
            status: 'COMPLETED_SPLIT_OCCURRED', 
            message: `Cluster ${originalClusterId} was split into ${components.length} new clusters.`,
            newClusterIds,
            originalClusterId: originalClusterId
        } as ClusterFinalizationStatusResponse;

    });

    return NextResponse.json(result, { status: 200 });

  } catch (error: any) {
    console.error(`Error finalizing review for cluster ${originalClusterId}:`, error);
    return NextResponse.json({ 
        status: 'ERROR', 
        message: `Failed to finalize review: ${error.message}`,
        originalClusterId: originalClusterId
    } as ClusterFinalizationStatusResponse, { status: 500 });
  }
}
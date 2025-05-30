// app/api/clusters/[clusterId]/finalize-review/route.ts
import { NextRequest, NextResponse } from 'next/server';
import { getIronSession } from 'iron-session';
import { query, withTransaction } from '@/types/db'; // queryOne not used here
import { getUserSchemaFromSession } from '@/utils/auth-db'; // DbUser not used here
import { sessionOptions } from '@/lib/session';
import { v4 as uuidv4 } from 'uuid';
import type { ClusterFinalizationStatusResponse } from '@/types/entity-resolution';
import type { UserSessionData } from '@/app/api/auth/login/route';

// Interface for service groups used in graph construction
interface ServiceGroupForGraph {
  id: string;
  service_id_1: string;
  service_id_2: string;
  group_cluster_id: string; // This is the service_group_cluster.id
}

export async function POST(
  request: NextRequest,
  { params }: { params: { clusterId: string } }
) {
  const { clusterId: originalClusterId } = params; // Correctly get clusterId from params

  if (!originalClusterId) {
    return NextResponse.json({ 
        status: 'ERROR', 
        message: 'Cluster ID is required.',
        originalClusterId: '' 
    } as ClusterFinalizationStatusResponse, { status: 400 });
  }

  const { searchParams } = new URL(request.url);
  const type = searchParams.get('type') || 'entity'; // Default to 'entity'

  let userSchema: string | null;
  let sessionUserId: string | null;
  let sessionId: string | null;

  try {
    const tempResponse = new NextResponse(); // Required for iron-session to work in Route Handlers
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

  if (!userSchema || !sessionUserId) { // Should be caught by above, but as a safeguard
    return NextResponse.json({ 
        status: 'ERROR', 
        message: 'User schema or session user ID could not be determined.', 
        originalClusterId: originalClusterId 
    } as ClusterFinalizationStatusResponse, { status: 500 });
  }

  try {
    if (type === 'service') {
      // --- Service Cluster Finalization Logic ---
      const result = await withTransaction(async (client) => {
        // 1. Check original service cluster exists.
        const originalClusterCheckSql = `SELECT id FROM "${userSchema}".service_group_cluster WHERE id = $1;`;
        const originalClusterCheckResult = await client.query(originalClusterCheckSql, [originalClusterId]);
        if (originalClusterCheckResult.rowCount === 0) {
            return { status: 'CLUSTER_NOT_FOUND', message: `Service cluster ${originalClusterId} not found.`, originalClusterId: originalClusterId } as ClusterFinalizationStatusResponse;
        }

        // 2. Check if fully reviewed (all service groups in this cluster).
        const reviewStatusSql = `
            SELECT 
            COUNT(*) AS total_groups,
            SUM(CASE WHEN confirmed_status IN ('CONFIRMED_MATCH', 'CONFIRMED_NON_MATCH', 'DENIED') THEN 1 ELSE 0 END) AS reviewed_groups
            FROM "${userSchema}".service_group
            WHERE group_cluster_id = $1; 
        `; // Using 'CONFIRMED_MATCH', 'CONFIRMED_NON_MATCH', 'DENIED' as reviewed statuses
        const reviewStatusResult = await client.query(reviewStatusSql, [originalClusterId]);
        const { total_groups, reviewed_groups } = reviewStatusResult.rows[0];

        if (parseInt(reviewed_groups, 10) < parseInt(total_groups, 10)) {
            return { 
                status: 'PENDING_FULL_REVIEW', 
                message: `Service cluster ${originalClusterId} is not fully reviewed. ${reviewed_groups}/${total_groups} groups reviewed.`,
                originalClusterId: originalClusterId
            } as ClusterFinalizationStatusResponse;
        }

        // 3. Fetch confirmed service groups.
        const confirmedGroupsSql = `
            SELECT id, service_id_1, service_id_2, group_cluster_id
            FROM "${userSchema}".service_group
            WHERE group_cluster_id = $1 AND confirmed_status = 'CONFIRMED_MATCH';
        `;
        const confirmedGroupsResult = await client.query(confirmedGroupsSql, [originalClusterId]);
        const confirmedServiceGroups: ServiceGroupForGraph[] = confirmedGroupsResult.rows;

        // Handle empty cluster (no confirmed matches)
        if (confirmedServiceGroups.length === 0) {
            await client.query(
                `UPDATE "${userSchema}".service_group_cluster 
                 SET service_count = 0, service_group_count = 0, description = COALESCE(description, '') || ' (Reviewed - Empty)', updated_at = CURRENT_TIMESTAMP
                 WHERE id = $1;`,
                [originalClusterId]
            );
            return { 
                status: 'COMPLETED_NO_SPLIT_NEEDED', 
                message: `Service cluster ${originalClusterId} has no confirmed service groups after review. Marked as empty.`,
                originalClusterId: originalClusterId
            } as ClusterFinalizationStatusResponse;
        }

        // Build graph & Find components using service IDs
        const adj: Record<string, string[]> = {};
        const allServices = new Set<string>();
        confirmedServiceGroups.forEach(group => { 
            adj[group.service_id_1] = (adj[group.service_id_1] || []).concat(group.service_id_2); 
            adj[group.service_id_2] = (adj[group.service_id_2] || []).concat(group.service_id_1); 
            allServices.add(group.service_id_1); 
            allServices.add(group.service_id_2); 
        });
        const visited = new Set<string>();
        const components: Array<Set<string>> = [];
        allServices.forEach(serviceId => { 
            if (!visited.has(serviceId)) { 
                const currentComponent = new Set<string>(); 
                const queue: string[] = [serviceId]; 
                visited.add(serviceId); 
                currentComponent.add(serviceId); 
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
            const mainComponentServices = components.length > 0 ? components[0] : new Set<string>();
            const groupsInMainComponent = confirmedServiceGroups.filter(g => mainComponentServices.has(g.service_id_1) && mainComponentServices.has(g.service_id_2));
            await client.query(
                `UPDATE "${userSchema}".service_group_cluster 
                 SET service_count = $1, service_group_count = $2, description = COALESCE(description, '') || ' (Review Finalized)', updated_at = CURRENT_TIMESTAMP
                 WHERE id = $3;`,
                [mainComponentServices.size, groupsInMainComponent.length, originalClusterId]
            );
            return { 
                status: 'COMPLETED_NO_SPLIT_NEEDED', 
                message: `Service cluster ${originalClusterId} review finalized. No split occurred.`,
                originalClusterId: originalClusterId
            } as ClusterFinalizationStatusResponse;
        }

        // Handle split
        const newClusterIds: string[] = [];
        for (const component of components) {
            const newClusterId = uuidv4();
            newClusterIds.push(newClusterId);
            const componentServices = Array.from(component);
            const groupsInComponent = confirmedServiceGroups.filter(g => component.has(g.service_id_1) && component.has(g.service_id_2));
            const newClusterName = `User Service Split from ${originalClusterId.substring(0,8)} - ${newClusterId.substring(0,8)}`;
            const newClusterDescription = `User-defined service cluster split from ${originalClusterId} by user ${sessionUserId} on ${new Date().toISOString()}. Contains ${componentServices.length} services and ${groupsInComponent.length} service groups.`;
            
            await client.query(
                `INSERT INTO "${userSchema}".service_group_cluster (id, name, description, service_count, service_group_count, created_at, updated_at) 
                 VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);`,
                [newClusterId, newClusterName, newClusterDescription, componentServices.length, groupsInComponent.length]
            );
            
            if (groupsInComponent.length > 0) {
                await client.query(
                    `UPDATE "${userSchema}".service_group SET group_cluster_id = $1, updated_at = CURRENT_TIMESTAMP WHERE id = ANY($2::text[]);`, 
                    [newClusterId, groupsInComponent.map(g => g.id)]
                );
            }
            
            // Update service_edge_visualization table
            if (componentServices.length >= 2) { // Edges exist only if there are at least 2 services
                // Find all service_edge_visualization records that were part of the original cluster
                // and whose services are now entirely within this new component.
                const updateVisualizationSql = `
                    UPDATE "${userSchema}".service_edge_visualization 
                    SET service_group_cluster_id = $1 -- No updated_at in schema for service_edge_visualization
                    WHERE service_group_cluster_id = $2 
                      AND service_id_1 = ANY($3::text[]) 
                      AND service_id_2 = ANY($3::text[]);
                `;
                await client.query(updateVisualizationSql, [newClusterId, originalClusterId, componentServices]);
            }
        }
        
        const originalClusterUpdateDesc = `Split into ${newClusterIds.length} new service clusters by user ${sessionUserId} (Session: ${sessionId}) on ${new Date().toISOString()}. Original had ${total_groups} service groups.`;
        await client.query(
            `UPDATE "${userSchema}".service_group_cluster SET description = $1, service_count = 0, service_group_count = 0, updated_at = CURRENT_TIMESTAMP WHERE id = $2;`, 
            [originalClusterUpdateDesc, originalClusterId]
        );
        // Set group_cluster_id to NULL for DENIED/CONFIRMED_NON_MATCH groups from the original cluster
        await client.query(
            `UPDATE "${userSchema}".service_group SET group_cluster_id = NULL, updated_at = CURRENT_TIMESTAMP WHERE group_cluster_id = $1 AND confirmed_status IN ('DENIED', 'CONFIRMED_NON_MATCH');`, 
            [originalClusterId]
        );

        return { 
            status: 'COMPLETED_SPLIT_OCCURRED', 
            message: `Service cluster ${originalClusterId} was split into ${components.length} new clusters.`,
            newClusterIds,
            originalClusterId: originalClusterId
        } as ClusterFinalizationStatusResponse;
      });
      return NextResponse.json(result, { status: 200 });

    } else {
      // --- Entity Cluster Finalization Logic (Existing) ---
      const result = await withTransaction(async (client) => {
        const originalClusterCheckSql = `SELECT id FROM "${userSchema}".entity_group_cluster WHERE id = $1;`;
        const originalClusterCheckResult = await client.query(originalClusterCheckSql, [originalClusterId]);
        if (originalClusterCheckResult.rowCount === 0) {
            return { status: 'CLUSTER_NOT_FOUND', message: `Entity cluster ${originalClusterId} not found.`, originalClusterId: originalClusterId } as ClusterFinalizationStatusResponse;
        }

        const reviewStatusSql = `
            SELECT 
            COUNT(*) AS total_groups,
            SUM(CASE WHEN confirmed_status IN ('CONFIRMED_MATCH', 'CONFIRMED_NON_MATCH', 'DENIED') THEN 1 ELSE 0 END) AS reviewed_groups
            FROM "${userSchema}".entity_group
            WHERE group_cluster_id = $1;
        `;
        const reviewStatusResult = await client.query(reviewStatusSql, [originalClusterId]);
        const { total_groups, reviewed_groups } = reviewStatusResult.rows[0];

        if (parseInt(reviewed_groups, 10) < parseInt(total_groups, 10)) {
            return { 
                status: 'PENDING_FULL_REVIEW', 
                message: `Entity cluster ${originalClusterId} is not fully reviewed. ${reviewed_groups}/${total_groups} groups reviewed.`,
                originalClusterId: originalClusterId
            } as ClusterFinalizationStatusResponse;
        }

        const confirmedGroupsSql = `
            SELECT id, entity_id_1, entity_id_2, group_cluster_id
            FROM "${userSchema}".entity_group
            WHERE group_cluster_id = $1 AND confirmed_status = 'CONFIRMED_MATCH';
        `;
        const confirmedGroupsResult = await client.query(confirmedGroupsSql, [originalClusterId]);
        const confirmedEntityGroups: ServiceGroupForGraph[] = confirmedGroupsResult.rows; // Re-using interface name, but it's for entities here

        if (confirmedEntityGroups.length === 0) {
            await client.query(
                `UPDATE "${userSchema}".entity_group_cluster 
                 SET entity_count = 0, group_count = 0, description = COALESCE(description, '') || ' (Reviewed - Empty)', updated_at = CURRENT_TIMESTAMP
                 WHERE id = $1;`,
                [originalClusterId]
            );
            return { 
                status: 'COMPLETED_NO_SPLIT_NEEDED', 
                message: `Entity cluster ${originalClusterId} has no confirmed entity groups after review. Marked as empty.`,
                originalClusterId: originalClusterId
            } as ClusterFinalizationStatusResponse;
        }

        const adj: Record<string, string[]> = {};
        const allEntities = new Set<string>();
        confirmedEntityGroups.forEach(group => { 
            adj[group.service_id_1/*entity_id_1*/] = (adj[group.service_id_1/*entity_id_1*/] || []).concat(group.service_id_2/*entity_id_2*/); 
            adj[group.service_id_2/*entity_id_2*/] = (adj[group.service_id_2/*entity_id_2*/] || []).concat(group.service_id_1/*entity_id_1*/); 
            allEntities.add(group.service_id_1/*entity_id_1*/); 
            allEntities.add(group.service_id_2/*entity_id_2*/); 
        });
        const visited = new Set<string>();
        const components: Array<Set<string>> = [];
        allEntities.forEach(entityId => { 
            if (!visited.has(entityId)) { 
                const currentComponent = new Set<string>(); 
                const queue: string[] = [entityId]; 
                visited.add(entityId); 
                currentComponent.add(entityId); 
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

        if (components.length <= 1) {
            const mainComponentEntities = components.length > 0 ? components[0] : new Set<string>();
            const groupsInMainComponent = confirmedEntityGroups.filter(g => mainComponentEntities.has(g.service_id_1/*entity_id_1*/) && mainComponentEntities.has(g.service_id_2/*entity_id_2*/));
            await client.query(
                `UPDATE "${userSchema}".entity_group_cluster 
                 SET entity_count = $1, group_count = $2, description = COALESCE(description, '') || ' (Review Finalized)', updated_at = CURRENT_TIMESTAMP
                 WHERE id = $3;`,
                [mainComponentEntities.size, groupsInMainComponent.length, originalClusterId]
            );
            return { 
                status: 'COMPLETED_NO_SPLIT_NEEDED', 
                message: `Entity cluster ${originalClusterId} review finalized. No split occurred.`,
                originalClusterId: originalClusterId
            } as ClusterFinalizationStatusResponse;
        }

        const newClusterIds: string[] = [];
        for (const component of components) {
            const newClusterId = uuidv4();
            newClusterIds.push(newClusterId);
            const componentEntities = Array.from(component);
            const groupsInComponent = confirmedEntityGroups.filter(g => component.has(g.service_id_1/*entity_id_1*/) && component.has(g.service_id_2/*entity_id_2*/));
            const newClusterName = `User Entity Split from ${originalClusterId.substring(0,8)} - ${newClusterId.substring(0,8)}`;
            const newClusterDescription = `User-defined entity cluster split from ${originalClusterId} by user ${sessionUserId} on ${new Date().toISOString()}. Contains ${componentEntities.length} entities and ${groupsInComponent.length} entity groups.`;
            
            await client.query(
                `INSERT INTO "${userSchema}".entity_group_cluster (id, name, description, entity_count, group_count, created_at, updated_at) 
                 VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);`,
                [newClusterId, newClusterName, newClusterDescription, componentEntities.length, groupsInComponent.length]
            );
            
            if (groupsInComponent.length > 0) {
                await client.query(
                    `UPDATE "${userSchema}".entity_group SET group_cluster_id = $1, updated_at = CURRENT_TIMESTAMP WHERE id = ANY($2::text[]);`, 
                    [newClusterId, groupsInComponent.map(g => g.id)]
                );
            }
            
            if (componentEntities.length >= 2) {
                const updateVisualizationSql = `
                    UPDATE "${userSchema}".entity_edge_visualization 
                    SET cluster_id = $1, updated_at = CURRENT_TIMESTAMP 
                    WHERE cluster_id = $2 AND entity_id_1 = ANY($3::text[]) AND entity_id_2 = ANY($3::text[]);
                `; // Assuming entity_edge_visualization has updated_at
                await client.query(updateVisualizationSql, [newClusterId, originalClusterId, componentEntities]);
            }
        }
        
        const originalClusterUpdateDesc = `Split into ${newClusterIds.length} new entity clusters by user ${sessionUserId} (Session: ${sessionId}) on ${new Date().toISOString()}. Original had ${total_groups} entity groups.`;
        await client.query(
            `UPDATE "${userSchema}".entity_group_cluster SET description = $1, entity_count = 0, group_count = 0, updated_at = CURRENT_TIMESTAMP WHERE id = $2;`, 
            [originalClusterUpdateDesc, originalClusterId]
        );
        await client.query(
            `UPDATE "${userSchema}".entity_group SET group_cluster_id = NULL, updated_at = CURRENT_TIMESTAMP WHERE group_cluster_id = $1 AND confirmed_status IN ('DENIED', 'CONFIRMED_NON_MATCH');`, 
            [originalClusterId]
        );

        return { 
            status: 'COMPLETED_SPLIT_OCCURRED', 
            message: `Entity cluster ${originalClusterId} was split into ${components.length} new clusters.`,
            newClusterIds,
            originalClusterId: originalClusterId
        } as ClusterFinalizationStatusResponse;
      });
      return NextResponse.json(result, { status: 200 });
    }
  } catch (error: any) {
    console.error(`Error finalizing review for ${type} cluster ${originalClusterId}:`, error);
    return NextResponse.json({ 
        status: 'ERROR', 
        message: `Failed to finalize ${type} review: ${error.message}`,
        originalClusterId: originalClusterId
    } as ClusterFinalizationStatusResponse, { status: 500 });
  }
}

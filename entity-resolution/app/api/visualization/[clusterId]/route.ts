// app/api/visualization/[clusterId]/route.ts
import { NextResponse, NextRequest } from 'next/server';
import { query } from '@/types/db';
import { getUserSchemaFromSession } from '@/utils/auth-db';

export async function GET(
  request: NextRequest,
  { params }: { params: { clusterId: string } }
) {
  const userSchema = await getUserSchemaFromSession(request);
  if (!userSchema) {
    return NextResponse.json(
      { error: 'Unauthorized: User session not found or invalid.' },
      { status: 401 }
    );
  }

  const { searchParams } = new URL(request.url);
  const type = searchParams.get('type') || 'entity'; // Default to 'entity'
  const { clusterId } = await params; // Correctly get clusterId from params

  try {
    let nodes, edges, groupsForTooltip: any[];
    let links;

    if (type === 'service') {
      // Get nodes (services) for this service cluster
      const nodesSql = `
        SELECT 
          s.id,
          s.name,
          s.source_system,
          s.source_id
        FROM public.service s
        JOIN "${userSchema}".service_group sg ON s.id = sg.service_id_1 OR s.id = sg.service_id_2
        WHERE sg.group_cluster_id = $1
        GROUP BY s.id, s.name, s.source_system, s.source_id
      `;
      nodes = await query(nodesSql, [clusterId]);

      // Get edges for this service cluster
      // service_edge_visualization does not have 'confirmed_status' in your schema.
      // Status will be derived client-side or based on related service_group statuses.
      const edgesSql = `
        SELECT 
          sev.id,
          sev.service_id_1,
          sev.service_id_2,
          sev.edge_weight as weight,
          sev.details,
          sev.created_at 
          -- No confirmed_status directly on service_edge_visualization
        FROM "${userSchema}".service_edge_visualization sev
        WHERE sev.service_group_cluster_id = $1 
      `;
      // Corrected table to userSchema.service_edge_visualization and FK to service_group_cluster_id
      edges = await query(edgesSql, [clusterId]);

      // Get service groups for detailed tooltips
      const serviceGroupsSql = `
        SELECT 
          sg.id,
          sg.service_id_1,
          sg.service_id_2,
          sg.confidence_score,
          sg.confirmed_status, // This status is important
          sg.match_values,
          sg.method_type
        FROM "${userSchema}".service_group sg
        WHERE sg.group_cluster_id = $1
      `;
      groupsForTooltip = await query(serviceGroupsSql, [clusterId]);

      links = edges.map(edge => {
        // Attempt to find a relevant group to derive status for the link
        const relevantGroup = groupsForTooltip.find(g =>
            (g.service_id_1 === edge.service_id_1 && g.service_id_2 === edge.service_id_2) ||
            (g.service_id_1 === edge.service_id_2 && g.service_id_2 === edge.service_id_1)
        );
        return {
          id: edge.id,
          source: edge.service_id_1,
          target: edge.service_id_2,
          weight: parseFloat(edge.weight), // Ensure weight is a number
          details: edge.details,
          // Derive status from the first matching service group, or default
          status: relevantGroup ? relevantGroup.confirmed_status : 'PENDING_REVIEW',
          created_at: edge.created_at
        };
      });

    } else {
      // Existing entity logic
      const nodesSql = `
        SELECT 
          e.id,
          e.organization_id,
          e.name,
          e.source_system,
          e.source_id
        FROM public.entity e
        JOIN "${userSchema}".entity_group eg ON e.id = eg.entity_id_1 OR e.id = eg.entity_id_2
        WHERE eg.group_cluster_id = $1
        GROUP BY e.id, e.organization_id, e.name, e.source_system, e.source_id
      `;
      nodes = await query(nodesSql, [clusterId]);

      const edgesSql = `
        SELECT 
          eev.id,
          eev.entity_id_1,
          eev.entity_id_2,
          eev.edge_weight as weight,
          eev.details,
          eev.confirmed_status,
          eev.created_at
        FROM "${userSchema}".entity_edge_visualization eev 
        WHERE eev.cluster_id = $1
      `;
      // Corrected table to userSchema.entity_edge_visualization
      edges = await query(edgesSql, [clusterId]);

      const entityGroupsSql = `
        SELECT 
          eg.id,
          eg.entity_id_1,
          eg.entity_id_2,
          eg.confidence_score,
          eg.confirmed_status,
          eg.match_values,
          eg.method_type
        FROM "${userSchema}".entity_group eg
        WHERE eg.group_cluster_id = $1
      `;
      groupsForTooltip = await query(entityGroupsSql, [clusterId]);

      links = edges.map(edge => ({
        id: edge.id,
        source: edge.entity_id_1,
        target: edge.entity_id_2,
        weight: edge.weight,
        details: edge.details,
        status: edge.confirmed_status,
        created_at: edge.created_at
      }));
    }

    return NextResponse.json({
      nodes,
      links,
      entityGroups: groupsForTooltip // Client expects 'entityGroups', so we use this key
    });
  } catch (error) {
    console.error(`Error fetching ${type} visualization data for cluster ${clusterId}:`, error);
    return NextResponse.json(
      { error: `Failed to fetch ${type} visualization data` },
      { status: 500 }
    );
  }
}

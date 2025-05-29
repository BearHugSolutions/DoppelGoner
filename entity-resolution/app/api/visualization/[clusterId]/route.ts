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

  const { clusterId } = await params;

  try {
    // Get nodes (entities) for this cluster
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
    const nodes = await query(nodesSql, [clusterId]);

    // Get edges with their review status - SIMPLIFIED!
    const edgesSql = `
      SELECT 
        eev.id,
        eev.entity_id_1,
        eev.entity_id_2,
        eev.edge_weight as weight,
        eev.details,
        eev.confirmed_status,  -- Now directly available!
        eev.created_at
      FROM public.entity_edge_visualization eev
      WHERE eev.cluster_id = $1
    `;
    const edges = await query(edgesSql, [clusterId]);

    // Get entity groups for detailed tooltips (still needed for match details)
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
    const entityGroups = await query(entityGroupsSql, [clusterId]);

    const links = edges.map(edge => ({
      id: edge.id,
      source: edge.entity_id_1,
      target: edge.entity_id_2,
      weight: edge.weight,
      details: edge.details,
      status: edge.confirmed_status,  // Direct from edge table!
      created_at: edge.created_at
    }));

    return NextResponse.json({
      nodes,
      links,
      entityGroups
    });
  } catch (error) {
    console.error('Error fetching visualization data:', error);
    return NextResponse.json(
      { error: 'Failed to fetch visualization data' },
      { status: 500 }
    );
  }
}
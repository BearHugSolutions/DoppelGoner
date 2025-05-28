// app/api/visualization/[clusterId]/route.ts
import { NextResponse, NextRequest } from 'next/server';
import { query } from '@/types/db';
// Import the function to get user schema from session
import { getUserSchemaFromSession } from '@/utils/auth-db'; // Adjust path if your auth-db.ts is elsewhere

export async function GET(
  request: NextRequest,
  { params }: { params: { clusterId: string } }
  // Note: If the error "params should be awaited" persists,
  // you might need to investigate how `params` is handled in your Next.js version/config.
  // The typical signature above should make `params.clusterId` directly accessible.
  // Refer to: https://nextjs.org/docs/messages/sync-dynamic-apis
) {
  // Retrieve the user's schema using the session
  const userSchema = await getUserSchemaFromSession(request);

  if (!userSchema) {
    return NextResponse.json(
      { error: 'Unauthorized: User session not found or invalid.' },
      { status: 401 }
    );
  }

  const { clusterId } = await params; // Accessing clusterId from params

  try {
    // Get nodes (entities) for this cluster.
    // 'entity' table is in the 'public' schema.
    // 'entity_group' table is in the user's schema.
    const nodesSql = `
      SELECT 
        e.id,
        e.organization_id,
        e.name,
        e.source_system,
        e.source_id
      FROM public.entity e  -- Corrected: Query 'public.entity'
      JOIN "${userSchema}".entity_group eg ON e.id = eg.entity_id_1 OR e.id = eg.entity_id_2
      WHERE eg.group_cluster_id = $1
      GROUP BY e.id, e.organization_id, e.name, e.source_system, e.source_id
    `;
    const nodes = await query(nodesSql, [clusterId]);

    // Get edges (connections between entities).
    // 'entity_edge_visualization' table is in the user's schema.
    const edgesSql = `
      SELECT 
        eev.id,
        eev.entity_id_1,
        eev.entity_id_2,
        eev.edge_weight as weight,
        eev.details,
        eev.created_at
      FROM "${userSchema}".entity_edge_visualization eev
      WHERE eev.cluster_id = $1
    `;
    const edges = await query(edgesSql, [clusterId]);

    // Get entity groups for tooltips.
    // 'entity_group' table is in the user's schema.
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
      created_at: edge.created_at
    }));

    const responseData = {
      nodes,
      links,
      entityGroups
    };
    
    // console.log('Visualization API Response:', JSON.stringify(responseData, null, 2));
    return NextResponse.json(responseData);
  } catch (error) {
    console.error('Error fetching visualization data:', error);
    console.error(`Error occurred for schema: ${userSchema}, clusterId: ${clusterId}`);
    return NextResponse.json(
      { error: 'Failed to fetch visualization data' },
      { status: 500 }
    );
  }
}

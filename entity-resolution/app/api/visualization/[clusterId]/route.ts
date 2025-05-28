// app/api/visualization/[clusterId]/route.ts
import { NextResponse } from 'next/server';
import { query } from '@/types/db';

export async function GET(
  request: Request,
  { params }: { params: Promise<{ clusterId: string }> }
) {
  const { clusterId } = await params;

  try {
    // Get nodes (entities) for this cluster
    const nodes = await query(`
      SELECT 
        e.id,
        e.organization_id,
        e.name,
        e.source_system,
        e.source_id
      FROM entity e
      JOIN entity_group eg ON e.id = eg.entity_id_1 OR e.id = eg.entity_id_2
      WHERE eg.group_cluster_id = $1
      GROUP BY e.id, e.organization_id, e.name, e.source_system, e.source_id
    `, [clusterId]);

    // Get edges (connections between entities)
    const edges = await query(`
      SELECT 
        eev.id,
        eev.entity_id_1,
        eev.entity_id_2,
        eev.edge_weight as weight,
        eev.details,
        eev.created_at
      FROM entity_edge_visualization eev
      WHERE eev.cluster_id = $1
    `, [clusterId]);

    // Get entity groups for tooltips
    const entityGroups = await query(`
      SELECT 
        eg.id,
        eg.entity_id_1,
        eg.entity_id_2,
        eg.confidence_score,
        eg.confirmed_status,
        eg.match_values,
        eg.method_type
      FROM entity_group eg
      WHERE eg.group_cluster_id = $1
    `, [clusterId]);

    // Transform edges to match EntityLink interface
    const links = edges.map(edge => ({
      id: edge.id,
      source: edge.entity_id_1,
      target: edge.entity_id_2,
      weight: edge.weight,
      details: edge.details,
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
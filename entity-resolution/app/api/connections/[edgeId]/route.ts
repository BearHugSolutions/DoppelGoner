// app/api/connections/[edgeId]/route.ts
import { query } from "@/types/db";
import { getUserSchemaFromSession } from "@/utils/auth-db";
import { NextRequest, NextResponse } from "next/server";

export async function GET(
  request: NextRequest,
  { params }: { params: { edgeId: string } }
) {
  const userSchema = await getUserSchemaFromSession(request);
  if (!userSchema) {
    return NextResponse.json(
      { error: "Unauthorized: User session not found or invalid." },
      { status: 401 }
    );
  }

  const { searchParams } = new URL(request.url);
  const type = searchParams.get("type") || "entity"; // Default to 'entity'
  const { edgeId } = await params; // Correctly get edgeId from params

  try {
    if (type === "service") {
      // Get service edge details
      // Note: service_edge_visualization does not have 'confirmed_status' in your schema.
      // The status will likely be derived from service_group(s).
      const edgeSql = `
        SELECT 
          sev.*,
          s1.name as service1_name,
          s2.name as service2_name
        FROM "${userSchema}".service_edge_visualization sev
        JOIN public.service s1 ON sev.service_id_1 = s1.id
        JOIN public.service s2 ON sev.service_id_2 = s2.id
        WHERE sev.id = $1
      `;
      const edgeResult = await query(edgeSql, [edgeId]);

      if (edgeResult.length === 0) {
        return NextResponse.json(
          { error: "Service edge not found" },
          { status: 404 }
        );
      }
      const edge = edgeResult[0];

      // Get service groups for detailed information
      const serviceGroupsSql = `
        SELECT 
          sg.id,
          sg.service_id_1,
          sg.service_id_2,
          sg.confidence_score,
          sg.method_type,
          sg.match_values,
          sg.pre_rl_confidence_score,
          sg.confirmed_status,
          sg.reviewed_at,
          sg.reviewer_id
        FROM "${userSchema}".service_group sg
        WHERE ((sg.service_id_1 = $1 AND sg.service_id_2 = $2)
           OR (sg.service_id_1 = $2 AND sg.service_id_2 = $1))
           AND sg.confirmed_status != 'DENIED' 
      `; // Assuming DENIED is a possible status, adjust if not.
      const serviceGroups = await query(serviceGroupsSql, [
        edge.service_id_1,
        edge.service_id_2,
      ]);

      // Get service details
      const service1Sql = `SELECT id, name, source_system, contributor_id, created, last_modified FROM public.service WHERE id = $1`;
      const service2Sql = `SELECT id, name, source_system, contributor_id, created, last_modified FROM public.service WHERE id = $1`;

      const [service1Result, service2Result] = await Promise.all([
        query(service1Sql, [edge.service_id_1]),
        query(service2Sql, [edge.service_id_2]),
      ]);

      // Determine overall status for the edge based on its groups
      // This is a simplified example; you might have more complex logic
      let derivedEdgeStatus = "PENDING_REVIEW";
      if (
        serviceGroups.some((sg) => sg.confirmed_status === "CONFIRMED_MATCH")
      ) {
        derivedEdgeStatus = "CONFIRMED_MATCH";
      } else if (
        serviceGroups.length > 0 &&
        serviceGroups.every(
          (sg) =>
            sg.confirmed_status === "CONFIRMED_NON_MATCH" ||
            sg.confirmed_status === "DENIED"
        )
      ) {
        derivedEdgeStatus = "CONFIRMED_NON_MATCH";
      }

      // ✅ Fixed: Use entity1/entity2 consistently, with source_id mapping
      const responseData = {
        edge: {
          id: edge.id,
          cluster_id: edge.service_group_cluster_id, // from service_edge_visualization
          service_id_1: edge.service_id_1,
          service_id_2: edge.service_id_2,
          edge_weight: edge.edge_weight,
          details: edge.details,
          pipeline_run_id: edge.pipeline_run_id,
          created_at: edge.created_at,
          service1_name: edge.service1_name,
          service2_name: edge.service2_name,
          // Deriving status as service_edge_visualization doesn't have it directly
          status: derivedEdgeStatus,
          display_weight:
            derivedEdgeStatus === "CONFIRMED_MATCH"
              ? 1.0
              : parseFloat(edge.edge_weight),
          color: derivedEdgeStatus === "CONFIRMED_MATCH" ? "#000000" : null,
        },
        // ✅ Fixed: Always use entity1/entity2 property names for consistency
        entity1: service1Result[0]
          ? {
              ...service1Result[0],
              source_system: service1Result[0].source_system || null,
              source_id:
                service1Result[0].contributor_id || service1Result[0].id, // Map contributor_id to source_id
              created_at: service1Result[0].created || null,
              updated_at: service1Result[0].last_modified || null,
            }
          : null,
        entity2: service2Result[0]
          ? {
              ...service2Result[0],
              source_system: service2Result[0].source_system || null,
              source_id:
                service2Result[0].contributor_id || service2Result[0].id, // Map contributor_id to source_id
              created_at: service2Result[0].created || null,
              updated_at: service2Result[0].last_modified || null,
            }
          : null,
        // API client expects 'entityGroups' key, so we use it but populate with ServiceGroup data
        entityGroups: serviceGroups.map((sg) => ({
          ...sg,
          confirmed_status: sg.confirmed_status || "PENDING_REVIEW",
        })),
      };

      return NextResponse.json(responseData);
    } else {
      // Existing entity logic
      const edgeSql = `
        SELECT 
          eev.*,
          e1.name as entity1_name,
          e2.name as entity2_name
        FROM "${userSchema}".entity_edge_visualization eev
        JOIN public.entity e1 ON eev.entity_id_1 = e1.id
        JOIN public.entity e2 ON eev.entity_id_2 = e2.id
        WHERE eev.id = $1
      `;
      // In your schema, entity_edge_visualization is in the user schema, not public
      // The provided example code had public.entity_edge_visualization, I'm using the one from your schema.
      const edgeResult = await query(edgeSql, [edgeId]);

      if (edgeResult.length === 0) {
        return NextResponse.json(
          { error: "Entity edge not found" },
          { status: 404 }
        );
      }
      const edge = edgeResult[0];

      const entityGroupsSql = `
        SELECT 
          eg.id,
          eg.entity_id_1,
          eg.entity_id_2,
          eg.confidence_score,
          eg.method_type,
          eg.match_values,
          eg.pre_rl_confidence_score,
          eg.confirmed_status,
          eg.reviewed_at,
          eg.reviewer_id
        FROM "${userSchema}".entity_group eg
        WHERE ((eg.entity_id_1 = $1 AND eg.entity_id_2 = $2)
           OR (eg.entity_id_1 = $2 AND eg.entity_id_2 = $1))
           AND eg.confirmed_status != 'DENIED'
      `;
      const entityGroups = await query(entityGroupsSql, [
        edge.entity_id_1,
        edge.entity_id_2,
      ]);

      const entity1Sql = `SELECT * FROM public.entity WHERE id = $1`;
      const entity2Sql = `SELECT * FROM public.entity WHERE id = $1`;

      const [entity1Result, entity2Result] = await Promise.all([
        query(entity1Sql, [edge.entity_id_1]),
        query(entity2Sql, [edge.entity_id_2]),
      ]);

      const responseData = {
        edge: {
          id: edge.id,
          cluster_id: edge.cluster_id,
          entity_id_1: edge.entity_id_1,
          entity_id_2: edge.entity_id_2,
          edge_weight: edge.edge_weight,
          details: edge.details,
          pipeline_run_id: edge.pipeline_run_id,
          created_at: edge.created_at,
          entity1_name: edge.entity1_name,
          entity2_name: edge.entity2_name,
          status: edge.confirmed_status,
          display_weight:
            edge.confirmed_status === "CONFIRMED_MATCH"
              ? 1.0
              : edge.edge_weight,
          color: edge.confirmed_status === "CONFIRMED_MATCH" ? "#000000" : null,
        },
        entity1: entity1Result[0] || null,
        entity2: entity2Result[0] || null,
        entityGroups: entityGroups.map((eg) => ({
          ...eg,
          confirmed_status: eg.confirmed_status || "PENDING_REVIEW",
        })),
      };

      return NextResponse.json(responseData);
    }
  } catch (error) {
    console.error(
      `Error fetching ${type} connection data for edge ${edgeId}:`,
      error
    );
    return NextResponse.json(
      { error: `Failed to fetch ${type} connection data` },
      { status: 500 }
    );
  }
}

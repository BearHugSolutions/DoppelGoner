// app/api/entity-groups/[groupId]/review/route.ts

import { NextRequest, NextResponse } from 'next/server';
import { getIronSession, SessionOptions } from 'iron-session';
import { query, queryOne, withTransaction } from '@/types/db';
import { getUserSchemaFromSession, findUserById, DbUser } from '@/utils/auth-db';
import { sessionOptions } from '@/lib/session';
import type { UserSessionData } from '@/app/api/auth/login/route'; // Import from login route

interface ReviewRequestBody {
  decision: 'CONFIRMED_MATCH' | 'CONFIRMED_NON_MATCH';
  reviewerId: string; // We'll ignore this in favor of session ID, but it's sent by client
  notes?: string;
}

interface EntityGroupRecord {
  id: string;
  entity_id_1: string;
  entity_id_2: string;
  method_type: string;
  match_values: any;
  pre_rl_confidence_score: number | null;
  confidence_score: number | null;
  group_cluster_id: string | null;
  confirmed_status: string | null;
  reviewed_at: Date | null;
  reviewer_id: string | null;
  created_at: Date | null;
  updated_at: Date | null;
}

export async function POST(
  request: NextRequest,
  { params }: { params: { groupId: string } }
) {
  const { groupId } = await params;

  if (!groupId) {
    return NextResponse.json({ error: 'Group ID is required.' }, { status: 400 });
  }

  let userSchema: string | null;
  let sessionUserId: string | null;
  let sessionId: string | null;

  try {
    const tempResponse = new NextResponse();
    const session = await getIronSession<UserSessionData>(request, tempResponse, sessionOptions);

    // Updated check for new session structure
    if (!session.isLoggedIn || !session.userSchema || !session.userId || !session.sessionId) {
      console.warn('User not authenticated or session data missing.');
      return NextResponse.json({ error: 'Unauthorized: User session not found, invalid, or missing data.' }, { status: 401 });
    }
    userSchema = session.userSchema;
    sessionUserId = session.userId; // Use session.userId
    sessionId = session.sessionId;

  } catch (error) {
    console.error('Session error:', error);
    return NextResponse.json({ error: 'Failed to retrieve user session.' }, { status: 500 });
  }

  if (!userSchema || !sessionUserId) {
    return NextResponse.json({ error: 'Unauthorized: User schema or session user ID could not be determined.' }, { status: 401 });
  }

  try {
    const body: ReviewRequestBody = await request.json();
    const { decision, notes } = body;

    if (!decision) {
      return NextResponse.json({ error: 'Missing required field: decision.' }, { status: 400 });
    }

    const actualReviewerId = sessionUserId; // Use userId from session

    const result = await withTransaction(async (client) => {
      // 1. Fetch entity group (removed pipeline_run_id from query since it doesn't exist in the table)
      const entityGroupSql = `
        SELECT 
          id, entity_id_1, entity_id_2, method_type, 
          match_values, pre_rl_confidence_score, confidence_score, 
          group_cluster_id, confirmed_status, reviewed_at, reviewer_id,
          created_at, updated_at
        FROM "${userSchema}".entity_group 
        WHERE id = $1;
      `;
      const entityGroupResult = await client.query(entityGroupSql, [groupId]);
      const entityGroup = entityGroupResult.rows[0] as EntityGroupRecord | undefined;

      if (!entityGroup) {
        throw new Error(`Entity group with ID ${groupId} not found in schema ${userSchema}.`);
      }

      // 2. Update entity group (using actualReviewerId from session)
      const confirmedStatus = decision === 'CONFIRMED_MATCH' ? 'CONFIRMED_MATCH' : 'CONFIRMED_NON_MATCH';
      const updateEntityGroupSql = `
        UPDATE "${userSchema}".entity_group
        SET 
          confirmed_status = $1,
          reviewer_id = $2,
          reviewed_at = CURRENT_TIMESTAMP,
          updated_at = CURRENT_TIMESTAMP
        WHERE id = $3;
      `;
      await client.query(updateEntityGroupSql, [confirmedStatus, actualReviewerId, groupId]);

      // 3. Create match_decision_details (set pipeline_run_id to NULL since we don't have it from entity_group)
      const insertMatchDecisionDetailsSql = `
        INSERT INTO clustering_metadata.match_decision_details (
          entity_group_id, pipeline_run_id, snapshotted_features,
          method_type_at_decision, pre_rl_confidence_at_decision,
          tuned_confidence_at_decision, confidence_tuner_version_at_decision
        ) VALUES ($1, $2, $3, $4, $5, $6, $7)
        RETURNING id;
      `;
      const matchDecisionDetailsResult = await client.query(insertMatchDecisionDetailsSql, [
        entityGroup.id, 
        null, // Set pipeline_run_id to NULL since it's not available in entity_group table
        entityGroup.match_values,
        entityGroup.method_type, 
        entityGroup.pre_rl_confidence_score,
        entityGroup.confidence_score, 
        null // confidence_tuner_version_at_decision
      ]);
      const matchDecisionId = matchDecisionDetailsResult.rows[0].id;

      if (!matchDecisionId) {
        throw new Error('Failed to create match_decision_details record.');
      }

      // 4. Create human_feedback (using actualReviewerId from session)
      const isMatchCorrect = decision === 'CONFIRMED_MATCH';
      const insertHumanFeedbackSql = `
        INSERT INTO clustering_metadata.human_feedback (
          entity_group_id, reviewer_id, feedback_timestamp,
          is_match_correct, notes, match_decision_id
        ) VALUES ($1, $2, CURRENT_TIMESTAMP, $3, $4, $5);
      `;
      await client.query(insertHumanFeedbackSql, [
        entityGroup.id, actualReviewerId, isMatchCorrect, notes || null, matchDecisionId
      ]);

      return { entityGroup, confirmedStatus };
    });

    console.log(`Review submitted for group ${groupId}. Status set to ${result.confirmedStatus}. Feedback logged by user ${actualReviewerId} (Session: ${sessionId}).`);
    return NextResponse.json({ message: 'Review submitted successfully.' }, { status: 200 });

  } catch (error: any) {
    console.error(`Error processing review for group ${groupId}:`, error);
    if (error.message.includes('not found')) {
      return NextResponse.json({ error: error.message }, { status: 404 });
    }
    return NextResponse.json({ error: `Failed to process review: ${error.message}` }, { status: 500 });
  }
}
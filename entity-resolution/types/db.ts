// types/db.ts

import { Pool, PoolConfig } from 'pg';

// Database connection configuration
export interface DbConfig extends PoolConfig {
  host: string;
  port: number;
  database: string;
  user: string;
  password: string;
  ssl?: boolean | object;
}

// Human Feedback types
export interface HumanFeedbackDbRecord {
  id: string;
  entity_group_id: string;
  is_match_correct: boolean;
  reviewer_id?: string;
  notes?: string;
  created_at?: Date;
  updated_at?: Date;
}

export interface NewHumanFeedback {
  entity_group_id: string;
  reviewer_id: string;
  is_match_correct: boolean;
  notes?: string;
  match_decision_id: string;
}

// Service Group types
export interface ServiceGroup {
  id: string;
  service_id_1: string;
  service_id_2: string;
  confidence_score: number;
  pre_rl_confidence_score: number;
  method_type: string;
  match_values: Record<string, unknown>;
  confirmed_status: 'PENDING_REVIEW' | 'CONFIRMED' | 'REJECTED';
  created_at: Date;
  updated_at: Date;
  group_cluster_id?: string | null;
  reviewed_at?: Date | null;
  reviewer_id?: string | null;
}

export interface NewServiceGroup {
  proposed_id: string;
  service_id_1: string;
  service_id_2: string;
  confidence_score: number;
  pre_rl_confidence_score: number;
  method_type: string;
  match_values: Record<string, unknown>;
}

// Entity Group types - Updated to match actual schema
export interface EntityGroup {
  id: string;
  entity_id_1: string;
  entity_id_2: string;
  confidence_score: number | null;
  pre_rl_confidence_score: number | null;
  method_type: string;
  match_values: Record<string, unknown>;
  confirmed_status: 'PENDING_REVIEW' | 'CONFIRMED' | 'DENIED';
  created_at: Date | null;
  updated_at: Date | null;
  group_cluster_id?: string | null;
  reviewed_at?: Date | null;
  reviewer_id?: string | null;
}

export interface NewEntityGroup {
  proposed_id: string;
  entity_id_1: string;
  entity_id_2: string;
  confidence_score: number;
  pre_rl_confidence_score: number;
  method_type: string;
  match_values: Record<string, unknown>;
}

// Entity Group Cluster types
export interface EntityGroupCluster {
  id: string;
  name: string | null;
  description: string | null;
  entity_count: number;
  group_count: number;
  average_coherence_score: number | null;
  created_at: Date | null;
  updated_at: Date | null;
  source_cluster_id?: string | null;
  is_user_modified?: boolean;
}

// Service Group Cluster types
export interface ServiceGroupCluster {
  id: string;
  name: string | null;
  description: string | null;
  service_count: number;
  service_group_count: number;
  average_coherence_score: number | null;
  created_at: Date | null;
  updated_at: Date | null;
}

// Entity types
export interface Entity {
  id: string;
  organization_id: string;
  name: string | null;
  created_at: Date | null;
  updated_at: Date | null;
  source_system: string | null;
  source_id: string | null;
}

// Entity Edge Visualization types
export interface EntityEdgeVisualization {
  id: string;
  cluster_id: string;
  entity_id_1: string;
  entity_id_2: string;
  edge_weight: number;
  details: Record<string, unknown> | null;
  pipeline_run_id: string | null;
  created_at: Date;
  updated_at?: Date | null;
}

// Service Edge Visualization types
export interface ServiceEdgeVisualization {
  id: string;
  service_group_cluster_id: string;
  service_id_1: string;
  service_id_2: string;
  edge_weight: number;
  details: Record<string, unknown> | null;
  pipeline_run_id: string;
  created_at: Date;
}

// Service Comparison Cache types
export interface ServiceComparisonCacheEntry {
  service_id_1: string;
  service_id_2: string;
  signature_1: string;
  signature_2: string;
  method_type: string;
  pipeline_run_id?: string;
  comparison_result: string;
  confidence_score?: number;
  snapshotted_features?: Record<string, unknown>;
  cached_at: Date;
}

// Service Data Signature types
export interface ServiceDataSignature {
  service_id: string;
  signature: string;
  relevant_attributes_snapshot?: Record<string, unknown>;
  source_data_last_updated_at?: Date;
  signature_calculated_at: Date;
}

// Cluster Formation Edge types
export interface ClusterFormationEdge {
  id: string;
  pipeline_run_id: string;
  source_group_id: string;
  target_group_id: string;
  calculated_edge_weight: number;
  contributing_shared_entities: string[];
  created_at: Date;
}

export interface NewClusterFormationEdge {
  pipeline_run_id: string;
  source_group_id: string;
  target_group_id: string;
  calculated_edge_weight: number;
  contributing_shared_entities: string[];
}

// Pipeline Run types
export interface PipelineRun {
  id: string;
  run_timestamp: Date;
  description?: string;
  total_entities: number;
  total_groups: number;
  total_clusters: number;
  total_service_matches: number;
  total_visualization_edges: number;
  entity_processing_time: number;
  context_feature_extraction_time: number;
  matching_time: number;
  clustering_time: number;
  visualization_edge_calculation_time: number;
  service_matching_time: number;
  total_processing_time: number;
  created_at: Date;
  updated_at: Date;
}

export interface NewPipelineRun {
  id: string;
  run_timestamp: Date;
  description?: string;
}

// Database client configuration
export function getDbConfig(): DbConfig {
  return {
    host: process.env.POSTGRES_HOST || 'localhost',
    port: parseInt(process.env.POSTGRES_PORT || '5432', 10),
    database: process.env.POSTGRES_DB || 'dataplatform',
    user: process.env.POSTGRES_USER || 'postgres',
    password: process.env.POSTGRES_PASSWORD || '',
    ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false,
    max: 20, // Maximum number of clients in the pool
    idleTimeoutMillis: 30000, // How long a client is allowed to remain idle before being closed
    connectionTimeoutMillis: 2000, // How long to try to connect before timing out
  };
}

// Initialize database connection pool
let pool: Pool | null = null;

export function getDbPool(): Pool {
  if (!pool) {
    pool = new Pool(getDbConfig());
    
    // Log connection events
    pool.on('connect', () => {
      console.log('New database connection established');
    });

    pool.on('error', (err) => {
      console.error('Unexpected error on idle client', err);
      process.exit(-1);
    });
  }
  return pool;
}

// Helper function to execute a query and return a single row
export async function queryOne<T = any>(
  query: string,
  params: any[] = []
): Promise<T | null> {
  const client = await getDbPool().connect();
  try {
    const result = await client.query(query, params);
    return result.rows[0] || null;
  } finally {
    client.release();
  }
}

// Helper function to execute a query and return all rows
export async function query<T = any>(
  query: string,
  params: any[] = []
): Promise<T[]> {
  const client = await getDbPool().connect();
  try {
    const result = await client.query(query, params);
    return result.rows;
  } finally {
    client.release();
  }
}

// Helper function to execute a query that doesn't return results
export async function execute(
  query: string,
  params: any[] = []
): Promise<number> {
  const client = await getDbPool().connect();
  try {
    const result = await client.query(query, params);
    return result.rowCount || 0;
  } finally {
    client.release();
  }
}

// Helper function to execute a transaction
export async function withTransaction<T>(
  callback: (client: any) => Promise<T>
): Promise<T> {
  const client = await getDbPool().connect();
  try {
    await client.query('BEGIN');
    const result = await callback(client);
    await client.query('COMMIT');
    return result;
  } catch (e) {
    await client.query('ROLLBACK');
    throw e;
  } finally {
    client.release();
  }
}
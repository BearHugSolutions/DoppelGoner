import { query, queryOne } from '../types/db';

export interface PaginationOptions {
  page?: number;
  pageSize?: number;
  sortBy?: string;
  sortOrder?: 'ASC' | 'DESC';
}

export interface PaginatedResult<T> {
  data: T[];
  pagination: {
    page: number;
    pageSize: number;
    totalItems: number;
    totalPages: number;
  };
}

export async function getPaginatedResults<T>(
  tableName: string,
  whereClause: string = '',
  params: any[] = [],
  options: PaginationOptions = {}
): Promise<PaginatedResult<T>> {
  const {
    page = 1,
    pageSize = 10,
    sortBy = 'created_at',
    sortOrder = 'DESC',
  } = options;

  const offset = (page - 1) * pageSize;
  
  // Build the base query
  let queryStr = `FROM ${tableName} ${whereClause ? 'WHERE ' + whereClause : ''}`;
  
  // Get total count
  const countResult = await queryOne<{ count: string }>(
    `SELECT COUNT(*) as count ${queryStr}`,
    params
  );
  
  const totalItems = parseInt(countResult?.count || '0', 10);
  const totalPages = Math.ceil(totalItems / pageSize);
  
  // Add sorting and pagination
  queryStr += ` ORDER BY ${sortBy} ${sortOrder} LIMIT $${params.length + 1} OFFSET $${params.length + 2}`;
  
  // Get paginated data
  const data = await query<T>(
    `SELECT * ${queryStr}`,
    [...params, pageSize, offset]
  );
  
  return {
    data,
    pagination: {
      page,
      pageSize,
      totalItems,
      totalPages,
    },
  };
}

export function buildSearchCondition(fields: string[], searchTerm?: string): { whereClause: string; params: any[] } {
  if (!searchTerm) return { whereClause: '', params: [] };
  
  const conditions = fields.map((field, index) => 
    `${field} ILIKE $${index + 1}`
  );
  
  return {
    whereClause: `(${conditions.join(' OR ')})`,
    params: Array(fields.length).fill(`%${searchTerm}%`),
  };
}

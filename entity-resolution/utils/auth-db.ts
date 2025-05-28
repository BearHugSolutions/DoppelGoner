// utils/auth-db.ts
import { query, queryOne } from '@/types/db'; // Assuming your query function is here
import bcrypt from 'bcryptjs';
import { NextRequest, NextResponse } from 'next/server';
import { getIronSession, SessionOptions } from 'iron-session';
// Assuming your sessionOptions are defined in @/lib/session as per your API route imports
// You'll need to ensure this file exists and exports sessionOptions correctly.
// Example: export const sessionOptions: SessionOptions = { cookieName: 'myapp_session', password: 'complex_password_at_least_32_characters_long', cookieOptions: { secure: process.env.NODE_ENV === 'production' } };
import { sessionOptions } from '@/lib/session';

const SALT_ROUNDS = 10; // Cost factor for bcrypt

/**
 * User interface for database operations.
 */
export interface DbUser {
  id: string;
  username: string;
  hashed_password?: string;
  email?: string | null;
  user_schema_name: string;
  is_active: boolean;
  created_at: Date;
  updated_at: Date;
  last_login_at?: Date | null;
}

/**
 * Interface for user data stored in the session.
 * Ensure this matches the structure you save upon login.
 */
interface UserSessionData {
  id: string;
  username: string;
  userSchema: string; // This is the schema name we need
  // Add other relevant session fields, e.g., isLoggedIn: boolean;
}

/**
 * Sanitizes a username to be used as a schema name.
 */
export function sanitizeUsernameForSchema(username: string): string {
  let schemaName = username.toLowerCase().replace(/[^a-z0-9_]/g, '_');
  if (schemaName.match(/^\d/)) {
    schemaName = `s_${schemaName}`;
  }
  if (!schemaName) {
    throw new Error('Invalid username for schema creation.');
  }
  return `${schemaName}_schema`;
}

/**
 * Creates a new schema for the user if it doesn't already exist.
 */
export async function createUserSchema(schemaName: string): Promise<void> {
  await query(`CREATE SCHEMA IF NOT EXISTS "${schemaName}";`);
  console.log(`Schema "${schemaName}" ensured.`);
}

/**
 * Replicates specified tables from the public schema to the user's schema.
 */
export async function replicateTablesToUserSchema(userSchema: string, tablesToReplicate: string[]): Promise<void> {
  for (const tableName of tablesToReplicate) {
    try {
      const targetTable = `"${userSchema}"."${tableName}"`;
      const sourceTable = `public."${tableName}"`;

      await query(`DROP TABLE IF EXISTS ${targetTable} CASCADE;`);
      console.log(`Dropped table ${targetTable} if it existed.`);

      await query(`CREATE TABLE ${targetTable} (LIKE ${sourceTable} INCLUDING ALL);`);
      console.log(`Created table structure ${targetTable} like ${sourceTable}.`);

      await query(`INSERT INTO ${targetTable} SELECT * FROM ${sourceTable};`);
      console.log(`Copied data from ${sourceTable} to ${targetTable}.`);

    } catch (error) {
      console.error(`Error replicating table ${tableName} to schema ${userSchema}:`, error);
      throw new Error(`Failed to replicate table ${tableName}. Reason: ${error instanceof Error ? error.message : String(error)}`);
    }
  }
  console.log(`All specified tables replicated to schema "${userSchema}".`);
}

/**
 * Finds a user by their username in the public.users table.
 */
export async function findUserByUsername(username: string): Promise<DbUser | null> {
  const result = await queryOne<DbUser>(
    `SELECT id, username, hashed_password, email, user_schema_name, is_active, created_at, updated_at, last_login_at 
     FROM public.users 
     WHERE username = $1`,
    [username]
  );
  return result || null;
}

/**
 * Finds a user by their schema name in the public.users table.
 */
export async function findUserBySchemaName(schemaName: string): Promise<DbUser | null> {
    const result = await queryOne<DbUser>(
      `SELECT id, username, email, user_schema_name, is_active, created_at, updated_at, last_login_at 
       FROM public.users 
       WHERE user_schema_name = $1`,
      [schemaName]
    );
    return result || null;
  }

/**
 * Finds a user by their ID in the public.users table.
 * @param userId The ID of the user.
 * @returns The user object if found, otherwise null.
 */
export async function findUserById(userId: string): Promise<DbUser | null> {
  const result = await queryOne<DbUser>(
    `SELECT id, username, email, user_schema_name, is_active, created_at, updated_at, last_login_at 
     FROM public.users 
     WHERE id = $1`,
    [userId]
  );
  return result || null;
}


/**
 * Creates a new user in the public.users table.
 */
export async function createUser(username: string, rawPassword: string, email?: string): Promise<Omit<DbUser, 'hashed_password'>> {
  const existingUser = await findUserByUsername(username);
  if (existingUser) {
    throw new Error('Username already exists.');
  }

  const hashedPassword = await bcrypt.hash(rawPassword, SALT_ROUNDS);
  const userSchemaName = sanitizeUsernameForSchema(username);

  const existingSchemaUser = await findUserBySchemaName(userSchemaName);
  if (existingSchemaUser) {
    throw new Error(`Generated schema name '${userSchemaName}' already in use. Please choose a different username.`);
  }

  const result = await queryOne<{ id: string; username: string; email: string | null; user_schema_name: string; is_active: boolean; created_at: Date; updated_at: Date; }>(
    `INSERT INTO public.users (username, hashed_password, email, user_schema_name)
     VALUES ($1, $2, $3, $4)
     RETURNING id, username, email, user_schema_name, is_active, created_at, updated_at`,
    [username, hashedPassword, email, userSchemaName]
  );

  if (!result) {
    throw new Error('Failed to create user in database.');
  }
  return result;
}

/**
 * Verifies a user's password.
 */
export async function verifyPassword(rawPassword: string, hashedPassword?: string): Promise<boolean> {
  if (!hashedPassword) return false;
  return bcrypt.compare(rawPassword, hashedPassword);
}

/**
 * Updates the last_login_at timestamp for a user.
 */
export async function updateUserLastLogin(userId: string): Promise<void> {
  await query(
    `UPDATE public.users SET last_login_at = CURRENT_TIMESTAMP WHERE id = $1`,
    [userId]
  );
}

/**
 * Retrieves the user's schema name from the iron-session.
 * This function is designed to be called from Next.js API Route Handlers.
 * @param request The incoming NextRequest.
 * @returns The user's schema name as a string, or null if not found or an error occurs.
 */
export async function getUserSchemaFromSession(request: NextRequest): Promise<string | null> {
  try {
    // getIronSession needs a response object to potentially set cookies (e.g., for session renewal).
    // In Route Handlers, we can create a temporary response object.
    // The actual API response will be constructed later by the route handler.
    const tempResponse = new NextResponse();
    const session = await getIronSession<UserSessionData>(request, tempResponse, sessionOptions);

    if (session && session.userSchema) {
      // If iron-session modified the session (e.g., auto-renewal),
      // the `tempResponse` might have 'set-cookie' headers.
      // These would typically need to be merged with the final response from the API route if changes were made.
      // For just reading the schema, this is usually not an issue unless session.save() is called.
      return session.userSchema;
    }
    
    // It's good practice to log when a session or expected data isn't found.
    console.warn("getUserSchemaFromSession: User schema not found in session. User might not be logged in or session is misconfigured.");
    return null;
  } catch (error) {
    // Log the error for server-side debugging.
    console.error("Error retrieving user schema from session:", error);
    // Avoid leaking internal error details to the client unless necessary.
    return null;
  }
}
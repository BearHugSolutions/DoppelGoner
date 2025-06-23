// @/lib/session.ts
import type { SessionOptions } from 'iron-session';

// Session data structure for team-based system
export interface UserSessionData {
  userId: string;
  sessionId: string;
  username: string;
  teamId: string;        // Team ID
  teamName: string;      // Human-readable team name
  teamSchema: string;    // Team's database schema name (e.g., "alpha")
  userPrefix: string;    // User's prefix within team schema (e.g., "john")
  isLoggedIn: true;
}

// Ensure SECRET_COOKIE_PASSWORD is set and is a string
const secretCookiePassword = process.env.SECRET_COOKIE_PASSWORD;
if (!secretCookiePassword || typeof secretCookiePassword !== 'string') {
  console.error(
    'FATAL ERROR: SECRET_COOKIE_PASSWORD is not defined in your environment variables or is not a string.'
  );
  throw new Error(
    'SECRET_COOKIE_PASSWORD is not defined or is not a string. Please check your .env file and ensure it is loaded correctly.'
  );
}

if (secretCookiePassword.length < 32) {
  console.error(
    `FATAL ERROR: SECRET_COOKIE_PASSWORD is too short. It must be at least 32 characters long. Currently, it has ${secretCookiePassword.length} characters.`
  );
  throw new Error(
    'SECRET_COOKIE_PASSWORD must be at least 32 characters long.'
  );
}

export const sessionOptions: SessionOptions = {
  cookieName: process.env.SESSION_COOKIE_NAME || 'entity-resolution-session',
  password: secretCookiePassword,
  cookieOptions: {
    secure: process.env.NODE_ENV === 'production', // HTTPS only in production
    httpOnly: true, // Prevent XSS attacks
    sameSite: 'lax', // CSRF protection
    maxAge: 7 * 24 * 60 * 60, // 7 days (recommended for team-based systems)
    // Note: Longer sessions are acceptable for team-based systems since users
    // are typically organization members with ongoing access
  },
};
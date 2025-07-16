/*
================================================================================
|
|   File: /lib/session.ts
|
|   Description: Defines server-side session configuration and data structure.
|   - Merges robust error handling with modern TypeScript module augmentation.
|   - Imports UserSessionData for better type organization.
|
================================================================================
*/
import type { SessionOptions } from "iron-session";
import type { UserSessionData } from "@/types/entity-resolution"; // Centralized type definition

// --- Environment Variable Validation ---
// Ensure SECRET_COOKIE_PASSWORD is set, is a string, and meets length requirements.
const secretCookiePassword = process.env.SECRET_COOKIE_PASSWORD;
if (!secretCookiePassword || typeof secretCookiePassword !== 'string') {
  const errorMessage = 'FATAL ERROR: SECRET_COOKIE_PASSWORD is not defined in your environment variables or is not a string. Please check your .env file.';
  console.error(errorMessage);
  throw new Error(errorMessage);
}

if (secretCookiePassword.length < 32) {
  const errorMessage = `FATAL ERROR: SECRET_COOKIE_PASSWORD is too short. It must be at least 32 characters long. Currently, it has ${secretCookiePassword.length} characters.`;
  console.error(errorMessage);
  throw new Error(errorMessage);
}

// --- Iron Session Configuration ---
export const sessionOptions: SessionOptions = {
  cookieName: process.env.SESSION_COOKIE_NAME || "entity-resolution-session",
  password: secretCookiePassword,
  cookieOptions: {
    secure: process.env.NODE_ENV === "production", // Use secure cookies in production (HTTPS)
    httpOnly: true,                                // Prevent client-side script access to the cookie (XSS protection)
    sameSite: "lax",                               // Mitigate CSRF attacks
    maxAge: 7 * 24 * 60 * 60,                      // Session expiration: 7 days
  },
};

// --- Session Data Type Augmentation ---
// This tells iron-session the shape of the data stored in the session.
// By using module declaration, we can ensure type safety across the application.
declare module "iron-session" {
  interface IronSessionData extends UserSessionData {}
}

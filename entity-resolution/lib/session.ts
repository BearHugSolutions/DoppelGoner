// @/lib/session.ts
import type { SessionOptions } from 'iron-session';

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
  cookieName: process.env.SESSION_COOKIE_NAME || 'your-app-session-name', // Replace 'your-app-session-name' or use an env var
  password: secretCookiePassword, // Use the validated password
  cookieOptions: {
    secure: process.env.NODE_ENV === 'production', // Important: Set to true in production
    httpOnly: true, // Recommended for security
    sameSite: 'lax', // Recommended for security
    maxAge: undefined, // Session cookie by default, or set a duration in seconds (e.g., 24 * 60 * 60 for 1 day)
    // path: '/', // Usually defaults to /
  },
};

// Log to confirm sessionOptions are set up, especially during development
console.log('Session options configured. Cookie name:', sessionOptions.cookieName);
if (process.env.NODE_ENV !== 'production') {
  console.log('SECRET_COOKIE_PASSWORD is set and has a length of:', secretCookiePassword.length);
}

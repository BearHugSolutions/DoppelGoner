// app/api/auth/login/route.ts
import { NextResponse, NextRequest } from 'next/server';
import { getIronSession } from 'iron-session';
import { v4 as uuidv4 } from 'uuid';
import { 
    findUserByUsername, 
    verifyPassword,
    updateUserLastLogin,
} from '@/utils/auth-db';
import { sessionOptions } from '@/lib/session'; // Ensure this path is correct

// Define the updated shape of the data stored in the session
export interface UserSessionData {
  userId: string; // Explicitly store public.users.id here
  sessionId: string; // Store a unique session ID
  username: string;
  userSchema: string;
  isLoggedIn: true;
}

export async function POST(request: NextRequest) {
  try {
    const { username, password } = await request.json();

    // --- Input Validation ---
    if (!username || typeof username !== 'string' || username.trim() === '') {
      return NextResponse.json({ error: 'Username is required.' }, { status: 400 });
    }
    if (!password || typeof password !== 'string') {
        return NextResponse.json({ error: 'Password is required.' }, { status: 400 });
    }

    const trimmedUsername = username.trim();

    // 1. Find user
    const user = await findUserByUsername(trimmedUsername);

    if (!user || !user.hashed_password) {
      return NextResponse.json({ error: 'Invalid username or password.' }, { status: 401 });
    }

    // 2. Check if active
    if (!user.is_active) {
        return NextResponse.json({ error: 'Your account is inactive. Please contact support.' }, { status: 403 });
    }

    // 3. Verify password
    const isPasswordValid = await verifyPassword(password, user.hashed_password);

    if (!isPasswordValid) {
      return NextResponse.json({ error: 'Invalid username or password.' }, { status: 401 });
    }

    // 4. Prepare Response Payload (for AuthContext)
    const userResponsePayload = {
        id: user.id, // Keep 'id' here for compatibility with AuthContext
        username: user.username,
        email: user.email,
        userSchema: user.user_schema_name,
    };

    const response = NextResponse.json({
      message: 'Login successful.',
      user: userResponsePayload,
    }, { status: 200 });

    // --- DETAILED LOGGING FOR IRON-SESSION ---
    console.log("--- Iron Session Debugging ---");
    console.log("Is process.env.SECRET_COOKIE_PASSWORD defined?", !!process.env.SECRET_COOKIE_PASSWORD);
    if (process.env.SECRET_COOKIE_PASSWORD) {
        console.log("Length of process.env.SECRET_COOKIE_PASSWORD:", process.env.SECRET_COOKIE_PASSWORD.length);
    } else {
        console.log("process.env.SECRET_COOKIE_PASSWORD is NOT DEFINED at the time of login route execution.");
    }

    console.log("Are sessionOptions defined?", !!sessionOptions);
    if (sessionOptions) {
        console.log("sessionOptions.cookieName:", sessionOptions.cookieName);
        console.log("Is sessionOptions.password defined?", !!sessionOptions.password);
        if (sessionOptions.password) {
            console.log("Type of sessionOptions.password:", typeof sessionOptions.password);
            console.log("Length of sessionOptions.password:", sessionOptions.password.length);
             // Avoid logging the actual password in production for security, but for local debugging it can be helpful:
            // console.log("Value of sessionOptions.password (first 5 chars):", sessionOptions.password.substring(0,5) + "...");
        } else {
            console.log("sessionOptions.password is NOT DEFINED.");
        }
        console.log("sessionOptions.cookieOptions:", JSON.stringify(sessionOptions.cookieOptions));
    } else {
        console.log("sessionOptions object is NOT DEFINED.");
    }
    console.log("--- End Iron Session Debugging ---");
    // --- END DETAILED LOGGING ---


    // 5. Create/Update Iron Session
    const session = await getIronSession<UserSessionData>(request, response, sessionOptions);

    // Generate a unique session ID
    const newSessionId = uuidv4();

    // Set the session data with new structure
    session.userId = user.id; // Store public.users.id as userId
    session.sessionId = newSessionId; // Store the new session ID
    session.username = user.username;
    session.userSchema = user.user_schema_name;
    session.isLoggedIn = true; // Mark as logged in

    await session.save(); // Save the session, setting the cookie

    // 6. Update last login
    updateUserLastLogin(user.id).catch(err => {
        console.error("Failed to update last login time:", err);
    });
    
    // Return the response (with cookie)
    return response;

  } catch (error) {
    console.error('Login error:', error);
    // Check if the error is the specific iron-session password error to provide a more targeted message
    if (error instanceof Error && error.message.includes("Password must be at least 32 characters long")) {
        console.error("Iron-session specific error: Ensure SECRET_COOKIE_PASSWORD is correctly set in .env and propagated to sessionOptions in @/lib/session.ts");
        return NextResponse.json({ error: 'Session configuration error. Please contact support. Password policy not met for session encryption.' }, { status: 500 });
    }
    return NextResponse.json({ error: 'Login failed. Please try again later.' }, { status: 500 });
  }
}

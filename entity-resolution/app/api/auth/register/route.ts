// app/api/auth/register/route.ts
import { NextResponse } from 'next/server';
import { 
    createUserSchema, 
    replicateTablesToUserSchema,
    createUser, // createUser already checks for username and email uniqueness if DB constraints exist
    findUserByUsername,
    // sanitizeUsernameForSchema // This is handled within createUser now
} from '@/utils/auth-db'; // Adjust path as needed

// Tables to replicate for each new user
const TABLES_TO_REPLICATE = [
  'entity_group',
  'entity_group_cluster',
  'entity_edge_visualization',
  'service_group',
  'service_group_cluster',
  'service_edge_visualization',
];

// Whitelist of emails allowed to register.
// Emails should be stored in lowercase to ensure case-insensitive comparison.
const EMAIL_REGISTRATION_WHITELIST = [
    'david.m.botos@gmail.com'
];


export async function POST(request: Request) {
  try {
    const { username, password, email } = await request.json();

    // Validate presence of required fields
    if (!username || typeof username !== 'string' || username.trim() === '') {
      return NextResponse.json({ error: 'Username is required.' }, { status: 400 });
    }
    if (!password || typeof password !== 'string' || password.length < 6) { 
      return NextResponse.json({ error: 'Password is required and must be at least 6 characters.' }, { status: 400 });
    }
    if (!email || typeof email !== 'string' || email.trim() === '') {
        return NextResponse.json({ error: 'Email is required.' }, { status: 400 });
    }
    
    const trimmedUsername = username.trim();
    const trimmedEmail = email.trim().toLowerCase(); // Normalize email to lowercase for comparison and storage

    // 1. Check if the provided email is in the whitelist
    if (!EMAIL_REGISTRATION_WHITELIST.includes(trimmedEmail)) {
        return NextResponse.json({ error: 'Registration for this email address is not allowed.' }, { status: 403 });
    }

    // 2. Check if username already exists (findUserByUsername handles this)
    // Note: createUser in auth-db.ts will also check for username and email uniqueness
    // if database constraints (UNIQUE) are set on those columns in public.users table.
    const existingUserByUsername = await findUserByUsername(trimmedUsername);
    if (existingUserByUsername) {
      return NextResponse.json({ error: 'Username already taken. Please choose another.' }, { status: 409 });
    }
    
    // 3. Create user in public.users table
    // The createUser function in auth-db.ts handles hashing, schema name generation,
    // and should ideally also check for email uniqueness before insertion if not handled by DB constraint.
    // Pass the lowercased email to createUser.
    const newUser = await createUser(trimmedUsername, password, trimmedEmail);

    // 4. Create user-specific schema using the schema name from the new user record
    await createUserSchema(newUser.user_schema_name);

    // 5. Replicate tables from public schema to user's schema
    await replicateTablesToUserSchema(newUser.user_schema_name, TABLES_TO_REPLICATE);
    
    return NextResponse.json({
      message: `User '${newUser.username}' registered successfully with email '${newUser.email}'. Schema '${newUser.user_schema_name}' created and tables replicated.`,
      user: { 
        id: newUser.id,
        username: newUser.username,
        email: newUser.email, // email is now definitely part of newUser
        userSchema: newUser.user_schema_name,
      },
    }, { status: 201 });

  } catch (error) {
    console.error('Registration error:', error);
    const errorMessage = error instanceof Error ? error.message : 'An unexpected error occurred during registration.';
    
    // Handle specific error messages for better client feedback
    if (errorMessage.toLowerCase().includes("username already exists") || 
        errorMessage.toLowerCase().includes("username already taken") ||
        errorMessage.toLowerCase().includes("unique constraint failed") && errorMessage.toLowerCase().includes("username")) { // More specific check for username
        return NextResponse.json({ error: 'Username already taken. Please choose another.' }, { status: 409 });
    }
    if (errorMessage.toLowerCase().includes("email already exists") || 
        errorMessage.toLowerCase().includes("email already in use") ||
        errorMessage.toLowerCase().includes("unique constraint failed") && errorMessage.toLowerCase().includes("email")) { // More specific check for email
        return NextResponse.json({ error: 'Email address already registered.' }, { status: 409 });
    }
    if (errorMessage.toLowerCase().includes("generated schema name") && errorMessage.toLowerCase().includes("already in use")){
         return NextResponse.json({ error: 'A user account conflict occurred. Please try a slightly different username.' }, { status: 409 });
    }

    return NextResponse.json({ error: 'Registration failed. Please try again later.' }, { status: 500 });
  }
}

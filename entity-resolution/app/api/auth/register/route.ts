// app/api/auth/register/route.ts
import { NextResponse, NextRequest } from 'next/server';
import { fetchFromGateway, handleGatewayError } from '@/utils/gateway-client'; // Import gateway client

// Expected response structure from the gateway's /auth/register endpoint
interface GatewayRegisterResponse {
  message: string;
  user: {
    id: string;
    username: string;
    email?: string;
    userSchemaName: string; // Note: field name from Rust model
    // Include other fields if your gateway returns them
  };
}

export async function POST(request: NextRequest) {
  try {
    const { username, password, email } = await request.json();

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
    const trimmedEmail = email.trim(); // Gateway will handle lowercasing if needed

    // Call the Rust gateway's register endpoint
    const gatewayResponse = await fetchFromGateway<GatewayRegisterResponse>(
      '/auth/register', // Path to the gateway's register endpoint
      {
        method: 'POST',
        headers: { // Explicitly set Content-Type to application/json
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          username: trimmedUsername,
          password,
          email: trimmedEmail,
        }),
        // No userSchema header needed for registration itself
      }
    );

    // The gateway handles user creation, schema creation, table replication, and uniqueness checks.
    
    const gatewayUser = gatewayResponse.user;

    return NextResponse.json({
      message: gatewayResponse.message, // Use message from gateway
      user: {
        id: gatewayUser.id,
        username: gatewayUser.username,
        email: gatewayUser.email,
        userSchema: gatewayUser.userSchemaName, // Use userSchemaName from gateway
      },
    }, { status: 201 });

  } catch (error: any) {
    console.error('Registration error:', error);
    // Use handleGatewayError for errors originating from the gateway call
    // You might want to map specific gateway error messages/codes to user-friendly messages
    if (error.responseBody?.message?.toLowerCase().includes("username already taken")) {
        return NextResponse.json({ error: 'Username already taken. Please choose another.' }, { status: 409 });
    }
    if (error.responseBody?.message?.toLowerCase().includes("email address already registered")) {
        return NextResponse.json({ error: 'Email address already registered.' }, { status: 409 });
    }
    return handleGatewayError(error, 'Registration failed. Please try again later.');
  }
}

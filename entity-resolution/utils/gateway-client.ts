// utils/gateway-client.ts
import { NextResponse } from "next/server";

const GATEWAY_BASE_URL =
  process.env.GATEWAY_BASE_URL || "http://localhost:4001/v1";
const GATEWAY_API_KEY = process.env.GATEWAY_API_KEY;

interface FetchOptions extends RequestInit {
  params?: Record<string, string | number | boolean | undefined>;
}

// Team context interface for the new system
export interface TeamContext {
  teamSchema: string;  // The team's schema name (e.g., "alpha")
  userPrefix: string;  // The user's prefix within the team (e.g., "john")
}

/**
 * A helper function to make requests to the Rust data gateway.
 * It automatically includes the GATEWAY_API_KEY and team context headers.
 *
 * @param path - The API path (e.g., "/clusters", "/connections/123").
 * @param options - Fetch options (method, body, etc.).
 * @param teamContext - Team context with schema and user prefix for the new team-based system.
 * @returns A Promise resolving to the JSON response from the gateway.
 */
// Debug version of fetchFromGateway function
// Add this debugging to your gateway-client.ts

export async function fetchFromGateway<T = any>(
  path: string,
  options: FetchOptions = {},
  teamContext?: TeamContext | null
): Promise<T> {
  console.log("=== GATEWAY CLIENT DEBUG ===");
  console.log("Path:", path);
  console.log("Team context received:", teamContext);
  
  if (!GATEWAY_API_KEY) {
    console.error(
      "GATEWAY_API_KEY is not defined. Cannot make requests to the gateway."
    );
    throw new Error(
      "Gateway API key is missing. Please check server configuration."
    );
  }

  const { params, ...fetchOpts } = options;
  let url = `${GATEWAY_BASE_URL}${path}`;

  if (params) {
    const queryParams = new URLSearchParams();
    Object.entries(params).forEach(([key, value]) => {
      if (value !== undefined) {
        queryParams.append(key, String(value));
      }
    });
    if (queryParams.toString()) {
      url += `?${queryParams.toString()}`;
    }
  }

  console.log("Final URL:", url);

  const headers = new Headers(fetchOpts.headers || {});
  headers.set("X-API-Key", GATEWAY_API_KEY);
  
  // Set team context headers if provided
  if (teamContext) {
    console.log("Setting team context headers:");
    console.log("  - X-Team-Schema:", teamContext.teamSchema);
    console.log("  - X-User-Prefix:", teamContext.userPrefix);
    
    headers.set("X-Team-Schema", teamContext.teamSchema);
    headers.set("X-User-Prefix", teamContext.userPrefix);
  } else {
    console.log("⚠️  No team context provided - headers will be missing!");
  }
  
  if (
    fetchOpts.body &&
    typeof fetchOpts.body === "object" &&
    !(fetchOpts.body instanceof FormData)
  ) {
    if (!headers.has("Content-Type")) {
      headers.set("Content-Type", "application/json");
    }
  }

  // Log all headers being sent
  console.log("All headers being sent:");
  headers.forEach((value, key) => {
    // Don't log the actual API key for security
    if (key.toLowerCase() === 'x-api-key') {
      console.log(`  - ${key}: [REDACTED]`);
    } else {
      console.log(`  - ${key}: ${value}`);
    }
  });

  try {
    console.log("Making request to:", url);
    const response = await fetch(url, {
      ...fetchOpts,
      headers,
    });

    console.log("Response status:", response.status);
    console.log("Response ok:", response.ok);

    if (!response.ok) {
      let errorData;
      try {
        errorData = await response.json();
        console.log("Error response data:", errorData);
      } catch (e) {
        errorData = {
          message:
            response.statusText ||
            `Gateway request failed with status ${response.status}`,
        };
        console.log("Could not parse error response as JSON");
      }
      
      const errorMessage =
        errorData.error ||
        errorData.message ||
        `Gateway request to ${path} failed with status ${response.status}`;
      
      console.error(
        `Gateway API Error (${response.status}) for ${fetchOpts.method} ${url}:`,
        errorMessage,
        errorData
      );
      
      const error = new Error(errorMessage) as any;
      error.status = response.status;
      error.responseBody = errorData;
      throw error;
    }

    if (response.status === 204) {
      console.log("204 response - returning empty object");
      return {} as T;
    }

    const responseData = await response.json();
    console.log("✅ Success! Response received");
    console.log("=== END GATEWAY CLIENT DEBUG ===");
    return responseData as Promise<T>;
  } catch (error) {
    console.error("❌ Network or other error when calling gateway:", error);
    console.log("=== END GATEWAY CLIENT DEBUG ===");
    throw error;
  }
}

/**
 * Handles errors from gateway calls and returns an appropriate NextResponse.
 * @param error The error object.
 * @param defaultMessage A default message if the error doesn't have one.
 * @param defaultStatus A default HTTP status code.
 * @returns NextResponse
 */
export function handleGatewayError(
  error: any,
  defaultMessage: string = "An error occurred",
  defaultStatus: number = 500
): NextResponse {
  console.error("Handling gateway error:", error);
  const message =
    error.responseBody?.message ||
    error.responseBody?.error ||
    error.message ||
    defaultMessage;
  const status = error.status || defaultStatus;
  return NextResponse.json(
    { error: message, details: error.responseBody?.details },
    { status }
  );
}
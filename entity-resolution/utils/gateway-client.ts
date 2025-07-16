/*
================================================================================
|
|   File: /utils/gateway-client.ts
|
|   Description: Low-level client for fetching data from the backend Gateway API.
|   - Merged production-ready error handling and the `opinionName` feature.
|   - Adds the `X-Opinion-Name` header to outgoing requests to the gateway.
|
================================================================================
*/
import { NextResponse } from "next/server";

// Define TeamContext based on what's passed in headers
export interface TeamContext {
  teamSchema: string;
  userPrefix: string;
}

// Define options for the fetch call, including query parameters
export interface FetchOptions extends RequestInit {
  params?: Record<string, string | number | boolean>;
}

// Custom error class to hold more context from the gateway
export class GatewayError extends Error {
  status: number;
  responseBody: any;

  constructor(message: string, status: number, responseBody: any) {
    super(message);
    this.name = "GatewayError";
    this.status = status;
    this.responseBody = responseBody;
  }
}

const GATEWAY_BASE_URL = process.env.GATEWAY_BASE_URL || "http://localhost:4001/v1";
const GATEWAY_API_KEY = process.env.GATEWAY_API_KEY;

if (!GATEWAY_BASE_URL || !GATEWAY_API_KEY) {
  throw new Error(
    "GATEWAY_BASE_URL and GATEWAY_API_KEY must be set in environment variables."
  );
}

/**
 * A generic fetch wrapper for communicating with the backend Gateway API.
 *
 * @param path The API endpoint path (e.g., "/clusters").
 * @param options Standard fetch options, plus an optional `params` object for query strings.
 * @param teamContext Optional team context to set `X-Team-Schema` and `X-User-Prefix` headers.
 * @param opinionName Optional name of the opinion to set the `X-Opinion-Name` header.
 * @returns The JSON response from the gateway.
 * @throws {GatewayError} If the fetch request fails or returns a non-2xx status.
 */
export async function fetchFromGateway<T = any>(
  path: string,
  options: FetchOptions = {},
  teamContext?: TeamContext | null,
  opinionName?: string | null // ✨ NEW PARAMETER
): Promise<T> {
  const { params, ...fetchOptions } = options;

  // Construct URL with query parameters
  const url = new URL(`${GATEWAY_BASE_URL}${path}`);
  if (params) {
    Object.entries(params).forEach(([key, value]) => {
      // Ensure value is not undefined before appending
      if (value !== undefined) {
        url.searchParams.append(key, String(value));
      }
    });
  }

  // Set up headers
  const headers = new Headers(fetchOptions.headers || {});
  headers.set("X-API-Key", GATEWAY_API_KEY || "");
  if (teamContext) {
    headers.set("X-Team-Schema", teamContext.teamSchema);
    headers.set("X-User-Prefix", teamContext.userPrefix);
  }

  // ✨ ADD NEW OPINION HEADER
  if (opinionName) {
    headers.set("X-Opinion-Name", opinionName);
  }

  if (fetchOptions.body && !headers.has("Content-Type")) {
    headers.set("Content-Type", "application/json");
  }

  const config: RequestInit = {
    ...fetchOptions,
    headers,
  };

  try {
    const response = await fetch(url.toString(), config);

    if (!response.ok) {
      let errorBody;
      try {
        errorBody = await response.json();
      } catch (e) {
        errorBody = { message: "Failed to parse error response from gateway." };
      }
      throw new GatewayError(
        `Gateway request failed: ${response.statusText}`,
        response.status,
        errorBody
      );
    }

    // Handle cases with no content
    if (response.status === 204) {
      return {} as T;
    }

    return await response.json();
  } catch (error) {
    if (error instanceof GatewayError) {
      // Re-throw gateway errors to be handled by the caller
      throw error;
    }
    // Handle network errors or other fetch-related issues
    console.error("Network or fetch error in fetchFromGateway:", error);
    throw new GatewayError(
      (error as Error).message || "An unexpected network error occurred.",
      500,
      { message: "Network Error" }
    );
  }
}

/**
 * Handles GatewayErrors in Next.js API routes, returning a standardized NextResponse.
 * @param error The error object, expected to be a GatewayError.
 * @param defaultMessage A fallback message if the error has no specific message.
 * @returns A NextResponse object with the appropriate status and error message.
 */
export function handleGatewayError(
  error: any,
  defaultMessage: string = "An internal server error occurred."
): NextResponse {
  if (error instanceof GatewayError) {
    return NextResponse.json(
      {
        error: error.responseBody?.message || error.message,
        details: error.responseBody?.details,
      },
      {
        status: error.status,
      }
    );
  }

  console.error("Non-GatewayError caught in handleGatewayError:", error);
  return NextResponse.json({ error: defaultMessage }, {
    status: 500,
  });
}

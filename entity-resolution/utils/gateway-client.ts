// utils/gateway-client.ts
import { NextResponse } from "next/server";

const GATEWAY_BASE_URL =
  process.env.GATEWAY_BASE_URL || "http://localhost:4001/v1";
const GATEWAY_API_KEY = process.env.GATEWAY_API_KEY;

interface FetchOptions extends RequestInit {
  params?: Record<string, string | number | boolean | undefined>;
}

/**
 * A helper function to make requests to the Rust data gateway.
 * It automatically includes the GATEWAY_API_KEY in the headers.
 *
 * @param path - The API path (e.g., "/clusters", "/connections/123").
 * @param options - Fetch options (method, body, etc.).
 * @param userSchema - Optional user schema to pass as a header.
 * @returns A Promise resolving to the JSON response from the gateway.
 */
export async function fetchFromGateway<T = any>(
  path: string,
  options: FetchOptions = {},
  userSchema?: string | null
): Promise<T> {
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

  const headers = new Headers(fetchOpts.headers || {});
  headers.set("X-API-Key", GATEWAY_API_KEY);
  if (userSchema) {
    headers.set("X-User-Schema", userSchema); // Assuming your gateway expects user schema this way
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

  try {
    const response = await fetch(url, {
      ...fetchOpts,
      headers,
    });

    if (!response.ok) {
      let errorData;
      try {
        errorData = await response.json();
      } catch (e) {
        // If response is not JSON, use status text or a generic message
        errorData = {
          message:
            response.statusText ||
            `Gateway request failed with status ${response.status}`,
        };
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
      // Construct an error object that mimics NextResponse for consistent error handling upstream if needed
      const error = new Error(errorMessage) as any;
      error.status = response.status;
      error.responseBody = errorData;
      throw error;
    }

    // Handle cases where the gateway might return no content (e.g., 204)
    if (response.status === 204) {
      return {} as T; // Or null, or undefined, depending on expected behavior
    }

    return response.json() as Promise<T>;
  } catch (error) {
    console.error(
      `Network or other error when calling gateway path ${path}:`,
      error
    );
    // Re-throw the error to be handled by the calling API route
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

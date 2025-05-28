import { NextApiResponse } from 'next';

export class ApiError extends Error {
  statusCode: number;
  details?: any;

  constructor(message: string, statusCode: number = 500, details?: any) {
    super(message);
    this.statusCode = statusCode;
    this.details = details;
    Object.setPrototypeOf(this, ApiError.prototype);
  }
}

export function handleApiError(error: unknown, res: NextApiResponse) {
  console.error('API Error:', error);
  
  if (error instanceof ApiError) {
    return res.status(error.statusCode).json({
      success: false,
      message: error.message,
      ...(error.details && { details: error.details }),
    });
  }

  const errorMessage = error instanceof Error ? error.message : 'An unknown error occurred';
  return res.status(500).json({
    success: false,
    message: 'Internal Server Error',
    error: errorMessage,
  });
}

export function validateRequestMethod(req: any, allowedMethods: string[]) {
  if (!allowedMethods.includes(req.method || '')) {
    throw new ApiError(
      `Method ${req.method} not allowed`,
      405,
      { allowedMethods }
    );
  }
}

export function validateRequiredFields(payload: Record<string, any>, requiredFields: string[]) {
  const missingFields = requiredFields.filter(field => {
    const value = payload[field];
    return value === undefined || value === null || value === '';
  });

  if (missingFields.length > 0) {
    throw new ApiError(
      `Missing required fields: ${missingFields.join(', ')}`,
      400,
      { missingFields }
    );
  }
}

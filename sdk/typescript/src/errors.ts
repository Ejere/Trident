import { z } from "zod";

export type TridentErrorCode =
  | "CONFIG"
  | "NOT_FOUND"
  | "UNAUTHORIZED"
  | "RATE_LIMITED"
  | "QUOTA_EXCEEDED"
  | "INVALID_ARGUMENT"
  | "TIMEOUT"
  | "INTERNAL"
  | "ITERATION_LIMIT"
  | "RETRY_EXHAUSTED";

export class TridentError extends Error {
  readonly code: TridentErrorCode;
  readonly cause?: unknown;
  /** Number of attempts made before this error was thrown (>1 if retried). */
  attempts: number;

  constructor(code: TridentErrorCode, message: string, cause?: unknown) {
    super(message);
    this.name = "TridentError";
    this.code = code;
    this.cause = cause;
    this.attempts = 1;
  }
}

/**
 * Structured error thrown by SDK methods on all non-2xx API responses.
 * Carries the HTTP status code, machine-readable error code, human-readable
 * message, and the optional field that caused a validation failure.
 */
export class TridentApiError extends Error {
  readonly status: number;
  readonly code: string;
  readonly field?: string;
  /** Number of attempts made before this error was thrown (>1 if retried). */
  attempts: number;

  constructor(status: number, code: string, message: string, field?: string) {
    super(message);
    this.name = "TridentApiError";
    this.status = status;
    this.code = code;
    this.field = field;
    this.attempts = 1;
  }
}

const ApiErrorEnvelopeSchema = z.object({
  error: z.object({
    code: z.string(),
    message: z.string(),
    field: z.string().optional(),
  }),
});

/**
 * Parse a non-2xx response body into a TridentApiError.
 * Falls back to code="INTERNAL" when the body is not a valid error envelope.
 */
export function parseApiError(status: number, body: string): TridentApiError {
  try {
    const parsed = ApiErrorEnvelopeSchema.parse(JSON.parse(body));
    const { code, message, field } = parsed.error;
    return new TridentApiError(status, code, message, field);
  } catch {
    return new TridentApiError(status, "INTERNAL", body || `HTTP ${status}`);
  }
}

/** @deprecated Use parseApiError for structured errors from the API. */
export function httpStatusToError(
  status: number,
  body: string,
): TridentError {
  switch (status) {
    case 401:
      return new TridentError("UNAUTHORIZED", body || "Unauthorized");
    case 404:
      return new TridentError("NOT_FOUND", body || "Not found");
    case 429:
      return new TridentError("RATE_LIMITED", body || "Rate limit exceeded");
    default:
      return new TridentError("INTERNAL", body || `HTTP ${status}`);
  }
}

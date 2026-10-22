/** Thrown for any failure reported by a RyeDB endpoint. */
export class RyeDBError extends Error {
  readonly code: string;
  readonly status: number | undefined;
  constructor(code: string, message: string, status?: number) {
    super(message);
    this.name = "RyeDBError";
    this.code = code;
    this.status = status;
  }
}

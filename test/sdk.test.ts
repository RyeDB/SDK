import { test } from "node:test";
import assert from "node:assert/strict";
import { RyeDBError, SDK_VERSION } from "../src/index.ts";

test("exports a version", () => {
  assert.match(SDK_VERSION, /^\d+\.\d+\.\d+$/);
});

test("RyeDBError carries code and status", () => {
  const e = new RyeDBError("denied", "no", 403);
  assert.equal(e.name, "RyeDBError");
  assert.equal(e.code, "denied");
  assert.equal(e.status, 403);
  assert.ok(e instanceof Error);
});

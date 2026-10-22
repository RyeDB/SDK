import { test } from "node:test";
import assert from "node:assert/strict";
import { createServer, type IncomingMessage } from "node:http";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { RyeChannel, RyeDBError } from "../src/index.ts";

const here = dirname(fileURLToPath(import.meta.url));
const spec = JSON.parse(readFileSync(join(here, "../contracts/channel-port.openapi.json"), "utf8")) as { paths: Record<string, Record<string, unknown>> };

const msg = (seq: number, body = "hi") => ({ seq, channel: "main", from_agent: "a", to_agent: "b", kind: "note", body, reply_to: null, via: "port", auth: 0, ts: 1 });

/** A stand-in daemon that answers the documented routes and records what it was sent. */
async function fake(handler: (req: IncomingMessage, url: URL, body: string, res: import("node:http").ServerResponse) => void) {
  const seen: { method: string; path: string; headers: IncomingMessage["headers"]; body: string }[] = [];
  const server = createServer((req, res) => {
    let b = "";
    req.on("data", (c) => (b += c));
    req.on("end", () => {
      const url = new URL(req.url!, "http://x");
      seen.push({ method: req.method!, path: url.pathname + url.search, headers: req.headers, body: b });
      handler(req, url, b, res);
    });
  });
  await new Promise<void>((r) => server.listen(0, "127.0.0.1", r));
  const port = (server.address() as import("node:net").AddressInfo).port;
  return { port, seen, close: () => new Promise<void>((r) => { server.closeAllConnections(); server.close(() => r()); }) };
}
const json = (res: import("node:http").ServerResponse, code: number, v: unknown) => { res.writeHead(code, { "content-type": "application/json" }); res.end(JSON.stringify(v)); };

test("a client needs a credential", () => {
  assert.throws(() => new RyeChannel({ port: 1 }), (e: unknown) => e instanceof RyeDBError && e.code === "no_credential");
});

test("with an agent credential the client sends it and never names a sender", async () => {
  const f = await fake((_r, _u, _b, res) => json(res, 200, msg(1)));
  try {
    const c = new RyeChannel({ port: f.port, agentToken: "ryc_x", agent: "ignored" });
    await c.post({ body: "hello", to: "bob", kind: "task", from: "mallory" });
    const s = f.seen[0]!;
    assert.equal(s.headers["x-ryedb-agent-token"], "ryc_x");
    assert.equal(s.headers["x-ryedb-token"], undefined);
    assert.deepEqual(JSON.parse(s.body), { body: "hello", to: "bob", kind: "task" });
  } finally { await f.close(); }
});

test("with the project token the client names the agent in the body and in queries", async () => {
  const f = await fake((_r, u, _b, res) => json(res, 200, u.pathname === "/channel/read" ? [msg(3)] : { acked: [3], state: "received" }));
  try {
    const c = new RyeChannel({ port: f.port, projectToken: "tok", agent: "worker-1" });
    const got = await c.read({ after: 2 });
    assert.equal(got[0]!.seq, 3);
    const q0 = new URL(f.seen[0]!.path, "http://x");
    assert.equal(q0.pathname, "/channel/read");
    assert.deepEqual(Object.fromEntries(q0.searchParams), { after: "2", agent: "worker-1" });
    assert.deepEqual(await c.ack([3, 4], { state: "done" }), [3]);
    assert.deepEqual(JSON.parse(f.seen[1]!.body), { seqs: [3, 4], state: "done" });
    assert.equal(f.seen[1]!.path, "/channel/ack?agent=worker-1");
  } finally { await f.close(); }
});

test("errors carry the server's message and a code by status", async () => {
  for (const [status, code] of [[401, "unauthorized"], [403, "forbidden"], [429, "rate_limited"], [400, "bad_request"]] as const) {
    const f = await fake((_r, _u, _b, res) => json(res, status, { error: "nope " + status }));
    try {
      const c = new RyeChannel({ port: f.port, projectToken: "t", agent: "a" });
      await assert.rejects(c.presence(), (e: unknown) => e instanceof RyeDBError && e.code === code && e.status === status && e.message === "nope " + status);
    } finally { await f.close(); }
  }
});

test("stream yields messages, resumes after a dropped connection without loss or duplicates, and stops on abort", async () => {
  let connection = 0;
  const lasts: string[] = [];
  const f = await fake((req, _u, _b, res) => {
    connection++;
    lasts.push(String(req.headers["last-event-id"]));
    res.writeHead(200, { "content-type": "text/event-stream" });
    const frame = (m: ReturnType<typeof msg>) => `id: ${m.seq}\nevent: message\ndata: ${JSON.stringify(m)}\n\n`;
    if (connection === 1) { res.write(frame(msg(1, "one")) + frame(msg(2, "two"))); setTimeout(() => res.end(), 20); } // drops
    else res.write(frame(msg(2, "two again")) + frame(msg(3, "three"))); // replays 2 (must be skipped) then 3
  });
  try {
    const ac = new AbortController();
    const c = new RyeChannel({ port: f.port, projectToken: "t", agent: "a" });
    const got: string[] = [];
    for await (const m of c.stream({ signal: ac.signal })) {
      got.push(m.body);
      if (m.seq === 3) ac.abort();
    }
    assert.deepEqual(got, ["one", "two", "three"]);
    assert.deepEqual(lasts, ["0", "2"]);
  } finally { await f.close(); }
});

test("stream does not retry a wrong credential", async () => {
  const f = await fake((_r, _u, _b, res) => json(res, 401, { error: "no" }));
  try {
    const c = new RyeChannel({ port: f.port, agentToken: "bad" });
    await assert.rejects(async () => { for await (const _ of c.stream()) { /* none */ } }, (e: unknown) => e instanceof RyeDBError && e.code === "unauthorized");
    assert.equal(f.seen.length, 1);
  } finally { await f.close(); }
});

test("contract: the client calls exactly the paths the OpenAPI document defines", () => {
  const src = readFileSync(join(here, "../src/channel.ts"), "utf8");
  const used = new Set([...src.matchAll(/\/channel\/([a-z]+)/g)].map((m) => `/channel/${m[1]}`));
  assert.deepEqual([...used].sort(), Object.keys(spec.paths).sort());
});

// Runs against the real daemon when a core checkout is built next to this one (RYEDB_CORE=/path/to/core). CI skips it.
const core = process.env.RYEDB_CORE;
test("against the real daemon: credentials, tasks, receipts, stream", { skip: core ? false : "set RYEDB_CORE to a built RyeDB/core checkout" }, async () => {
  const { mkdtempSync, mkdirSync, writeFileSync, rmSync, realpathSync } = await import("node:fs");
  const { tmpdir } = await import("node:os");
  const net = await import("node:net");
  const { startDaemon } = (await import(join(core!, "packages/daemon/dist/index.js"))) as { startDaemon: (o: { root: string; idleMs: number }) => Promise<{ store: any; close(): Promise<void> }> };
  const root = realpathSync(mkdtempSync(join(tmpdir(), "ryedb-sdk-")));
  const port = await new Promise<number>((r) => { const s = net.createServer().listen(0, "127.0.0.1", () => { const p = (s.address() as import("node:net").AddressInfo).port; s.close(() => r(p)); }); });
  mkdirSync(join(root, ".ryedb"), { recursive: true });
  writeFileSync(join(root, ".ryedb", "config.json"), JSON.stringify({ httpPort: port }));
  writeFileSync(join(root, ".ryedb", "token"), "s".repeat(48));
  const d = await startDaemon({ root, idleMs: 60_000 });
  try {
    const lead = new RyeChannel({ port, agentToken: d.store.issueCredential("lead") });
    const worker = new RyeChannel({ port, agentToken: d.store.issueCredential("worker-1") });
    const ac = new AbortController();
    const seen: string[] = [];
    const reader = (async () => { for await (const m of worker.stream({ signal: ac.signal })) { seen.push(m.body); await worker.ack(m.seq); if (seen.length === 2) ac.abort(); } })();
    const task = await lead.post({ to: "worker-1", kind: "task", body: "rebuild the index" });
    assert.equal(task.auth, 1);
    assert.equal(task.from_agent, "lead");
    await lead.post({ to: "worker-1", body: "and tell me when done" });
    await reader;
    assert.deepEqual(seen, ["rebuild the index", "and tell me when done"]);
    assert.equal((await worker.tasks()).length, 1);
    await worker.post({ to: "lead", kind: "result", body: "index rebuilt", replyTo: task.seq });
    assert.equal((await worker.tasks()).length, 0, "a result reply closes the task");
    assert.equal((await worker.tasks({ state: "done" })).length, 1);
    await assert.rejects(new RyeChannel({ port, agentToken: "ryc_wrong" }).presence(), (e: unknown) => e instanceof RyeDBError && e.code === "unauthorized");
    assert.ok((await lead.presence()).some((a) => a.id === "worker-1"));
  } finally {
    await d.close();
    rmSync(root, { recursive: true, force: true });
  }
});

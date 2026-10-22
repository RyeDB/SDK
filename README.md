# RyeDB SDK

TypeScript client for RyeDB.

> **Status: scaffold.** This repository holds the package, build, tests and CI. There is no hosted RyeDB API yet, so there is no client surface to ship. Nothing here pretends otherwise.

## Plan

- **Contract first.** The client will be generated from the OpenAPI document for the hosted service, so the types and errors come from one source. The generated output will be committed and never edited by hand.
- **Same tools everywhere.** The operations mirror the agent tools already working locally in [`RyeDB/core`](https://github.com/RyeDB/core): `memory_brief`, `error_search`, `error_resolve`, `rule_propose`, `decision_record`, `task_set`, `channel_post`, `channel_read`.
- **Local first.** Programs that only need the local store can use `@ryedb/daemon` from `core` today. Agents use the tools through the [MCP server](https://github.com/RyeDB/mcp), whose machine-readable contract (`contracts/mcp-tools.json`) is the source this client will be generated from.

## Development

```sh
npm ci
npm run typecheck
npm test
```

Requires Node 22.18 or newer. Licensed under Apache-2.0.

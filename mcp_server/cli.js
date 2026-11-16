#!/usr/bin/env node
/**
 * Standalone entrypoint for the pure actual-mcp server (no NATS, no PocketBase SSE, no gateway-mcp).
 */
import { startServer } from "./server.js";

try {
  process.loadEnvFile?.();
} catch {}

startServer({ failFastConfig: false }).catch((err) => {
  console.error(`[Actual MCP] Fatal startup error: ${err.message}`);
  process.exit(1);
});

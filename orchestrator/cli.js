#!/usr/bin/env node
/**
 * Standalone entrypoint for running the actual-mcp orchestrator over Streamable HTTP.
 */
import { StreamableHttpToolCaller } from "./toolCaller.js";
import { GatewayActualConfigProvider } from "./providers.js";
import {
  startNatsTokenSubscriber,
  startPeriodicReceiptSync,
  startPocketBaseRealtimeSync,
} from "./runner.js";

try {
  process.loadEnvFile?.();
} catch {}

function parseFlag(name, fallback) {
  const idx = process.argv.indexOf(name);
  if (idx !== -1 && process.argv[idx + 1]) return process.argv[idx + 1];
  return fallback;
}

const mcpUrl = parseFlag("--mcp-url", process.env.ACTUAL_MCP_URL || "http://127.0.0.1:9991/mcp");
const gatewayUrl = parseFlag("--gateway-url", process.env.GATEWAY_URL || null);

const toolCaller = new StreamableHttpToolCaller(mcpUrl);
const configProvider = new GatewayActualConfigProvider(gatewayUrl);

console.error(`[actual-mcp orchestrator] Targeting ${mcpUrl}`);
void startNatsTokenSubscriber(configProvider);
startPocketBaseRealtimeSync(toolCaller, configProvider);
startPeriodicReceiptSync(toolCaller);

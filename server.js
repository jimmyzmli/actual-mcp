#!/usr/bin/env node
/**
 * Root service entrypoint for actual-mcp:
 * injects GatewayActualConfigProvider and runs pure mcp_server wrapped with orchestrator.
 */
import {
  handleExecuteTool,
  refreshActualConfig,
  setConfigProvider,
  startServer,
} from "./mcp_server/server.js";
import { GatewayActualConfigProvider } from "./orchestrator/providers.js";
import {
  startNatsTokenSubscriber,
  startPeriodicReceiptSync,
  startPocketBaseRealtimeSync,
} from "./orchestrator/runner.js";
import { InProcessToolCaller } from "./orchestrator/toolCaller.js";
import { installWebUiConsoleTee } from "./logger.js";

installWebUiConsoleTee();

async function main() {
  const mcpOnly = process.argv.includes("--mcp-only");

  let configProvider = null;
  if (!mcpOnly) {
    configProvider = new GatewayActualConfigProvider();
    setConfigProvider(configProvider);
  }

  // Never fail-fast on gateway-mcp at boot: Actual MCP and Gateway MCP share a PT3M boot trigger,
  // and ensureInit() already resolves Actual config lazily on the first Actual API call.
  await startServer({ failFastConfig: false });

  if (!mcpOnly && configProvider) {
    const toolCaller = new InProcessToolCaller(handleExecuteTool);
    void startNatsTokenSubscriber(configProvider, {
      onActualRefreshed: refreshActualConfig,
    });
    startPocketBaseRealtimeSync(toolCaller, configProvider);
    startPeriodicReceiptSync(toolCaller);
  }
}

const isDirectExecution = !process.argv[1] || (
  process.argv[1].endsWith("server.js") ||
  process.argv[1].endsWith("index.js") ||
  process.argv[1].endsWith("actual-mcp")
);

if (isDirectExecution) {
  main().catch((err) => {
    console.error(`[Actual MCP] Fatal startup error: ${err.message}`);
    process.exit(1);
  });
}

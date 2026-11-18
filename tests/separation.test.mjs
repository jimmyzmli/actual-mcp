import assert from "node:assert/strict";
import fs from "node:fs";
import net from "node:net";
import path from "node:path";
import { spawn } from "node:child_process";
import test from "node:test";
import { fileURLToPath } from "node:url";

import { handleExecuteTool, setConfigProvider } from "../mcp_server/server.js";
import { DefaultActualConfigProvider } from "../mcp_server/providers.js";
import { InProcessToolCaller, StreamableHttpToolCaller } from "../orchestrator/toolCaller.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT_DIR = path.resolve(__dirname, "..");

function getFreePort() {
  return new Promise((resolve, reject) => {
    const srv = net.createServer();
    srv.listen(0, "127.0.0.1", () => {
      const addr = srv.address();
      srv.close((err) => (err ? reject(err) : resolve(addr.port)));
    });
  });
}

test("mcp_server and pocketbase.js have no NATS or gateway-mcp coupling", () => {
  const mcpSrv = fs.readFileSync(path.join(ROOT_DIR, "mcp_server", "server.js"), "utf-8");
  const pbJs = fs.readFileSync(path.join(ROOT_DIR, "pocketbase.js"), "utf-8");
  assert.equal(mcpSrv.includes('from "nats"'), false);
  assert.equal(mcpSrv.includes("/api/secrets/actual"), false);
  assert.equal(pbJs.includes("/api/secrets/pocketbase"), false);
  assert.equal(pbJs.includes("startRealtimeListener"), false);
});

test("DefaultActualConfigProvider and InProcessToolCaller work standalone", async () => {
  setConfigProvider(new DefaultActualConfigProvider());
  const caller = new InProcessToolCaller(handleExecuteTool);
  const res = await caller.call("actual_list_receipts", { limit: 1 });
  assert.ok(res && typeof res === "object");
});

test("Standalone mcp_server/cli.js answers /health and StreamableHttpToolCaller", async () => {
  const port = await getFreePort();
  const env = { ...process.env };
  delete env.NATS_URL;
  delete env.BIND_PORT;
  delete env.PORT;
  delete env.BIND_HOST;
  delete env.HOST;

  const child = spawn(process.execPath, [path.join(ROOT_DIR, "mcp_server", "cli.js"), "--port", String(port), "--host", "127.0.0.1"], {
    cwd: ROOT_DIR,
    env,
    stdio: ["ignore", "pipe", "pipe"],
  });

  let stderrBuf = "";
  child.stderr.on("data", (d) => { stderrBuf += d.toString(); });

  try {
    const deadline = Date.now() + 15000;
    let healthy = false;
    while (Date.now() < deadline) {
      if (child.exitCode !== null) {
        assert.fail(`Standalone actual-mcp exited early (${child.exitCode}): ${stderrBuf}`);
      }
      try {
        const resp = await fetch(`http://127.0.0.1:${port}/health`);
        if (resp.ok) {
          healthy = true;
          break;
        }
      } catch {}
      await new Promise((r) => setTimeout(r, 200));
    }
    assert.equal(healthy, true, `Timed out waiting for /health: ${stderrBuf}`);

    const caller = new StreamableHttpToolCaller(`http://127.0.0.1:${port}/mcp`);
    const res = await caller.call("actual_list_receipts", { limit: 1 });
    assert.ok(res && typeof res === "object");
  } finally {
    child.kill("SIGTERM");
    await new Promise((r) => child.once("exit", r));
  }
});

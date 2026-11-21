import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

function loadAppConfig() {
  try {
    const cfgPath = path.resolve(__dirname, "..", "config", "app.json");
    if (fs.existsSync(cfgPath)) {
      return JSON.parse(fs.readFileSync(cfgPath, "utf-8")) || {};
    }
  } catch {
    // ignore
  }
  return {};
}

/**
 * Default .env / local file ConfigProvider for standalone pure-MCP mode.
 * Does not depend on gateway-mcp or NATS.
 */
export class DefaultActualConfigProvider {
  async getActualConfig() {
    const serverUrl = process.env.ACTUAL_SERVER_URL || "";
    const password = process.env.ACTUAL_PASSWORD || "";
    const syncId = process.env.ACTUAL_SYNC_ID || "";
    const encryptionPassword = process.env.ACTUAL_ENCRYPTION_PASSWORD || undefined;

    if (serverUrl && syncId) {
      return { serverUrl, password, syncId, encryptionPassword };
    }

    const rcPath = process.env.ACTUAL_RC_PATH || path.join(os.homedir(), ".actualrc");
    if (fs.existsSync(rcPath)) {
      const parsed = JSON.parse(fs.readFileSync(rcPath, "utf-8"));
      if (parsed.serverUrl && parsed.syncId) {
        return {
          serverUrl: parsed.serverUrl,
          password: parsed.password || "",
          syncId: parsed.syncId,
          encryptionPassword: parsed.encryptionPassword,
        };
      }
    }

    throw new Error(
      "Actual config unavailable. Set ACTUAL_SERVER_URL, ACTUAL_PASSWORD, ACTUAL_SYNC_ID (or ~/.actualrc)."
    );
  }

  invalidateActualConfig() {}

  async getPocketBaseAuth() {
    const appCfg = loadAppConfig();
    const baseUrl = (
      process.env.PB_UPLOAD_BASE_URL ||
      process.env.PB_BASE_URL ||
      appCfg.pocketbase_url ||
      "https://pb.lzn.ca"
    ).replace(/\/+$/, "");
    const apiToken = process.env.PB_API_TOKEN || null;
    const token = process.env.PB_TOKEN || apiToken || null;
    if (!token) {
      throw new Error("PocketBase token unavailable. Set PB_API_TOKEN or PB_TOKEN in env/.env.");
    }
    return { baseUrl, token, apiToken };
  }

  invalidatePocketBaseToken() {}
}

let activeProvider = new DefaultActualConfigProvider();

export function setConfigProvider(provider) {
  activeProvider = provider;
}

export function getConfigProvider() {
  return activeProvider;
}

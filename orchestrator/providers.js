import os from "node:os";

/**
 * Gateway-backed ConfigProvider injected when actual-mcp runs wrapped.
 * Fetches Actual and PocketBase credentials from gateway-mcp and supports NATS invalidation.
 */
export class GatewayActualConfigProvider {
  constructor(gatewayUrl = null) {
    this.gatewayUrl = (
      gatewayUrl ||
      process.env.GATEWAY_URL ||
      (os.platform() === "darwin" ? "http://pc.lzn.ca:9999" : "http://127.0.0.1:9999")
    ).replace(/\/+$/, "");
    this.cachedPbToken = null;
    this.pbTokenExpiry = 0;
    this.pbBaseUrl = (process.env.PB_UPLOAD_BASE_URL || process.env.PB_BASE_URL || "https://pb.lzn.ca").replace(/\/+$/, "");
    this.pbApiToken = process.env.PB_API_TOKEN || null;
  }

  async getActualConfig() {
    const resp = await fetch(`${this.gatewayUrl}/api/actual/token`, {
      signal: AbortSignal.timeout(5000),
    });
    if (!resp.ok) {
      throw new Error(`Failed to fetch Actual config from ${this.gatewayUrl}/api/actual/token (HTTP ${resp.status})`);
    }
    const data = await resp.json();
    if (!data.server_url || !data.sync_id) {
      throw new Error(`Incomplete Actual config returned by ${this.gatewayUrl}/api/actual/token`);
    }
    return {
      serverUrl: data.server_url,
      password: data.password,
      syncId: data.sync_id,
      encryptionPassword: data.encryption_password,
    };
  }

  invalidateActualConfig() {}

  async getPocketBaseAuth() {
    const now = Date.now();
    if (this.cachedPbToken && this.pbTokenExpiry > now + 60000) {
      return {
        baseUrl: this.pbBaseUrl,
        token: this.cachedPbToken,
        apiToken: this.pbApiToken,
      };
    }

    const resp = await fetch(`${this.gatewayUrl}/api/pocketbase/token?role=actual_orch`, {
      signal: AbortSignal.timeout(5000),
    });
    if (!resp.ok) {
      throw new Error(`Failed to fetch PocketBase token from gateway-mcp (HTTP ${resp.status})`);
    }
    const data = await resp.json();
    if (data.base_url) this.pbBaseUrl = String(data.base_url).replace(/\/+$/, "");
    if (data.api_token) this.pbApiToken = data.api_token;
    if (data.token) {
      this.cachedPbToken = data.token;
      this.pbTokenExpiry = data.expires_at ? Number(data.expires_at) * 1000 : now + 3600 * 1000;
      return {
        baseUrl: this.pbBaseUrl,
        token: this.cachedPbToken,
        apiToken: this.pbApiToken,
      };
    }
    if (this.pbApiToken) {
      return {
        baseUrl: this.pbBaseUrl,
        token: this.pbApiToken,
        apiToken: this.pbApiToken,
      };
    }
    throw new Error("PocketBase token missing in gateway-mcp /api/pocketbase/token response");
  }

  invalidatePocketBaseToken() {
    this.cachedPbToken = null;
    this.pbTokenExpiry = 0;
  }
}

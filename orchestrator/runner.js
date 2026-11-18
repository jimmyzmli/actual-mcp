import os from "node:os";
import { connect as connectNats } from "nats";

export async function startNatsTokenSubscriber(configProvider, { onActualRefreshed = null, natsUrl = null } = {}) {
  const url =
    natsUrl ||
    process.env.NATS_URL ||
    (os.platform() === "darwin" ? "nats://pc.lzn.ca:4222" : "nats://127.0.0.1:4222");

  try {
    const nc = await connectNats({
      servers: url,
      name: "actual-mcp",
      reconnect: true,
      maxReconnectAttempts: -1,
      reconnectTimeWait: 2000,
    });

    const subToken = nc.subscribe("api.token.refreshed");
    (async () => {
      for await (const msg of subToken) {
        let provider = "";
        try {
          const parsed = msg.data?.length ? JSON.parse(new TextDecoder().decode(msg.data)) : {};
          provider = String(parsed?.provider || "").trim().toLowerCase();
        } catch {
          continue;
        }
        if (provider === "pocketbase") {
          configProvider.invalidatePocketBaseToken?.();
        } else if (provider === "actual") {
          try {
            configProvider.invalidateActualConfig?.();
            if (onActualRefreshed) {
              await onActualRefreshed();
            }
          } catch (err) {
            console.error(`[Actual MCP] Failed to refresh Actual config on api.token.refreshed: ${err.message}`);
          }
        }
      }
    })().catch(() => {});

    return nc;
  } catch (err) {
    console.error(`[Actual MCP] NATS connection failed (${url}): ${err.message}`);
    return null;
  }
}

export function startPocketBaseRealtimeSync(toolCaller, configProvider) {
  const abortController = new AbortController();
  const { signal } = abortController;
  let reconnectTimer = null;

  const connect = async () => {
    if (signal.aborted) return;
    try {
      const auth = await configProvider.getPocketBaseAuth();
      const baseUrl = String(auth.baseUrl || "https://pb.lzn.ca").replace(/\/+$/, "");
      const headers = {};
      if (auth.token) {
        if (auth.apiToken && auth.token === auth.apiToken) {
          headers["x-api-token"] = auth.token;
        } else {
          headers["Authorization"] = auth.token;
        }
      }

      const res = await fetch(`${baseUrl}/api/realtime`, { headers, signal });
      if (!res.ok || !res.body) {
        throw new Error(`Realtime connection failed: HTTP ${res.status}`);
      }

      const reader = res.body.getReader();
      const decoder = new TextDecoder();
      let buffer = "";

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });
        const parts = buffer.split("\n\n");
        buffer = parts.pop() || "";

        for (const rawEvent of parts) {
          const trimmed = rawEvent.trim();
          if (!trimmed) continue;

          const lines = trimmed.split("\n");
          let eventType = "message";
          let eventData = "";
          for (const line of lines) {
            if (line.startsWith("event:")) {
              eventType = line.slice(6).trim();
            } else if (line.startsWith("data:")) {
              eventData += line.slice(5).trim();
            }
          }

          if (eventType === "PB_CONNECT") {
            try {
              const parsed = JSON.parse(eventData);
              const clientId = parsed.clientId;
              await fetch(`${baseUrl}/api/realtime`, {
                method: "POST",
                headers: { "Content-Type": "application/json", ...headers },
                body: JSON.stringify({ clientId, subscriptions: ["receipts/*"] }),
                signal,
              });
              console.error(`[PocketBase Realtime] Subscribed to receipts/* (clientId: ${clientId})`);
            } catch (subErr) {
              console.error(`[PocketBase Realtime] Subscription error: ${subErr.message}`);
            }
          } else if (eventData) {
            try {
              const payload = JSON.parse(eventData);
              if (payload.action && payload.record?.id) {
                await toolCaller.call("actual_sync_receipts", { id: payload.record.id });
                console.error(`[PocketBase Realtime] ${payload.action} refreshed record ${payload.record.id}`);
              }
            } catch (err) {
              console.error(`[PocketBase Realtime] Event refresh error: ${err.message}`);
            }
          }
        }
      }
    } catch (err) {
      if (!signal.aborted) {
        console.error(`[PocketBase Realtime] Stream disconnected (${err.message}), reconnecting in 5s...`);
        reconnectTimer = setTimeout(connect, 5000);
      }
    }
  };

  void connect();
  return () => {
    abortController.abort();
    if (reconnectTimer) clearTimeout(reconnectTimer);
  };
}

export function startPeriodicReceiptSync(toolCaller, intervalMs = 15 * 60 * 1000) {
  // Initial backfill check if local SQLite cache is empty or unmigrated
  toolCaller.call("actual_sync_receipts", { initial_if_empty: true }).catch((err) => {
    console.error(`[PocketBase] Initial receipt sync check error: ${err.message}`);
  });

  const timer = setInterval(() => {
    toolCaller.call("actual_sync_receipts", {}).catch((err) => {
      console.error(`[PocketBase Background Sync] Error: ${err.message}`);
    });
  }, intervalMs);
  timer.unref?.();
  return () => clearInterval(timer);
}

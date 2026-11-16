import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";

export class InProcessToolCaller {
  constructor(handler) {
    this.handler = handler;
  }

  async call(name, args = {}) {
    const res = await this.handler(name, args);
    const text = res?.content?.[0]?.text;
    if (typeof text === "string") {
      try {
        return JSON.parse(text);
      } catch {
        return text;
      }
    }
    return res;
  }
}

export class StreamableHttpToolCaller {
  constructor(mcpUrl) {
    this.mcpUrl = String(mcpUrl).replace(/\/+$/, "");
  }

  async call(name, args = {}) {
    const client = new Client({ name: "actual-mcp-orchestrator", version: "1.0.0" }, { capabilities: {} });
    const transport = new StreamableHTTPClientTransport(new URL(this.mcpUrl));
    try {
      await client.connect(transport);
      const res = await client.callTool({ name, arguments: args });
      if (res?.isError) {
        throw new Error(res?.content?.[0]?.text || `MCP tool '${name}' failed`);
      }
      if (res?.structuredContent !== undefined) {
        return res.structuredContent;
      }
      const text = res?.content?.[0]?.text;
      if (typeof text === "string") {
        try {
          return JSON.parse(text);
        } catch {
          return text;
        }
      }
      return res;
    } finally {
      await client.close().catch(() => {});
    }
  }
}

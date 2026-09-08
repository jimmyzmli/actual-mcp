import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { DatabaseSync } from "node:sqlite";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Load .env file if present
try {
  const envPath = path.join(__dirname, ".env");
  if (fs.existsSync(envPath)) {
    process.loadEnvFile(envPath);
  }
} catch (e) {
  // Ignore
}

export const OAUTH_CLIENT_ID =
  process.env.OAUTH_CLIENT_ID ||
  process.env.ACTUAL_OAUTH_CLIENT_ID ||
  null;

export const OAUTH_CLIENT_SECRET =
  process.env.OAUTH_CLIENT_SECRET ||
  process.env.ACTUAL_OAUTH_CLIENT_SECRET ||
  null;

/**
 * Validate that OAuth credentials are configured.
 * Called at startup when auth is enabled — aborts early with a clear message.
 */
export function validateOAuthConfig() {
  const missing = [];
  if (!OAUTH_CLIENT_ID) missing.push("OAUTH_CLIENT_ID");
  if (!OAUTH_CLIENT_SECRET) missing.push("OAUTH_CLIENT_SECRET");
  if (missing.length > 0) {
    console.error(`[OAuth] FATAL: Missing required environment variables: ${missing.join(", ")}`);
    console.error(`[OAuth] Set them in .env or as environment variables, or start with --no-auth to disable OAuth.`);
    process.exit(1);
  }
}

// Parse allowed redirect domains from .env
export function getAllowedRedirectDomains() {
  const envVal = process.env.ALLOWED_REDIRECT_DOMAINS || "oauth-redirect.googleusercontent.com";
  return envVal
    .split(",")
    .map((d) => d.trim().toLowerCase())
    .filter(Boolean);
}

// ─── SQLite Database Setup ───
const DATA_DIR = path.join(__dirname, "data");
if (!fs.existsSync(DATA_DIR)) {
  fs.mkdirSync(DATA_DIR, { recursive: true });
}
const DB_PATH = path.join(DATA_DIR, "auth.sqlite");

const db = new DatabaseSync(DB_PATH);
try {
  fs.chmodSync(DB_PATH, 0o600);
} catch (e) {}

try {
  db.exec(`PRAGMA busy_timeout = 5000;`);
  db.exec(`PRAGMA journal_mode = WAL;`);
} catch (e) {
  console.error(`[OAuth] Warning setting pragmas on auth.sqlite: ${e.message}`);
}


// Initialize tables
db.exec(`
  CREATE TABLE IF NOT EXISTS oauth_tokens (
    access_token TEXT PRIMARY KEY,
    refresh_token TEXT UNIQUE,
    client_id TEXT NOT NULL,
    scope TEXT,
    created_at INTEGER NOT NULL,
    expires_at INTEGER NOT NULL,
    revoked INTEGER DEFAULT 0
  );

  CREATE INDEX IF NOT EXISTS idx_tokens_refresh ON oauth_tokens (refresh_token);
  CREATE INDEX IF NOT EXISTS idx_tokens_expires ON oauth_tokens (expires_at);

  CREATE TABLE IF NOT EXISTS login_activity (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    client_id TEXT,
    event_type TEXT NOT NULL,
    ip TEXT,
    user_agent TEXT,
    redirect_uri TEXT,
    details TEXT,
    timestamp INTEGER NOT NULL,
    created_at TEXT NOT NULL
  );

  CREATE INDEX IF NOT EXISTS idx_activity_time ON login_activity (timestamp);
  CREATE INDEX IF NOT EXISTS idx_activity_client ON login_activity (client_id);
`);

console.error(`[OAuth] Initialized persistent SQLite database at: ${DB_PATH}`);

// In-memory token cache for fast validation
const tokenCache = new Map();

// Load active tokens into memory on startup
function loadActiveTokens() {
  const now = Date.now();
  const stmt = db.prepare(`
    SELECT * FROM oauth_tokens WHERE revoked = 0 AND expires_at > ?
  `);
  const active = stmt.all(now);
  tokenCache.clear();
  for (const t of active) {
    tokenCache.set(t.access_token, t);
  }
  console.error(`[OAuth] Loaded ${tokenCache.size} active session token(s) from auth.sqlite.`);
}
loadActiveTokens();

// Periodically purge expired and revoked tokens from SQLite to prevent unbounded growth
function purgeStaleTokens() {
  try {
    const now = Date.now();
    const stmt = db.prepare(`DELETE FROM oauth_tokens WHERE revoked = 1 OR expires_at <= ?`);
    const result = stmt.run(now);
    if (result.changes > 0) {
      console.error(`[OAuth] Purged ${result.changes} expired/revoked token(s) from auth.sqlite.`);
    }
    // Also evict stale entries from the in-memory cache
    for (const [key, val] of tokenCache.entries()) {
      if (val.revoked || val.expires_at <= now) {
        tokenCache.delete(key);
      }
    }
  } catch (err) {
    console.error("[OAuth] Error purging stale tokens:", err.message);
  }
}
// Run token purge every hour
setInterval(purgeStaleTokens, 60 * 60 * 1000).unref();

// ─── Activity Logging Helper ───
export function logActivity({ clientId, eventType, req, redirectUri, details }) {
  try {
    let ip = "unknown";
    let userAgent = "unknown";

    if (req) {
      ip = req.headers?.["x-forwarded-for"] || req.socket?.remoteAddress || req.ip || "unknown";
      userAgent = req.headers?.["user-agent"] || "unknown";
    }

    const now = Date.now();
    const isoDate = new Date(now).toISOString();

    const insertStmt = db.prepare(`
      INSERT INTO login_activity (client_id, event_type, ip, user_agent, redirect_uri, details, timestamp, created_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `);

    insertStmt.run(
      clientId || null,
      eventType,
      ip,
      userAgent,
      redirectUri || null,
      details || null,
      now,
      isoDate
    );
  } catch (err) {
    console.error("[OAuth] Error logging activity to SQLite:", err.message);
  }
}

// ─── Token Persistence Helpers ───
function saveTokenToDb({ accessToken, refreshToken, clientId, scope, createdAt, expiresAt }) {
  const stmt = db.prepare(`
    INSERT OR REPLACE INTO oauth_tokens (access_token, refresh_token, client_id, scope, created_at, expires_at, revoked)
    VALUES (?, ?, ?, ?, ?, ?, 0)
  `);
  stmt.run(accessToken, refreshToken, clientId, scope || "mcp", createdAt, expiresAt);

  tokenCache.set(accessToken, {
    access_token: accessToken,
    refresh_token: refreshToken,
    client_id: clientId,
    scope: scope || "mcp",
    created_at: createdAt,
    expires_at: expiresAt,
    revoked: 0,
  });
}

function getTokenFromDb(accessToken) {
  const cached = tokenCache.get(accessToken);
  if (cached && cached.expires_at > Date.now() && !cached.revoked) {
    return cached;
  }

  const stmt = db.prepare(`
    SELECT * FROM oauth_tokens WHERE access_token = ? AND revoked = 0 AND expires_at > ?
  `);
  const record = stmt.get(accessToken, Date.now());
  if (record) {
    tokenCache.set(accessToken, record);
    return record;
  }
  return null;
}

function getRefreshTokenFromDb(refreshToken) {
  const stmt = db.prepare(`
    SELECT * FROM oauth_tokens WHERE refresh_token = ? AND revoked = 0
  `);
  return stmt.get(refreshToken);
}

function revokeTokenInDb(accessToken) {
  tokenCache.delete(accessToken);
  const stmt = db.prepare(`
    UPDATE oauth_tokens SET revoked = 1 WHERE access_token = ?
  `);
  stmt.run(accessToken);
}

// In-memory auth codes store (code -> { clientId, redirectUri, codeChallenge, codeChallengeMethod, expiresAt, scope })
const authCodes = new Map();
const MAX_AUTH_CODES = 1000;

function cleanExpiredAuthCodes() {
  const now = Date.now();
  for (const [code, data] of authCodes.entries()) {
    if (data.expiresAt <= now) {
      authCodes.delete(code);
    }
  }
  // If still above cap, evict oldest entries
  if (authCodes.size > MAX_AUTH_CODES) {
    const keysToDelete = Array.from(authCodes.keys()).slice(0, authCodes.size - MAX_AUTH_CODES);
    for (const k of keysToDelete) {
      authCodes.delete(k);
    }
  }
}
// Clean up expired auth codes every 5 minutes
setInterval(cleanExpiredAuthCodes, 5 * 60 * 1000).unref();


/**
 * Timing-safe string comparison
 */
function safeCompare(a, b) {
  if (typeof a !== "string" || typeof b !== "string") return false;
  const bufA = Buffer.from(a);
  const bufB = Buffer.from(b);
  if (bufA.length !== bufB.length) return false;
  return crypto.timingSafeEqual(bufA, bufB);
}

/**
 * Validate redirect URI against configured allowed domains
 */
export function isAllowedRedirectUri(redirectUri) {
  if (!redirectUri) return false;
  try {
    const url = new URL(redirectUri);
    const hostname = url.hostname.toLowerCase();
    const allowedDomains = getAllowedRedirectDomains();

    return allowedDomains.some((allowed) => {
      if (allowed === hostname) return true;
      if (allowed.startsWith("*.") && hostname.endsWith(allowed.slice(1))) return true;
      if (allowed.startsWith(".") && hostname.endsWith(allowed)) return true;
      if (hostname.endsWith(`.${allowed}`)) return true;
      return false;
    });
  } catch (e) {
    return false;
  }
}

/**
 * Express middleware to validate Bearer token on protected endpoints
 */
export function oauthMiddleware(req, res, next) {
  const authHeader = req.headers.authorization;

  if (!authHeader || !authHeader.startsWith("Bearer ")) {
    res.setHeader(
      "WWW-Authenticate",
      `Bearer realm="actual-mcp", error="unauthorized", error_description="Bearer token required"`
    );
    const host = req.get("host");
    const proto = req.headers["x-forwarded-proto"] || req.protocol;
    if (host) {
      res.setHeader("Link", `<${proto}://${host}/.well-known/oauth-protected-resource>; rel="describedby"`);
    }
    return res.status(401).json({
      jsonrpc: "2.0",
      error: {
        code: -32000,
        message: "Unauthorized: Bearer token is required. Authenticate via OAuth.",
      },
      id: null,
    });
  }

  const token = authHeader.slice(7).trim();
  const tokenData = getTokenFromDb(token);

  if (!tokenData) {
    logActivity({
      clientId: null,
      eventType: "auth_failed",
      req,
      details: "Invalid or expired Bearer token presented on /mcp",
    });

    res.setHeader(
      "WWW-Authenticate",
      `Bearer realm="actual-mcp", error="invalid_token", error_description="The access token expired or is invalid"`
    );
    return res.status(401).json({
      jsonrpc: "2.0",
      error: {
        code: -32000,
        message: "Unauthorized: Access token is invalid or expired.",
      },
      id: null,
    });
  }

  req.oauthUser = {
    clientId: tokenData.client_id,
    scope: tokenData.scope,
    createdAt: tokenData.created_at,
    expiresAt: tokenData.expires_at,
  };

  console.error(`[OAuth] [Client: ${tokenData.client_id}] Authenticated request: ${req.method} ${req.originalUrl || req.url}`);
  next();
}

/**
 * Mount OAuth2 authorization, token, and discovery routes on the Express app
 */
export function setupOAuthRoutes(app) {
  // ─── RFC 8414 & OpenID Connect Discovery Metadata ───
  const discoveryHandler = (req, res) => {
    const host = req.get("host") || "localhost";
    const proto = req.headers["x-forwarded-proto"] || req.protocol;
    const baseUrl = `${proto}://${host}`;

    res.json({
      issuer: baseUrl,
      authorization_endpoint: `${baseUrl}/oauth/authorize`,
      token_endpoint: `${baseUrl}/oauth/token`,
      response_types_supported: ["code"],
      grant_types_supported: ["authorization_code", "refresh_token"],
      code_challenge_methods_supported: ["S256", "plain"],
      token_endpoint_auth_methods_supported: ["client_secret_basic", "client_secret_post"],
      service_documentation: "https://modelcontextprotocol.io",
      mcp_endpoint: `${baseUrl}/mcp`,
    });
  };

  app.get("/.well-known/oauth-authorization-server", discoveryHandler);
  app.get("/.well-known/openid-configuration", discoveryHandler);

  // ─── RFC 9728 OAuth 2.0 Protected Resource Metadata ───
  app.get("/.well-known/oauth-protected-resource", (req, res) => {
    const host = req.get("host") || "localhost";
    const proto = req.headers["x-forwarded-proto"] || req.protocol;
    const baseUrl = `${proto}://${host}`;

    res.json({
      resource: `${baseUrl}/mcp`,
      authorization_servers: [baseUrl],
      scopes_supported: ["mcp"],
      bearer_methods_supported: ["header"],
    });
  });

  // ─── Authorization Endpoint (Auto-Approve Instant Redirect) ───
  const authorizeHandler = (req, res) => {
    const {
      client_id,
      redirect_uri,
      response_type,
      state,
      scope,
      code_challenge,
      code_challenge_method,
    } = req.query;

    console.error(`[OAuth] Authorization request received from: ${redirect_uri || "unknown"}`);

    if (!redirect_uri) {
      logActivity({
        clientId: client_id,
        eventType: "auth_failed",
        req,
        details: "Missing redirect_uri parameter",
      });
      return res.status(400).send("Bad Request: redirect_uri parameter is missing.");
    }

    // Validate redirect_uri against allowed domains list
    if (!isAllowedRedirectUri(redirect_uri)) {
      const allowed = getAllowedRedirectDomains().join(", ");
      console.error(`[OAuth] Blocked unauthorized redirect_uri: "${redirect_uri}". Allowed domains: [${allowed}]`);
      logActivity({
        clientId: client_id,
        eventType: "auth_blocked",
        req,
        redirectUri: redirect_uri,
        details: `Unauthorized redirect_uri domain. Allowed: [${allowed}]`,
      });
      return res.status(400).send(`Bad Request: redirect_uri domain is not authorized. Allowed domains: ${allowed}`);
    }

    // Validate client_id
    if (client_id && !safeCompare(client_id, OAUTH_CLIENT_ID)) {
      console.error(`[OAuth] Invalid client_id received: "${client_id}", expected: "${OAUTH_CLIENT_ID}"`);
      logActivity({
        clientId: client_id,
        eventType: "auth_failed",
        req,
        redirectUri: redirect_uri,
        details: `Invalid client_id "${client_id}"`,
      });
      return res.status(400).send(`Bad Request: Invalid client_id "${client_id}".`);
    }

    // Generate secure one-time authorization code
    const code = crypto.randomBytes(32).toString("hex");

    // Store auth code with 10-minute expiration
    authCodes.set(code, {
      clientId: client_id || OAUTH_CLIENT_ID,
      redirectUri: redirect_uri,
      codeChallenge: code_challenge,
      codeChallengeMethod: code_challenge_method || "plain",
      expiresAt: Date.now() + 10 * 60 * 1000,
      scope: scope || "mcp",
    });

    logActivity({
      clientId: client_id || OAUTH_CLIENT_ID,
      eventType: "authorize_granted",
      req,
      redirectUri: redirect_uri,
      details: "Auto-approved authorization code issued",
    });

    // Build redirect URL
    const targetUrl = new URL(redirect_uri);
    targetUrl.searchParams.set("code", code);
    if (state) {
      targetUrl.searchParams.set("state", state);
    }

    console.error(`[OAuth] Auto-approving and redirecting to: ${targetUrl.origin}${targetUrl.pathname}`);
    res.redirect(302, targetUrl.toString());
  };

  app.get("/authorize", authorizeHandler);
  app.get("/oauth/authorize", authorizeHandler);

  // ─── Token Endpoint ───
  const tokenHandler = (req, res) => {
    res.setHeader("Cache-Control", "no-store");
    res.setHeader("Pragma", "no-cache");

    let clientId = req.body?.client_id;
    let clientSecret = req.body?.client_secret;

    // Check Basic Auth header if present
    const authHeader = req.headers.authorization;
    if (authHeader && authHeader.startsWith("Basic ")) {
      try {
        const decoded = Buffer.from(authHeader.slice(6), "base64").toString("utf8");
        const colonIdx = decoded.indexOf(":");
        if (colonIdx !== -1) {
          clientId = decoded.slice(0, colonIdx);
          clientSecret = decoded.slice(colonIdx + 1);
        }
      } catch (e) {
        // Fallback to body
      }
    }

    // Validate client credentials
    if (!clientId || !clientSecret || !safeCompare(clientId, OAUTH_CLIENT_ID) || !safeCompare(clientSecret, OAUTH_CLIENT_SECRET)) {
      console.error(`[OAuth] Token request rejected: Invalid client credentials.`);
      logActivity({
        clientId: clientId || "unknown",
        eventType: "token_failed",
        req,
        details: "Invalid Client ID or Client Secret during token exchange",
      });
      return res.status(401).json({
        error: "invalid_client",
        error_description: "Client authentication failed. Invalid Client ID or Client Secret.",
      });
    }

    const grantType = req.body?.grant_type;

    // 1. Authorization Code Grant
    if (grantType === "authorization_code") {
      const code = req.body?.code;
      const redirectUri = req.body?.redirect_uri;
      const codeVerifier = req.body?.code_verifier;

      if (!code || !authCodes.has(code)) {
        logActivity({
          clientId,
          eventType: "token_failed",
          req,
          redirectUri,
          details: "Invalid or expired authorization code",
        });
        return res.status(400).json({
          error: "invalid_grant",
          error_description: "Invalid or expired authorization code.",
        });
      }

      const codeData = authCodes.get(code);
      authCodes.delete(code);

      if (codeData.expiresAt <= Date.now()) {
        logActivity({
          clientId,
          eventType: "token_failed",
          req,
          redirectUri,
          details: "Authorization code has expired",
        });
        return res.status(400).json({
          error: "invalid_grant",
          error_description: "Authorization code has expired.",
        });
      }

      // Verify redirect_uri matches
      if (redirectUri && codeData.redirectUri && redirectUri !== codeData.redirectUri) {
        logActivity({
          clientId,
          eventType: "token_failed",
          req,
          redirectUri,
          details: `redirect_uri mismatch (got ${redirectUri}, expected ${codeData.redirectUri})`,
        });
        return res.status(400).json({
          error: "invalid_grant",
          error_description: "redirect_uri does not match the authorization request.",
        });
      }

      // Verify PKCE code_verifier if code_challenge was provided
      if (codeData.codeChallenge) {
        if (!codeVerifier) {
          logActivity({
            clientId,
            eventType: "token_failed",
            req,
            redirectUri,
            details: "Missing PKCE code_verifier",
          });
          return res.status(400).json({
            error: "invalid_request",
            error_description: "Missing code_verifier for PKCE challenge.",
          });
        }

        let computedChallenge = codeVerifier;
        if (codeData.codeChallengeMethod === "S256") {
          computedChallenge = crypto.createHash("sha256").update(codeVerifier).digest("base64url");
        }

        if (!safeCompare(computedChallenge, codeData.codeChallenge)) {
          logActivity({
            clientId,
            eventType: "token_failed",
            req,
            redirectUri,
            details: "PKCE verification failed",
          });
          return res.status(400).json({
            error: "invalid_grant",
            error_description: "PKCE verification failed.",
          });
        }
      }

      // Issue access token and refresh token
      const accessToken = crypto.randomBytes(32).toString("hex");
      const refreshToken = crypto.randomBytes(32).toString("hex");
      const expiresIn = 30 * 24 * 60 * 60; // 30 days in seconds
      const now = Date.now();

      // Persist to SQLite
      saveTokenToDb({
        accessToken,
        refreshToken,
        clientId,
        scope: codeData.scope || "mcp",
        createdAt: now,
        expiresAt: now + expiresIn * 1000,
      });

      logActivity({
        clientId,
        eventType: "token_issue",
        req,
        redirectUri,
        details: "Issued new access_token and refresh_token",
      });

      console.error(`[OAuth] Successfully issued access_token and refresh_token for ${clientId} (saved to SQLite)`);

      return res.status(200).json({
        access_token: accessToken,
        token_type: "Bearer",
        expires_in: expiresIn,
        refresh_token: refreshToken,
        scope: codeData.scope || "mcp",
      });
    }

    // 2. Refresh Token Grant
    if (grantType === "refresh_token") {
      const refreshToken = req.body?.refresh_token;

      if (!refreshToken) {
        return res.status(400).json({
          error: "invalid_grant",
          error_description: "Missing refresh_token parameter.",
        });
      }

      const existingRecord = getRefreshTokenFromDb(refreshToken);

      if (!existingRecord) {
        logActivity({
          clientId,
          eventType: "token_failed",
          req,
          details: "Refresh token not found or revoked",
        });
        return res.status(400).json({
          error: "invalid_grant",
          error_description: "Invalid or revoked refresh token.",
        });
      }

      // Revoke old token
      revokeTokenInDb(existingRecord.access_token);

      // Issue new access token and refresh token
      const newAccessToken = crypto.randomBytes(32).toString("hex");
      const newRefreshToken = crypto.randomBytes(32).toString("hex");
      const expiresIn = 30 * 24 * 60 * 60; // 30 days
      const now = Date.now();

      saveTokenToDb({
        accessToken: newAccessToken,
        refreshToken: newRefreshToken,
        clientId: existingRecord.client_id,
        scope: existingRecord.scope,
        createdAt: now,
        expiresAt: now + expiresIn * 1000,
      });

      logActivity({
        clientId,
        eventType: "token_refresh",
        req,
        details: "Refreshed access_token via refresh_token",
      });

      console.error(`[OAuth] Successfully refreshed access token for ${clientId} (updated SQLite)`);

      return res.status(200).json({
        access_token: newAccessToken,
        token_type: "Bearer",
        expires_in: expiresIn,
        refresh_token: newRefreshToken,
        scope: existingRecord.scope,
      });
    }

    logActivity({
      clientId,
      eventType: "token_failed",
      req,
      details: `Unsupported grant_type: "${grantType}"`,
    });

    return res.status(400).json({
      error: "unsupported_grant_type",
      error_description: `Grant type "${grantType}" is not supported.`,
    });
  };

  app.post("/token", tokenHandler);
  app.post("/oauth/token", tokenHandler);

  // ─── RFC 7009 Token Revocation Endpoint ───
  const revokeHandler = (req, res) => {
    let clientId = req.body?.client_id;
    let clientSecret = req.body?.client_secret;

    const authHeader = req.headers.authorization;
    if (authHeader && authHeader.startsWith("Basic ")) {
      try {
        const decoded = Buffer.from(authHeader.slice(6), "base64").toString("utf8");
        const colonIdx = decoded.indexOf(":");
        if (colonIdx !== -1) {
          clientId = decoded.slice(0, colonIdx);
          clientSecret = decoded.slice(colonIdx + 1);
        }
      } catch (e) {}
    }

    if (!clientId || !clientSecret || !safeCompare(clientId, OAUTH_CLIENT_ID) || !safeCompare(clientSecret, OAUTH_CLIENT_SECRET)) {
      return res.status(401).json({ error: "invalid_client" });
    }

    const tokenToRevoke = req.body?.token;
    if (tokenToRevoke) {
      revokeTokenInDb(tokenToRevoke);
      logActivity({
        clientId,
        eventType: "token_revoked",
        req,
        details: "Token explicitly revoked via /oauth/revoke",
      });
      console.error(`[OAuth] Token revoked for client: ${clientId}`);
    }

    // RFC 7009 specifies 200 OK regardless of whether the token previously existed
    return res.status(200).json({ status: "revoked" });
  };

  app.post("/revoke", revokeHandler);
  app.post("/oauth/revoke", revokeHandler);
}


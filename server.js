#!/usr/bin/env node

import net from "node:net";
import dns from "node:dns";

// Disable Happy Eyeballs / network family autoselection and prefer IPv4.
// Prevents connection timeouts when remote hosts advertise AAAA records (e.g. Pikapod)
// but local routing/ISP cannot reach the IPv6 destination.
if (typeof net.setDefaultAutoSelectFamily === "function") {
  net.setDefaultAutoSelectFamily(false);
}
if (typeof dns.setDefaultResultOrder === "function") {
  dns.setDefaultResultOrder("ipv4first");
}

import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { SSEServerTransport } from "@modelcontextprotocol/sdk/server/sse.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import { isInitializeRequest } from "@modelcontextprotocol/sdk/types.js";
import { randomUUID } from "node:crypto";
import { AsyncLocalStorage } from "node:async_hooks";
import express from "express";
import cors from "cors";
import {
  CallToolRequestSchema, ListToolsRequestSchema,
  ListResourcesRequestSchema, ReadResourceRequestSchema,
  ListPromptsRequestSchema, GetPromptRequestSchema
} from "@modelcontextprotocol/sdk/types.js";
import {
  oauthMiddleware,
  setupOAuthRoutes,
  OAUTH_CLIENT_ID,
  validateOAuthConfig
} from "./oauth.js";
import {
  authStorage,
  logCommand,
  logMessage,
  LOG_DIR,
  PID_LOG_FILE,
  COMMAND_LOG_FILE,
} from "./logger.js";
import { handleReceipts, queryReceipts, enrichTransactionsWithReceipts } from "./pocketbase.js";

import fs from "fs/promises";
import fsSync from "fs";
import path from "path";
import os from "os";
import { execFileSync, exec } from "child_process";

import { fileURLToPath, pathToFileURL } from "url";
import { createRequire } from "module";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const require = createRequire(import.meta.url);

// Load .env file if present
try {
  const envPath = path.join(__dirname, ".env");
  if (fsSync.existsSync(envPath)) {
    process.loadEnvFile(envPath);
  }
} catch (e) {
  // Ignore
}

const ACTUAL_RC_PATH = process.env.ACTUAL_RC_PATH || path.join(os.homedir(), '.actualrc.json');
const ACTUAL_DATA_ROOT = process.env.ACTUAL_DATA_DIR || path.join(os.homedir(), '.actual-data');
if (!fsSync.existsSync(ACTUAL_DATA_ROOT)) {
  fsSync.mkdirSync(ACTUAL_DATA_ROOT, { recursive: true });
}
const INSTANCE_DATA_DIR = path.join(ACTUAL_DATA_ROOT, String(process.pid));
if (!fsSync.existsSync(INSTANCE_DATA_DIR)) {
  fsSync.mkdirSync(INSTANCE_DATA_DIR, { recursive: true });
}

// Clean up stale PID directories from previous crashed instances
function cleanStalePidDirs() {
  try {
    if (!fsSync.existsSync(ACTUAL_DATA_ROOT)) return;
    const entries = fsSync.readdirSync(ACTUAL_DATA_ROOT, { withFileTypes: true });
    for (const entry of entries) {
      if (!entry.isDirectory()) continue;
      const pid = parseInt(entry.name, 10);
      if (isNaN(pid)) continue; // skip non-PID dirs
      // Check if this PID is still running
      try {
        process.kill(pid, 0); // signal 0 = existence check, doesn't kill
      } catch (e) {
        // Process not running — remove stale dir
        const staleDir = path.join(ACTUAL_DATA_ROOT, entry.name);
        console.error(`[actual-mcp] Removing stale data dir (PID ${pid} no longer running): ${staleDir}`);
        fsSync.rmSync(staleDir, { recursive: true, force: true });
      }
    }
  } catch (e) {
    // Best-effort
  }
}

cleanStalePidDirs();

// ───────────────────────────────────────────────────────────
// Persistent API Connection
// ───────────────────────────────────────────────────────────
let api = null;
let apiInitialized = false;
let budgetLoaded = false;
let rcConfig = null;
let apiModuleUrl = null;

async function loadApi() {
  if (apiModuleUrl) {
    return await import(apiModuleUrl);
  }
  return await import('@actual-app/api');
}

async function readActualRc() {
  try {
    const data = await fs.readFile(ACTUAL_RC_PATH, 'utf8');
    return JSON.parse(data);
  } catch (e) {
    return {};
  }
}

function getServerUrl() {
  return process.env.ACTUAL_SERVER_URL || rcConfig?.serverUrl;
}

function getServerPassword() {
  return process.env.ACTUAL_PASSWORD || rcConfig?.password;
}

function getSyncId() {
  return process.env.ACTUAL_SYNC_ID || rcConfig?.syncId;
}

function getEncryptionPassword() {
  return process.env.ACTUAL_ENCRYPTION_PASSWORD || rcConfig?.encryptionPassword;
}

async function ensureInit() {
  if (apiInitialized) return;

  // Dynamic import of @actual-app/api
  api = await loadApi();
  rcConfig = await readActualRc();

  const serverURL = getServerUrl();
  const password = getServerPassword();

  const initOpts = {
    serverURL,
    dataDir: INSTANCE_DATA_DIR,
  };

  if (password) {
    initOpts.password = password;
  }

  await logMessage('API init: connecting to server...');
  await api.init(initOpts);
  apiInitialized = true;
  await logMessage('API init: connected');
}

async function ensureBudget() {
  await ensureInit();
  if (budgetLoaded) return;

  const syncId = getSyncId();
  const encryptionPassword = getEncryptionPassword();

  if (!syncId) {
    throw new Error('syncId not found in .actualrc.json or ACTUAL_SYNC_ID environment variable — cannot load budget');
  }

  try {
    await logMessage(`Downloading budget ${syncId}...`);
    await api.downloadBudget(syncId, {
      password: encryptionPassword,
    });
    budgetLoaded = true;
    await logMessage('Budget loaded');
  } catch (err) {
    if (isSchemaMismatch(err)) {
      await logMessage(`Schema mismatch during budget load: ${err.message}. Upgrading packages...`);
      await autoUpgradePackages();
      await logMessage(`Retrying budget download after upgrade...`);
      await api.downloadBudget(syncId, {
        password: encryptionPassword,
      });
      budgetLoaded = true;
      await logMessage('Budget loaded successfully after upgrade');
    } else if (isSqliteCorrupt(err)) {
      await logMessage(`SQLite corruption during budget load: ${err.message}. Rebuilding...`);
      await rebuildBudget();
    } else {
      throw err;
    }
  }
}

// ───────────────────────────────────────────────────────────
// SQLite Corruption Recovery
// ───────────────────────────────────────────────────────────
const RETRYABLE_SQLITE_PATTERNS = [
  'SQLITE_CORRUPT',
  'SQLITE_BUSY',
  'SQLITE_IOERR',
  'database disk image is malformed',
  'database is locked',
];

function isSqliteCorrupt(err) {
  if (!err) return false;
  const msg = String(err.message || err);
  const code = err.code || '';
  return RETRYABLE_SQLITE_PATTERNS.some(p => msg.includes(p) || code.includes(p));
}

// ───────────────────────────────────────────────────────────
// Schema Mismatch Detection & Auto-Upgrade Recovery
// ───────────────────────────────────────────────────────────
const SCHEMA_MISMATCH_PATTERNS = [
  'newer database schema than this version of Actual supports',
  'invalid-schema',
  'uses a newer database schema',
  'Make sure you are using the latest version',
  'no such column: account_group_id',
  'out-of-sync-migrations',
  'Database is out of sync with migrations',
  'Cannot destructure property \'id\' of \'getPrefs(...)\'',
];

function isSchemaMismatch(err) {
  if (!err) return false;
  const msg = String(err.message || err.stack || err);
  const reason = String(err.reason || '');
  return SCHEMA_MISMATCH_PATTERNS.some(p => msg.includes(p) || reason.includes(p));
}

let upgradeInProgress = null;

async function autoUpgradePackages(explicitClientId = null) {
  if (upgradeInProgress) {
    await logMessage('[actual-mcp] Upgrade already in progress, awaiting completion...', explicitClientId);
    return upgradeInProgress;
  }

  upgradeInProgress = (async () => {
    await logMessage('[actual-mcp] Database schema mismatch detected! Starting automatic upgrade of Actual packages...', explicitClientId);

    // 1. Shut down current API connection cleanly
    if (api && apiInitialized) {
      try { await api.shutdown(); } catch (e) { /* ignore */ }
    }
    apiInitialized = false;
    budgetLoaded = false;
    invalidateCache();

    // 2. Remove instance data dir so budget re-downloads cleanly
    try {
      await logMessage(`[actual-mcp] Cleaning instance data dir: ${INSTANCE_DATA_DIR}`, explicitClientId);
      await fs.rm(INSTANCE_DATA_DIR, { recursive: true, force: true });
    } catch (e) { /* ignore */ }

    // 3. Upgrade local @actual-app/api in actual-mcp
    await logMessage('[actual-mcp] Upgrading local @actual-app/api@latest in actual-mcp...', explicitClientId);
    await new Promise((resolve) => {
      exec('npm install @actual-app/api@latest', { cwd: __dirname, timeout: 120000 }, (err, stdout, stderr) => {
        if (err) {
          logMessage(`[actual-mcp] Warning: npm install @actual-app/api@latest error: ${err.message}`, explicitClientId);
        } else {
          logMessage(`[actual-mcp] Successfully updated local @actual-app/api`, explicitClientId);
        }
        resolve();
      });
    });

    // 4. Upgrade global @actual-app/cli
    await logMessage('[actual-mcp] Upgrading global @actual-app/cli@latest...', explicitClientId);
    await new Promise((resolve) => {
      exec('npm install -g @actual-app/cli@latest', { timeout: 120000 }, (err, stdout, stderr) => {
        if (err) {
          logMessage(`[actual-mcp] Warning: npm install -g @actual-app/cli@latest error: ${err.message}`, explicitClientId);
        } else {
          logMessage(`[actual-mcp] Successfully updated global @actual-app/cli`, explicitClientId);
        }
        resolve();
      });
    });

    // 5. Reload @actual-app/api with cache busting
    await logMessage('[actual-mcp] Reloading upgraded @actual-app/api in-process...', explicitClientId);
    try {
      const resolved = require.resolve('@actual-app/api');
      apiModuleUrl = `${pathToFileURL(resolved).href}?t=${Date.now()}`;
      api = await import(apiModuleUrl);
      await logMessage('[actual-mcp] Re-import of @actual-app/api succeeded.', explicitClientId);
    } catch (importErr) {
      await logMessage(`[actual-mcp] In-process reload failed: ${importErr.message}. Exiting process to allow supervisor restart.`, explicitClientId);
      process.exit(1);
    }

    // 6. Re-init API
    await ensureInit();
    await logMessage('[actual-mcp] Automatic upgrade completed and API re-initialized successfully.', explicitClientId);
  })();

  try {
    await upgradeInProgress;
  } finally {
    upgradeInProgress = null;
  }
}

async function rebuildBudget(explicitClientId = null) {
  await logMessage('Rebuilding budget: shutting down API...', explicitClientId);

  // Shut down the current API connection
  if (api && apiInitialized) {
    try { await api.shutdown(); } catch (e) { /* ignore */ }
  }
  apiInitialized = false;
  budgetLoaded = false;
  invalidateCache();

  // Delete this instance's entire data directory and recreate it fresh
  try {
    await logMessage(`Removing instance data dir: ${INSTANCE_DATA_DIR}`, explicitClientId);
    await fs.rm(INSTANCE_DATA_DIR, { recursive: true, force: true });
  } catch (e) {
    await logMessage(`Warning: could not remove instance data dir: ${e.message}`, explicitClientId);
  }

  // Re-initialize and re-download
  await logMessage('Rebuilding budget: re-initializing API...', explicitClientId);
  api = await loadApi();
  const serverURL = getServerUrl();
  const password = getServerPassword();
  const syncId = getSyncId();
  const encryptionPassword = getEncryptionPassword();

  const initOpts = {
    serverURL,
    dataDir: INSTANCE_DATA_DIR,
  };
  if (password) {
    initOpts.password = password;
  }
  await api.init(initOpts);
  apiInitialized = true;

  await logMessage(`Rebuilding budget: downloading ${syncId}...`, explicitClientId);
  await api.downloadBudget(syncId, {
    password: encryptionPassword,
  });
  budgetLoaded = true;
  invalidateCache();
  await logMessage('Budget rebuilt successfully', explicitClientId);
}

// ───────────────────────────────────────────────────────────
// Process-level SQLite Corruption Safety Net
// ───────────────────────────────────────────────────────────
// The @actual-app/api internally fires advanceSchedulesService (and similar)
// during downloadBudget in async paths that don't propagate back through our
// await, so SqliteError surfaces as an uncaught exception / unhandled rejection
// that crashes the process. These handlers catch it, synchronously clean the
// corrupted budget cache, and let the MCP server survive — the next request
// triggers a fresh download.

function cleanBudgetCacheSync() {
  try {
    if (fsSync.existsSync(INSTANCE_DATA_DIR)) {
      console.error(`[actual-mcp] Removing corrupted instance data dir: ${INSTANCE_DATA_DIR}`);
      fsSync.rmSync(INSTANCE_DATA_DIR, { recursive: true, force: true });
    }
  } catch (e) {
    console.error(`[actual-mcp] Budget cache cleanup failed: ${e.message}`);
  }

  // Reset state so the next MCP call re-downloads from scratch
  apiInitialized = false;
  budgetLoaded = false;
  api = null;
}

process.on('uncaughtException', (err) => {
  if (isSchemaMismatch(err)) {
    console.error(`[actual-mcp] Uncaught schema mismatch error: ${err.message}. Triggering auto-upgrade...`);
    autoUpgradePackages().catch(e => {
      console.error('[actual-mcp] Auto-upgrade from uncaught exception failed:', e);
      process.exit(1);
    });
  } else if (isSqliteCorrupt(err)) {
    console.error(`[actual-mcp] Uncaught SQLite error: ${err.message}. Cleaning budget cache for recovery on next call...`);
    cleanBudgetCacheSync();
    // Don't exit — let the MCP server continue running.
    // The current in-flight request will fail/timeout, but subsequent
    // requests will re-init the API and download a fresh budget.
  } else {
    console.error('[actual-mcp] Fatal uncaught exception:', err);
    process.exit(1);
  }
});

process.on('unhandledRejection', (err) => {
  if (isSchemaMismatch(err)) {
    console.error(`[actual-mcp] Unhandled schema mismatch rejection: ${err?.message || err}. Triggering auto-upgrade...`);
    autoUpgradePackages().catch(e => {
      console.error('[actual-mcp] Auto-upgrade from unhandled rejection failed:', e);
    });
  } else if (isSqliteCorrupt(err)) {
    console.error(`[actual-mcp] Unhandled SQLite rejection: ${err?.message || err}. Cleaning budget cache for recovery on next call...`);
    cleanBudgetCacheSync();
  } else {
    console.error('[actual-mcp] Unhandled rejection:', err);
  }
});

// ───────────────────────────────────────────────────────────
// In-memory Cache
// ───────────────────────────────────────────────────────────
const cache = {
  accounts: null,
  accountGroups: null,
  categories: null,
  categoryGroups: null,
  payees: null,
  tags: null,
};

function invalidateCache() {
  cache.accounts = null;
  cache.accountGroups = null;
  cache.categories = null;
  cache.categoryGroups = null;
  cache.payees = null;
  cache.tags = null;
}

async function getCachedAccounts() {
  if (!cache.accounts) {
    cache.accounts = await api.getAccounts();
  }
  return cache.accounts;
}

async function getCachedAccountGroups() {
  if (!cache.accountGroups) {
    cache.accountGroups = await api.getAccountGroups();
  }
  return cache.accountGroups;
}

async function getCachedCategories() {
  if (!cache.categories) {
    cache.categories = await api.getCategories();
  }
  return cache.categories;
}

async function getCachedCategoryGroups() {
  if (!cache.categoryGroups) {
    cache.categoryGroups = await api.getCategoryGroups();
  }
  return cache.categoryGroups;
}

async function getCachedPayees() {
  if (!cache.payees) {
    cache.payees = await api.getPayees();
  }
  return cache.payees;
}

async function getCachedTags() {
  if (!cache.tags) {
    cache.tags = await api.getTags();
  }
  return cache.tags;
}

// ───────────────────────────────────────────────────────────
// Argument Parsing Helpers (compatible with CLI-style args)
// ───────────────────────────────────────────────────────────
function parseArgs(args) {
  const result = { _positional: [] };
  let i = 0;
  while (i < args.length) {
    const arg = args[i];
    if (arg.startsWith('--')) {
      const key = arg.slice(2);
      // Boolean flags (no value following)
      if (i + 1 >= args.length || args[i + 1].startsWith('--')) {
        result[key] = true;
        i++;
      } else {
        result[key] = args[i + 1];
        i += 2;
      }
    } else {
      result._positional.push(arg);
      i++;
    }
  }
  return result;
}

function parseBoolFlag(value, flagName) {
  if (value === true || value === 'true') return true;
  if (value === false || value === 'false') return false;
  throw new Error(`Invalid ${flagName}: "${value}". Expected "true" or "false".`);
}

function parseIntFlag(value, flagName) {
  const parsed = typeof value === 'number' ? value : Number(value);
  if (!Number.isInteger(parsed)) {
    throw new Error(`Invalid ${flagName}: "${value}". Expected an integer.`);
  }
  return parsed;
}

function readJsonInput(parsed) {
  if (parsed.data && parsed.file) {
    throw new Error('Cannot use both --data and --file');
  }
  if (parsed.data) {
    return JSON.parse(parsed.data);
  }
  if (parsed.file) {
    if (parsed.file !== '-') {
      // Prevent path traversal: resolve and ensure the path is within the project directory
      const resolved = path.resolve(parsed.file);
      if (!resolved.startsWith(__dirname + path.sep) && resolved !== __dirname) {
        throw new Error(`Forbidden: --file path "${parsed.file}" is outside the project directory.`);
      }
    }
    const content = parsed.file === '-'
      ? fsSync.readFileSync(0, 'utf-8')
      : fsSync.readFileSync(parsed.file, 'utf-8');
    return JSON.parse(content);
  }
  throw new Error('Either --data or --file is required');
}

function isRecord(value) {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

// ───────────────────────────────────────────────────────────
// Output Formatting (matches CLI output exactly)
// ───────────────────────────────────────────────────────────
function formatOutput(data, format = 'json') {
  switch (format) {
    case 'json':
      return JSON.stringify(data, null, 2);
    case 'csv':
      return formatCsv(data);
    case 'table':
      // For MCP, just use JSON since table rendering isn't useful
      return JSON.stringify(data, null, 2);
    default:
      return JSON.stringify(data, null, 2);
  }
}

const AMOUNT_FIELDS = new Set([
  'amount', 'balance', 'balance_available', 'balance_current',
  'balance_limit', 'budgeted', 'spent', 'carryover',
]);

function formatCellValue(key, value) {
  if (AMOUNT_FIELDS.has(key) && typeof value === 'number') {
    return (value / 100).toFixed(2);
  }
  return String(value ?? '');
}

function escapeCsv(value) {
  if (value.includes(',') || value.includes('"') || value.includes('\n')) {
    return '"' + value.replace(/"/g, '""') + '"';
  }
  return value;
}

function formatCsv(data) {
  if (!Array.isArray(data)) {
    if (data && typeof data === 'object') {
      const entries = Object.entries(data);
      const header = entries.map(([k]) => escapeCsv(k)).join(',');
      const values = entries.map(([k, v]) => escapeCsv(formatCellValue(k, v))).join(',');
      return header + '\n' + values;
    }
    return String(data);
  }
  if (data.length === 0) return '';
  const keys = Object.keys(data[0]);
  const header = keys.map(k => escapeCsv(k)).join(',');
  const rows = data.map(row => {
    return keys.map(k => escapeCsv(formatCellValue(k, row[k]))).join(',');
  });
  return [header, ...rows].join('\n');
}

// ───────────────────────────────────────────────────────────
// AQL Query Builder (matches CLI query command exactly)
// ───────────────────────────────────────────────────────────
const TABLE_SCHEMA = {
  transactions: {
    id: { type: 'id' },
    account: { type: 'id', ref: 'accounts' },
    date: { type: 'date' },
    amount: { type: 'integer' },
    payee: { type: 'id', ref: 'payees' },
    category: { type: 'id', ref: 'categories' },
    notes: { type: 'string' },
    imported_id: { type: 'string' },
    transfer_id: { type: 'id' },
    cleared: { type: 'boolean' },
    reconciled: { type: 'boolean' },
    starting_balance_flag: { type: 'boolean' },
    imported_payee: { type: 'string' },
    is_parent: { type: 'boolean' },
    is_child: { type: 'boolean' },
    parent_id: { type: 'id' },
    sort_order: { type: 'float' },
    schedule: { type: 'id', ref: 'schedules' },
    error: { type: 'json' },
    tombstone: { type: 'boolean' },
    raw_synced_data: { type: 'string' },
    has_receipt: { type: 'boolean' },
    receipt: { type: 'json' },
    'account.name': { type: 'string', ref: 'accounts' },
    'account.offbudget': { type: 'boolean', ref: 'accounts' },
    'account.closed': { type: 'boolean', ref: 'accounts' },
    'payee.name': { type: 'string', ref: 'payees' },
    'payee.transfer_acct': { type: 'id', ref: 'payees' },
    'category.name': { type: 'string', ref: 'categories' },
    'category.is_income': { type: 'boolean', ref: 'categories' },
    'category.group': { type: 'id', ref: 'category_groups' },
    'category.group.name': { type: 'string', ref: 'category_groups' },
  },
  accounts: {
    id: { type: 'id' },
    name: { type: 'string' },
    offbudget: { type: 'boolean' },
    closed: { type: 'boolean' },
    sort_order: { type: 'float' },
    tombstone: { type: 'boolean' },
    account_id: { type: 'string' },
    official_name: { type: 'string' },
    account_sync_source: { type: 'string' },
    last_reconciled: { type: 'string' },
    last_sync: { type: 'string' },
    bank_sync_status: { type: 'string' },
    account_group_id: { type: 'id', ref: 'account_groups' },
  },
  account_groups: {
    id: { type: 'id' },
    name: { type: 'string' },
    sort_order: { type: 'float' },
    tombstone: { type: 'boolean' },
  },
  categories: {
    id: { type: 'id' },
    name: { type: 'string' },
    is_income: { type: 'boolean' },
    group: { type: 'id', ref: 'category_groups' },
    group_id: { type: 'id', ref: 'category_groups' },
    goal_def: { type: 'string' },
    cleanup_def: { type: 'string' },
    template_settings: { type: 'json' },
    sort_order: { type: 'float' },
    hidden: { type: 'boolean' },
    tombstone: { type: 'boolean' },
    'group.name': { type: 'string', ref: 'category_groups' },
  },
  category_groups: {
    id: { type: 'id' },
    name: { type: 'string' },
    is_income: { type: 'boolean' },
    hidden: { type: 'boolean' },
    sort_order: { type: 'float' },
    tombstone: { type: 'boolean' },
  },
  cleanup_groups: {
    id: { type: 'id' },
    name: { type: 'string' },
    tombstone: { type: 'boolean' },
  },
  payees: {
    id: { type: 'id' },
    name: { type: 'string' },
    transfer_acct: { type: 'id', ref: 'accounts' },
    tombstone: { type: 'boolean' },
    favorite: { type: 'boolean' },
    learn_categories: { type: 'boolean' },
  },
  payee_locations: {
    id: { type: 'id' },
    payee_id: { type: 'id', ref: 'payees' },
    latitude: { type: 'float' },
    longitude: { type: 'float' },
    created_at: { type: 'integer' },
    tombstone: { type: 'boolean' },
  },
  rules: {
    id: { type: 'id' },
    stage: { type: 'string' },
    conditions_op: { type: 'string' },
    conditions: { type: 'json' },
    actions: { type: 'json' },
    tombstone: { type: 'boolean' },
  },
  schedules: {
    id: { type: 'id' },
    name: { type: 'string' },
    rule: { type: 'id', ref: 'rules' },
    next_date: { type: 'date' },
    completed: { type: 'boolean' },
    posts_transaction: { type: 'boolean' },
    custom_upcoming_length: { type: 'string' },
    tombstone: { type: 'boolean' },
    sort_order: { type: 'float' },
    _payee: { type: 'id', ref: 'payees' },
    _account: { type: 'id', ref: 'accounts' },
    _amount: { type: 'json' },
    _amountOp: { type: 'string' },
    _date: { type: 'json' },
    _conditions: { type: 'json' },
    _actions: { type: 'json' },
    _has_splits: { type: 'boolean' },
  },
  notes: {
    id: { type: 'id' },
    note: { type: 'string' },
  },
  preferences: {
    id: { type: 'id' },
    value: { type: 'string' },
  },
  transaction_filters: {
    id: { type: 'id' },
    name: { type: 'string' },
    conditions_op: { type: 'string' },
    conditions: { type: 'json' },
    tombstone: { type: 'boolean' },
  },
  custom_reports: {
    id: { type: 'id' },
    name: { type: 'string' },
    start_date: { type: 'string' },
    end_date: { type: 'string' },
    date_static: { type: 'integer' },
    date_range: { type: 'string' },
    mode: { type: 'string' },
    group_by: { type: 'string' },
    sort_by: { type: 'string' },
    balance_type: { type: 'string' },
    show_empty: { type: 'integer' },
    show_offbudget: { type: 'integer' },
    show_hidden: { type: 'integer' },
    show_uncategorized: { type: 'integer' },
    trim_intervals: { type: 'integer' },
    show_trend_lines: { type: 'integer' },
    include_current: { type: 'integer' },
    graph_type: { type: 'string' },
    conditions: { type: 'json' },
    conditions_op: { type: 'string' },
    metadata: { type: 'json' },
    interval: { type: 'string' },
    color_scheme: { type: 'json' },
    tombstone: { type: 'boolean' },
  },
  reflect_budgets: {
    id: { type: 'id' },
    month: { type: 'integer' },
    category: { type: 'string' },
    amount: { type: 'integer' },
    carryover: { type: 'integer' },
    goal: { type: 'integer' },
    long_goal: { type: 'integer' },
  },
  zero_budgets: {
    id: { type: 'id' },
    month: { type: 'integer' },
    category: { type: 'string', ref: 'categories' },
    amount: { type: 'integer' },
    carryover: { type: 'integer' },
    goal: { type: 'integer' },
    long_goal: { type: 'integer' },
  },
  dashboard_pages: {
    id: { type: 'id' },
    name: { type: 'string' },
    tombstone: { type: 'boolean' },
  },
  dashboard: {
    id: { type: 'id' },
    dashboard_page_id: { type: 'id', ref: 'dashboard_pages' },
    type: { type: 'string' },
    width: { type: 'integer' },
    height: { type: 'integer' },
    x: { type: 'integer' },
    y: { type: 'integer' },
    meta: { type: 'json' },
    tombstone: { type: 'boolean' },
  },
  receipts: {
    id: { type: 'id' },
    tx_id: { type: 'id', ref: 'transactions' },
    tx_id_transfer: { type: 'id', ref: 'transactions' },
    date: { type: 'date' },
    amount: { type: 'float' },
    payee: { type: 'string' },
    source: { type: 'string' },
    source_ref: { type: 'string' },
    source_hash: { type: 'string' },
    lines: { type: 'json' },
    parsed: { type: 'json' },
    flag: { type: 'string' },
    image: { type: 'json' },
    image_urls: { type: 'json' },
    collection_id: { type: 'string' },
    created: { type: 'string' },
    updated: { type: 'string' },
  },
};

const AVAILABLE_TABLES = Object.keys(TABLE_SCHEMA).join(', ');

const LAST_DEFAULT_SELECT = [
  'date', 'account.name', 'payee.name', 'category.name', 'amount', 'notes',
];

function parseOrderBy(input) {
  return input.split(',').map(part => {
    const trimmed = part.trim();
    if (!trimmed) throw new Error('--order-by contains an empty field');
    const colonIndex = trimmed.indexOf(':');
    if (colonIndex === -1) return trimmed;
    const field = trimmed.slice(0, colonIndex).trim();
    if (!field) throw new Error(`Invalid order field in "${trimmed}". Field name cannot be empty.`);
    const direction = trimmed.slice(colonIndex + 1);
    if (direction !== 'asc' && direction !== 'desc') {
      throw new Error(`Invalid order direction "${direction}" for field "${field}". Expected "asc" or "desc".`);
    }
    return { [field]: direction };
  });
}

function buildQuery(opts) {
  const last = opts.last ? parseIntFlag(opts.last, '--last') : undefined;

  if (last !== undefined) {
    if (opts.table && opts.table !== 'transactions') {
      throw new Error('--last implies --table transactions. Cannot use with --table ' + opts.table);
    }
    if (opts.limit) {
      throw new Error('--last and --limit are mutually exclusive');
    }
  }

  const table = opts.table ?? (last !== undefined ? 'transactions' : undefined);
  if (!table) throw new Error('--table is required (or use --file or --last)');
  if (!(table in TABLE_SCHEMA)) {
    throw new Error(`Unknown table "${table}". Available tables: ${AVAILABLE_TABLES}`);
  }

  if (opts.where && opts.filter) {
    throw new Error('--where and --filter are mutually exclusive');
  }
  if (opts.count && opts.select) {
    throw new Error('--count and --select are mutually exclusive');
  }

  let queryObj = api.q(table);

  if (opts.count) {
    queryObj = queryObj.calculate({ $count: '*' });
  } else if (opts.select) {
    queryObj = queryObj.select(opts.select.split(','));
  } else if (last !== undefined) {
    queryObj = queryObj.select(LAST_DEFAULT_SELECT);
  }

  const filterStr = opts.filter ?? opts.where;
  if (filterStr) {
    queryObj = queryObj.filter(JSON.parse(filterStr));
  }

  const orderByStr = opts['order-by'] ??
    (last !== undefined && !opts.count ? 'date:desc' : undefined);
  if (orderByStr) {
    queryObj = queryObj.orderBy(parseOrderBy(orderByStr));
  }

  const limitVal = last ??
    (opts.limit ? parseIntFlag(opts.limit, '--limit') : undefined);
  if (limitVal !== undefined) {
    queryObj = queryObj.limit(limitVal);
  }

  if (opts.offset) {
    queryObj = queryObj.offset(parseIntFlag(opts.offset, '--offset'));
  }

  if (opts['group-by']) {
    queryObj = queryObj.groupBy(opts['group-by'].split(','));
  }

  return queryObj;
}

function buildQueryFromFile(parsed, fallbackTable) {
  const table = typeof parsed.table === 'string' ? parsed.table : fallbackTable;
  if (!table) throw new Error('--table is required when the input file lacks a "table" field');
  let queryObj = api.q(table);
  if (Array.isArray(parsed.select)) queryObj = queryObj.select(parsed.select);
  if (isRecord(parsed.filter)) queryObj = queryObj.filter(parsed.filter);
  if (Array.isArray(parsed.orderBy)) queryObj = queryObj.orderBy(parsed.orderBy);
  if (typeof parsed.limit === 'number') queryObj = queryObj.limit(parsed.limit);
  if (typeof parsed.offset === 'number') queryObj = queryObj.offset(parsed.offset);
  if (Array.isArray(parsed.groupBy)) queryObj = queryObj.groupBy(parsed.groupBy);
  if (isRecord(parsed.options)) queryObj = queryObj.options(parsed.options);
  return queryObj;
}

// ───────────────────────────────────────────────────────────
// Command Handlers — Direct API calls, no subprocess spawning
// ───────────────────────────────────────────────────────────

async function resolveNoteId(id, type) {
  if (!id) throw new Error('Note ID (or entity ID) is required');
  const rawId = String(id).trim();
  if (type === 'account') {
    return rawId.startsWith('account-') ? rawId : `account-${rawId}`;
  }
  if (type === 'budget') {
    return rawId.startsWith('budget-') ? rawId : `budget-${rawId}`;
  }
  if (type === 'category' || type === 'raw') {
    return rawId;
  }
  if (rawId.startsWith('account-') || rawId.startsWith('budget-')) {
    return rawId;
  }
  if (/^\d{4}-\d{2}$/.test(rawId)) {
    return `budget-${rawId}`;
  }
  const accounts = await getCachedAccounts();
  if (accounts.some(a => a.id === rawId)) {
    return `account-${rawId}`;
  }
  return rawId;
}

async function handleNotes(subCmd, opts) {
  switch (subCmd) {
    case 'list': {
      await ensureBudget();
      const result = await api.aqlQuery(api.q('notes').select(['id', 'note']));
      const rows = Array.isArray(result?.data) ? result.data : [];
      const includeEmpty = !!opts['include-empty'] || !!opts.include_empty;
      const filterType = opts.type;
      const [accounts, categories, categoryGroups] = await Promise.all([
        getCachedAccounts(),
        getCachedCategories(),
        getCachedCategoryGroups(),
      ]);
      const accountMap = new Map(accounts.map(a => [a.id, a.name]));
      const categoryMap = new Map(categories.map(c => [c.id, c.name]));
      const groupMap = new Map(categoryGroups.map(g => [g.id, g.name]));

      const enriched = [];
      for (const row of rows) {
        if (!includeEmpty && (!row.note || row.note.trim() === '')) continue;
        let entityType = 'other';
        let entityId = row.id;
        let entityName = null;
        if (row.id.startsWith('account-')) {
          entityType = 'account';
          entityId = row.id.slice('account-'.length);
          entityName = accountMap.get(entityId) ?? null;
        } else if (row.id.startsWith('budget-')) {
          entityType = 'budget';
          entityId = row.id.slice('budget-'.length);
          entityName = entityId;
        } else if (categoryMap.has(row.id)) {
          entityType = 'category';
          entityName = categoryMap.get(row.id);
        } else if (groupMap.has(row.id)) {
          entityType = 'category_group';
          entityName = groupMap.get(row.id);
        } else if (accountMap.has(row.id)) {
          entityType = 'account';
          entityName = accountMap.get(row.id);
        }
        if (filterType && filterType !== 'all' && entityType !== filterType) continue;
        enriched.push({
          id: entityId,
          note_id: row.id,
          type: entityType,
          name: entityName,
          note: row.note,
        });
      }
      return enriched;
    }
    case 'get': {
      await ensureBudget();
      const rawId = opts._positional[0] || opts.id;
      if (!rawId) throw new Error('Note ID (or entity ID) is required');
      const noteId = await resolveNoteId(rawId, opts.type);
      let res = await api.getNote(noteId);
      if (!res && noteId === rawId && !rawId.startsWith('account-') && !rawId.startsWith('budget-')) {
        const acctRes = await api.getNote(`account-${rawId}`);
        if (acctRes) {
          return { id: rawId, note_id: `account-${rawId}`, note: acctRes.note };
        }
      }
      return {
        id: rawId,
        note_id: noteId,
        note: res ? res.note : null,
      };
    }
    case 'set':
    case 'update': {
      await ensureBudget();
      const rawId = opts._positional[0] || opts.id;
      if (!rawId) throw new Error('Note ID (or entity ID) is required');
      if (opts.note === undefined) throw new Error('--note is required');
      const noteId = await resolveNoteId(rawId, opts.type);
      const noteText = opts.note === null ? '' : String(opts.note);
      await api.updateNote(noteId, noteText);
      return { success: true, id: rawId, note_id: noteId, note: noteText };
    }
    default:
      throw new Error(`Unknown notes subcommand: ${subCmd}`);
  }
}

async function handlePreferences(subCmd, opts) {
  switch (subCmd) {
    case 'list':
    case 'get':
    case '': {
      await ensureBudget();
      const prefs = await api.getPreferences();
      const key = opts._positional[0] || opts.key || opts.id;
      if (key) {
        return { key, value: prefs?.[key] ?? null };
      }
      return prefs;
    }
    case 'set':
    case 'update': {
      await ensureBudget();
      const key = opts._positional[0] || opts.key || opts.id;
      if (!key) throw new Error('Preference key (--key or positional) is required');
      if (opts.value === undefined) throw new Error('--value is required');
      const val = opts.value === 'null' || opts.value === null ? undefined : String(opts.value);
      await api.setPreference(key, val);
      return { success: true, key, value: val ?? null };
    }
    default:
      throw new Error(`Unknown preferences subcommand: ${subCmd}`);
  }
}

async function handleAccountGroups(subCmd, opts) {
  switch (subCmd) {
    case 'list': {
      await ensureBudget();
      return await getCachedAccountGroups();
    }
    case 'create': {
      await ensureBudget();
      if (!opts.name) throw new Error('--name is required');
      const id = await api.createAccountGroup({ name: opts.name });
      cache.accountGroups = null;
      return { id };
    }
    case 'update': {
      await ensureBudget();
      const id = opts._positional[0] || opts.id;
      if (!id) throw new Error('Account group ID is required');
      if (!opts.name) throw new Error('--name is required');
      await api.updateAccountGroup(id, { name: opts.name });
      cache.accountGroups = null;
      return { success: true, id };
    }
    case 'delete': {
      await ensureBudget();
      const id = opts._positional[0] || opts.id;
      if (!id) throw new Error('Account group ID is required');
      await api.deleteAccountGroup(id);
      cache.accountGroups = null;
      cache.accounts = null;
      return { success: true, id };
    }
    default:
      throw new Error(`Unknown account-groups subcommand: ${subCmd}`);
  }
}

async function handleAccounts(subCmd, opts) {
  switch (subCmd) {
    case 'list': {
      await ensureBudget();
      const allAccounts = await getCachedAccounts();
      const accounts = allAccounts.filter(a => opts['include-closed'] || !a.closed);
      accounts.sort((a, b) => Number(a.offbudget) - Number(b.offbudget));
      const [balances, notesResult] = await Promise.all([
        Promise.all(accounts.map(a => api.getAccountBalance(a.id))),
        api.aqlQuery(api.q('notes').select(['id', 'note'])).catch(() => ({ data: [] })),
      ]);
      const noteMap = new Map();
      if (Array.isArray(notesResult?.data)) {
        for (const n of notesResult.data) {
          if (n.note) noteMap.set(n.id, n.note);
        }
      }
      return accounts.map((a, i) => ({
        id: a.id,
        name: a.name,
        offbudget: a.offbudget,
        closed: a.closed,
        balance: balances[i],
        account_group_id: a.account_group_id ?? null,
        note: noteMap.get(`account-${a.id}`) ?? null,
      }));
    }
    case 'create': {
      await ensureBudget();
      const balance = opts.balance ? parseIntFlag(opts.balance, '--balance') : 0;
      const accountPayload = { name: opts.name, offbudget: !!opts.offbudget };
      if (opts['account-group-id']) accountPayload.account_group_id = opts['account-group-id'];
      const id = await api.createAccount(
        accountPayload,
        balance,
      );
      if (opts.note !== undefined) {
        await api.updateNote(`account-${id}`, opts.note === null ? '' : String(opts.note));
      }
      cache.accounts = null;
      return { id };
    }
    case 'update': {
      await ensureBudget();
      const id = opts._positional[0];
      if (!id) throw new Error('Account ID is required');
      const fields = {};
      if (opts.name !== undefined) {
        const trimmed = opts.name.trim();
        if (trimmed === '') throw new Error('Invalid --name: must be a non-empty string.');
        fields.name = trimmed;
      }
      if (opts.offbudget !== undefined) {
        fields.offbudget = parseBoolFlag(opts.offbudget, '--offbudget');
      }
      if (opts['account-group-id'] !== undefined) {
        fields.account_group_id = (opts['account-group-id'] === '' || opts['account-group-id'] === 'null') ? null : opts['account-group-id'];
      }
      if (Object.keys(fields).length === 0 && opts.note === undefined) {
        throw new Error('No update fields provided. Use --name, --offbudget, --account-group-id, or --note.');
      }
      if (Object.keys(fields).length > 0) {
        await api.updateAccount(id, fields);
        cache.accounts = null;
      }
      if (opts.note !== undefined) {
        await api.updateNote(`account-${id}`, opts.note === null ? '' : String(opts.note));
      }
      return { success: true, id };
    }
    case 'get-note':
    case 'note': {
      return handleNotes('get', { ...opts, type: 'account' });
    }
    case 'update-note':
    case 'set-note': {
      return handleNotes('update', { ...opts, type: 'account' });
    }
    case 'close': {
      await ensureBudget();
      const id = opts._positional[0];
      if (!id) throw new Error('Account ID is required');
      await api.closeAccount(id, opts['transfer-account'], opts['transfer-category']);
      cache.accounts = null;
      return { success: true, id };
    }
    case 'reopen': {
      await ensureBudget();
      const id = opts._positional[0];
      if (!id) throw new Error('Account ID is required');
      await api.reopenAccount(id);
      cache.accounts = null;
      return { success: true, id };
    }
    case 'delete': {
      await ensureBudget();
      const id = opts._positional[0];
      if (!id) throw new Error('Account ID is required');
      await api.deleteAccount(id);
      cache.accounts = null;
      return { success: true, id };
    }
    case 'balance': {
      await ensureBudget();
      const id = opts._positional[0];
      if (!id) throw new Error('Account ID is required');
      let cutoff;
      if (opts.cutoff) {
        const cutoffDate = new Date(opts.cutoff);
        if (Number.isNaN(cutoffDate.getTime())) {
          throw new Error('Invalid cutoff date: expected a valid date (e.g. YYYY-MM-DD).');
        }
        cutoff = cutoffDate;
      }
      const balance = await api.getAccountBalance(id, cutoff);
      return { id, balance };
    }
    default:
      throw new Error(`Unknown accounts subcommand: ${subCmd}`);
  }
}

async function handleBudgets(subCmd, opts) {
  switch (subCmd) {
    case 'list': {
      // budgets list doesn't need budget loaded, just server connection
      await ensureInit();
      const result = await api.getBudgets();
      return result;
    }
    case 'load': {
      await ensureInit();
      const budgetId = opts._positional[0] || opts.id;
      if (!budgetId) throw new Error('Budget ID is required');
      await api.loadBudget(budgetId);
      budgetLoaded = true;
      invalidateCache();
      return { success: true, id: budgetId };
    }
    case 'import': {
      await ensureInit();
      const filepath = opts._positional[0] || opts.file;
      if (!filepath) throw new Error('File path (--file or positional) is required');
      const importOpts = {};
      if (opts.type) importOpts.type = opts.type;
      if (opts.filename) importOpts.filename = opts.filename;
      const result = await api.importBudget(filepath, importOpts);
      budgetLoaded = true;
      invalidateCache();
      return { success: true, ...result };
    }
    case 'download': {
      await ensureInit();
      const syncId = opts._positional[0] || getSyncId();
      if (!syncId) throw new Error('syncId is required');
      const password = opts['encryption-password'] || getEncryptionPassword();
      try {
        await api.downloadBudget(syncId, { password });
      } catch (err) {
        if (isSchemaMismatch(err)) {
          await logMessage(`Schema mismatch during budget download: ${err.message}. Upgrading packages...`);
          await autoUpgradePackages();
          await api.downloadBudget(syncId, { password });
        } else {
          throw err;
        }
      }
      budgetLoaded = true;
      invalidateCache();
      return { success: true, syncId };
    }
    case 'months': {
      await ensureBudget();
      return await api.getBudgetMonths();
    }
    case 'month': {
      await ensureBudget();
      const month = opts._positional[0];
      if (!month) throw new Error('Month (YYYY-MM) is required');
      return await api.getBudgetMonth(month);
    }
    case 'set-amount': {
      await ensureBudget();
      const amount = parseIntFlag(opts.amount, '--amount');
      await api.setBudgetAmount(opts.month, opts.category, amount);
      return { success: true };
    }
    case 'set-carryover': {
      await ensureBudget();
      const flag = parseBoolFlag(opts.flag, '--flag');
      await api.setBudgetCarryover(opts.month, opts.category, flag);
      return { success: true };
    }
    case 'hold-next-month': {
      await ensureBudget();
      const amount = parseIntFlag(opts.amount, '--amount');
      await api.holdBudgetForNextMonth(opts.month, amount);
      return { success: true };
    }
    case 'reset-hold': {
      await ensureBudget();
      await api.resetBudgetHold(opts.month);
      return { success: true };
    }
    case 'export': {
      await ensureBudget();
      const zipBytes = await api.exportBudget();
      return {
        success: true,
        data: Buffer.from(zipBytes).toString('base64'),
        size: zipBytes.length
      };
    }
    default:
      throw new Error(`Unknown budgets subcommand: ${subCmd}`);
  }
}

async function handleTransactions(subCmd, opts) {
  switch (subCmd) {
    case 'list': {
      await ensureBudget();
      if (!opts.account) throw new Error('--account is required');
      if (!opts.start) throw new Error('--start is required');
      if (!opts.end) throw new Error('--end is required');
      const txs = await api.getTransactions(opts.account, opts.start, opts.end);
      if (opts['no-receipts'] || opts.no_receipts || opts['with-receipts'] === false) {
        return txs;
      }
      return enrichTransactionsWithReceipts(txs);
    }
    case 'add': {
      await ensureBudget();
      if (!opts.account) throw new Error('--account is required');
      const transactions = readJsonInput(opts);
      const result = await api.addTransactions(opts.account, transactions, {
        learnCategories: !!opts['learn-categories'],
        runTransfers: !!opts['run-transfers'],
      });
      return result;
    }
    case 'import': {
      await ensureBudget();
      if (!opts.account) throw new Error('--account is required');
      const transactions = readJsonInput(opts);
      const result = await api.importTransactions(opts.account, transactions, {
        defaultCleared: true,
        dryRun: !!opts['dry-run'],
      });
      return result;
    }
    case 'update': {
      await ensureBudget();
      const id = opts._positional[0];
      if (!id) throw new Error('Transaction ID is required');
      const fields = readJsonInput(opts);

      if (fields.payee_name) {
        if (!cache.payees) {
          cache.payees = await api.getPayees();
        }
        let payeeObj = cache.payees.find(p => p.name.toLowerCase() === fields.payee_name.toLowerCase());
        if (payeeObj) {
          fields.payee = payeeObj.id;
        } else {
          const newId = await api.createPayee({ name: fields.payee_name });
          fields.payee = newId;
          cache.payees = null; // Invalidate cache
        }
        delete fields.payee_name;
      }

      await api.updateTransaction(id, fields);
      return { success: true, id };
    }
    case 'delete': {
      await ensureBudget();
      const id = opts._positional[0];
      if (!id) throw new Error('Transaction ID is required');
      await api.deleteTransaction(id);
      return { success: true, id };
    }
    case 'merge': {
      await ensureBudget();
      const ids = opts._positional.length >= 2
        ? opts._positional.slice(0, 2)
        : (opts.ids ? String(opts.ids).split(',').map(s => s.trim()).filter(Boolean) : []);
      if (ids.length !== 2) {
        throw new Error('Exactly two transaction IDs are required to merge (pass as positional arguments or --ids id1,id2)');
      }
      const keptId = await api.mergeTransactions([ids[0], ids[1]]);
      return { success: true, id: keptId, merged: ids };
    }
    case 'restore-notes': {
      await ensureBudget();
      const ids = opts._positional.length > 0 ? opts._positional : (opts.ids ? opts.ids.split(',') : []);
      if (!ids || ids.length === 0) throw new Error('Transaction IDs are required as positional arguments');

      // Sanitize IDs to prevent injection
      for (const id of ids) {
        if (!/^[a-zA-Z0-9_-]+$/.test(id)) {
          throw new Error(`Invalid transaction ID format: "${id}"`);
        }
      }

      const idsList = ids.map(id => `'${id}'`).join(', ');
      const query = `SELECT id, notes FROM transactions WHERE id IN (${idsList});`;
      const output = execFileSync('sqlite3', ['-json', 'backup.sqlite', query], { encoding: 'utf8' });

      let rows = [];
      try {
        if (output.trim()) {
          rows = JSON.parse(output);
        }
      } catch (e) {
        throw new Error('Failed to parse sqlite output: ' + e.message);
      }

      const notesMap = {};
      for (const row of rows) {
        notesMap[row.id] = row.notes;
      }

      let restoredCount = 0;
      const results = [];
      for (const id of ids) {
        const notes = notesMap[id];
        if (notes != null) {
          await api.updateTransaction(id, { notes });
          restoredCount++;
          results.push({ id, status: 'restored', notes });
        } else {
          results.push({ id, status: 'skipped (not found)' });
        }
      }
      return { success: true, restoredCount, results };
    }
    case 'restore-transaction': {
      await ensureBudget();
      const ids = opts._positional.length > 0 ? opts._positional : (opts.ids ? opts.ids.split(',') : []);
      if (!ids || ids.length === 0) throw new Error('Transaction IDs are required as positional arguments');

      // Sanitize IDs to prevent injection
      for (const id of ids) {
        if (!/^[a-zA-Z0-9_-]+$/.test(id)) {
          throw new Error(`Invalid transaction ID format: "${id}"`);
        }
      }

      const idsList = ids.map(id => `'${id}'`).join(', ');
      const query = `SELECT * FROM transactions WHERE id IN (${idsList});`;
      const output = execFileSync('sqlite3', ['-json', 'backup.sqlite', query], { encoding: 'utf8' });


      let rows = [];
      try {
        if (output.trim()) {
          rows = JSON.parse(output);
        }
      } catch (e) {
        throw new Error('Failed to parse sqlite output: ' + e.message);
      }

      let restoredCount = 0;
      const results = [];
      for (const row of rows) {
        const apiTx = {};
        if (row.acct) apiTx.account = row.acct;
        if (row.amount != null) apiTx.amount = row.amount;
        if (row.category) apiTx.category = row.category;
        if (row.description) apiTx.payee = row.description;
        apiTx.notes = row.notes || '';
        if (row.date) {
          const d = String(row.date);
          if (d.length === 8) {
            apiTx.date = `${d.slice(0, 4)}-${d.slice(4, 6)}-${d.slice(6, 8)}`;
          }
        }
        if (row.cleared != null) apiTx.cleared = Boolean(row.cleared);

        await api.updateTransaction(row.id, apiTx);
        restoredCount++;
        results.push({ id: row.id, status: 'restored', fields: apiTx });
      }

      // Check for missing ids
      const foundIds = new Set(rows.map(r => r.id));
      for (const id of ids) {
        if (!foundIds.has(id)) {
          results.push({ id, status: 'skipped (not found)' });
        }
      }

      return { success: true, restoredCount, results };
    }
    default:
      throw new Error(`Unknown transactions subcommand: ${subCmd}`);
  }
}

async function handleCategories(subCmd, opts) {
  switch (subCmd) {
    case 'list': {
      await ensureBudget();
      const categories = await getCachedCategories();
      if (opts['include-notes'] || opts.include_notes) {
        const notesRes = await api.aqlQuery(api.q('notes').select(['id', 'note'])).catch(() => ({ data: [] }));
        const noteMap = new Map((notesRes?.data || []).map(n => [n.id, n.note]));
        return categories.map(c => ({ ...c, note: noteMap.get(c.id) ?? null }));
      }
      return categories;
    }
    case 'create': {
      await ensureBudget();
      if (!opts.name) throw new Error('--name is required');
      if (!opts['group-id']) throw new Error('--group-id is required');
      const id = await api.createCategory({
        name: opts.name,
        group_id: opts['group-id'],
        is_income: !!opts['is-income'],
        hidden: false,
      });
      if (opts.note !== undefined) {
        await api.updateNote(id, opts.note === null ? '' : String(opts.note));
      }
      cache.categories = null;
      return { id };
    }
    case 'update': {
      await ensureBudget();
      const id = opts._positional[0];
      if (!id) throw new Error('Category ID is required');
      const fields = {};
      if (opts.name !== undefined) fields.name = opts.name;
      if (opts.hidden !== undefined) fields.hidden = parseBoolFlag(opts.hidden, '--hidden');
      if (opts['group-id'] !== undefined) fields.group_id = opts['group-id'];
      if (Object.keys(fields).length === 0 && opts.note === undefined) {
        throw new Error('No update fields provided. Use --name, --hidden, --group-id, or --note.');
      }
      if (Object.keys(fields).length > 0) {
        await api.updateCategory(id, fields);
        cache.categories = null;
        cache.categoryGroups = null;
      }
      if (opts.note !== undefined) {
        await api.updateNote(id, opts.note === null ? '' : String(opts.note));
      }
      return { success: true, id };
    }
    case 'delete': {
      await ensureBudget();
      const id = opts._positional[0];
      if (!id) throw new Error('Category ID is required');
      await api.deleteCategory(id, opts['transfer-to']);
      cache.categories = null;
      return { success: true, id };
    }
    default:
      throw new Error(`Unknown categories subcommand: ${subCmd}`);
  }
}

async function handleCategoryGroups(subCmd, opts) {
  switch (subCmd) {
    case 'list': {
      await ensureBudget();
      return await getCachedCategoryGroups();
    }
    case 'create': {
      await ensureBudget();
      if (!opts.name) throw new Error('--name is required');
      const id = await api.createCategoryGroup({
        name: opts.name,
        is_income: !!opts['is-income'],
        hidden: false,
      });
      cache.categoryGroups = null;
      return { id };
    }
    case 'update': {
      await ensureBudget();
      const id = opts._positional[0];
      if (!id) throw new Error('Category group ID is required');
      const fields = {};
      if (opts.name !== undefined) fields.name = opts.name;
      if (opts.hidden !== undefined) fields.hidden = parseBoolFlag(opts.hidden, '--hidden');
      if (Object.keys(fields).length === 0) {
        throw new Error('No update fields provided. Use --name or --hidden.');
      }
      await api.updateCategoryGroup(id, fields);
      cache.categoryGroups = null;
      return { success: true, id };
    }
    case 'delete': {
      await ensureBudget();
      const id = opts._positional[0];
      if (!id) throw new Error('Category group ID is required');
      await api.deleteCategoryGroup(id, opts['transfer-to']);
      cache.categoryGroups = null;
      return { success: true, id };
    }
    default:
      throw new Error(`Unknown category-groups subcommand: ${subCmd}`);
  }
}

async function handlePayees(subCmd, opts) {
  switch (subCmd) {
    case 'list': {
      await ensureBudget();
      return await getCachedPayees();
    }
    case 'common': {
      await ensureBudget();
      return await api.getCommonPayees();
    }
    case 'create': {
      await ensureBudget();
      if (!opts.name) throw new Error('--name is required');
      const id = await api.createPayee({ name: opts.name });
      cache.payees = null;
      return { id };
    }
    case 'update': {
      await ensureBudget();
      const id = opts._positional[0];
      if (!id) throw new Error('Payee ID is required');
      const fields = {};
      if (opts.name) fields.name = opts.name;
      if (Object.keys(fields).length === 0) {
        throw new Error('No fields to update. Use --name to specify a new name.');
      }
      await api.updatePayee(id, fields);
      cache.payees = null;
      return { success: true, id };
    }
    case 'delete': {
      await ensureBudget();
      const id = opts._positional[0];
      if (!id) throw new Error('Payee ID is required');
      await api.deletePayee(id);
      cache.payees = null;
      return { success: true, id };
    }
    case 'merge': {
      await ensureBudget();
      if (!opts.target) throw new Error('--target is required');
      if (!opts.ids) throw new Error('--ids is required');
      const mergeIds = opts.ids.split(',').map(id => id.trim()).filter(id => id.length > 0);
      if (mergeIds.length === 0) {
        throw new Error('No valid payee IDs provided in --ids.');
      }
      await api.mergePayees(opts.target, mergeIds);
      cache.payees = null;
      return { success: true };
    }
    case 'locations': {
      await ensureBudget();
      const payeeId = opts._positional[0] || opts['payee-id'] || opts.payee_id;
      if (payeeId && api.internal?.send) {
        return await api.internal.send('api/payee-locations-get', { payeeId });
      }
      let q = api.q('payee_locations').select(['*']);
      if (payeeId) q = q.filter({ payee_id: payeeId });
      const res = await api.aqlQuery(q);
      return res?.data ?? [];
    }
    case 'location-create': {
      await ensureBudget();
      const payeeId = opts._positional[0] || opts['payee-id'] || opts.payee_id;
      if (!payeeId) throw new Error('Payee ID is required');
      if (opts.latitude === undefined || opts.longitude === undefined) {
        throw new Error('Both --latitude and --longitude are required');
      }
      const latitude = Number(opts.latitude);
      const longitude = Number(opts.longitude);
      if (Number.isNaN(latitude) || Number.isNaN(longitude)) {
        throw new Error('--latitude and --longitude must be valid numbers');
      }
      const id = await api.internal.send('api/payee-location-create', { payeeId, latitude, longitude });
      return { id, payee_id: payeeId, latitude, longitude };
    }
    case 'location-delete': {
      await ensureBudget();
      const id = opts._positional[0] || opts.id;
      if (!id) throw new Error('Payee location ID is required');
      await api.internal.send('api/payee-location-delete', { id });
      return { success: true, id };
    }
    case 'nearby': {
      await ensureBudget();
      if (opts.latitude === undefined || opts.longitude === undefined) {
        throw new Error('Both --latitude and --longitude are required');
      }
      const latitude = Number(opts.latitude);
      const longitude = Number(opts.longitude);
      const maxDistance = opts['max-distance'] !== undefined ? Number(opts['max-distance']) : undefined;
      return await api.internal.send('api/payees-get-nearby', { latitude, longitude, maxDistance });
    }
    default:
      throw new Error(`Unknown payees subcommand: ${subCmd}`);
  }
}

async function handleTags(subCmd, opts) {
  switch (subCmd) {
    case 'list': {
      await ensureBudget();
      return await getCachedTags();
    }
    case 'create': {
      await ensureBudget();
      if (!opts.tag) throw new Error('--tag is required');
      const id = await api.createTag({
        tag: opts.tag,
        color: opts.color,
        description: opts.description,
      });
      cache.tags = null;
      return { id };
    }
    case 'update': {
      await ensureBudget();
      const id = opts._positional[0];
      if (!id) throw new Error('Tag ID is required');
      const fields = {};
      if (opts.tag !== undefined) fields.tag = opts.tag;
      if (opts.color !== undefined) fields.color = opts.color;
      if (opts.description !== undefined) fields.description = opts.description;
      if (Object.keys(fields).length === 0) {
        throw new Error('At least one of --tag, --color, or --description is required');
      }
      await api.updateTag(id, fields);
      cache.tags = null;
      return { success: true, id };
    }
    case 'delete': {
      await ensureBudget();
      const id = opts._positional[0];
      if (!id) throw new Error('Tag ID is required');
      await api.deleteTag(id);
      cache.tags = null;
      return { success: true, id };
    }
    default:
      throw new Error(`Unknown tags subcommand: ${subCmd}`);
  }
}

async function handleRules(subCmd, opts) {
  switch (subCmd) {
    case 'list': {
      await ensureBudget();
      return await api.getRules();
    }
    case 'payee-rules': {
      await ensureBudget();
      const payeeId = opts._positional[0];
      if (!payeeId) throw new Error('Payee ID is required');
      return await api.getPayeeRules(payeeId);
    }
    case 'create': {
      await ensureBudget();
      const rule = readJsonInput(opts);
      const id = await api.createRule(rule);
      return { id };
    }
    case 'update': {
      await ensureBudget();
      const rule = readJsonInput(opts);
      await api.updateRule(rule);
      return { success: true };
    }
    case 'delete': {
      await ensureBudget();
      const id = opts._positional[0];
      if (!id) throw new Error('Rule ID is required');
      await api.deleteRule(id);
      return { success: true, id };
    }
    default:
      throw new Error(`Unknown rules subcommand: ${subCmd}`);
  }
}

async function handleSchedules(subCmd, opts) {
  switch (subCmd) {
    case 'list': {
      await ensureBudget();
      return await api.getSchedules();
    }
    case 'dates': {
      await ensureBudget();
      let dateConfig = opts.date || opts.data;
      const scheduleId = opts._positional[0] || opts.id || opts['schedule-id'];
      if (scheduleId && !dateConfig) {
        const schedules = await api.getSchedules();
        const found = schedules.find(s => s.id === scheduleId || s.name === scheduleId);
        if (!found) throw new Error(`Schedule not found: ${scheduleId}`);
        dateConfig = found.date;
      } else if (typeof dateConfig === 'string') {
        const trimmed = dateConfig.trim();
        if (trimmed.startsWith('{')) {
          dateConfig = JSON.parse(trimmed);
        }
      }
      if (!dateConfig) {
        throw new Error('Either a schedule ID (positional or --id) or --date (YYYY-MM-DD or RecurConfig JSON) is required');
      }
      const dateOpts = {};
      if (opts.start) dateOpts.start = opts.start;
      if (opts.end) dateOpts.end = opts.end;
      if (opts.count !== undefined) dateOpts.count = parseIntFlag(opts.count, '--count');
      const dates = await api.getScheduleDates(dateConfig, dateOpts);
      return { dates };
    }
    case 'create': {
      await ensureBudget();
      const schedule = readJsonInput(opts);
      const id = await api.createSchedule(schedule);
      return { id };
    }
    case 'update': {
      await ensureBudget();
      const id = opts._positional[0];
      if (!id) throw new Error('Schedule ID is required');
      const fields = readJsonInput(opts);
      await api.updateSchedule(id, fields, !!opts['reset-next-date']);
      return { success: true, id };
    }
    case 'delete': {
      await ensureBudget();
      const id = opts._positional[0];
      if (!id) throw new Error('Schedule ID is required');
      await api.deleteSchedule(id);
      return { success: true, id };
    }
    default:
      throw new Error(`Unknown schedules subcommand: ${subCmd}`);
  }
}

async function handleSync(subCmd, opts) {
  if (opts.status || subCmd === 'status') {
    // We don't have a direct equivalent; just report that connection is alive
    await ensureBudget();
    return { status: 'connected', budgetLoaded: true };
  }
  if (opts.clear || subCmd === 'clear') {
    // Reset everything
    budgetLoaded = false;
    invalidateCache();
    if (api && apiInitialized) {
      try { await api.shutdown(); } catch (e) { /* ignore */ }
    }
    apiInitialized = false;
    api = null;
    return { success: true, message: 'Cache cleared. Will reconnect on next call.' };
  }
  // Default: sync
  await ensureBudget();
  await api.sync();
  invalidateCache();
  return { success: true };
}

async function handleQuery(subCmd, opts) {
  switch (subCmd) {
    case 'tables': {
      return Object.keys(TABLE_SCHEMA).map(name => ({ name }));
    }
    case 'fields': {
      const table = opts._positional[0];
      if (!table) throw new Error('Table name is required');
      const schema = TABLE_SCHEMA[table];
      if (!schema) {
        throw new Error(`Unknown table "${table}". Available tables: ${AVAILABLE_TABLES}`);
      }
      return Object.entries(schema).map(([name, info]) => ({
        name, type: info.type, ...(info.ref ? { ref: info.ref } : {}),
      }));
    }
    case 'run': {
      if (opts.table === 'receipts') {
        return queryReceipts(opts);
      }
      await ensureBudget();
      let queryObj;
      let effectiveTable = opts.table;
      if (opts.file || opts.data) {
        const parsed = readJsonInput(opts);
        if (!isRecord(parsed)) throw new Error('Query file must contain a JSON object');
        if (parsed.table === 'receipts' || effectiveTable === 'receipts') {
          return queryReceipts({ ...opts, ...parsed });
        }
        effectiveTable = typeof parsed.table === 'string' ? parsed.table : effectiveTable;
        queryObj = buildQueryFromFile(parsed, opts.table);
      } else {
        queryObj = buildQuery(opts);
        effectiveTable = effectiveTable || 'transactions';
      }
      const result = await api.aqlQuery(queryObj);
      if (!isRecord(result) || !('data' in result)) {
        throw new Error('Query result missing data');
      }
      if (opts.count) {
        return { count: result.data };
      }
      const data = result.data;
      if (Array.isArray(data) && effectiveTable === 'transactions') {
        if (!opts['no-receipts'] && !opts.no_receipts && opts['with-receipts'] !== false) {
          return enrichTransactionsWithReceipts(data);
        }
      }
      return data;
    }
    default:
      throw new Error(`Unknown query subcommand: ${subCmd}`);
  }
}

async function handleServer(subCmd, opts) {
  switch (subCmd) {
    case 'version': {
      await ensureInit(); // server version doesn't need budget
      const version = await api.getServerVersion();
      return { version };
    }
    case 'info': {
      // Return server and instance info
      return { dataDir: INSTANCE_DATA_DIR };
    }
    case 'get-id': {
      await ensureBudget();
      if (!opts.type) throw new Error('--type is required');
      if (!opts.name) throw new Error('--name is required');
      const normalizedType = opts.type.replace(/-/g, '_');
      if (['accounts', 'schedules', 'categories', 'payees'].includes(normalizedType)) {
        const id = await api.getIDByName(normalizedType, opts.name);
        return { id, type: opts.type, name: opts.name };
      }
      if (['category_groups', 'account_groups'].includes(normalizedType)) {
        const res = await api.aqlQuery(api.q(normalizedType).filter({ name: opts.name }).select(['id']));
        if (!res?.data?.length) throw new Error(`Not found: ${opts.type} with name ${opts.name}`);
        return { id: res.data[0].id, type: opts.type, name: opts.name };
      }
      if (normalizedType === 'tags') {
        const tags = await getCachedTags();
        const found = tags.find(t => t.tag === opts.name);
        if (!found) throw new Error(`Not found: tags with name ${opts.name}`);
        return { id: found.id, type: opts.type, name: opts.name };
      }
      const id = await api.getIDByName(opts.type, opts.name);
      return { id, type: opts.type, name: opts.name };
    }
    case 'bank-sync': {
      await ensureBudget();
      const args = opts.account ? { accountId: opts.account } : undefined;
      const syncResult = await api.runBankSync(args);
      return { success: true, ...(syncResult || {}) };
    }
    default:
      throw new Error(`Unknown server subcommand: ${subCmd}`);
  }
}

// ───────────────────────────────────────────────────────────
// Command Router
// ───────────────────────────────────────────────────────────
const COMMAND_HANDLERS = {
  accounts: handleAccounts,
  'account-groups': handleAccountGroups,
  notes: handleNotes,
  preferences: handlePreferences,
  budgets: handleBudgets,
  transactions: handleTransactions,
  categories: handleCategories,
  'category-groups': handleCategoryGroups,
  payees: handlePayees,
  tags: handleTags,
  rules: handleRules,
  schedules: handleSchedules,
  sync: handleSync,
  query: handleQuery,
  server: handleServer,
  receipts: handleReceipts,
};

async function executeCommand(cliArgs, explicitClientId = null) {
  const command = cliArgs[0];
  if (!command) throw new Error('No command specified');

  const handler = COMMAND_HANDLERS[command];
  if (!handler) throw new Error(`Unknown command: ${command}`);

  // Parse subcommand and options
  const remaining = cliArgs.slice(1);
  const parsed = parseArgs(remaining);
  const subCmd = parsed._positional[0];
  // For commands like sync that may not have a subcommand
  const positionalAfterSub = parsed._positional.slice(1);

  // Re-parse with subcommand consumed
  const opts = { ...parsed, _positional: positionalAfterSub };

  await logCommand(`${command} ${remaining.join(' ')}`, explicitClientId);

  return handler(subCmd || '', opts);
}

// ───────────────────────────────────────────────────────────
// MCP Server Setup
// ───────────────────────────────────────────────────────────

const COMMAND_SCHEMAS = {
  accounts: {
    desc: "Manage accounts.",
    details: "Subcommands:\n" +
      "- list [--include-closed] [--format json|table|csv]\n" +
      "- create --name <name> [--offbudget] [--balance <cents>] [--account-group-id <id>] [--note <text>]\n" +
      "- update <id> [--name <name>] [--offbudget <bool>] [--account-group-id <id>] [--note <text>]\n" +
      "- get-note <id>\n" +
      "- update-note <id> --note <text>\n" +
      "- close <id> [--transfer-account <id>] [--transfer-category <id>]\n" +
      "- reopen <id>\n" +
      "- delete <id>\n" +
      "- balance <id> [--cutoff <YYYY-MM-DD>]",
    rules: "Rules:\n- Accounts must be referenced by their UUID.\n- Use `balance` with an account ID to get its current balance."
  },
  'account-groups': {
    desc: "Manage account groups.",
    details: "Subcommands:\n" +
      "- list\n" +
      "- create --name <name>\n" +
      "- update <id> --name <name>\n" +
      "- delete <id>",
    rules: "Rules:\n- Account groups organize accounts in the sidebar."
  },
  notes: {
    desc: "Manage entity notes (accounts, categories, budget months).",
    details: "Subcommands:\n" +
      "- list [--type account|category|budget|all] [--include-empty]\n" +
      "- get <id> [--type account|category|budget|raw]\n" +
      "- update <id> --note <text> [--type account|category|budget|raw]",
    rules: "Rules:\n- Automatically resolves account UUIDs to `account-<id>` and YYYY-MM months to `budget-<YYYY-MM>`."
  },
  preferences: {
    desc: "Manage synced budget preferences.",
    details: "Subcommands:\n" +
      "- get [<key>]\n" +
      "- set <key> --value <value>",
    rules: "Rules:\n- Preferences are stored as string key-value pairs."
  },
  budgets: {
    desc: "Manage budgets.",
    details: "Subcommands:\n" +
      "- list\n" +
      "- load <budgetId>\n" +
      "- import --file <path> [--type actual|ynab4|nynab] [--filename <name>]\n" +
      "- download <syncId> [--encryption-password <pw>]\n" +
      "- months\n" +
      "- month <YYYY-MM>\n" +
      "- set-amount --month <YYYY-MM> --category <id> --amount <cents>\n" +
      "- set-carryover --month <YYYY-MM> --category <id> --flag <bool>\n" +
      "- hold-next-month --month <YYYY-MM> --amount <cents>\n" +
      "- reset-hold --month <YYYY-MM>\n" +
      "- export",
    rules: "Rules:\n- Budgets list does not require syncId.\n- `--month` must be formatted as YYYY-MM."
  },
  transactions: {
    desc: "Manage transactions.",
    details: "Subcommands:\n" +
      "- list --account <id> --start <YYYY-MM-DD> --end <YYYY-MM-DD> [--no-receipts]\n" +
      "- add --account <id> (--data <json> | --file <path>) [--learn-categories] [--run-transfers]\n" +
      "- import --account <id> (--data <json> | --file <path>) [--dry-run]\n" +
      "- update <id> (--data <json> | --file <path>)\n" +
      "- delete <id>\n" +
      "- merge <id1> <id2>\n" +
      "- restore-notes <id1> <id2> ...\n" +
      "- restore-transaction <id1> <id2> ...",
    rules: "Rules:\n- list: ALL of `--account`, `--start`, and `--end` are absolutely required or the command will fail.\n- add/import: MUST provide `--account` and either `--data` (as JSON string) or `--file`.\n- update: Takes the transaction ID as a positional argument. The `--data` flag must contain fields to update.\n- merge: Merges two transactions into one.\n- restore-notes: Takes a list of transaction IDs and restores their notes from backup.sqlite.\n- restore-transaction: Takes a list of transaction IDs and restores all fields from backup.sqlite."
  },
  categories: {
    desc: "Manage categories.",
    details: "Subcommands:\n" +
      "- list [--include-hidden] [--include-notes]\n" +
      "- create --name <name> --group-id <id> [--is-income] [--note <text>]\n" +
      "- update <id> [--name <name>] [--hidden <bool>] [--group-id <id>] [--note <text>]\n" +
      "- delete <id> [--transfer-to <id>]",
    rules: "Rules:\n- create MUST have both `--name` and `--group-id`.\n- update MUST have at least one flag to change."
  },
  'category-groups': {
    desc: "Manage category groups.",
    details: "Subcommands:\n" +
      "- list [--include-hidden]\n" +
      "- create --name <name> [--is-income]\n" +
      "- update <id> [--name <name>] [--hidden <bool>]\n" +
      "- delete <id> [--transfer-to <id>]",
    rules: "Rules:\n- list output will include categories nested inside the group if standard format is used."
  },
  payees: {
    desc: "Manage payees.",
    details: "Subcommands:\n" +
      "- list\n" +
      "- common\n" +
      "- create --name <name>\n" +
      "- update <id> --name <name>\n" +
      "- delete <id>\n" +
      "- merge --target <id> --ids <id1,id2,id3>\n" +
      "- locations [<payeeId>]\n" +
      "- location-create <payeeId> --latitude <lat> --longitude <lng>\n" +
      "- location-delete <locationId>\n" +
      "- nearby --latitude <lat> --longitude <lng> [--max-distance <meters>]",
    rules: "Rules:\n- merge requires `--target` (the final ID) and `--ids` (comma separated list to merge)."
  },
  tags: {
    desc: "Manage tags.",
    details: "Subcommands:\n" +
      "- list\n" +
      "- create --tag <name> [--color <color>] [--description <text>]\n" +
      "- update <id> [--tag <name>] [--color <color>] [--description <text>]\n" +
      "- delete <id>",
    rules: "Rules:\n- `--tag` is used for the name instead of `--name`."
  },
  rules: {
    desc: "Manage transaction rules.",
    details: "Subcommands:\n" +
      "- list\n" +
      "- payee-rules <payeeId>\n" +
      "- create (--data <json> | --file <path>)\n" +
      "- update (--data <json> | --file <path>)\n" +
      "- delete <id>",
    rules: "Rules:\n- update: CRITICAL NOTE: unlike other commands, `update` does NOT take a positional `<id>` argument. The `id` must be provided inside the JSON payload."
  },
  schedules: {
    desc: "Manage scheduled transactions.",
    details: "Subcommands:\n" +
      "- list\n" +
      "- dates [<scheduleId>] [--date <YYYY-MM-DD|json>] [--start <YYYY-MM-DD>] [--end <YYYY-MM-DD>] [--count <n>]\n" +
      "- create (--data <json> | --file <path>)\n" +
      "- update <id> (--data <json> | --file <path>) [--reset-next-date]\n" +
      "- delete <id>",
    rules: "Rules:\n- create and update use JSON via `--data` for configuration."
  },
  sync: {
    desc: "Sync the local cached budget with the server.",
    details: "Flags:\n" +
      "- (none) Syncs the local cached budget with the server\n" +
      "- --status Prints cache status\n" +
      "- --clear Deletes the local cache state file",
    rules: "Rules:\n- Call with no arguments to force a sync."
  },
  query: {
    desc: "Run ActualQL queries.",
    details: "Subcommands:\n" +
      "- tables\n" +
      "- fields <table>\n" +
      "- run [--table <table>] [--select <fields>] [--filter <json>] [--order-by <fields>] [--limit <n>] [--count] [--file <path>]",
    rules: "Rules:\n- Use `--filter` with ActualQL JSON syntax (e.g. `'{\"amount\": {\"$lt\": 0}}'`).\n- `--table` is usually required unless using `--file` or `--last`."
  },
  server: {
    desc: "Server utilities.",
    details: "Subcommands:\n" +
      "- version\n" +
      "- get-id --type <type> --name <name>\n" +
      "- bank-sync [--account <id>]",
    rules: "Rules:\n- `get-id` requires `--type` (e.g. 'accounts', 'account_groups', 'categories', 'category_groups', 'payees', 'schedules', 'tags') and `--name`."
  },
  receipts: {
    desc: "Manage and query receipts (PocketBase backed with local SQLite cache).",
    details: "Subcommands:\n" +
      "- list [--tx-id <id>] [--tx-ids <id1,id2>] [--unlinked-only] [--date <YYYY-MM-DD>] [--start <YYYY-MM-DD>] [--end <YYYY-MM-DD>] [--source <src>] [--source-ref <ref>] [--source-hash <hash>] [--payee <name>] [--search <str>] [--limit <n>] [--offset <n>]\n" +
      "- get <id> [--tx-id <id>] [--source-hash <hash>] [--source-ref <ref>]\n" +
      "- image <id> [--filename <name>] [--page-index <n>]\n" +
      "- link <id> --tx-id <id> [--tx-id-transfer <id>]\n" +
      "- unlink <id>\n" +
      "- exists [--source-hash <hash>] [--source-ref <ref>] [--tx-id <id>] [--active-only]\n" +
      "- save (--data <json> | --file <path>)\n" +
      "- delete <id> [--source-hash <hash>] [--tx-id <id>]\n" +
      "- sync [--full]",
    rules: "Rules:\n- Backed by PocketBase with automatic fast local SQLite caching.\n- Supports querying receipts as an integrated ActualQL table via `query run --table receipts`."
  }
};

const CLI_COMMANDS = Object.keys(COMMAND_SCHEMAS);

// ───────────────────────────────────────────────────────────
// Granular Tool Definitions (Explicit Read-Only vs. Mutating)
// ───────────────────────────────────────────────────────────

export const MCP_TOOL_DEFINITIONS = [
  // ─── READ-ONLY TOOLS (Auto-approved for Gemini Spark) ───
  {
    name: "actual_query",
    readOnly: true,
    description: `[READ-ONLY] Run an ActualQL query against budget data (e.g., transactions, accounts, categories, payees). Does not modify data.

**Querying Best Practices**:
- **Select Fields**: Use --select "date,amount,payee.name,notes,category.name,category.group.name,account.name,account.offbudget".
- **Filter Syntax**: Use ActualQL JSON filter object, e.g. {"amount": {"$lt": 0}} or {"category.group.name": {"$oneof": ["Food", "Bills"]}}.
- **Historical Analysis**: Run a single query to fetch transactions rather than multiple per-month API calls.`,
    inputSchema: {
      type: "object",
      properties: {
        table: { type: "string", description: "Target table (e.g. transactions, accounts, categories, payees)" },
        select: { type: "string", description: "Comma-separated field list (e.g. 'date,amount,payee.name,notes,category.name')" },
        filter: { type: ["object", "string"], description: "ActualQL filter object or JSON string" },
        order_by: { type: "string", description: "Sort fields" },
        limit: { type: "integer", description: "Maximum rows" },
        count: { type: "boolean", description: "Return count only" },
        data: { type: ["object", "string"], description: "Custom query JSON object or string" },
        file: { type: "string", description: "Path to query JSON file" },
        args: { type: "array", items: { type: "string" }, description: "Legacy CLI argument list" }
      }
    },
    toCli: (input = {}) => {
      if (Array.isArray(input.args)) {
        return ["query", ...(input.args[0] === "run" || input.args[0] === "tables" || input.args[0] === "fields" ? [] : ["run"]), ...input.args];
      }
      const cli = ["query", "run"];
      if (input.table) cli.push("--table", input.table);
      if (input.select) cli.push("--select", input.select);
      if (input.filter) cli.push("--filter", typeof input.filter === "object" ? JSON.stringify(input.filter) : input.filter);
      if (input.order_by || input["order-by"]) cli.push("--order-by", input.order_by || input["order-by"]);
      if (input.limit !== undefined) cli.push("--limit", String(input.limit));
      if (input.count) cli.push("--count");
      if (input.data) cli.push("--data", typeof input.data === "object" ? JSON.stringify(input.data) : input.data);
      if (input.file) cli.push("--file", input.file);
      return cli;
    }
  },
  {
    name: "actual_query_tables",
    readOnly: true,
    description: "[READ-ONLY] List all available ActualQL tables in the budget schema.",
    inputSchema: {
      type: "object",
      properties: {
        args: { type: "array", items: { type: "string" } }
      }
    },
    toCli: (input = {}) => ["query", "tables", ...(input.args || [])]
  },
  {
    name: "actual_query_fields",
    readOnly: true,
    description: "[READ-ONLY] Get field definitions and schema for a specific ActualQL table.",
    inputSchema: {
      type: "object",
      properties: {
        table: { type: "string", description: "Table name (e.g. transactions, accounts)" },
        args: { type: "array", items: { type: "string" } }
      },
      required: ["table"]
    },
    toCli: (input = {}) => {
      if (Array.isArray(input.args)) return ["query", "fields", ...input.args];
      return ["query", "fields", input.table];
    }
  },
  {
    name: "actual_list_accounts",
    readOnly: true,
    description: "[READ-ONLY] List all budget and off-budget accounts, including IDs, names, closed status, current balances in cents, account_group_id, and attached account notes.",
    inputSchema: {
      type: "object",
      properties: {
        include_closed: { type: "boolean", description: "Include closed accounts (default: false)" },
        format: { type: "string", enum: ["json", "table", "csv"], description: "Output format" },
        args: { type: "array", items: { type: "string" } }
      }
    },
    toCli: (input = {}) => {
      if (Array.isArray(input.args)) return ["accounts", "list", ...input.args];
      const cli = ["accounts", "list"];
      if (input.include_closed || input["include-closed"]) cli.push("--include-closed");
      if (input.format) cli.push("--format", input.format);
      return cli;
    }
  },
  {
    name: "actual_get_account_balance",
    readOnly: true,
    description: "[READ-ONLY] Get the current or historical balance for an account by ID (in integer cents).",
    inputSchema: {
      type: "object",
      properties: {
        id: { type: "string", description: "Account UUID" },
        cutoff: { type: "string", description: "Optional cutoff date (YYYY-MM-DD)" },
        args: { type: "array", items: { type: "string" } }
      },
      required: ["id"]
    },
    toCli: (input = {}) => {
      if (Array.isArray(input.args)) return ["accounts", "balance", ...input.args];
      const cli = ["accounts", "balance", input.id];
      if (input.cutoff) cli.push("--cutoff", input.cutoff);
      return cli;
    }
  },
  {
    name: "actual_list_account_groups",
    readOnly: true,
    description: "[READ-ONLY] List all account groups in the budget.",
    inputSchema: {
      type: "object",
      properties: {
        args: { type: "array", items: { type: "string" } }
      }
    },
    toCli: (input = {}) => ["account-groups", "list", ...(input.args || [])]
  },
  {
    name: "actual_get_note",
    readOnly: true,
    description: "[READ-ONLY] Get the note attached to an account, category, category group, budget month (YYYY-MM), or raw note ID. Accepts bare account UUIDs (automatically resolves 'account-<id>'), category UUIDs, or 'YYYY-MM' months.",
    inputSchema: {
      type: "object",
      properties: {
        id: { type: "string", description: "Entity UUID (account or category), month (YYYY-MM), or raw note ID (e.g. 'account-<uuid>', 'budget-YYYY-MM')" },
        type: { type: "string", enum: ["account", "category", "budget", "raw"], description: "Optional entity type hint ('account', 'category', 'budget', 'raw')" },
        args: { type: "array", items: { type: "string" } }
      },
      required: ["id"]
    },
    toCli: (input = {}) => {
      if (Array.isArray(input.args)) return ["notes", "get", ...input.args];
      const cli = ["notes", "get", input.id];
      if (input.type) cli.push("--type", input.type);
      return cli;
    }
  },
  {
    name: "actual_list_notes",
    readOnly: true,
    description: "[READ-ONLY] List all notes attached to accounts, categories, category groups, and budget months, enriched with entity names and types.",
    inputSchema: {
      type: "object",
      properties: {
        type: { type: "string", enum: ["account", "category", "category_group", "budget", "all"], description: "Filter by entity type (default: 'all')" },
        include_empty: { type: "boolean", description: "Include empty/cleared notes (default: false)" },
        args: { type: "array", items: { type: "string" } }
      }
    },
    toCli: (input = {}) => {
      if (Array.isArray(input.args)) return ["notes", "list", ...input.args];
      const cli = ["notes", "list"];
      if (input.type) cli.push("--type", input.type);
      if (input.include_empty || input["include-empty"]) cli.push("--include-empty");
      return cli;
    }
  },
  {
    name: "actual_get_preferences",
    readOnly: true,
    description: "[READ-ONLY] Get all budget preferences (or a single preference value by key, e.g. 'firstDayOfWeekIdx', 'dateFormat', 'numberFormat', 'defaultCurrencyCode').",
    inputSchema: {
      type: "object",
      properties: {
        key: { type: "string", description: "Optional preference key to retrieve a single setting" },
        args: { type: "array", items: { type: "string" } }
      }
    },
    toCli: (input = {}) => {
      if (Array.isArray(input.args)) return ["preferences", "get", ...input.args];
      const cli = ["preferences", "get"];
      if (input.key) cli.push(input.key);
      return cli;
    }
  },
  {
    name: "actual_list_budgets",
    readOnly: true,
    description: "[READ-ONLY] List all budgets available on the Actual server.",
    inputSchema: {
      type: "object",
      properties: {
        args: { type: "array", items: { type: "string" } }
      }
    },
    toCli: (input = {}) => ["budgets", "list", ...(input.args || [])]
  },
  {
    name: "actual_export_budget",
    readOnly: true,
    description: "[READ-ONLY] Export the current Actual Budget database as a base64-encoded ZIP archive (containing db.sqlite and metadata.json).",
    inputSchema: {
      type: "object",
      properties: {
        args: { type: "array", items: { type: "string" } }
      }
    },
    toCli: (input = {}) => ["budgets", "export", ...(input.args || [])]
  },
  {
    name: "actual_list_budget_months",
    readOnly: true,
    description: "[READ-ONLY] List all available budget months.",
    inputSchema: {
      type: "object",
      properties: {
        args: { type: "array", items: { type: "string" } }
      }
    },
    toCli: (input = {}) => ["budgets", "months", ...(input.args || [])]
  },
  {
    name: "actual_get_budget_month",
    readOnly: true,
    description: "[READ-ONLY] Get budget allocations and summary for a specific month (YYYY-MM).",
    inputSchema: {
      type: "object",
      properties: {
        month: { type: "string", description: "Month formatted as YYYY-MM" },
        args: { type: "array", items: { type: "string" } }
      },
      required: ["month"]
    },
    toCli: (input = {}) => {
      if (Array.isArray(input.args)) return ["budgets", "month", ...input.args];
      return ["budgets", "month", input.month];
    }
  },
  {
    name: "actual_list_transactions",
    readOnly: true,
    description: "[READ-ONLY] List transactions for a specific account within a date range. Automatically attaches matching receipt metadata and image URLs.",
    inputSchema: {
      type: "object",
      properties: {
        account: { type: "string", description: "Account UUID" },
        start: { type: "string", description: "Start date (YYYY-MM-DD)" },
        end: { type: "string", description: "End date (YYYY-MM-DD)" },
        no_receipts: { type: "boolean", description: "Set true to disable automatic receipt enrichment (default: false)" },
        args: { type: "array", items: { type: "string" } }
      },
      required: ["account", "start", "end"]
    },
    toCli: (input = {}) => {
      if (Array.isArray(input.args)) return ["transactions", "list", ...input.args];
      const cli = ["transactions", "list"];
      if (input.account) cli.push("--account", input.account);
      if (input.start) cli.push("--start", input.start);
      if (input.end) cli.push("--end", input.end);
      if (input.no_receipts || input["no-receipts"]) cli.push("--no-receipts");
      return cli;
    }
  },
  {
    name: "actual_list_categories",
    readOnly: true,
    description: "[READ-ONLY] List all budget categories with IDs, names, group IDs, hidden status, and optional category notes.",
    inputSchema: {
      type: "object",
      properties: {
        include_hidden: { type: "boolean", description: "Include hidden categories" },
        include_notes: { type: "boolean", description: "Include category notes (default: false)" },
        args: { type: "array", items: { type: "string" } }
      }
    },
    toCli: (input = {}) => {
      if (Array.isArray(input.args)) return ["categories", "list", ...input.args];
      const cli = ["categories", "list"];
      if (input.include_hidden || input["include-hidden"]) cli.push("--include-hidden");
      if (input.include_notes || input["include-notes"]) cli.push("--include-notes");
      return cli;
    }
  },
  {
    name: "actual_list_category_groups",
    readOnly: true,
    description: "[READ-ONLY] List all category groups with nested categories.",
    inputSchema: {
      type: "object",
      properties: {
        include_hidden: { type: "boolean", description: "Include hidden category groups" },
        args: { type: "array", items: { type: "string" } }
      }
    },
    toCli: (input = {}) => {
      if (Array.isArray(input.args)) return ["category-groups", "list", ...input.args];
      const cli = ["category-groups", "list"];
      if (input.include_hidden || input["include-hidden"]) cli.push("--include-hidden");
      return cli;
    }
  },
  {
    name: "actual_list_payees",
    readOnly: true,
    description: "[READ-ONLY] List all payees in the budget.",
    inputSchema: {
      type: "object",
      properties: {
        args: { type: "array", items: { type: "string" } }
      }
    },
    toCli: (input = {}) => ["payees", "list", ...(input.args || [])]
  },
  {
    name: "actual_get_common_payees",
    readOnly: true,
    description: "[READ-ONLY] List frequently used payees.",
    inputSchema: {
      type: "object",
      properties: {
        args: { type: "array", items: { type: "string" } }
      }
    },
    toCli: (input = {}) => ["payees", "common", ...(input.args || [])]
  },
  {
    name: "actual_list_payee_locations",
    readOnly: true,
    description: "[READ-ONLY] List saved geographic locations for all payees or a specific payee.",
    inputSchema: {
      type: "object",
      properties: {
        payee_id: { type: "string", description: "Optional payee UUID to filter locations" },
        args: { type: "array", items: { type: "string" } }
      }
    },
    toCli: (input = {}) => {
      if (Array.isArray(input.args)) return ["payees", "locations", ...input.args];
      const cli = ["payees", "locations"];
      if (input.payee_id || input["payee-id"]) cli.push("--payee-id", input.payee_id || input["payee-id"]);
      return cli;
    }
  },
  {
    name: "actual_get_nearby_payees",
    readOnly: true,
    description: "[READ-ONLY] Find payees near a given latitude and longitude.",
    inputSchema: {
      type: "object",
      properties: {
        latitude: { type: "number", description: "Latitude coordinate" },
        longitude: { type: "number", description: "Longitude coordinate" },
        max_distance: { type: "number", description: "Optional maximum distance in meters" },
        args: { type: "array", items: { type: "string" } }
      },
      required: ["latitude", "longitude"]
    },
    toCli: (input = {}) => {
      if (Array.isArray(input.args)) return ["payees", "nearby", ...input.args];
      const cli = ["payees", "nearby", "--latitude", String(input.latitude), "--longitude", String(input.longitude)];
      if (input.max_distance !== undefined || input["max-distance"] !== undefined) {
        cli.push("--max-distance", String(input.max_distance ?? input["max-distance"]));
      }
      return cli;
    }
  },
  {
    name: "actual_list_tags",
    readOnly: true,
    description: "[READ-ONLY] List all transaction tags.",
    inputSchema: {
      type: "object",
      properties: {
        args: { type: "array", items: { type: "string" } }
      }
    },
    toCli: (input = {}) => ["tags", "list", ...(input.args || [])]
  },
  {
    name: "actual_list_rules",
    readOnly: true,
    description: "[READ-ONLY] List all transaction rules.",
    inputSchema: {
      type: "object",
      properties: {
        args: { type: "array", items: { type: "string" } }
      }
    },
    toCli: (input = {}) => ["rules", "list", ...(input.args || [])]
  },
  {
    name: "actual_get_payee_rules",
    readOnly: true,
    description: "[READ-ONLY] Get transaction rules associated with a specific payee ID.",
    inputSchema: {
      type: "object",
      properties: {
        payee_id: { type: "string", description: "Payee UUID" },
        args: { type: "array", items: { type: "string" } }
      },
      required: ["payee_id"]
    },
    toCli: (input = {}) => {
      if (Array.isArray(input.args)) return ["rules", "payee-rules", ...input.args];
      return ["rules", "payee-rules", input.payee_id || input.id];
    }
  },
  {
    name: "actual_list_schedules",
    readOnly: true,
    description: "[READ-ONLY] List all scheduled transactions and recurring rules.",
    inputSchema: {
      type: "object",
      properties: {
        args: { type: "array", items: { type: "string" } }
      }
    },
    toCli: (input = {}) => ["schedules", "list", ...(input.args || [])]
  },
  {
    name: "actual_get_schedule_dates",
    readOnly: true,
    description: "[READ-ONLY] Compute upcoming occurrence dates for a scheduled transaction (by schedule ID/name or RecurConfig/date).",
    inputSchema: {
      type: "object",
      properties: {
        id: { type: "string", description: "Schedule UUID or name (looks up the schedule's RecurConfig)" },
        date: { type: ["object", "string"], description: "Date string (YYYY-MM-DD) or RecurConfig JSON object if not using schedule id" },
        start: { type: "string", description: "Optional start date (YYYY-MM-DD)" },
        end: { type: "string", description: "Optional end date (YYYY-MM-DD)" },
        count: { type: "integer", description: "Optional number of upcoming occurrences to compute" },
        args: { type: "array", items: { type: "string" } }
      }
    },
    toCli: (input = {}) => {
      if (Array.isArray(input.args)) return ["schedules", "dates", ...input.args];
      const cli = ["schedules", "dates"];
      if (input.id || input.schedule_id) cli.push("--id", input.id || input.schedule_id);
      if (input.date) cli.push("--date", typeof input.date === "object" ? JSON.stringify(input.date) : input.date);
      if (input.start) cli.push("--start", input.start);
      if (input.end) cli.push("--end", input.end);
      if (input.count !== undefined) cli.push("--count", String(input.count));
      return cli;
    }
  },
  {
    name: "actual_get_server_version",
    readOnly: true,
    description: "[READ-ONLY] Get the Actual Budget server version.",
    inputSchema: {
      type: "object",
      properties: {
        args: { type: "array", items: { type: "string" } }
      }
    },
    toCli: (input = {}) => ["server", "version", ...(input.args || [])]
  },
  {
    name: "actual_get_id",
    readOnly: true,
    description: "[READ-ONLY] Look up an entity UUID by its type ('accounts', 'account_groups', 'categories', 'category_groups', 'payees', 'schedules', 'tags') and name.",
    inputSchema: {
      type: "object",
      properties: {
        type: { type: "string", description: "Entity type ('accounts', 'account_groups', 'categories', 'category_groups', 'payees', 'schedules', 'tags')" },
        name: { type: "string", description: "Exact name to look up" },
        args: { type: "array", items: { type: "string" } }
      },
      required: ["type", "name"]
    },
    toCli: (input = {}) => {
      if (Array.isArray(input.args)) return ["server", "get-id", ...input.args];
      return ["server", "get-id", "--type", input.type, "--name", input.name];
    }
  },
  {
    name: "actual_get_sync_status",
    readOnly: true,
    description: "[READ-ONLY] Check budget connection and cache status.",
    inputSchema: {
      type: "object",
      properties: {
        args: { type: "array", items: { type: "string" } }
      }
    },
    toCli: (input = {}) => ["sync", "--status", ...(input.args || [])]
  },
  {
    name: "actual_list_receipts",
    readOnly: true,
    description: "[READ-ONLY] List receipts stored in PocketBase (cached locally in SQLite for fast lookup). Supports filtering by tx_id, date, source, payee, search keyword, or unlinked_only.",
    inputSchema: {
      type: "object",
      properties: {
        tx_id: { type: "string", description: "Filter by Actual Budget transaction ID" },
        tx_ids: { type: "array", items: { type: "string" }, description: "Filter by multiple transaction IDs" },
        unlinked_only: { type: "boolean", description: "Filter only unlinked receipts (tx_id is empty or null)" },
        date: { type: "string", description: "Exact date (YYYY-MM-DD)" },
        start: { type: "string", description: "Start date (YYYY-MM-DD)" },
        end: { type: "string", description: "End date (YYYY-MM-DD)" },
        source: { type: "string", description: "Source type (e.g. file, email, paperless, icloud)" },
        source_ref: { type: "string", description: "Source reference (e.g. filename, email message ID)" },
        source_hash: { type: "string", description: "SHA-256 hash of receipt source" },
        payee: { type: "string", description: "Payee name search" },
        search: { type: "string", description: "Full keyword search across payee, source_ref, lines, and parsed text" },
        limit: { type: "integer", description: "Maximum rows to return (default: 100)" },
        offset: { type: "integer", description: "Offset for pagination" },
        active_only: { type: "boolean", description: "Exclude ignored receipts" },
        args: { type: "array", items: { type: "string" }, description: "CLI arguments" }
      }
    },
    toCli: (input = {}) => {
      if (Array.isArray(input.args)) return ["receipts", "list", ...input.args];
      const cli = ["receipts", "list"];
      if (input.tx_id) cli.push("--tx-id", input.tx_id);
      if (input.tx_ids) cli.push("--tx-ids", Array.isArray(input.tx_ids) ? input.tx_ids.join(",") : input.tx_ids);
      if (input.unlinked_only) cli.push("--unlinked-only");
      if (input.date) cli.push("--date", input.date);
      if (input.start) cli.push("--start", input.start);
      if (input.end) cli.push("--end", input.end);
      if (input.source) cli.push("--source", input.source);
      if (input.source_ref) cli.push("--source-ref", input.source_ref);
      if (input.source_hash) cli.push("--source-hash", input.source_hash);
      if (input.payee) cli.push("--payee", input.payee);
      if (input.search) cli.push("--search", input.search);
      if (input.limit !== undefined) cli.push("--limit", String(input.limit));
      if (input.offset !== undefined) cli.push("--offset", String(input.offset));
      if (input.active_only) cli.push("--active-only");
      return cli;
    }
  },
  {
    name: "actual_get_receipt",
    readOnly: true,
    description: "[READ-ONLY] Retrieve a specific receipt by record ID, transaction ID (tx_id), source_hash, or source_ref.",
    inputSchema: {
      type: "object",
      properties: {
        id: { type: "string", description: "PocketBase record ID" },
        tx_id: { type: "string", description: "Actual Budget transaction ID" },
        source_hash: { type: "string", description: "SHA-256 hash of receipt source" },
        source_ref: { type: "string", description: "Source reference (filename or message ID)" },
        args: { type: "array", items: { type: "string" } }
      }
    },
    toCli: (input = {}) => {
      if (Array.isArray(input.args)) return ["receipts", "get", ...input.args];
      const cli = ["receipts", "get"];
      if (input.id) cli.push(input.id);
      if (input.tx_id) cli.push("--tx-id", input.tx_id);
      if (input.source_hash) cli.push("--source-hash", input.source_hash);
      if (input.source_ref) cli.push("--source-ref", input.source_ref);
      return cli;
    }
  },
  {
    name: "actual_get_receipt_image",
    readOnly: true,
    description: "[READ-ONLY] Fetch receipt image binary (as base64) and direct PocketBase URL by receipt record ID and optional page index or filename.",
    inputSchema: {
      type: "object",
      properties: {
        id: { type: "string", description: "PocketBase receipt record ID" },
        filename: { type: "string", description: "Specific image filename (defaults to first attached image)" },
        page_index: { type: "integer", description: "Image index if multiple pages/images attached (default: 0)" },
        args: { type: "array", items: { type: "string" } }
      },
      required: ["id"]
    },
    toCli: (input = {}) => {
      if (Array.isArray(input.args)) return ["receipts", "image", ...input.args];
      const cli = ["receipts", "image", input.id];
      if (input.filename) cli.push("--filename", input.filename);
      if (input.page_index !== undefined) cli.push("--page-index", String(input.page_index));
      return cli;
    }
  },
  {
    name: "actual_get_receipts_for_transactions",
    readOnly: true,
    description: "[READ-ONLY] Batch retrieve matching receipts for a list of Actual Budget transaction IDs.",
    inputSchema: {
      type: "object",
      properties: {
        tx_ids: {
          type: "array",
          items: { type: "string" },
          description: "List of Actual Budget transaction UUIDs"
        },
        args: { type: "array", items: { type: "string" } }
      },
      required: ["tx_ids"]
    },
    toCli: (input = {}) => {
      if (Array.isArray(input.args)) return ["receipts", "for-transactions", ...input.args];
      const cli = ["receipts", "for-transactions"];
      if (input.tx_ids) cli.push("--tx-ids", Array.isArray(input.tx_ids) ? input.tx_ids.join(",") : input.tx_ids);
      return cli;
    }
  },
  {
    name: "actual_check_receipt_exists",
    readOnly: true,
    description: "[READ-ONLY] Check whether a receipt already exists in PocketBase by source_hash, source_ref, or tx_id (fast O(1) local SQLite check).",
    inputSchema: {
      type: "object",
      properties: {
        source_hash: { type: "string", description: "SHA-256 hash" },
        source_ref: { type: "string", description: "Source reference" },
        tx_id: { type: "string", description: "Transaction ID" },
        active_only: { type: "boolean", description: "Exclude ignored receipts" },
        args: { type: "array", items: { type: "string" } }
      }
    },
    toCli: (input = {}) => {
      if (Array.isArray(input.args)) return ["receipts", "exists", ...input.args];
      const cli = ["receipts", "exists"];
      if (input.source_hash) cli.push("--source-hash", input.source_hash);
      if (input.source_ref) cli.push("--source-ref", input.source_ref);
      if (input.tx_id) cli.push("--tx-id", input.tx_id);
      if (input.active_only) cli.push("--active-only");
      return cli;
    }
  },

  // ─── MUTATING TOOLS ───
  {
    name: "actual_update_note",
    readOnly: false,
    description: "[MUTATING] Create, update, or clear a note attached to an account, category, category group, budget month (YYYY-MM), or raw note ID. Accepts bare account UUIDs (automatically resolves 'account-<id>'), category UUIDs, or 'YYYY-MM' months.",
    inputSchema: {
      type: "object",
      properties: {
        id: { type: "string", description: "Entity UUID (account or category), month (YYYY-MM), or raw note ID (e.g. 'account-<uuid>', 'budget-YYYY-MM')" },
        note: { type: ["string", "null"], description: "Note text content (pass empty string or null to clear)" },
        type: { type: "string", enum: ["account", "category", "budget", "raw"], description: "Optional entity type hint ('account', 'category', 'budget', 'raw')" },
        args: { type: "array", items: { type: "string" } }
      },
      required: ["id", "note"]
    },
    toCli: (input = {}) => {
      if (Array.isArray(input.args)) return ["notes", "update", ...input.args];
      const cli = ["notes", "update", input.id, "--note", input.note === null ? "" : String(input.note)];
      if (input.type) cli.push("--type", input.type);
      return cli;
    }
  },
  {
    name: "actual_set_preference",
    readOnly: false,
    description: "[MUTATING] Set a budget preference value by key (e.g. 'firstDayOfWeekIdx', 'dateFormat', 'numberFormat', 'defaultCurrencyCode', 'hideFraction').",
    inputSchema: {
      type: "object",
      properties: {
        key: { type: "string", description: "Preference key" },
        value: { type: ["string", "number", "boolean", "null"], description: "Preference value" },
        args: { type: "array", items: { type: "string" } }
      },
      required: ["key", "value"]
    },
    toCli: (input = {}) => {
      if (Array.isArray(input.args)) return ["preferences", "set", ...input.args];
      return ["preferences", "set", input.key, "--value", input.value === null ? "null" : String(input.value)];
    }
  },
  {
    name: "actual_create_account",
    readOnly: false,
    description: "[MUTATING] Create a new account in Actual Budget, with optional starting balance, account group, and account note.",
    inputSchema: {
      type: "object",
      properties: {
        name: { type: "string", description: "Account name" },
        offbudget: { type: "boolean", description: "Whether account is offbudget (default: false)" },
        balance: { type: "integer", description: "Starting balance in cents (default: 0)" },
        account_group_id: { type: "string", description: "Optional account group UUID" },
        note: { type: "string", description: "Optional initial note attached to the account" },
        args: { type: "array", items: { type: "string" } }
      },
      required: ["name"]
    },
    toCli: (input = {}) => {
      if (Array.isArray(input.args)) return ["accounts", "create", ...input.args];
      const cli = ["accounts", "create", "--name", input.name];
      if (input.offbudget) cli.push("--offbudget");
      if (input.balance !== undefined) cli.push("--balance", String(input.balance));
      if (input.account_group_id || input["account-group-id"]) {
        cli.push("--account-group-id", input.account_group_id || input["account-group-id"]);
      }
      if (input.note !== undefined) cli.push("--note", input.note === null ? "" : String(input.note));
      return cli;
    }
  },
  {
    name: "actual_update_account",
    readOnly: false,
    description: "[MUTATING] Update an existing account's name, offbudget status, account group, or attached note.",
    inputSchema: {
      type: "object",
      properties: {
        id: { type: "string", description: "Account UUID" },
        name: { type: "string", description: "New account name" },
        offbudget: { type: "boolean", description: "New offbudget status" },
        account_group_id: { type: ["string", "null"], description: "New account group UUID (or null/empty to ungroup)" },
        note: { type: ["string", "null"], description: "Account note text (or empty string to clear)" },
        args: { type: "array", items: { type: "string" } }
      },
      required: ["id"]
    },
    toCli: (input = {}) => {
      if (Array.isArray(input.args)) return ["accounts", "update", ...input.args];
      const cli = ["accounts", "update", input.id];
      if (input.name !== undefined) cli.push("--name", input.name);
      if (input.offbudget !== undefined) cli.push("--offbudget", String(input.offbudget));
      if (input.account_group_id !== undefined || input["account-group-id"] !== undefined) {
        const gid = input.account_group_id !== undefined ? input.account_group_id : input["account-group-id"];
        cli.push("--account-group-id", gid === null ? "null" : String(gid));
      }
      if (input.note !== undefined) cli.push("--note", input.note === null ? "" : String(input.note));
      return cli;
    }
  },
  {
    name: "actual_create_account_group",
    readOnly: false,
    description: "[MUTATING] Create a new account group.",
    inputSchema: {
      type: "object",
      properties: {
        name: { type: "string", description: "Account group name" },
        args: { type: "array", items: { type: "string" } }
      },
      required: ["name"]
    },
    toCli: (input = {}) => {
      if (Array.isArray(input.args)) return ["account-groups", "create", ...input.args];
      return ["account-groups", "create", "--name", input.name];
    }
  },
  {
    name: "actual_update_account_group",
    readOnly: false,
    description: "[MUTATING] Update an account group's name.",
    inputSchema: {
      type: "object",
      properties: {
        id: { type: "string", description: "Account group UUID" },
        name: { type: "string", description: "New account group name" },
        args: { type: "array", items: { type: "string" } }
      },
      required: ["id", "name"]
    },
    toCli: (input = {}) => {
      if (Array.isArray(input.args)) return ["account-groups", "update", ...input.args];
      return ["account-groups", "update", input.id, "--name", input.name];
    }
  },
  {
    name: "actual_delete_account_group",
    readOnly: false,
    description: "[MUTATING] Delete an account group by ID.",
    inputSchema: {
      type: "object",
      properties: {
        id: { type: "string", description: "Account group UUID" },
        args: { type: "array", items: { type: "string" } }
      },
      required: ["id"]
    },
    toCli: (input = {}) => {
      if (Array.isArray(input.args)) return ["account-groups", "delete", ...input.args];
      return ["account-groups", "delete", input.id];
    }
  },
  {
    name: "actual_close_account",
    readOnly: false,
    description: "[MUTATING] Close an account with optional transfer account/category.",
    inputSchema: {
      type: "object",
      properties: {
        id: { type: "string", description: "Account UUID" },
        transfer_account: { type: "string", description: "Optional transfer account ID" },
        transfer_category: { type: "string", description: "Optional transfer category ID" },
        args: { type: "array", items: { type: "string" } }
      },
      required: ["id"]
    },
    toCli: (input = {}) => {
      if (Array.isArray(input.args)) return ["accounts", "close", ...input.args];
      const cli = ["accounts", "close", input.id];
      if (input.transfer_account || input["transfer-account"]) cli.push("--transfer-account", input.transfer_account || input["transfer-account"]);
      if (input.transfer_category || input["transfer-category"]) cli.push("--transfer-category", input.transfer_category || input["transfer-category"]);
      return cli;
    }
  },
  {
    name: "actual_reopen_account",
    readOnly: false,
    description: "[MUTATING] Reopen a closed account.",
    inputSchema: {
      type: "object",
      properties: {
        id: { type: "string", description: "Account UUID" },
        args: { type: "array", items: { type: "string" } }
      },
      required: ["id"]
    },
    toCli: (input = {}) => {
      if (Array.isArray(input.args)) return ["accounts", "reopen", ...input.args];
      return ["accounts", "reopen", input.id];
    }
  },
  {
    name: "actual_delete_account",
    readOnly: false,
    description: "[MUTATING] Permanently delete an account.",
    inputSchema: {
      type: "object",
      properties: {
        id: { type: "string", description: "Account UUID" },
        args: { type: "array", items: { type: "string" } }
      },
      required: ["id"]
    },
    toCli: (input = {}) => {
      if (Array.isArray(input.args)) return ["accounts", "delete", ...input.args];
      return ["accounts", "delete", input.id];
    }
  },
  {
    name: "actual_load_budget",
    readOnly: false,
    description: "[MUTATING] Load an already-downloaded local budget by its local budget ID.",
    inputSchema: {
      type: "object",
      properties: {
        id: { type: "string", description: "Local budget ID" },
        args: { type: "array", items: { type: "string" } }
      },
      required: ["id"]
    },
    toCli: (input = {}) => {
      if (Array.isArray(input.args)) return ["budgets", "load", ...input.args];
      return ["budgets", "load", input.id];
    }
  },
  {
    name: "actual_import_budget",
    readOnly: false,
    description: "[MUTATING] Import a budget archive file (Actual, YNAB4, or nYNAB) into Actual Budget.",
    inputSchema: {
      type: "object",
      properties: {
        file: { type: "string", description: "Local file path to the budget archive" },
        type: { type: "string", enum: ["actual", "ynab4", "nynab"], description: "Import format type (default: 'actual')" },
        filename: { type: "string", description: "Optional budget name/filename override" },
        args: { type: "array", items: { type: "string" } }
      },
      required: ["file"]
    },
    toCli: (input = {}) => {
      if (Array.isArray(input.args)) return ["budgets", "import", ...input.args];
      const cli = ["budgets", "import", "--file", input.file];
      if (input.type) cli.push("--type", input.type);
      if (input.filename) cli.push("--filename", input.filename);
      return cli;
    }
  },
  {
    name: "actual_download_budget",
    readOnly: false,
    description: "[MUTATING] Download and load a budget by syncId.",
    inputSchema: {
      type: "object",
      properties: {
        sync_id: { type: "string", description: "Budget syncId" },
        encryption_password: { type: "string", description: "Optional encryption password" },
        args: { type: "array", items: { type: "string" } }
      },
      required: ["sync_id"]
    },
    toCli: (input = {}) => {
      if (Array.isArray(input.args)) return ["budgets", "download", ...input.args];
      const cli = ["budgets", "download", input.sync_id || input.syncId];
      if (input.encryption_password || input["encryption-password"]) cli.push("--encryption-password", input.encryption_password || input["encryption-password"]);
      return cli;
    }
  },
  {
    name: "actual_set_budget_amount",
    readOnly: false,
    description: "[MUTATING] Set the budgeted amount for a category in a specific month.",
    inputSchema: {
      type: "object",
      properties: {
        month: { type: "string", description: "Month formatted as YYYY-MM" },
        category: { type: "string", description: "Category UUID" },
        amount: { type: "integer", description: "Budget amount in integer cents" },
        args: { type: "array", items: { type: "string" } }
      },
      required: ["month", "category", "amount"]
    },
    toCli: (input = {}) => {
      if (Array.isArray(input.args)) return ["budgets", "set-amount", ...input.args];
      return ["budgets", "set-amount", "--month", input.month, "--category", input.category, "--amount", String(input.amount)];
    }
  },
  {
    name: "actual_set_budget_carryover",
    readOnly: false,
    description: "[MUTATING] Toggle carryover flag for a category in a specific month.",
    inputSchema: {
      type: "object",
      properties: {
        month: { type: "string", description: "Month formatted as YYYY-MM" },
        category: { type: "string", description: "Category UUID" },
        flag: { type: "boolean", description: "Carryover boolean flag" },
        args: { type: "array", items: { type: "string" } }
      },
      required: ["month", "category", "flag"]
    },
    toCli: (input = {}) => {
      if (Array.isArray(input.args)) return ["budgets", "set-carryover", ...input.args];
      return ["budgets", "set-carryover", "--month", input.month, "--category", input.category, "--flag", String(input.flag)];
    }
  },
  {
    name: "actual_hold_budget_next_month",
    readOnly: false,
    description: "[MUTATING] Hold a specific amount from the current month's budget for next month.",
    inputSchema: {
      type: "object",
      properties: {
        month: { type: "string", description: "Month formatted as YYYY-MM" },
        amount: { type: "integer", description: "Amount in integer cents" },
        args: { type: "array", items: { type: "string" } }
      },
      required: ["month", "amount"]
    },
    toCli: (input = {}) => {
      if (Array.isArray(input.args)) return ["budgets", "hold-next-month", ...input.args];
      return ["budgets", "hold-next-month", "--month", input.month, "--amount", String(input.amount)];
    }
  },
  {
    name: "actual_reset_budget_hold",
    readOnly: false,
    description: "[MUTATING] Reset the budget hold amount for a specific month.",
    inputSchema: {
      type: "object",
      properties: {
        month: { type: "string", description: "Month formatted as YYYY-MM" },
        args: { type: "array", items: { type: "string" } }
      },
      required: ["month"]
    },
    toCli: (input = {}) => {
      if (Array.isArray(input.args)) return ["budgets", "reset-hold", ...input.args];
      return ["budgets", "reset-hold", "--month", input.month];
    }
  },
  {
    name: "actual_add_transactions",
    readOnly: false,
    description: "[MUTATING] Add one or more new transactions to an account.",
    inputSchema: {
      type: "object",
      properties: {
        account: { type: "string", description: "Account UUID" },
        data: { type: ["array", "object", "string"], description: "Transaction object, array of transactions, or JSON string" },
        file: { type: "string", description: "Path to JSON file containing transactions" },
        learn_categories: { type: "boolean", description: "Automatically learn payee categories" },
        run_transfers: { type: "boolean", description: "Automatically resolve transfers" },
        args: { type: "array", items: { type: "string" } }
      },
      required: ["account"]
    },
    toCli: (input = {}) => {
      if (Array.isArray(input.args)) return ["transactions", "add", ...input.args];
      const cli = ["transactions", "add", "--account", input.account];
      if (input.data) cli.push("--data", typeof input.data === "object" ? JSON.stringify(input.data) : input.data);
      if (input.file) cli.push("--file", input.file);
      if (input.learn_categories || input["learn-categories"]) cli.push("--learn-categories");
      if (input.run_transfers || input["run-transfers"]) cli.push("--run-transfers");
      return cli;
    }
  },
  {
    name: "actual_import_transactions",
    readOnly: false,
    description: "[MUTATING] Import transactions with deduplication and default cleared status.",
    inputSchema: {
      type: "object",
      properties: {
        account: { type: "string", description: "Account UUID" },
        data: { type: ["array", "object", "string"], description: "Transaction array or JSON string" },
        file: { type: "string", description: "Path to transactions JSON file" },
        dry_run: { type: "boolean", description: "Perform dry run without saving" },
        args: { type: "array", items: { type: "string" } }
      },
      required: ["account"]
    },
    toCli: (input = {}) => {
      if (Array.isArray(input.args)) return ["transactions", "import", ...input.args];
      const cli = ["transactions", "import", "--account", input.account];
      if (input.data) cli.push("--data", typeof input.data === "object" ? JSON.stringify(input.data) : input.data);
      if (input.file) cli.push("--file", input.file);
      if (input.dry_run || input["dry-run"]) cli.push("--dry-run");
      return cli;
    }
  },
  {
    name: "actual_update_transaction",
    readOnly: false,
    description: "[MUTATING] Update fields of an existing transaction by ID.",
    inputSchema: {
      type: "object",
      properties: {
        id: { type: "string", description: "Transaction UUID" },
        data: { type: ["object", "string"], description: "JSON object or string with fields to update (e.g. notes, payee, category, amount, date)" },
        file: { type: "string", description: "Path to JSON file with update fields" },
        args: { type: "array", items: { type: "string" } }
      },
      required: ["id"]
    },
    toCli: (input = {}) => {
      if (Array.isArray(input.args)) return ["transactions", "update", ...input.args];
      const cli = ["transactions", "update", input.id];
      if (input.data) cli.push("--data", typeof input.data === "object" ? JSON.stringify(input.data) : input.data);
      if (input.file) cli.push("--file", input.file);
      return cli;
    }
  },
  {
    name: "actual_delete_transaction",
    readOnly: false,
    description: "[MUTATING] Delete a transaction by ID.",
    inputSchema: {
      type: "object",
      properties: {
        id: { type: "string", description: "Transaction UUID" },
        args: { type: "array", items: { type: "string" } }
      },
      required: ["id"]
    },
    toCli: (input = {}) => {
      if (Array.isArray(input.args)) return ["transactions", "delete", ...input.args];
      return ["transactions", "delete", input.id];
    }
  },
  {
    name: "actual_merge_transactions",
    readOnly: false,
    description: "[MUTATING] Merge two transactions into one (keeps one transaction and deletes the duplicate).",
    inputSchema: {
      type: "object",
      properties: {
        ids: { type: ["array", "string"], description: "Array of two transaction UUIDs or comma-separated string 'id1,id2'" },
        args: { type: "array", items: { type: "string" } }
      },
      required: ["ids"]
    },
    toCli: (input = {}) => {
      if (Array.isArray(input.args)) return ["transactions", "merge", ...input.args];
      const ids = Array.isArray(input.ids) ? input.ids : String(input.ids).split(",").map(s => s.trim()).filter(Boolean);
      return ["transactions", "merge", ...ids];
    }
  },
  {
    name: "actual_restore_transaction_notes",
    readOnly: false,
    description: "[MUTATING] Restore transaction notes for specified transaction IDs from backup.sqlite.",
    inputSchema: {
      type: "object",
      properties: {
        ids: { type: ["array", "string"], description: "List of transaction IDs or comma-separated string" },
        args: { type: "array", items: { type: "string" } }
      }
    },
    toCli: (input = {}) => {
      if (Array.isArray(input.args)) return ["transactions", "restore-notes", ...input.args];
      const ids = Array.isArray(input.ids) ? input.ids : (input.ids ? String(input.ids).split(",") : []);
      return ["transactions", "restore-notes", ...ids];
    }
  },
  {
    name: "actual_restore_transactions",
    readOnly: false,
    description: "[MUTATING] Restore entire transaction records for specified IDs from backup.sqlite.",
    inputSchema: {
      type: "object",
      properties: {
        ids: { type: ["array", "string"], description: "List of transaction IDs or comma-separated string" },
        args: { type: "array", items: { type: "string" } }
      }
    },
    toCli: (input = {}) => {
      if (Array.isArray(input.args)) return ["transactions", "restore-transaction", ...input.args];
      const ids = Array.isArray(input.ids) ? input.ids : (input.ids ? String(input.ids).split(",") : []);
      return ["transactions", "restore-transaction", ...ids];
    }
  },
  {
    name: "actual_create_category",
    readOnly: false,
    description: "[MUTATING] Create a new category with optional note.",
    inputSchema: {
      type: "object",
      properties: {
        name: { type: "string", description: "Category name" },
        group_id: { type: "string", description: "Parent category group UUID" },
        is_income: { type: "boolean", description: "Whether category is income (default: false)" },
        note: { type: "string", description: "Optional initial note attached to the category" },
        args: { type: "array", items: { type: "string" } }
      },
      required: ["name", "group_id"]
    },
    toCli: (input = {}) => {
      if (Array.isArray(input.args)) return ["categories", "create", ...input.args];
      const cli = ["categories", "create", "--name", input.name, "--group-id", input.group_id || input["group-id"]];
      if (input.is_income || input["is-income"]) cli.push("--is-income");
      if (input.note !== undefined) cli.push("--note", input.note === null ? "" : String(input.note));
      return cli;
    }
  },
  {
    name: "actual_update_category",
    readOnly: false,
    description: "[MUTATING] Update a category's name, hidden status, group_id, or attached note.",
    inputSchema: {
      type: "object",
      properties: {
        id: { type: "string", description: "Category UUID" },
        name: { type: "string", description: "New category name" },
        hidden: { type: "boolean", description: "Hidden flag" },
        group_id: { type: "string", description: "New parent category group UUID" },
        note: { type: ["string", "null"], description: "Category note text (or empty string to clear)" },
        args: { type: "array", items: { type: "string" } }
      },
      required: ["id"]
    },
    toCli: (input = {}) => {
      if (Array.isArray(input.args)) return ["categories", "update", ...input.args];
      const cli = ["categories", "update", input.id];
      if (input.name !== undefined) cli.push("--name", input.name);
      if (input.hidden !== undefined) cli.push("--hidden", String(input.hidden));
      if (input.group_id !== undefined || input["group-id"] !== undefined) {
        cli.push("--group-id", input.group_id ?? input["group-id"]);
      }
      if (input.note !== undefined) cli.push("--note", input.note === null ? "" : String(input.note));
      return cli;
    }
  },
  {
    name: "actual_delete_category",
    readOnly: false,
    description: "[MUTATING] Delete a category with optional category transfer.",
    inputSchema: {
      type: "object",
      properties: {
        id: { type: "string", description: "Category UUID" },
        transfer_to: { type: "string", description: "Optional category ID to transfer transactions to" },
        args: { type: "array", items: { type: "string" } }
      },
      required: ["id"]
    },
    toCli: (input = {}) => {
      if (Array.isArray(input.args)) return ["categories", "delete", ...input.args];
      const cli = ["categories", "delete", input.id];
      if (input.transfer_to || input["transfer-to"]) cli.push("--transfer-to", input.transfer_to || input["transfer-to"]);
      return cli;
    }
  },
  {
    name: "actual_create_category_group",
    readOnly: false,
    description: "[MUTATING] Create a new category group.",
    inputSchema: {
      type: "object",
      properties: {
        name: { type: "string", description: "Category group name" },
        is_income: { type: "boolean", description: "Whether group is income (default: false)" },
        args: { type: "array", items: { type: "string" } }
      },
      required: ["name"]
    },
    toCli: (input = {}) => {
      if (Array.isArray(input.args)) return ["category-groups", "create", ...input.args];
      const cli = ["category-groups", "create", "--name", input.name];
      if (input.is_income || input["is-income"]) cli.push("--is-income");
      return cli;
    }
  },
  {
    name: "actual_update_category_group",
    readOnly: false,
    description: "[MUTATING] Update a category group's name or hidden status.",
    inputSchema: {
      type: "object",
      properties: {
        id: { type: "string", description: "Category group UUID" },
        name: { type: "string", description: "New name" },
        hidden: { type: "boolean", description: "Hidden flag" },
        args: { type: "array", items: { type: "string" } }
      },
      required: ["id"]
    },
    toCli: (input = {}) => {
      if (Array.isArray(input.args)) return ["category-groups", "update", ...input.args];
      const cli = ["category-groups", "update", input.id];
      if (input.name !== undefined) cli.push("--name", input.name);
      if (input.hidden !== undefined) cli.push("--hidden", String(input.hidden));
      return cli;
    }
  },
  {
    name: "actual_delete_category_group",
    readOnly: false,
    description: "[MUTATING] Delete a category group with optional category group transfer.",
    inputSchema: {
      type: "object",
      properties: {
        id: { type: "string", description: "Category group UUID" },
        transfer_to: { type: "string", description: "Optional category group ID to transfer to" },
        args: { type: "array", items: { type: "string" } }
      },
      required: ["id"]
    },
    toCli: (input = {}) => {
      if (Array.isArray(input.args)) return ["category-groups", "delete", ...input.args];
      const cli = ["category-groups", "delete", input.id];
      if (input.transfer_to || input["transfer-to"]) cli.push("--transfer-to", input.transfer_to || input["transfer-to"]);
      return cli;
    }
  },
  {
    name: "actual_create_payee",
    readOnly: false,
    description: "[MUTATING] Create a new payee.",
    inputSchema: {
      type: "object",
      properties: {
        name: { type: "string", description: "Payee name" },
        args: { type: "array", items: { type: "string" } }
      },
      required: ["name"]
    },
    toCli: (input = {}) => {
      if (Array.isArray(input.args)) return ["payees", "create", ...input.args];
      return ["payees", "create", "--name", input.name];
    }
  },
  {
    name: "actual_update_payee",
    readOnly: false,
    description: "[MUTATING] Update a payee name.",
    inputSchema: {
      type: "object",
      properties: {
        id: { type: "string", description: "Payee UUID" },
        name: { type: "string", description: "New name" },
        args: { type: "array", items: { type: "string" } }
      },
      required: ["id", "name"]
    },
    toCli: (input = {}) => {
      if (Array.isArray(input.args)) return ["payees", "update", ...input.args];
      return ["payees", "update", input.id, "--name", input.name];
    }
  },
  {
    name: "actual_delete_payee",
    readOnly: false,
    description: "[MUTATING] Delete a payee.",
    inputSchema: {
      type: "object",
      properties: {
        id: { type: "string", description: "Payee UUID" },
        args: { type: "array", items: { type: "string" } }
      },
      required: ["id"]
    },
    toCli: (input = {}) => {
      if (Array.isArray(input.args)) return ["payees", "delete", ...input.args];
      return ["payees", "delete", input.id];
    }
  },
  {
    name: "actual_merge_payees",
    readOnly: false,
    description: "[MUTATING] Merge multiple payees into a single target payee.",
    inputSchema: {
      type: "object",
      properties: {
        target: { type: "string", description: "Target payee UUID" },
        ids: { type: ["array", "string"], description: "List of payee IDs or comma-separated string to merge" },
        args: { type: "array", items: { type: "string" } }
      },
      required: ["target", "ids"]
    },
    toCli: (input = {}) => {
      if (Array.isArray(input.args)) return ["payees", "merge", ...input.args];
      const ids = Array.isArray(input.ids) ? input.ids.join(",") : input.ids;
      return ["payees", "merge", "--target", input.target, "--ids", ids];
    }
  },
  {
    name: "actual_create_payee_location",
    readOnly: false,
    description: "[MUTATING] Create a geographic location for a payee.",
    inputSchema: {
      type: "object",
      properties: {
        payee_id: { type: "string", description: "Payee UUID" },
        latitude: { type: "number", description: "Latitude coordinate" },
        longitude: { type: "number", description: "Longitude coordinate" },
        args: { type: "array", items: { type: "string" } }
      },
      required: ["payee_id", "latitude", "longitude"]
    },
    toCli: (input = {}) => {
      if (Array.isArray(input.args)) return ["payees", "location-create", ...input.args];
      return ["payees", "location-create", "--payee-id", input.payee_id || input["payee-id"], "--latitude", String(input.latitude), "--longitude", String(input.longitude)];
    }
  },
  {
    name: "actual_delete_payee_location",
    readOnly: false,
    description: "[MUTATING] Delete a payee geographic location by ID.",
    inputSchema: {
      type: "object",
      properties: {
        id: { type: "string", description: "Payee location UUID" },
        args: { type: "array", items: { type: "string" } }
      },
      required: ["id"]
    },
    toCli: (input = {}) => {
      if (Array.isArray(input.args)) return ["payees", "location-delete", ...input.args];
      return ["payees", "location-delete", input.id];
    }
  },
  {
    name: "actual_create_tag",
    readOnly: false,
    description: "[MUTATING] Create a new tag.",
    inputSchema: {
      type: "object",
      properties: {
        tag: { type: "string", description: "Tag name" },
        color: { type: "string", description: "Optional color hex/name" },
        description: { type: "string", description: "Optional description" },
        args: { type: "array", items: { type: "string" } }
      },
      required: ["tag"]
    },
    toCli: (input = {}) => {
      if (Array.isArray(input.args)) return ["tags", "create", ...input.args];
      const cli = ["tags", "create", "--tag", input.tag];
      if (input.color) cli.push("--color", input.color);
      if (input.description) cli.push("--description", input.description);
      return cli;
    }
  },
  {
    name: "actual_update_tag",
    readOnly: false,
    description: "[MUTATING] Update a tag.",
    inputSchema: {
      type: "object",
      properties: {
        id: { type: "string", description: "Tag UUID" },
        tag: { type: "string", description: "New tag name" },
        color: { type: "string", description: "New color" },
        description: { type: "string", description: "New description" },
        args: { type: "array", items: { type: "string" } }
      },
      required: ["id"]
    },
    toCli: (input = {}) => {
      if (Array.isArray(input.args)) return ["tags", "update", ...input.args];
      const cli = ["tags", "update", input.id];
      if (input.tag !== undefined) cli.push("--tag", input.tag);
      if (input.color !== undefined) cli.push("--color", input.color);
      if (input.description !== undefined) cli.push("--description", input.description);
      return cli;
    }
  },
  {
    name: "actual_delete_tag",
    readOnly: false,
    description: "[MUTATING] Delete a tag.",
    inputSchema: {
      type: "object",
      properties: {
        id: { type: "string", description: "Tag UUID" },
        args: { type: "array", items: { type: "string" } }
      },
      required: ["id"]
    },
    toCli: (input = {}) => {
      if (Array.isArray(input.args)) return ["tags", "delete", ...input.args];
      return ["tags", "delete", input.id];
    }
  },
  {
    name: "actual_create_rule",
    readOnly: false,
    description: "[MUTATING] Create a new transaction rule.",
    inputSchema: {
      type: "object",
      properties: {
        data: { type: ["object", "string"], description: "Rule JSON object or string" },
        file: { type: "string", description: "Path to rule JSON file" },
        args: { type: "array", items: { type: "string" } }
      }
    },
    toCli: (input = {}) => {
      if (Array.isArray(input.args)) return ["rules", "create", ...input.args];
      const cli = ["rules", "create"];
      if (input.data) cli.push("--data", typeof input.data === "object" ? JSON.stringify(input.data) : input.data);
      if (input.file) cli.push("--file", input.file);
      return cli;
    }
  },
  {
    name: "actual_update_rule",
    readOnly: false,
    description: "[MUTATING] Update a transaction rule.",
    inputSchema: {
      type: "object",
      properties: {
        data: { type: ["object", "string"], description: "Rule JSON object (must include 'id') or string" },
        file: { type: "string", description: "Path to rule JSON file" },
        args: { type: "array", items: { type: "string" } }
      }
    },
    toCli: (input = {}) => {
      if (Array.isArray(input.args)) return ["rules", "update", ...input.args];
      const cli = ["rules", "update"];
      if (input.data) cli.push("--data", typeof input.data === "object" ? JSON.stringify(input.data) : input.data);
      if (input.file) cli.push("--file", input.file);
      return cli;
    }
  },
  {
    name: "actual_delete_rule",
    readOnly: false,
    description: "[MUTATING] Delete a transaction rule.",
    inputSchema: {
      type: "object",
      properties: {
        id: { type: "string", description: "Rule UUID" },
        args: { type: "array", items: { type: "string" } }
      },
      required: ["id"]
    },
    toCli: (input = {}) => {
      if (Array.isArray(input.args)) return ["rules", "delete", ...input.args];
      return ["rules", "delete", input.id];
    }
  },
  {
    name: "actual_create_schedule",
    readOnly: false,
    description: "[MUTATING] Create a scheduled transaction.",
    inputSchema: {
      type: "object",
      properties: {
        data: { type: ["object", "string"], description: "Schedule JSON object or string" },
        file: { type: "string", description: "Path to schedule JSON file" },
        args: { type: "array", items: { type: "string" } }
      }
    },
    toCli: (input = {}) => {
      if (Array.isArray(input.args)) return ["schedules", "create", ...input.args];
      const cli = ["schedules", "create"];
      if (input.data) cli.push("--data", typeof input.data === "object" ? JSON.stringify(input.data) : input.data);
      if (input.file) cli.push("--file", input.file);
      return cli;
    }
  },
  {
    name: "actual_update_schedule",
    readOnly: false,
    description: "[MUTATING] Update a scheduled transaction.",
    inputSchema: {
      type: "object",
      properties: {
        id: { type: "string", description: "Schedule UUID" },
        data: { type: ["object", "string"], description: "Schedule JSON object or string" },
        file: { type: "string", description: "Path to schedule JSON file" },
        reset_next_date: { type: "boolean", description: "Reset the next scheduled date" },
        args: { type: "array", items: { type: "string" } }
      },
      required: ["id"]
    },
    toCli: (input = {}) => {
      if (Array.isArray(input.args)) return ["schedules", "update", ...input.args];
      const cli = ["schedules", "update", input.id];
      if (input.data) cli.push("--data", typeof input.data === "object" ? JSON.stringify(input.data) : input.data);
      if (input.file) cli.push("--file", input.file);
      if (input.reset_next_date || input["reset-next-date"]) cli.push("--reset-next-date");
      return cli;
    }
  },
  {
    name: "actual_delete_schedule",
    readOnly: false,
    description: "[MUTATING] Delete a scheduled transaction.",
    inputSchema: {
      type: "object",
      properties: {
        id: { type: "string", description: "Schedule UUID" },
        args: { type: "array", items: { type: "string" } }
      },
      required: ["id"]
    },
    toCli: (input = {}) => {
      if (Array.isArray(input.args)) return ["schedules", "delete", ...input.args];
      return ["schedules", "delete", input.id];
    }
  },
  {
    name: "actual_sync_budget",
    readOnly: false,
    description: "[MUTATING] Synchronize the local budget state with the remote Actual server.",
    inputSchema: {
      type: "object",
      properties: {
        args: { type: "array", items: { type: "string" } }
      }
    },
    toCli: (input = {}) => ["sync", ...(input.args || [])]
  },
  {
    name: "actual_clear_cache",
    readOnly: false,
    description: "[MUTATING] Clear local budget cache state and reset connection.",
    inputSchema: {
      type: "object",
      properties: {
        args: { type: "array", items: { type: "string" } }
      }
    },
    toCli: (input = {}) => ["sync", "--clear", ...(input.args || [])]
  },
  {
    name: "actual_run_bank_sync",
    readOnly: false,
    description: "[MUTATING] Trigger bank synchronization for an account.",
    inputSchema: {
      type: "object",
      properties: {
        account: { type: "string", description: "Optional account UUID" },
        args: { type: "array", items: { type: "string" } }
      }
    },
    toCli: (input = {}) => {
      if (Array.isArray(input.args)) return ["server", "bank-sync", ...input.args];
      const cli = ["server", "bank-sync"];
      if (input.account) cli.push("--account", input.account);
      return cli;
    }
  },
  {
    name: "actual_save_receipt",
    readOnly: false,
    description: "[MUTATING] Save or update a receipt in PocketBase and local cache, linking it to an Actual Budget transaction. Supports attaching images via base64 or file paths.",
    inputSchema: {
      type: "object",
      properties: {
        id: { type: "string", description: "PocketBase record ID (if updating existing receipt)" },
        tx_id: { type: "string", description: "Actual Budget transaction UUID" },
        tx_id_transfer: { type: "string", description: "Transfer transaction UUID" },
        date: { type: "string", description: "Transaction date (YYYY-MM-DD)" },
        amount: { type: "number", description: "Total amount in dollars" },
        payee: { type: "string", description: "Payee / store name" },
        source: { type: "string", description: "Source type (e.g. file, email, paperless)" },
        source_ref: { type: "string", description: "Source reference (filename / msg_id)" },
        source_hash: { type: "string", description: "Source file hash for dedup" },
        lines: { type: ["array", "string"], description: "Extracted line items array or JSON string" },
        parsed: { type: ["object", "string"], description: "Full parsed data object or JSON string" },
        flag: { type: "string", description: "Flag or tags (e.g. ignore)" },
        images: {
          type: "array",
          items: {
            type: "object",
            properties: {
              filename: { type: "string" },
              data_base64: { type: "string" },
              mime_type: { type: "string" }
            },
            required: ["data_base64"]
          },
          description: "Base64-encoded receipt images"
        },
        image_paths: {
          type: "array",
          items: { type: "string" },
          description: "Local filesystem paths to receipt images"
        },
        data: { type: ["object", "string"], description: "Full receipt payload" },
        args: { type: "array", items: { type: "string" } }
      }
    },
    toCli: (input = {}) => {
      if (Array.isArray(input.args)) return ["receipts", "save", ...input.args];
      const cli = ["receipts", "save"];
      const payload = input.data ? (typeof input.data === 'string' ? input.data : JSON.stringify(input.data)) : JSON.stringify(input);
      cli.push("--data", payload);
      return cli;
    }
  },
  {
    name: "actual_link_receipt",
    readOnly: false,
    description: "[MUTATING] Link an existing PocketBase receipt to an Actual Budget transaction.",
    inputSchema: {
      type: "object",
      properties: {
        receipt_id: { type: "string", description: "PocketBase receipt record ID" },
        tx_id: { type: "string", description: "Actual Budget transaction UUID" },
        tx_id_transfer: { type: "string", description: "Transfer transaction UUID (optional)" },
        args: { type: "array", items: { type: "string" } }
      },
      required: ["receipt_id", "tx_id"]
    },
    toCli: (input = {}) => {
      if (Array.isArray(input.args)) return ["receipts", "link", ...input.args];
      const cli = ["receipts", "link", input.receipt_id, "--tx-id", input.tx_id];
      if (input.tx_id_transfer) cli.push("--tx-id-transfer", input.tx_id_transfer);
      return cli;
    }
  },
  {
    name: "actual_unlink_receipt",
    readOnly: false,
    description: "[MUTATING] Unlink a PocketBase receipt from an Actual Budget transaction.",
    inputSchema: {
      type: "object",
      properties: {
        receipt_id: { type: "string", description: "PocketBase receipt record ID" },
        args: { type: "array", items: { type: "string" } }
      },
      required: ["receipt_id"]
    },
    toCli: (input = {}) => {
      if (Array.isArray(input.args)) return ["receipts", "unlink", ...input.args];
      return ["receipts", "unlink", input.receipt_id];
    }
  },
  {
    name: "actual_delete_receipt",
    readOnly: false,
    description: "[MUTATING] Delete a receipt from PocketBase and local cache by ID or source_hash.",
    inputSchema: {
      type: "object",
      properties: {
        id: { type: "string", description: "PocketBase record ID" },
        source_hash: { type: "string", description: "Source hash" },
        tx_id: { type: "string", description: "Transaction ID" },
        args: { type: "array", items: { type: "string" } }
      }
    },
    toCli: (input = {}) => {
      if (Array.isArray(input.args)) return ["receipts", "delete", ...input.args];
      const cli = ["receipts", "delete"];
      if (input.id) cli.push(input.id);
      if (input.source_hash) cli.push("--source-hash", input.source_hash);
      if (input.tx_id) cli.push("--tx-id", input.tx_id);
      return cli;
    }
  },
  {
    name: "actual_sync_receipts",
    readOnly: false,
    description: "Sync all receipts from PocketBase into the local SQLite cache for fast bulk lookups.",
    inputSchema: {
      type: "object",
      properties: {
        full: { type: "boolean", description: "Perform full sweep instead of incremental" },
        args: { type: "array", items: { type: "string" } }
      }
    },
    toCli: (input = {}) => {
      if (Array.isArray(input.args)) return ["receipts", "sync", ...input.args];
      const cli = ["receipts", "sync"];
      if (input.full) cli.push("--full");
      return cli;
    }
  },
  {
    name: "actual_execute",
    readOnly: false,
    description: `[MUTATING/GENERIC] Execute a generic command using the Actual Budget CLI. E.g., args: ['server', 'version'].

**Technical Lessons & Usage Notes**:
- **Usage**: Do not run \`actual\` CLI commands directly via bash or python \`subprocess\`. Instead, always use the provided MCP tools.
- **Querying**: For querying, use \`actual_query\` with \`--select "date,amount,payee.name,notes,category.name,category.group.name,account.name,account.offbudget"\`.
- **Entity IDs**: Use \`actual_get_id\` with type and name to find IDs for queries.`,
    inputSchema: {
      type: "object",
      properties: {
        args: {
          type: "array",
          items: { type: "string" },
          description: "The list of arguments to pass to the CLI. Do not include the 'actual' executable name."
        }
      },
      required: ["args"]
    },
    toCli: (input = {}) => input.args || []
  }
];

const MCP_TOOL_MAP = new Map(MCP_TOOL_DEFINITIONS.map(t => [t.name, t]));

// ───────────────────────────────────────────────────────────
// Shared command queue and tool executor (used by all sessions)
// ───────────────────────────────────────────────────────────
let commandQueue = Promise.resolve();

async function handleExecuteTool(name, toolInput, explicitClientId = null) {
  const currentClientId = explicitClientId !== null ? explicitClientId : (authStorage.getStore()?.clientId || null);
  let cliArgs = [];

  const toolDef = MCP_TOOL_MAP.get(name);
  if (toolDef) {
    // If toolInput is an array, wrap it in { args: toolInput }
    const inputObj = Array.isArray(toolInput) ? { args: toolInput } : (toolInput || {});
    cliArgs = toolDef.toCli(inputObj);
  } else if (name.startsWith('actual_') && name !== 'actual_execute') {
    // Legacy tool fallback: e.g. actual_accounts, actual_transactions, actual_sync
    const cmd = name.replace('actual_', '').replace(/_/g, '-');
    if (CLI_COMMANDS.includes(cmd)) {
      const rawArgs = Array.isArray(toolInput)
        ? toolInput
        : (toolInput && Array.isArray(toolInput.args) ? toolInput.args : []);
      cliArgs = [cmd, ...rawArgs];
    } else {
      throw new Error(`Unknown tool: ${name}`);
    }
  } else if (name === 'actual_execute') {
    const rawArgs = Array.isArray(toolInput)
      ? toolInput
      : (toolInput && Array.isArray(toolInput.args) ? toolInput.args : []);
    cliArgs = rawArgs;
  } else {
    throw new Error(`Unknown tool: ${name}`);
  }

  // Extract format from args (--format json|csv|table)
  let format = 'json';
  const formatIdx = cliArgs.indexOf('--format');
  if (formatIdx !== -1 && formatIdx + 1 < cliArgs.length) {
    format = cliArgs[formatIdx + 1];
    // Remove --format from args so handlers don't see it
    cliArgs = [...cliArgs.slice(0, formatIdx), ...cliArgs.slice(formatIdx + 2)];
  }

  // Remove --verbose flag (we log to file instead)
  const verboseIdx = cliArgs.indexOf('--verbose');
  if (verboseIdx !== -1) {
    cliArgs = [...cliArgs.slice(0, verboseIdx), ...cliArgs.slice(verboseIdx + 1)];
  }

  const extractError = (e) => {
    if (e instanceof Error) return e.stack || e.message;
    if (typeof e === 'object' && e !== null) {
      try { return JSON.stringify(e); } catch { return String(e); }
    }
    return String(e);
  };

  const executeWithRetry = async () => {
    try {
      const result = await executeCommand(cliArgs, currentClientId);
      const output = formatOutput(result, format);
      return {
        content: [{ type: "text", text: output }],
        isError: false,
      };
    } catch (err) {
      if (isSchemaMismatch(err)) {
        const errMsg = extractError(err);
        await logMessage(`Schema mismatch detected: ${errMsg}. Upgrading Actual packages and retrying...`, currentClientId);
        try {
          await autoUpgradePackages(currentClientId);
          const result = await executeCommand(cliArgs, currentClientId);
          const output = formatOutput(result, format);
          return {
            content: [{ type: "text", text: output }],
            isError: false,
          };
        } catch (retryErr) {
          const retryMsg = extractError(retryErr);
          await logMessage(`ERROR after upgrade retry: ${retryMsg}`, currentClientId);
          return {
            content: [{ type: "text", text: `Error (after upgrade retry): ${retryMsg}` }],
            isError: true,
          };
        }
      }
      // Retry once on SQLite corruption after rebuilding the budget
      if (isSqliteCorrupt(err)) {
        const errMsg = extractError(err);
        await logMessage(`SQLite error detected: ${errMsg}. Rebuilding and retrying...`, currentClientId);
        try {
          await rebuildBudget(currentClientId);
          const result = await executeCommand(cliArgs, currentClientId);
          const output = formatOutput(result, format);
          return {
            content: [{ type: "text", text: output }],
            isError: false,
          };
        } catch (retryErr) {
          if (isSqliteCorrupt(retryErr)) {
            await logMessage(`FATAL: SQLite corruption persists after rebuild. Marking server as unhealthy.`, currentClientId);
            isHealthy = false;
          }
          const retryMsg = extractError(retryErr);
          await logMessage(`ERROR after rebuild retry: ${retryMsg}`, currentClientId);
          return {
            content: [{ type: "text", text: `Error (after rebuild retry): ${retryMsg}` }],
            isError: true,
          };
        }
      }
      const message = extractError(err);
      await logMessage(`ERROR: ${message}`, currentClientId);
      return {
        content: [{ type: "text", text: `Error: ${message}` }],
        isError: true,
      };
    }
  };

  const resultPromise = commandQueue.then(() => executeWithRetry());
  // Ensure the queue continues even if a command throws internally
  commandQueue = resultPromise.catch(() => {});
  return resultPromise;
}

// ───────────────────────────────────────────────────────────
// MCP Server Factory — creates a new Server instance per session
// All sessions share the same API connection, cache, and command queue
// ───────────────────────────────────────────────────────────
function createServer() {
  const server = new Server({
    name: "actual-cli-mcp",
    version: "2.0.0"
  }, {
    capabilities: {
      tools: {},
      resources: {},
      prompts: {}
    }
  });

  server.setRequestHandler(ListToolsRequestSchema, async () => {
    const tools = MCP_TOOL_DEFINITIONS.map(t => ({
      name: t.name,
      description: t.description,
      inputSchema: t.inputSchema
    }));
    return { tools };
  });

  // Expose SKILL.md as an MCP Resource
  server.setRequestHandler(ListResourcesRequestSchema, async () => {
    return {
      resources: [
        {
          uri: "actual://skill-docs",
          name: "Actual Budget Skill Documentation",
          description: "Comprehensive guide, CLI reference, and best practices for interacting with the Actual Budget API.",
          mimeType: "text/markdown"
        }
      ]
    };
  });

  server.setRequestHandler(ReadResourceRequestSchema, async (request) => {
    const uri = request.params.uri;
    if (uri === "actual://skill-docs") {
      const candidates = [
        process.env.ACTUAL_SKILL_PATH,
        path.join(__dirname, 'SKILL.md'),
        path.join(os.homedir(), '.gemini/config/skills/actual-budget/SKILL.md'),
      ].filter(Boolean);

      let content = null;
      for (const p of candidates) {
        try {
          if (fsSync.existsSync(p)) {
            content = await fs.readFile(p, 'utf8');
            break;
          }
        } catch (e) {}
      }

      if (!content) {
        content = `# Actual Budget MCP Documentation & Best Practices

## Overview
This server provides tools to query and mutate Actual Budget entities via the official Actual API.

## Core Technical Guidelines
- **Usage**: Use the MCP tools rather than executing direct CLI subprocesses.
- **Querying**: Query transactions using \`actual_query\` with \`--select "date,amount,payee.name,notes,category.name,category.group.name,account.name,account.offbudget"\`.
- **Filtering Groups**: Filter by \`category.group.name\` using \`{"$oneof": [...]}\` for multi-group reports.
- **Output**: Use \`--format json\` for reliable structured parsing.
- **Amounts**: Stored as integer cents (e.g., 1000 = $10.00). Positive represents deposit/income, negative represents expense.
- **Entity IDs**: Use \`actual_get_id\` with \`--type <accounts|categories|category_groups> --name <Name>\` to resolve names to UUIDs.
- **Split Transactions**: Default is \`inline\`. Pass options with \`splits: "grouped"\` for parent-child relationship visibility.
`;
      }

      return {
        contents: [
          {
            uri: "actual://skill-docs",
            mimeType: "text/markdown",
            text: content
          }
        ]
      };
    }
    throw new Error(`Unknown resource: ${uri}`);
  });

  // Expose Prompts to instruct agents
  server.setRequestHandler(ListPromptsRequestSchema, async () => {
    return {
      prompts: [
        {
          name: "actual_budget_guidelines",
          description: "Get instructions and best practices for interacting with Actual Budget.",
        }
      ]
    };
  });

  server.setRequestHandler(GetPromptRequestSchema, async (request) => {
    if (request.params.name === "actual_budget_guidelines") {
      return {
        description: "Actual Budget Integration Guidelines",
        messages: [
          {
            role: "user",
            content: {
              type: "text",
              text: `Please read the Actual Budget Skill Documentation resource at 'actual://skill-docs' to understand how to correctly query, format, and interact with the actual budget tools. Pay special attention to the advanced learnings, ActualQL querying, and transaction linking best practices.

**Technical Lessons**:
- **Usage**: Do not run \`actual\` CLI commands directly via bash or python \`subprocess\`. Instead, always use the provided MCP tools.
- **Querying**: Always use \`actual_query\` with \`--select "date,amount,payee.name,notes,category.name,category.group.name,account.name,account.offbudget"\`.
- **Filtering Groups**: Filter by \`category.group.name\` using \`{"$oneof": [...]}\` for multi-group reports.
- **Output**: Include \`--format\` and \`json\` in your tool \`args\` for reliable structured parsing.
- **Split Transactions**: Default is \`inline\`. Use \`--file\` with \`{"options": {"splits": "grouped"}}\` for parent-child relationship visibility.
- **Entity IDs**: Use \`actual_get_id\` with \`--type <accounts|categories|category_groups> --name <Name>\` to find IDs for queries.
- **Efficiency**: Use a single \`actual_query\` call to fetch 12 months of transactions for historical analysis. This reduces sync overhead and execution time by 10x compared to per-month API calls.`
            }
          }
        ]
      };
    }
    throw new Error(`Unknown prompt: ${request.params.name}`);
  });

  // Handle tool calls
  server.setRequestHandler(CallToolRequestSchema, async (request) => {
    const { name, arguments: args } = request.params;
    const currentClientId = authStorage.getStore()?.clientId || null;
    return handleExecuteTool(name, args, currentClientId);
  });

  return server;
}

// ───────────────────────────────────────────────────────────
// Startup
// ───────────────────────────────────────────────────────────
async function run() {
  let isHealthy = true;
  const args = parseArgs(process.argv.slice(2));

  const rawPort = args.port || process.env.BIND_PORT || process.env.PORT;
  if (rawPort) {
    const port = parseIntFlag(rawPort, "port");
    const rawHost = args.host || process.env.BIND_HOST || process.env.HOST || "127.0.0.1";
    const hosts = [...new Set(String(rawHost).split(",").map(h => h.trim()).filter(Boolean))];
    const isAuthDisabled =
      args["no-auth"] === true || args["no-auth"] === "true" ||
      args["noauth"] === true || args["noauth"] === "true" ||
      args["no-oauth"] === true || args["no-oauth"] === "true" ||
      args["auth"] === false || args["auth"] === "false" ||
      process.env.NO_AUTH === "true";
    const enableAuth = !isAuthDisabled;

    const app = express();
    app.disable('x-powered-by');
    app.use((req, res, next) => {
      res.setHeader('X-Content-Type-Options', 'nosniff');
      res.setHeader('X-Frame-Options', 'DENY');
      res.setHeader('Referrer-Policy', 'strict-origin-when-cross-origin');
      next();
    });
    app.use(cors({
      origin: false,  // Deny browser cross-origin requests; MCP calls don't use CORS
      methods: ['GET', 'POST', 'DELETE'],
      allowedHeaders: ['Content-Type', 'Authorization', 'Accept', 'Mcp-Session-Id'],
    }));
    app.use(express.json({ limit: '1mb' }));
    app.use(express.urlencoded({ extended: true, limit: '1mb' }));


    // ─── Setup OAuth2 Discovery and Endpoints ───
    if (enableAuth) {
      validateOAuthConfig();
      setupOAuthRoutes(app);
    }

    // Per-session transport map — supports multiple concurrent clients
    const transports = new Map();
    const sessionClients = new Map();

    // ─── Health Check Endpoint ───
    app.get("/health", (req, res) => {
      if (isHealthy) {
        res.status(200).json({ status: 'OK' });
      } else {
        res.status(503).json({ status: 'UNHEALTHY', error: 'Internal database corruption' });
      }
    });

    // ─── Streamable HTTP Transport (protocol version 2025-11-25) ───
    // Stateless mode — each POST is self-contained, no session to manage.
    const mcpMiddleware = enableAuth ? [oauthMiddleware] : [];
    app.all("/mcp", ...mcpMiddleware, async (req, res) => {
      const sessionId = req.headers['mcp-session-id'];
      const clientId = req.oauthUser?.clientId || (sessionId ? sessionClients.get(sessionId) : null) || null;
      if (sessionId && req.oauthUser?.clientId) {
        sessionClients.set(sessionId, req.oauthUser.clientId);
      }
      return authStorage.run({ clientId }, async () => {
        try {
          // Check for existing session
          let transport;

          if (sessionId && transports.has(sessionId)) {
            const existing = transports.get(sessionId);
            if (existing instanceof StreamableHTTPServerTransport) {
              transport = existing;
            } else if (existing instanceof SSEServerTransport) {
              await existing.handlePostMessage(req, res, req.body);
              return;
            } else {
              res.status(400).json({
                jsonrpc: '2.0',
                error: { code: -32000, message: 'Session exists but uses a different transport protocol' },
                id: null
              });
              return;
            }
          } else if (sessionId && !transports.has(sessionId)) {
            // Rehydrate StreamableHTTP session after server restart so existing clients continue working seamlessly
            transport = new StreamableHTTPServerTransport({
              sessionIdGenerator: () => sessionId,
            });
            if (!isInitializeRequest(req.body)) {
              transport._webStandardTransport.sessionId = sessionId;
              transport._webStandardTransport._initialized = true;
            }
            transports.set(sessionId, transport);
            if (clientId) {
              sessionClients.set(sessionId, clientId);
            }
            transport.onclose = () => {
              const sid = transport.sessionId;
              if (sid && transports.has(sid)) {
                transports.delete(sid);
                sessionClients.delete(sid);
                console.error(`[StreamableHTTP] Session closed: ${sid} (${transports.size} active)`);
              }
            };
            const server = createServer();
            await server.connect(transport);
            const clientPrefix = req.oauthUser?.clientId ? `[Client: ${req.oauthUser.clientId}] ` : "";
            console.error(`[StreamableHTTP] ${clientPrefix}Rehydrated session: ${sessionId} (${transports.size} active)`);
          } else if (!sessionId && req.method === 'GET' && (req.headers['accept']?.includes('text/event-stream') || req.path === '/sse')) {
            // Standalone SSE stream request (e.g. from IDE or older SSE clients)
            const sseTransport = new SSEServerTransport("/messages", res);
            const sid = sseTransport.sessionId;
            transports.set(sid, sseTransport);
            if (clientId) {
              sessionClients.set(sid, clientId);
            }
            const clientPrefix = req.oauthUser?.clientId ? `[Client: ${req.oauthUser.clientId}] ` : "";
            console.error(`[SSE] ${clientPrefix}New session: ${sid} (${transports.size} active)`);

            res.on('close', () => {
              transports.delete(sid);
              sessionClients.delete(sid);
              console.error(`[SSE] Session closed: ${sid} (${transports.size} active)`);
            });

            const server = createServer();
            await server.connect(sseTransport);
            return;
          } else if (!sessionId && req.method === 'POST' && isInitializeRequest(req.body)) {
            // New Streamable HTTP session
            transport = new StreamableHTTPServerTransport({
              sessionIdGenerator: () => randomUUID(),
              onsessioninitialized: (sid) => {
                transports.set(sid, transport);
                if (clientId) {
                  sessionClients.set(sid, clientId);
                }
                const clientPrefix = req.oauthUser?.clientId ? `[Client: ${req.oauthUser.clientId}] ` : "";
                console.error(`[StreamableHTTP] ${clientPrefix}New session: ${sid} (${transports.size} active)`);
              }
            });

            transport.onclose = () => {
              const sid = transport.sessionId;
              if (sid && transports.has(sid)) {
                transports.delete(sid);
                sessionClients.delete(sid);
                console.error(`[StreamableHTTP] Session closed: ${sid} (${transports.size} active)`);
              }
            };

            const server = createServer();
            await server.connect(transport);
          } else {
            res.status(400).json({
              jsonrpc: '2.0',
              error: { code: -32000, message: 'Bad Request: No valid session ID provided' },
              id: null
            });
            return;
          }

          await transport.handleRequest(req, res, req.body);
        } catch (error) {
          console.error('Error handling Streamable HTTP request:', error);
          if (!res.headersSent) {
            res.status(500).json({
              jsonrpc: '2.0',
              error: { code: -32603, message: 'Internal server error' },
              id: null
            });
          }
        }
      });
    });

    // ─── SSE Transport (protocol version 2024-11-05) ───
    app.get("/sse", ...mcpMiddleware, async (req, res) => {
      const sessionId = req.headers['mcp-session-id'];
      const clientId = req.oauthUser?.clientId || (sessionId ? sessionClients.get(sessionId) : null) || null;
      return authStorage.run({ clientId }, async () => {
        const transport = new SSEServerTransport("/messages", res);
        const sid = transport.sessionId;
        transports.set(sid, transport);
        if (clientId) {
          sessionClients.set(sid, clientId);
        }
        const clientPrefix = req.oauthUser?.clientId ? `[Client: ${req.oauthUser.clientId}] ` : "";
        console.error(`[SSE] ${clientPrefix}New session on /sse: ${sid} (${transports.size} active)`);

        res.on('close', () => {
          transports.delete(sid);
          sessionClients.delete(sid);
          console.error(`[SSE] Session closed: ${sid} (${transports.size} active)`);
        });

        const server = createServer();
        await server.connect(transport);
      });
    });

    app.post("/messages", ...mcpMiddleware, async (req, res) => {
      const sessionId = req.query.sessionId || req.headers['mcp-session-id'];
      const clientId = req.oauthUser?.clientId || (sessionId ? sessionClients.get(sessionId) : null) || null;
      return authStorage.run({ clientId }, async () => {
        const transport = transports.get(sessionId);
        if (transport && transport instanceof SSEServerTransport) {
          await transport.handlePostMessage(req, res, req.body);
        } else {
          res.status(400).json({
            jsonrpc: '2.0',
            error: { code: -32000, message: 'No active SSE session found for sessionId' },
            id: null
          });
        }
      });
    });

    const activeServers = [];
    for (const host of hosts) {
      const serverInstance = app.listen(port, host, () => {
        console.error(`Actual Budget MCP server v2.0.0 running on http://${host}:${port} (StreamableHTTP) (PID ${process.pid}, dataDir: ${INSTANCE_DATA_DIR})`);
      });
      serverInstance.on("error", (err) => {
        console.error(`[Actual MCP] Warning: Failed to bind on ${host}:${port}: ${err.message}`);
      });
      activeServers.push(serverInstance);
    }
    if (enableAuth) {
      console.error(`[OAuth] OAuth2 protection enabled (Client ID: ${OAUTH_CLIENT_ID})`);
      console.error(`[OAuth] Discovery: http://localhost:${port}/.well-known/oauth-authorization-server`);
    } else {
      console.error(`[OAuth] OAuth2 protection disabled via flag`);
    }
  } else {
    const server = createServer();
    const transport = new StdioServerTransport();
    await server.connect(transport);
    console.error(`Actual Budget MCP server v2.0.0 running (stdio) (PID ${process.pid}, dataDir: ${INSTANCE_DATA_DIR})`);
  }
}

// Clean up instance data dir on graceful shutdown
function cleanupOnExit() {
  try {
    if (fsSync.existsSync(INSTANCE_DATA_DIR)) {
      fsSync.rmSync(INSTANCE_DATA_DIR, { recursive: true, force: true });
    }
  } catch (e) { /* best effort */ }
}
process.on('exit', cleanupOnExit);
process.on('SIGINT', () => { cleanupOnExit(); process.exit(0); });
process.on('SIGTERM', () => { cleanupOnExit(); process.exit(0); });

const isDirectExecution = !process.argv[1] || (
  process.argv[1].endsWith('server.js') ||
  process.argv[1].endsWith('index.js') ||
  process.argv[1].endsWith('actual-mcp')
);

if (isDirectExecution) {
  run().catch(console.error);
}

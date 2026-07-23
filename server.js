#!/usr/bin/env node

import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { SSEServerTransport } from "@modelcontextprotocol/sdk/server/sse.js";
import express from "express";
import cors from "cors";
import {
  CallToolRequestSchema, ListToolsRequestSchema,
  ListResourcesRequestSchema, ReadResourceRequestSchema,
  ListPromptsRequestSchema, GetPromptRequestSchema
} from "@modelcontextprotocol/sdk/types.js";
import fs from "fs/promises";
import fsSync from "fs";
import path from "path";
import os from "os";
import { execSync } from "child_process";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const LOG_DIR = path.join(os.homedir(), 'Projects', 'actual-mcp', 'logs');
const LOG_FILE = path.join(LOG_DIR, 'actual-commands.log');
const ACTUAL_RC_PATH = path.join(os.homedir(), '.actualrc.json');
const ACTUAL_DATA_ROOT = path.join(os.homedir(), '.actual-data');
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
// Logging
// ───────────────────────────────────────────────────────────
async function logMessage(msg) {
  const timestamp = new Date().toISOString().replace('T', ' ').substring(0, 19);
  const line = `${timestamp} - ${msg}\n`;
  try {
    if (!fsSync.existsSync(LOG_DIR)) {
      await fs.mkdir(LOG_DIR, { recursive: true });
    }
    await fs.appendFile(LOG_FILE, line);
  } catch (e) {
    console.error(`Failed to write to log file: ${e.message}`);
  }
}

// ───────────────────────────────────────────────────────────
// Persistent API Connection
// ───────────────────────────────────────────────────────────
let api = null;
let apiInitialized = false;
let budgetLoaded = false;
let rcConfig = null;

async function readActualRc() {
  try {
    const data = await fs.readFile(ACTUAL_RC_PATH, 'utf8');
    return JSON.parse(data);
  } catch (e) {
    return {};
  }
}

async function ensureInit() {
  if (apiInitialized) return;

  // Dynamic import of @actual-app/api
  api = await import('@actual-app/api');
  rcConfig = await readActualRc();

  const initOpts = {
    serverURL: rcConfig.serverUrl,
    dataDir: INSTANCE_DATA_DIR,
  };

  if (rcConfig.password) {
    initOpts.password = rcConfig.password;
  }

  await logMessage('API init: connecting to server...');
  await api.init(initOpts);
  apiInitialized = true;
  await logMessage('API init: connected');
}

async function ensureBudget() {
  await ensureInit();
  if (budgetLoaded) return;

  if (!rcConfig.syncId) {
    throw new Error('syncId not found in .actualrc.json — cannot load budget');
  }

  try {
    await logMessage(`Downloading budget ${rcConfig.syncId}...`);
    await api.downloadBudget(rcConfig.syncId, {
      password: rcConfig.encryptionPassword,
    });
    budgetLoaded = true;
    await logMessage('Budget loaded');
  } catch (err) {
    if (isSqliteCorrupt(err)) {
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

async function rebuildBudget() {
  await logMessage('Rebuilding budget: shutting down API...');

  // Shut down the current API connection
  if (api && apiInitialized) {
    try { await api.shutdown(); } catch (e) { /* ignore */ }
  }
  apiInitialized = false;
  budgetLoaded = false;
  invalidateCache();

  // Delete this instance's entire data directory and recreate it fresh
  try {
    await logMessage(`Removing instance data dir: ${INSTANCE_DATA_DIR}`);
    await fs.rm(INSTANCE_DATA_DIR, { recursive: true, force: true });
  } catch (e) {
    await logMessage(`Warning: could not remove instance data dir: ${e.message}`);
  }

  // Re-initialize and re-download
  await logMessage('Rebuilding budget: re-initializing API...');
  api = await import('@actual-app/api');
  const initOpts = {
    serverURL: rcConfig.serverUrl,
    dataDir: INSTANCE_DATA_DIR,
  };
  if (rcConfig.password) {
    initOpts.password = rcConfig.password;
  }
  await api.init(initOpts);
  apiInitialized = true;

  await logMessage(`Rebuilding budget: downloading ${rcConfig.syncId}...`);
  await api.downloadBudget(rcConfig.syncId, {
    password: rcConfig.encryptionPassword,
  });
  budgetLoaded = true;
  invalidateCache();
  await logMessage('Budget rebuilt successfully');
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
  if (isSqliteCorrupt(err)) {
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
  if (isSqliteCorrupt(err)) {
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
  categories: null,
  categoryGroups: null,
  payees: null,
  tags: null,
};

function invalidateCache() {
  cache.accounts = null;
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
    'account.name': { type: 'string', ref: 'accounts' },
    'payee.name': { type: 'string', ref: 'payees' },
    'category.name': { type: 'string', ref: 'categories' },
    'category.group.name': { type: 'string', ref: 'category_groups' },
  },
  accounts: {
    id: { type: 'id' },
    name: { type: 'string' },
    offbudget: { type: 'boolean' },
    closed: { type: 'boolean' },
    sort_order: { type: 'float' },
  },
  categories: {
    id: { type: 'id' },
    name: { type: 'string' },
    is_income: { type: 'boolean' },
    group_id: { type: 'id', ref: 'category_groups' },
    sort_order: { type: 'float' },
    hidden: { type: 'boolean' },
    'group.name': { type: 'string', ref: 'category_groups' },
  },
  payees: {
    id: { type: 'id' },
    name: { type: 'string' },
    transfer_acct: { type: 'id', ref: 'accounts' },
  },
  rules: {
    id: { type: 'id' },
    stage: { type: 'string' },
    conditions_op: { type: 'string' },
    conditions: { type: 'json' },
    actions: { type: 'json' },
  },
  schedules: {
    id: { type: 'id' },
    name: { type: 'string' },
    rule: { type: 'id', ref: 'rules' },
    next_date: { type: 'date' },
    completed: { type: 'boolean' },
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
  return queryObj;
}

// ───────────────────────────────────────────────────────────
// Command Handlers — Direct API calls, no subprocess spawning
// ───────────────────────────────────────────────────────────

async function handleAccounts(subCmd, opts) {
  switch (subCmd) {
    case 'list': {
      await ensureBudget();
      const allAccounts = await getCachedAccounts();
      const accounts = allAccounts.filter(a => opts['include-closed'] || !a.closed);
      accounts.sort((a, b) => Number(a.offbudget) - Number(b.offbudget));
      const balances = await Promise.all(accounts.map(a => api.getAccountBalance(a.id)));
      return accounts.map((a, i) => ({
        id: a.id, name: a.name, offbudget: a.offbudget, closed: a.closed, balance: balances[i],
      }));
    }
    case 'create': {
      await ensureBudget();
      const balance = opts.balance ? parseIntFlag(opts.balance, '--balance') : 0;
      const id = await api.createAccount(
        { name: opts.name, offbudget: !!opts.offbudget },
        balance,
      );
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
      if (Object.keys(fields).length === 0) {
        throw new Error('No update fields provided. Use --name or --offbudget.');
      }
      await api.updateAccount(id, fields);
      cache.accounts = null;
      return { success: true, id };
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
    case 'download': {
      await ensureInit();
      const syncId = opts._positional[0];
      if (!syncId) throw new Error('syncId is required');
      const password = opts['encryption-password'] || rcConfig.encryptionPassword;
      await api.downloadBudget(syncId, { password });
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
      return await api.getTransactions(opts.account, opts.start, opts.end);
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
    case 'restore-notes': {
      await ensureBudget();
      const ids = opts._positional.length > 0 ? opts._positional : (opts.ids ? opts.ids.split(',') : []);
      if (!ids || ids.length === 0) throw new Error('Transaction IDs are required as positional arguments');

      const idsList = ids.map(id => `'${id}'`).join(', ');
      const query = `SELECT id, notes FROM transactions WHERE id IN (${idsList});`;
      const output = execSync(`sqlite3 -json backup.sqlite "${query}"`, { encoding: 'utf8' });

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

      const idsList = ids.map(id => `'${id}'`).join(', ');
      const query = `SELECT * FROM transactions WHERE id IN (${idsList});`;
      const output = execSync(`sqlite3 -json backup.sqlite "${query}"`, { encoding: 'utf8' });

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
      return await getCachedCategories();
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
      if (Object.keys(fields).length === 0) {
        throw new Error('No update fields provided. Use --name or --hidden.');
      }
      await api.updateCategory(id, fields);
      cache.categories = null;
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
      await ensureBudget();
      let queryObj;
      if (opts.file || opts.data) {
        const parsed = readJsonInput(opts);
        if (!isRecord(parsed)) throw new Error('Query file must contain a JSON object');
        queryObj = buildQueryFromFile(parsed, opts.table);
      } else {
        queryObj = buildQuery(opts);
      }
      const result = await api.aqlQuery(queryObj);
      if (!isRecord(result) || !('data' in result)) {
        throw new Error('Query result missing data');
      }
      if (opts.count) {
        return { count: result.data };
      }
      return result.data;
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
      const id = await api.getIDByName(opts.type, opts.name);
      return { id, type: opts.type, name: opts.name };
    }
    case 'bank-sync': {
      await ensureBudget();
      const args = opts.account ? { accountId: opts.account } : undefined;
      await api.runBankSync(args);
      return { success: true };
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
};

async function executeCommand(cliArgs) {
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

  await logMessage(`${command} ${remaining.join(' ')}`);
  const startTime = Date.now();

  const result = await handler(subCmd || '', opts);

  const elapsed = Date.now() - startTime;
  await logMessage(`${command} completed in ${elapsed}ms`);

  return result;
}

// ───────────────────────────────────────────────────────────
// MCP Server Setup
// ───────────────────────────────────────────────────────────

const COMMAND_SCHEMAS = {
  accounts: {
    desc: "Manage accounts.",
    details: "Subcommands:\n" +
      "- list [--include-closed] [--format json|table|csv]\n" +
      "- create --name <name> [--offbudget] [--balance <cents>]\n" +
      "- update <id> [--name <name>] [--offbudget <bool>]\n" +
      "- close <id> [--transfer-account <id>] [--transfer-category <id>]\n" +
      "- reopen <id>\n" +
      "- delete <id>\n" +
      "- balance <id> [--cutoff <YYYY-MM-DD>]",
    rules: "Rules:\n- Accounts must be referenced by their UUID.\n- Use `balance` with an account ID to get its current balance."
  },
  budgets: {
    desc: "Manage budgets.",
    details: "Subcommands:\n" +
      "- list\n" +
      "- download <syncId> [--encryption-password <pw>]\n" +
      "- months\n" +
      "- month <YYYY-MM>\n" +
      "- set-amount --month <YYYY-MM> --category <id> --amount <cents>\n" +
      "- set-carryover --month <YYYY-MM> --category <id> --flag <bool>\n" +
      "- hold-next-month --month <YYYY-MM> --amount <cents>\n" +
      "- reset-hold --month <YYYY-MM>",
    rules: "Rules:\n- Budgets list does not require syncId.\n- `--month` must be formatted as YYYY-MM."
  },
  transactions: {
    desc: "Manage transactions.",
    details: "Subcommands:\n" +
      "- list --account <id> --start <YYYY-MM-DD> --end <YYYY-MM-DD>\n" +
      "- add --account <id> (--data <json> | --file <path>) [--learn-categories] [--run-transfers]\n" +
      "- import --account <id> (--data <json> | --file <path>) [--dry-run]\n" +
      "- update <id> (--data <json> | --file <path>)\n" +
      "- delete <id>\n" +
      "- restore-notes <id1> <id2> ...\n" +
      "- restore-transaction <id1> <id2> ...",
    rules: "Rules:\n- list: ALL of `--account`, `--start`, and `--end` are absolutely required or the command will fail.\n- add/import: MUST provide `--account` and either `--data` (as JSON string) or `--file`.\n- update: Takes the transaction ID as a positional argument. The `--data` flag must contain fields to update.\n- restore-notes: Takes a list of transaction IDs and restores their notes from backup.sqlite.\n- restore-transaction: Takes a list of transaction IDs and restores all fields from backup.sqlite."
  },
  categories: {
    desc: "Manage categories.",
    details: "Subcommands:\n" +
      "- list [--include-hidden]\n" +
      "- create --name <name> --group-id <id> [--is-income]\n" +
      "- update <id> [--name <name>] [--hidden <bool>]\n" +
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
      "- merge --target <id> --ids <id1,id2,id3>",
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
    rules: "Rules:\n- `get-id` requires `--type` (e.g. 'accounts') and `--name`."
  }
};

const CLI_COMMANDS = Object.keys(COMMAND_SCHEMAS);

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
  const tools = Object.entries(COMMAND_SCHEMAS).map(([cmd, schema]) => ({
    name: `actual_${cmd.replace(/-/g, '_')}`,
    description: `Execute 'actual ${cmd}' command. ${schema.desc}\n\n${schema.details}\n\n${schema.rules}\n\nGlobal flags like --format json, --verbose are also supported. Amounts are in integer cents (e.g. 5000 = $50.00).`,
    inputSchema: {
      type: "object",
      properties: {
        args: {
          type: "array",
          items: { type: "string" },
          description: `The list of arguments/flags to pass to 'actual ${cmd}'. E.g., ["list", "--format", "json"] or ["create", "--name", "My Account"]`
        }
      },
      required: ["args"]
    }
  }));

  // Add the generic actual_execute tool
  tools.push({
    name: "actual_execute",
    description: "Execute a generic command using the Actual Budget CLI. E.g., args: ['help'].",
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
    }
  });

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
    const skillPath = '~/.gemini/antigravity/skills/actual/SKILL.md';
    try {
      const content = await fs.readFile(skillPath, 'utf8');
      return {
        contents: [
          {
            uri: "actual://skill-docs",
            mimeType: "text/markdown",
            text: content
          }
        ]
      };
    } catch (e) {
      throw new Error(`Failed to read SKILL.md: ${e.message}`);
    }
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
            text: "Please read the Actual Budget Skill Documentation resource at 'actual://skill-docs' to understand how to correctly query, format, and interact with the actual budget tools. Pay special attention to the advanced learnings, ActualQL querying, and transaction linking best practices."
          }
        }
      ]
    };
  }
  throw new Error(`Unknown prompt: ${request.params.name}`);
});

let commandQueue = Promise.resolve();

async function handleExecuteTool(name, cliArgsRaw) {
  let cliArgs = cliArgsRaw || [];

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

  // If a specific tool is called, prepend the command name
  if (name.startsWith('actual_') && name !== 'actual_execute') {
    const cmd = name.replace('actual_', '').replace(/_/g, '-');
    if (CLI_COMMANDS.includes(cmd)) {
      cliArgs = [cmd, ...cliArgs];
    } else {
      throw new Error(`Unknown tool: ${name}`);
    }
  } else if (name !== 'actual_execute') {
    throw new Error(`Unknown tool: ${name}`);
  }

  const executeWithRetry = async () => {
    try {
      const result = await executeCommand(cliArgs);
      const output = formatOutput(result, format);
      return {
        content: [{ type: "text", text: output }],
        isError: false,
      };
    } catch (err) {
      // Retry once on SQLite corruption after rebuilding the budget
      if (isSqliteCorrupt(err)) {
        const errMsg = err instanceof Error ? err.message : String(err);
        await logMessage(`SQLite error detected: ${errMsg}. Rebuilding and retrying...`);
        try {
          await rebuildBudget();
          const result = await executeCommand(cliArgs);
          const output = formatOutput(result, format);
          return {
            content: [{ type: "text", text: output }],
            isError: false,
          };
        } catch (retryErr) {
          const retryMsg = retryErr instanceof Error ? retryErr.message : String(retryErr);
          await logMessage(`ERROR after rebuild retry: ${retryMsg}`);
          return {
            content: [{ type: "text", text: `Error (after rebuild retry): ${retryMsg}` }],
            isError: true,
          };
        }
      }
      const message = err instanceof Error ? err.message : String(err);
      await logMessage(`ERROR: ${message}`);
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

// Handle tool calls
server.setRequestHandler(CallToolRequestSchema, async (request) => {
  const { name, arguments: args } = request.params;
  return handleExecuteTool(name, args.args);
});

// ───────────────────────────────────────────────────────────
// Startup
// ───────────────────────────────────────────────────────────
async function run() {
  const args = parseArgs(process.argv.slice(2));

  if (args.port) {
    const port = parseIntFlag(args.port, "port");
    const app = express();
    app.use(cors());
    app.use(express.json());

    let sseTransport;

    app.get("/sse", async (req, res) => {
      sseTransport = new SSEServerTransport("/message", res);
      await server.connect(sseTransport);
    });

    app.post("/message", async (req, res) => {
      if (sseTransport) {
        await sseTransport.handlePostMessage(req, res, req.body);
      } else {
        res.status(503).send("SSE transport not initialized. Connect to /sse first.");
      }
    });

    app.post("/api/mcp/execute", async (req, res) => {
      const { tool_name, arguments: argsObj } = req.body;
      if (!tool_name) {
        return res.status(400).json({ detail: "tool_name is required" });
      }
      try {
        const result = await handleExecuteTool(tool_name, argsObj ? argsObj.args : []);
        if (result.isError) {
          return res.status(400).json({ detail: result.content[0].text });
        }
        return res.json({ stdout: result.content[0].text });
      } catch (e) {
        return res.status(500).json({ detail: e.message });
      }
    });

    app.listen(port, () => {
      console.error(`Actual Budget MCP server v2.0.0 running on port ${port} (SSE/HTTP) (PID ${process.pid}, dataDir: ${INSTANCE_DATA_DIR})`);
    });
  } else {
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

run().catch(console.error);
setInterval(() => console.error('still alive'), 5000);

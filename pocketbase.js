import path from "node:path";
import fs from "node:fs";
import { DatabaseSync } from "node:sqlite";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Helper to load env variables from candidate paths if not already in process.env
function loadFallbackEnv() {
  const candidateEnvPaths = [
    path.join(__dirname, ".env"),
    path.join(__dirname, "..", "actual-utils", ".env"),
    path.join(__dirname, "..", "actual-orch", ".env"),
  ];

  for (const envPath of candidateEnvPaths) {
    if (fs.existsSync(envPath)) {
      try {
        const content = fs.readFileSync(envPath, "utf-8");
        for (const line of content.split("\n")) {
          const trimmed = line.trim();
          if (!trimmed || trimmed.startsWith("#")) continue;
          const eqIdx = trimmed.indexOf("=");
          if (eqIdx !== -1) {
            const key = trimmed.slice(0, eqIdx).trim();
            let val = trimmed.slice(eqIdx + 1).trim();
            if ((val.startsWith('"') && val.endsWith('"')) || (val.startsWith("'") && val.endsWith("'"))) {
              val = val.slice(1, -1);
            }
            if (!process.env[key]) {
              process.env[key] = val;
            }
          }
        }
      } catch (e) {
        // Ignore read errors
      }
    }
  }
}

loadFallbackEnv();

const PB_BASE_URL = (process.env.PB_UPLOAD_BASE_URL || process.env.PB_BASE_URL || "https://pb.lzn.ca").replace(/\/+$/, "");
const PB_USER = process.env.PB_USER || "actual_orch@lizen.org";
const PB_PASSWORD = process.env.PB_PASSWORD || "t3xiKBWd6JpM3F";
const PB_API_TOKEN = process.env.PB_API_TOKEN || "t9arjlT4M7oeg0JEeRD9fbnVNaC4T%9GOaA7AgFa4pnl@V9BaIh6*Mdz9IN7t9o#";

// ─── Local SQLite Database Setup ───
const DATA_DIR = path.join(__dirname, "data");
if (!fs.existsSync(DATA_DIR)) {
  fs.mkdirSync(DATA_DIR, { recursive: true });
}
const DB_PATH = path.join(DATA_DIR, "pocketbase.sqlite");

const db = new DatabaseSync(DB_PATH);
try {
  fs.chmodSync(DB_PATH, 0o600);
} catch (e) {}

try {
  db.exec(`PRAGMA busy_timeout = 5000;`);
  db.exec(`PRAGMA journal_mode = WAL;`);
} catch (e) {
  console.error(`[PocketBase] Warning setting pragmas on pocketbase.sqlite: ${e.message}`);
}

db.exec(`
  CREATE TABLE IF NOT EXISTS receipts (
    id TEXT PRIMARY KEY,
    tx_id TEXT,
    tx_id_transfer TEXT,
    date TEXT,
    amount REAL,
    payee TEXT,
    source TEXT,
    source_ref TEXT,
    source_hash TEXT,
    lines TEXT,
    parsed TEXT,
    flag TEXT,
    created TEXT,
    updated TEXT
  );
  CREATE INDEX IF NOT EXISTS idx_receipts_tx_id ON receipts(tx_id);
  CREATE INDEX IF NOT EXISTS idx_receipts_source_hash ON receipts(source_hash);
  CREATE INDEX IF NOT EXISTS idx_receipts_source_ref ON receipts(source_ref);
  CREATE INDEX IF NOT EXISTS idx_receipts_date ON receipts(date);
  CREATE INDEX IF NOT EXISTS idx_receipts_payee ON receipts(payee);
  CREATE INDEX IF NOT EXISTS idx_receipts_source ON receipts(source);
  CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT
  );
`);

// ─── Auth State ───
let cachedToken = null;
let tokenExpiry = 0;

async function getAuthToken() {
  const now = Date.now();
  if (cachedToken && tokenExpiry > now + 60000) {
    return cachedToken;
  }

  // Try user/password authentication
  if (PB_USER && PB_PASSWORD) {
    try {
      const resp = await fetch(`${PB_BASE_URL}/api/collections/users/auth-with-password`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ identity: PB_USER, password: PB_PASSWORD }),
      });
      if (resp.ok) {
        const data = await resp.json();
        if (data.token) {
          cachedToken = data.token;
          tokenExpiry = now + 7 * 24 * 3600 * 1000; // default 7 days
          return cachedToken;
        }
      }
    } catch (err) {
      console.error(`[PocketBase] Password auth failed, falling back to API token: ${err.message}`);
    }
  }

  return PB_API_TOKEN;
}

async function pbRequest(endpoint, options = {}) {
  const token = await getAuthToken();
  const url = `${PB_BASE_URL}${endpoint.startsWith("/") ? endpoint : "/" + endpoint}`;
  const isFormData = typeof FormData !== "undefined" && options.body instanceof FormData;
  const headers = {
    ...(isFormData ? {} : { "Content-Type": "application/json" }),
    ...(options.headers || {}),
  };

  if (token) {
    if (token === PB_API_TOKEN) {
      headers["x-api-token"] = token;
    } else {
      headers["Authorization"] = token;
    }
  }

  const response = await fetch(url, {
    ...options,
    headers,
  });

  if (!response.ok) {
    let errBody = "";
    try {
      errBody = await response.text();
    } catch (e) {}
    throw new Error(`PocketBase request failed [${response.status} ${response.statusText}]: ${errBody}`);
  }

  if (response.status === 204) return null;
  return await response.json();
}

function normalizeReceiptRecord(r) {
  let linesStr = "";
  if (typeof r.lines === "string") {
    linesStr = r.lines;
  } else if (r.lines !== undefined && r.lines !== null) {
    linesStr = JSON.stringify(r.lines);
  }

  let parsedStr = "";
  if (typeof r.parsed === "string") {
    parsedStr = r.parsed;
  } else if (r.parsed !== undefined && r.parsed !== null) {
    parsedStr = JSON.stringify(r.parsed);
  }

  let flagStr = "";
  if (Array.isArray(r.flag)) {
    flagStr = r.flag.join(", ");
  } else if (typeof r.flag === "string") {
    flagStr = r.flag;
  }

  let dateStr = r.date || "";
  if (typeof dateStr === "string" && dateStr.length >= 10) {
    dateStr = dateStr.slice(0, 10);
  }

  return {
    id: r.id || "",
    tx_id: r.tx_id || null,
    tx_id_transfer: r.tx_id_transfer || null,
    date: dateStr,
    amount: typeof r.amount === "number" ? r.amount : (parseFloat(r.amount) || 0.0),
    payee: r.payee || "",
    source: r.source || "file",
    source_ref: r.source_ref || "",
    source_hash: r.source_hash || "",
    lines: linesStr,
    parsed: parsedStr,
    flag: flagStr,
    created: r.created || "",
    updated: r.updated || "",
  };
}

function upsertReceiptSqlite(rec) {
  const norm = normalizeReceiptRecord(rec);
  const stmt = db.prepare(`
    INSERT OR REPLACE INTO receipts (
      id, tx_id, tx_id_transfer, date, amount, payee, source, source_ref, source_hash, lines, parsed, flag, created, updated
    ) VALUES (
      ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
    )
  `);
  stmt.run(
    norm.id,
    norm.tx_id,
    norm.tx_id_transfer,
    norm.date,
    norm.amount,
    norm.payee,
    norm.source,
    norm.source_ref,
    norm.source_hash,
    norm.lines,
    norm.parsed,
    norm.flag,
    norm.created,
    norm.updated
  );
}

// ─── Sync All Receipts into SQLite ───
export async function syncReceipts(options = {}) {
  const startTime = Date.now();
  let page = 1;
  const perPage = options.perPage || 500;
  let totalSaved = 0;

  const insertMany = (records) => {
    db.exec("BEGIN TRANSACTION;");
    try {
      const stmt = db.prepare(`
        INSERT OR REPLACE INTO receipts (
          id, tx_id, tx_id_transfer, date, amount, payee, source, source_ref, source_hash, lines, parsed, flag, created, updated
        ) VALUES (
          ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
      `);
      for (const r of records) {
        const norm = normalizeReceiptRecord(r);
        stmt.run(
          norm.id,
          norm.tx_id,
          norm.tx_id_transfer,
          norm.date,
          norm.amount,
          norm.payee,
          norm.source,
          norm.source_ref,
          norm.source_hash,
          norm.lines,
          norm.parsed,
          norm.flag,
          norm.created,
          norm.updated
        );
      }
      db.exec("COMMIT;");
    } catch (e) {
      db.exec("ROLLBACK;");
      throw e;
    }
  };

  while (true) {
    const queryParams = new URLSearchParams({
      page: String(page),
      perPage: String(perPage),
      skipTotal: "true",
    });
    if (options.filter) {
      queryParams.set("filter", options.filter);
    }

    const res = await pbRequest(`/api/collections/receipts/records?${queryParams.toString()}`);
    const items = res.items || [];
    if (items.length > 0) {
      insertMany(items);
      totalSaved += items.length;
    }

    if (items.length < perPage) {
      break;
    }
    page += 1;
  }

  const durationMs = Date.now() - startTime;
  const updateMeta = db.prepare("INSERT OR REPLACE INTO meta (key, value) VALUES ('last_sync', ?)");
  updateMeta.run(new Date().toISOString());

  return {
    success: true,
    totalSaved,
    pages: page,
    durationMs,
  };
}

// ─── Check Receipt Exists (Fast Local O(1)) ───
export function checkReceiptExists({ source_hash, source_ref, tx_id, active_only = false }) {
  if (source_hash) {
    let sql = "SELECT 1 FROM receipts WHERE source_hash = ?";
    if (active_only) {
      sql += " AND (flag IS NULL OR flag NOT LIKE '%ignore%')";
    }
    const stmt = db.prepare(sql);
    const row = stmt.get(source_hash);
    if (row) return true;
  }

  if (source_ref) {
    const cleanRef = source_ref.includes("/") ? source_ref.split("/").pop() : source_ref;
    let sql = "SELECT 1 FROM receipts WHERE source_ref = ? OR source_ref = ?";
    if (active_only) {
      sql += " AND (flag IS NULL OR flag NOT LIKE '%ignore%')";
    }
    const stmt = db.prepare(sql);
    const row = stmt.get(source_ref, cleanRef);
    if (row) return true;
  }

  if (tx_id) {
    let sql = "SELECT 1 FROM receipts WHERE tx_id = ?";
    if (active_only) {
      sql += " AND (flag IS NULL OR flag NOT LIKE '%ignore%')";
    }
    const stmt = db.prepare(sql);
    const row = stmt.get(tx_id);
    if (row) return true;
  }

  return false;
}

// ─── Get Single Receipt ───
export async function getReceipt({ id, tx_id, source_hash, source_ref }) {
  let row = null;
  if (id) {
    row = db.prepare("SELECT * FROM receipts WHERE id = ?").get(id);
  } else if (tx_id) {
    row = db.prepare("SELECT * FROM receipts WHERE tx_id = ? ORDER BY date DESC LIMIT 1").get(tx_id);
  } else if (source_hash) {
    row = db.prepare("SELECT * FROM receipts WHERE source_hash = ? LIMIT 1").get(source_hash);
  } else if (source_ref) {
    const cleanRef = source_ref.includes("/") ? source_ref.split("/").pop() : source_ref;
    row = db.prepare("SELECT * FROM receipts WHERE source_ref = ? OR source_ref = ? LIMIT 1").get(source_ref, cleanRef);
  }

  if (!row && id) {
    try {
      const remote = await pbRequest(`/api/collections/receipts/records/${encodeURIComponent(id)}`);
      if (remote) {
        upsertReceiptSqlite(remote);
        row = db.prepare("SELECT * FROM receipts WHERE id = ?").get(id);
      }
    } catch (e) {
      // not found
    }
  }

  if (!row) return null;

  return formatReceiptOutput(row);
}

function formatReceiptOutput(row) {
  let lines = [];
  try {
    lines = row.lines ? JSON.parse(row.lines) : [];
  } catch (e) {
    lines = row.lines;
  }

  let parsed = null;
  try {
    parsed = row.parsed ? JSON.parse(row.parsed) : null;
  } catch (e) {
    parsed = row.parsed;
  }

  return {
    ...row,
    lines,
    parsed,
  };
}

// ─── List Receipts ───
export function listReceipts(opts = {}) {
  const conditions = [];
  const params = [];

  if (opts.tx_id) {
    conditions.push("tx_id = ?");
    params.push(opts.tx_id);
  }

  if (opts.tx_ids) {
    const ids = Array.isArray(opts.tx_ids)
      ? opts.tx_ids
      : opts.tx_ids.split(",").map((s) => s.trim()).filter(Boolean);
    if (ids.length > 0) {
      conditions.push(`tx_id IN (${ids.map(() => "?").join(", ")})`);
      params.push(...ids);
    }
  }

  if (opts.date) {
    conditions.push("date = ?");
    params.push(opts.date);
  }

  if (opts.start) {
    conditions.push("date >= ?");
    params.push(opts.start);
  }

  if (opts.end) {
    conditions.push("date <= ?");
    params.push(opts.end);
  }

  if (opts.source) {
    conditions.push("source = ?");
    params.push(opts.source);
  }

  if (opts.source_ref) {
    conditions.push("source_ref = ?");
    params.push(opts.source_ref);
  }

  if (opts.source_hash) {
    conditions.push("source_hash = ?");
    params.push(opts.source_hash);
  }

  if (opts.payee) {
    conditions.push("payee LIKE ?");
    params.push(`%${opts.payee}%`);
  }

  if (opts.exclude_ignored || opts.active_only) {
    conditions.push("(flag IS NULL OR flag NOT LIKE '%ignore%')");
  }

  const whereClause = conditions.length > 0 ? `WHERE ${conditions.join(" AND ")}` : "";
  const limit = opts.limit !== undefined ? parseInt(opts.limit, 10) : 100;
  const offset = opts.offset !== undefined ? parseInt(opts.offset, 10) : 0;
  const orderBy = opts.order_by || "date DESC, created DESC";

  const sql = `SELECT * FROM receipts ${whereClause} ORDER BY ${orderBy} LIMIT ? OFFSET ?`;
  params.push(limit, offset);

  const stmt = db.prepare(sql);
  const rows = stmt.all(...params);

  return rows.map(formatReceiptOutput);
}

// ─── Save / Upsert Receipt ───
export async function saveReceipt(data) {
  let existingId = data.id || null;

  // 1. Search existing from local SQLite cache first (O(1) local check)
  if (!existingId && data.source_hash) {
    const row = db.prepare("SELECT id FROM receipts WHERE source_hash = ? LIMIT 1").get(data.source_hash);
    if (row) existingId = row.id;
  }
  if (!existingId && data.source_ref) {
    const cleanRef = data.source_ref.includes("/") ? data.source_ref.split("/").pop() : data.source_ref;
    const row = db.prepare("SELECT id FROM receipts WHERE source_ref = ? OR source_ref = ? LIMIT 1").get(data.source_ref, cleanRef);
    if (row) existingId = row.id;
  }

  // 2. Fallback to remote PocketBase search if not in local cache
  if (!existingId && (data.source_hash || data.source_ref)) {
    const filters = [];
    if (data.source_hash) filters.push(`source_hash='${data.source_hash}'`);
    if (data.source_ref) {
      const cleanRef = data.source_ref.includes("/") ? data.source_ref.split("/").pop() : data.source_ref;
      filters.push(`source_ref='${data.source_ref}' || source_ref='${cleanRef}'`);
    }
    try {
      const searchRes = await pbRequest(`/api/collections/receipts/records?filter=${encodeURIComponent(filters.join(" || "))}&perPage=1&skipTotal=true`);
      if (searchRes.items && searchRes.items.length > 0) {
        existingId = searchRes.items[0].id;
      }
    } catch (e) {
      // Ignore search error, will create
    }
  }

  // Build payload only from explicitly provided fields to support partial updates/PATCH
  const pbPayload = {};
  if ("tx_id" in data) pbPayload.tx_id = data.tx_id;
  if ("tx_id_transfer" in data) pbPayload.tx_id_transfer = data.tx_id_transfer;
  if ("date" in data) {
    const d = data.date ? String(data.date).slice(0, 10) : "";
    pbPayload.date = d ? `${d} 00:00:00.000Z` : null;
  }
  if ("amount" in data) {
    pbPayload.amount = typeof data.amount === "number" ? data.amount : (parseFloat(data.amount) || 0.0);
  }
  if ("payee" in data) pbPayload.payee = data.payee;
  if ("source" in data) pbPayload.source = data.source;
  if ("source_ref" in data) pbPayload.source_ref = data.source_ref;
  if ("source_hash" in data) pbPayload.source_hash = data.source_hash;
  if ("lines" in data) {
    pbPayload.lines = typeof data.lines === "string" ? data.lines : JSON.stringify(data.lines);
  }
  if ("parsed" in data) {
    pbPayload.parsed = typeof data.parsed === "string" ? data.parsed : JSON.stringify(data.parsed);
  }
  if ("flag" in data) {
    pbPayload.flag = Array.isArray(data.flag) ? data.flag.join(", ") : data.flag;
  }

  // If creating new record, set sensible defaults for required fields
  if (!existingId) {
    if (pbPayload.amount === undefined) pbPayload.amount = 0.0;
    if (pbPayload.payee === undefined) pbPayload.payee = "";
    if (pbPayload.source === undefined) pbPayload.source = "file";
    if (pbPayload.source_ref === undefined) pbPayload.source_ref = "";
    if (pbPayload.source_hash === undefined) pbPayload.source_hash = "";
  }

  const imagePaths = Array.isArray(data.image_paths) ? data.image_paths : [];
  let requestBody;
  if (imagePaths.length > 0) {
    const formData = new FormData();
    for (const [k, v] of Object.entries(pbPayload)) {
      if (v !== undefined && v !== null) {
        formData.append(k, typeof v === "object" ? JSON.stringify(v) : String(v));
      }
    }
    for (let i = 0; i < imagePaths.length; i++) {
      const p = imagePaths[i];
      if (fs.existsSync(p)) {
        const fileBytes = fs.readFileSync(p);
        const blob = new Blob([fileBytes], { type: "image/jpeg" });
        formData.append("image", blob, `page_${i}.jpg`);
      }
    }
    requestBody = formData;
  } else {
    requestBody = JSON.stringify(pbPayload);
  }

  let result;
  if (existingId) {
    result = await pbRequest(`/api/collections/receipts/records/${encodeURIComponent(existingId)}`, {
      method: "PATCH",
      body: requestBody,
    });
  } else {
    result = await pbRequest(`/api/collections/receipts/records`, {
      method: "POST",
      body: requestBody,
    });
  }

  upsertReceiptSqlite(result);
  return formatReceiptOutput(result);
}

// ─── Delete Receipt ───
export async function deleteReceipt({ id, source_hash, tx_id }) {
  let targetId = id;
  if (!targetId && source_hash) {
    const row = db.prepare("SELECT id FROM receipts WHERE source_hash = ?").get(source_hash);
    if (row) targetId = row.id;
  }
  if (!targetId && tx_id) {
    const row = db.prepare("SELECT id FROM receipts WHERE tx_id = ?").get(tx_id);
    if (row) targetId = row.id;
  }

  if (!targetId) {
    return { success: false, message: "Receipt not found to delete" };
  }

  try {
    await pbRequest(`/api/collections/receipts/records/${encodeURIComponent(targetId)}`, {
      method: "DELETE",
    });
  } catch (e) {
    console.error(`[PocketBase] Remote delete error: ${e.message}`);
  }

  db.prepare("DELETE FROM receipts WHERE id = ?").run(targetId);
  return { success: true, deletedId: targetId };
}

const VALID_RECEIPT_COLUMNS = new Set([
  "id", "tx_id", "tx_id_transfer", "date", "amount", "payee", "source",
  "source_ref", "source_hash", "lines", "parsed", "flag", "created", "updated"
]);

// ─── ActualQL Query Translator for Receipts Table ───
export function queryReceipts(opts = {}) {
  let countOnly = !!opts.count;
  let selectClause = "*";
  if (countOnly) {
    selectClause = "COUNT(*) as count";
  } else if (opts.select) {
    const fields = opts.select.split(",").map(f => f.trim()).filter(Boolean);
    const selectedCols = fields.map(f => {
      if (VALID_RECEIPT_COLUMNS.has(f)) {
        return `\`${f}\``;
      } else if (f === "image") {
        return "NULL as `image`";
      } else {
        return `NULL as \`${f}\``;
      }
    });
    selectClause = selectedCols.length > 0 ? selectedCols.join(", ") : "*";
  }

  const { whereClause, params } = buildWhereClause(opts.filter);
  const whereSql = whereClause ? `WHERE ${whereClause}` : "";

  if (countOnly) {
    const sql = `SELECT ${selectClause} FROM receipts ${whereSql}`;
    const row = db.prepare(sql).get(...params);
    return { count: row ? row.count : 0 };
  }

  let orderSql = "";
  if (opts.order_by) {
    orderSql = `ORDER BY ${opts.order_by}`;
  } else {
    orderSql = `ORDER BY date DESC`;
  }

  let limitSql = "";
  if (opts.limit !== undefined) {
    limitSql = `LIMIT ${parseInt(opts.limit, 10)}`;
    if (opts.offset !== undefined) {
      limitSql += ` OFFSET ${parseInt(opts.offset, 10)}`;
    }
  }

  const sql = `SELECT ${selectClause} FROM receipts ${whereSql} ${orderSql} ${limitSql}`;
  const rows = db.prepare(sql).all(...params);

  return rows.map((row) => {
    const formatted = { ...row };
    if ("lines" in formatted && typeof formatted.lines === "string") {
      try { formatted.lines = JSON.parse(formatted.lines); } catch (e) {}
    }
    if ("parsed" in formatted && typeof formatted.parsed === "string") {
      try { formatted.parsed = JSON.parse(formatted.parsed); } catch (e) {}
    }
    return formatted;
  });
}

function parsePbFilterString(filterStr) {
  if (!filterStr || typeof filterStr !== "string") return { whereClause: "", params: [] };
  const orClauses = [];
  const params = [];

  const orParts = filterStr.split("||").map(s => s.trim()).filter(Boolean);
  for (const orPart of orParts) {
    const andParts = orPart.split("&&").map(s => s.trim()).filter(Boolean);
    const andClauses = [];
    for (const term of andParts) {
      const match = term.match(/^([a-zA-Z0-9_]+)\s*(=|!=|~|!~)\s*['"]?(.*?)['"]?$/);
      if (match) {
        const [, field, op, rawVal] = match;
        const val = rawVal.replace(/^['"]|['"]$/g, "");
        if (op === "=") {
          andClauses.push(`\`${field}\` = ?`);
          params.push(val);
        } else if (op === "!=") {
          andClauses.push(`\`${field}\` != ?`);
          params.push(val);
        } else if (op === "~") {
          andClauses.push(`\`${field}\` LIKE ?`);
          params.push(`%${val}%`);
        } else if (op === "!~") {
          andClauses.push(`(\`${field}\` IS NULL OR \`${field}\` NOT LIKE ?)`);
          params.push(`%${val}%`);
        }
      }
    }
    if (andClauses.length > 0) {
      orClauses.push(andClauses.length === 1 ? andClauses[0] : `(${andClauses.join(" AND ")})`);
    }
  }

  return {
    whereClause: orClauses.length === 1 ? orClauses[0] : (orClauses.length > 1 ? `(${orClauses.join(" OR ")})` : ""),
    params,
  };
}

function buildWhereClause(filter) {
  if (!filter) return { whereClause: "", params: [] };

  let filterObj = filter;
  if (typeof filter === "string") {
    const trimmed = filter.trim();
    if (trimmed.startsWith("{") && trimmed.endsWith("}")) {
      try {
        filterObj = JSON.parse(trimmed);
      } catch (e) {
        return parsePbFilterString(trimmed);
      }
    } else {
      return parsePbFilterString(trimmed);
    }
  }

  if (typeof filterObj !== "object" || filterObj === null) {
    return { whereClause: "", params: [] };
  }

  const clauses = [];
  const params = [];

  for (const [key, val] of Object.entries(filterObj)) {
    if (key === "$and" && Array.isArray(val)) {
      const subClauses = [];
      for (const item of val) {
        const sub = buildWhereClause(item);
        if (sub.whereClause) {
          subClauses.push(`(${sub.whereClause})`);
          params.push(...sub.params);
        }
      }
      if (subClauses.length > 0) {
        clauses.push(`(${subClauses.join(" AND ")})`);
      }
    } else if (key === "$or" && Array.isArray(val)) {
      const subClauses = [];
      for (const item of val) {
        const sub = buildWhereClause(item);
        if (sub.whereClause) {
          subClauses.push(`(${sub.whereClause})`);
          params.push(...sub.params);
        }
      }
      if (subClauses.length > 0) {
        clauses.push(`(${subClauses.join(" OR ")})`);
      }
    } else if (typeof val === "object" && val !== null && !Array.isArray(val)) {
      // Comparison operators
      for (const [op, opVal] of Object.entries(val)) {
        if (op === "$eq") {
          clauses.push(`\`${key}\` = ?`);
          params.push(opVal);
        } else if (op === "$ne") {
          clauses.push(`\`${key}\` != ?`);
          params.push(opVal);
        } else if (op === "$gt") {
          clauses.push(`\`${key}\` > ?`);
          params.push(opVal);
        } else if (op === "$gte") {
          clauses.push(`\`${key}\` >= ?`);
          params.push(opVal);
        } else if (op === "$lt") {
          clauses.push(`\`${key}\` < ?`);
          params.push(opVal);
        } else if (op === "$lte") {
          clauses.push(`\`${key}\` <= ?`);
          params.push(opVal);
        } else if (op === "$like") {
          clauses.push(`\`${key}\` LIKE ?`);
          params.push(opVal);
        } else if (op === "$oneof" && Array.isArray(opVal)) {
          clauses.push(`\`${key}\` IN (${opVal.map(() => "?").join(", ")})`);
          params.push(...opVal);
        }
      }
    } else {
      // Simple equality
      clauses.push(`\`${key}\` = ?`);
      params.push(val);
    }
  }

  return {
    whereClause: clauses.join(" AND "),
    params,
  };
}

// ─── CLI Command Handler for receipts ───
export async function handleReceipts(subCmd, opts) {
  switch (subCmd) {
    case "list": {
      return listReceipts(opts);
    }
    case "get": {
      const id = opts._positional && opts._positional[0] ? opts._positional[0] : opts.id;
      return await getReceipt({
        id,
        tx_id: opts["tx-id"] || opts.tx_id,
        source_hash: opts["source-hash"] || opts.source_hash,
        source_ref: opts["source-ref"] || opts.source_ref,
      });
    }
    case "exists": {
      return {
        exists: checkReceiptExists({
          source_hash: opts["source-hash"] || opts.source_hash,
          source_ref: opts["source-ref"] || opts.source_ref,
          tx_id: opts["tx-id"] || opts.tx_id,
          active_only: !!(opts["active-only"] || opts.active_only),
        }),
      };
    }
    case "save": {
      let data = opts.data;
      if (typeof data === "string") {
        try { data = JSON.parse(data); } catch (e) {}
      }
      if (!data) data = opts;
      return await saveReceipt(data);
    }
    case "delete": {
      const id = opts._positional && opts._positional[0] ? opts._positional[0] : opts.id;
      return await deleteReceipt({
        id,
        source_hash: opts["source-hash"] || opts.source_hash,
        tx_id: opts["tx-id"] || opts.tx_id,
      });
    }
    case "sync": {
      return await syncReceipts({ full: !!opts.full });
    }
    default:
      throw new Error(`Unknown receipts subcommand: "${subCmd}". Available: list, get, exists, save, delete, sync`);
  }
}

// Check if initial sync is needed on import
(async () => {
  try {
    const row = db.prepare("SELECT COUNT(*) as count FROM receipts").get();
    if (!row || row.count === 0) {
      console.error("[PocketBase] Local cache empty, triggering initial sync...");
      syncReceipts().catch((err) => {
        console.error(`[PocketBase] Background sync error: ${err.message}`);
      });
    }
  } catch (e) {
    // Ignore
  }
})();

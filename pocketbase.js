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

// Configure SQLite pragmas for high performance zero-copy reads, WAL, and fast writes
try {
  db.exec(`PRAGMA busy_timeout = 5000;`);
  db.exec(`PRAGMA journal_mode = WAL;`);
  db.exec(`PRAGMA synchronous = NORMAL;`);
  db.exec(`PRAGMA mmap_size = 268435456;`); // 256 MB memory-mapped I/O
  db.exec(`PRAGMA cache_size = -64000;`);   // 64 MB in-memory page cache
  db.exec(`PRAGMA temp_store = MEMORY;`);
} catch (e) {
  console.error(`[PocketBase] Warning setting pragmas on pocketbase.sqlite: ${e.message}`);
}

// Initialize tables and comprehensive indexes
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
    image TEXT,
    collection_id TEXT,
    created TEXT,
    updated TEXT
  );
  CREATE INDEX IF NOT EXISTS idx_receipts_tx_id ON receipts(tx_id);
  CREATE INDEX IF NOT EXISTS idx_receipts_tx_id_transfer ON receipts(tx_id_transfer);
  CREATE INDEX IF NOT EXISTS idx_receipts_source_hash ON receipts(source_hash);
  CREATE INDEX IF NOT EXISTS idx_receipts_source_ref ON receipts(source_ref);
  CREATE INDEX IF NOT EXISTS idx_receipts_date ON receipts(date);
  CREATE INDEX IF NOT EXISTS idx_receipts_payee ON receipts(payee);
  CREATE INDEX IF NOT EXISTS idx_receipts_source ON receipts(source);
  CREATE INDEX IF NOT EXISTS idx_receipts_date_created ON receipts(date DESC, created DESC);
  CREATE INDEX IF NOT EXISTS idx_receipts_unlinked ON receipts(date DESC, created DESC)
    WHERE (tx_id IS NULL OR tx_id = '') AND (flag IS NULL OR flag NOT LIKE '%ignore%');
  CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT
  );
`);

// Safe migrations for pre-existing databases
try { db.exec(`ALTER TABLE receipts ADD COLUMN image TEXT;`); } catch (e) {}
try { db.exec(`ALTER TABLE receipts ADD COLUMN collection_id TEXT;`); } catch (e) {}
try { db.exec(`CREATE INDEX IF NOT EXISTS idx_receipts_tx_id_transfer ON receipts(tx_id_transfer);`); } catch (e) {}
try { db.exec(`CREATE INDEX IF NOT EXISTS idx_receipts_date_created ON receipts(date DESC, created DESC);`); } catch (e) {}
try { db.exec(`CREATE INDEX IF NOT EXISTS idx_receipts_unlinked ON receipts(date DESC, created DESC) WHERE (tx_id IS NULL OR tx_id = '') AND (flag IS NULL OR flag NOT LIKE '%ignore%');`); } catch (e) {}

// Pre-compiled prepared statements
let _upsertStmt = null;
function getUpsertStmt() {
  if (!_upsertStmt) {
    _upsertStmt = db.prepare(`
      INSERT OR REPLACE INTO receipts (
        id, tx_id, tx_id_transfer, date, amount, payee, source, source_ref, source_hash, lines, parsed, flag, image, collection_id, created, updated
      ) VALUES (
        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
      )
    `);
  }
  return _upsertStmt;
}

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

  let imageStr = "[]";
  if (typeof r.image === "string") {
    if (r.image.startsWith("[")) {
      imageStr = r.image;
    } else if (r.image.trim()) {
      imageStr = JSON.stringify([r.image.trim()]);
    }
  } else if (Array.isArray(r.image)) {
    imageStr = JSON.stringify(r.image);
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
    image: imageStr,
    collection_id: r.collectionId || r.collection_id || "pbc_1571142587",
    created: r.created || "",
    updated: r.updated || "",
  };
}

function upsertReceiptSqlite(rec) {
  const norm = normalizeReceiptRecord(rec);
  const stmt = getUpsertStmt();
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
    norm.image,
    norm.collection_id,
    norm.created,
    norm.updated
  );
}

// ─── Sync Receipts into SQLite (Full or Incremental) ───
export async function syncReceipts(options = {}) {
  const startTime = Date.now();
  let page = 1;
  const perPage = options.perPage || 500;
  let totalSaved = 0;
  const isFull = !!options.full;

  let filter = options.filter || "";
  if (!filter && !isFull) {
    const metaRow = db.prepare("SELECT value FROM meta WHERE key = 'last_sync'").get();
    if (metaRow && metaRow.value) {
      const dateFormatted = metaRow.value.replace("T", " ").slice(0, 19);
      filter = `updated > "${dateFormatted}"`;
    }
  }

  const stmt = getUpsertStmt();
  const insertMany = (records) => {
    db.exec("BEGIN TRANSACTION;");
    try {
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
          norm.image,
          norm.collection_id,
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
    if (filter) {
      queryParams.set("filter", filter);
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

  // Checkpoint WAL passively after sync
  try { db.exec(`PRAGMA wal_checkpoint(PASSIVE);`); } catch (e) {}

  return {
    success: true,
    totalSaved,
    pages: page,
    filterUsed: filter || null,
    isFull,
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
    let sql = "SELECT 1 FROM receipts WHERE tx_id = ? OR tx_id_transfer = ?";
    if (active_only) {
      sql += " AND (flag IS NULL OR flag NOT LIKE '%ignore%')";
    }
    const stmt = db.prepare(sql);
    const row = stmt.get(tx_id, tx_id);
    if (row) return true;
  }

  return false;
}

// ─── Get Single Receipt (with remote fallback) ───
export async function getReceipt({ id, tx_id, source_hash, source_ref }) {
  let row = null;
  if (id) {
    row = db.prepare("SELECT * FROM receipts WHERE id = ?").get(id);
  } else if (tx_id) {
    row = db.prepare("SELECT * FROM receipts WHERE tx_id = ? OR tx_id_transfer = ? ORDER BY date DESC LIMIT 1").get(tx_id, tx_id);
  } else if (source_hash) {
    row = db.prepare("SELECT * FROM receipts WHERE source_hash = ? LIMIT 1").get(source_hash);
  } else if (source_ref) {
    const cleanRef = source_ref.includes("/") ? source_ref.split("/").pop() : source_ref;
    row = db.prepare("SELECT * FROM receipts WHERE source_ref = ? OR source_ref = ? LIMIT 1").get(source_ref, cleanRef);
  }

  // Remote fallback if not found locally
  if (!row) {
    try {
      let remote = null;
      if (id) {
        remote = await pbRequest(`/api/collections/receipts/records/${encodeURIComponent(id)}`);
      } else if (source_hash) {
        const safeHash = String(source_hash).replace(/'/g, "\\'");
        const res = await pbRequest(`/api/collections/receipts/records?filter=${encodeURIComponent(`source_hash='${safeHash}'`)}&perPage=1&skipTotal=true`);
        if (res.items && res.items.length > 0) remote = res.items[0];
      } else if (source_ref) {
        const cleanRef = source_ref.includes("/") ? source_ref.split("/").pop() : source_ref;
        const safeRef = String(source_ref).replace(/'/g, "\\'");
        const safeClean = String(cleanRef).replace(/'/g, "\\'");
        const res = await pbRequest(`/api/collections/receipts/records?filter=${encodeURIComponent(`source_ref='${safeRef}' || source_ref='${safeClean}'`)}&perPage=1&skipTotal=true`);
        if (res.items && res.items.length > 0) remote = res.items[0];
      } else if (tx_id) {
        const safeTx = String(tx_id).replace(/'/g, "\\'");
        const res = await pbRequest(`/api/collections/receipts/records?filter=${encodeURIComponent(`tx_id='${safeTx}' || tx_id_transfer='${safeTx}'`)}&perPage=1&skipTotal=true`);
        if (res.items && res.items.length > 0) remote = res.items[0];
      }

      if (remote) {
        upsertReceiptSqlite(remote);
        row = db.prepare("SELECT * FROM receipts WHERE id = ?").get(remote.id);
      }
    } catch (e) {
      // Not found or network error
    }
  }

  if (!row) return null;
  return formatReceiptOutput(row);
}

// ─── Get Receipt Image (Remote Streamable Tool) ───
export async function getReceiptImage({ id, filename, page_index = 0 }) {
  if (!id) {
    throw new Error("Receipt ID is required");
  }

  let row = db.prepare("SELECT id, image, collection_id FROM receipts WHERE id = ?").get(id);
  if (!row) {
    try {
      const remote = await pbRequest(`/api/collections/receipts/records/${encodeURIComponent(id)}`);
      if (remote) {
        upsertReceiptSqlite(remote);
        row = db.prepare("SELECT id, image, collection_id FROM receipts WHERE id = ?").get(id);
      }
    } catch (e) {}
  }

  if (!row) {
    return { found: false, message: `Receipt with ID "${id}" not found` };
  }

  let images = [];
  try {
    images = row.image ? (typeof row.image === "string" && row.image.startsWith("[") ? JSON.parse(row.image) : [row.image]) : [];
  } catch (e) {
    images = [];
  }

  let targetFilename = filename;
  if (!targetFilename) {
    if (images.length === 0) {
      return { found: false, id, message: "Receipt record has no attached images" };
    }
    const idx = Math.min(Math.max(0, page_index || 0), images.length - 1);
    targetFilename = images[idx];
  }

  const collId = row.collection_id || "pbc_1571142587";
  const fileUrl = `${PB_BASE_URL}/api/files/${collId}/${row.id}/${targetFilename}`;

  const token = await getAuthToken();
  const headers = {};
  if (token) {
    if (token === PB_API_TOKEN) {
      headers["x-api-token"] = token;
    } else {
      headers["Authorization"] = token;
    }
  }

  const res = await fetch(fileUrl, { headers });
  if (!res.ok) {
    return {
      found: false,
      id,
      filename: targetFilename,
      url: fileUrl,
      message: `Failed to download image: HTTP ${res.status} ${res.statusText}`,
    };
  }

  const contentType = res.headers.get("content-type") || "image/jpeg";
  const arrayBuffer = await res.arrayBuffer();
  const buffer = Buffer.from(arrayBuffer);
  const dataBase64 = buffer.toString("base64");

  return {
    found: true,
    id,
    filename: targetFilename,
    mime_type: contentType,
    size_bytes: buffer.byteLength,
    data_base64: dataBase64,
    url: fileUrl,
  };
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

  let images = [];
  try {
    images = row.image ? (typeof row.image === "string" && row.image.startsWith("[") ? JSON.parse(row.image) : [row.image]) : [];
  } catch (e) {
    images = [];
  }

  const collId = row.collection_id || "pbc_1571142587";
  const image_urls = (images || []).map(img => `${PB_BASE_URL}/api/files/${collId}/${row.id}/${img}`);

  return {
    ...row,
    lines,
    parsed,
    image: images,
    image_urls,
  };
}

// ─── List Receipts ───
export function listReceipts(opts = {}) {
  const conditions = [];
  const params = [];

  if (opts.tx_id) {
    conditions.push("(tx_id = ? OR tx_id_transfer = ?)");
    params.push(opts.tx_id, opts.tx_id);
  }

  if (opts.tx_ids) {
    const ids = Array.isArray(opts.tx_ids)
      ? opts.tx_ids
      : opts.tx_ids.split(",").map((s) => s.trim()).filter(Boolean);
    if (ids.length > 0) {
      const placeholders = ids.map(() => "?").join(", ");
      conditions.push(`(tx_id IN (${placeholders}) OR tx_id_transfer IN (${placeholders}))`);
      params.push(...ids, ...ids);
    }
  }

  if (opts.unlinked_only || opts["unlinked-only"]) {
    conditions.push("(tx_id IS NULL OR tx_id = '')");
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

  if (opts.search) {
    conditions.push("(payee LIKE ? OR source_ref LIKE ? OR lines LIKE ? OR parsed LIKE ?)");
    params.push(`%${opts.search}%`, `%${opts.search}%`, `%${opts.search}%`, `%${opts.search}%`);
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

// ─── Batch Receipt Lookup by Transaction IDs ───
export function getReceiptsForTransactions(txIds) {
  if (!Array.isArray(txIds) || txIds.length === 0) return new Map();
  const map = new Map();
  const chunkSize = 400; // SQLite variable limit safety
  for (let i = 0; i < txIds.length; i += chunkSize) {
    const chunk = txIds.slice(i, i + chunkSize).filter(Boolean);
    if (chunk.length === 0) continue;
    const placeholders = chunk.map(() => "?").join(", ");
    const sql = `
      SELECT id, tx_id, tx_id_transfer, payee, amount, date, source, source_ref, image, collection_id, flag, lines, parsed
      FROM receipts
      WHERE tx_id IN (${placeholders}) OR tx_id_transfer IN (${placeholders})
    `;
    const rows = db.prepare(sql).all(...chunk, ...chunk);
    for (const row of rows) {
      const formatted = formatReceiptOutput(row);
      const summary = {
        id: formatted.id,
        payee: formatted.payee,
        amount: formatted.amount,
        date: formatted.date,
        source: formatted.source,
        source_ref: formatted.source_ref,
        has_image: Array.isArray(formatted.image) && formatted.image.length > 0,
        image_urls: formatted.image_urls,
        item_count: Array.isArray(formatted.lines) ? formatted.lines.length : 0,
      };
      if (row.tx_id) {
        if (!map.has(row.tx_id)) map.set(row.tx_id, []);
        map.get(row.tx_id).push(summary);
      }
      if (row.tx_id_transfer && row.tx_id_transfer !== row.tx_id) {
        if (!map.has(row.tx_id_transfer)) map.set(row.tx_id_transfer, []);
        map.get(row.tx_id_transfer).push(summary);
      }
    }
  }
  return map;
}

// ─── In-Flight Transaction Receipt Enrichment ───
export function enrichTransactionsWithReceipts(transactions) {
  if (!Array.isArray(transactions) || transactions.length === 0) {
    return transactions;
  }
  const txIds = [];
  for (const tx of transactions) {
    if (tx && typeof tx === "object" && typeof tx.id === "string" && tx.id) {
      txIds.push(tx.id);
    }
  }
  if (txIds.length === 0) return transactions;

  const receiptMap = getReceiptsForTransactions(txIds);
  return transactions.map(tx => {
    if (!tx || typeof tx !== "object" || !tx.id) return tx;
    const receipts = receiptMap.get(tx.id) || [];
    return {
      ...tx,
      has_receipt: receipts.length > 0,
      receipt: receipts.length > 0 ? receipts[0] : null,
      ...(receipts.length > 1 ? { receipts } : {}),
    };
  });
}

// ─── Save / Upsert Receipt ───
export async function saveReceipt(data) {
  let existingId = data.id || data.receipt_id || null;

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
    if (data.source_hash) {
      const safeHash = String(data.source_hash).replace(/'/g, "\\'");
      filters.push(`source_hash='${safeHash}'`);
    }
    if (data.source_ref) {
      const cleanRef = data.source_ref.includes("/") ? data.source_ref.split("/").pop() : data.source_ref;
      const safeRef = String(data.source_ref).replace(/'/g, "\\'");
      const safeClean = String(cleanRef).replace(/'/g, "\\'");
      filters.push(`source_ref='${safeRef}' || source_ref='${safeClean}'`);
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
  if ("image-" in data) {
    pbPayload["image-"] = data["image-"];
  }

  // If creating new record, set sensible defaults for required fields
  if (!existingId) {
    if (pbPayload.amount === undefined) pbPayload.amount = 0.0;
    if (pbPayload.payee === undefined) pbPayload.payee = "";
    if (pbPayload.source === undefined) pbPayload.source = "file";
    if (pbPayload.source_ref === undefined) pbPayload.source_ref = "";
    if (pbPayload.source_hash === undefined) pbPayload.source_hash = "";
  }

  // Check for file uploads: supports local paths (`image_paths`) and base64 images (`images`)
  const hasImagePaths = data.image_paths && Array.isArray(data.image_paths) && data.image_paths.length > 0;
  const hasBase64Images = data.images && Array.isArray(data.images) && data.images.length > 0;
  const hasSingleBase64 = data.image_base64 || (data.image && typeof data.image === "object" && data.image.data_base64);
  const hasImageDeletions = !!data["image-"];

  let requestBody;
  if (hasImagePaths || hasBase64Images || hasSingleBase64 || hasImageDeletions) {
    const formData = new FormData();
    for (const [k, v] of Object.entries(pbPayload)) {
      if (k === "image-") {
        const toDelete = Array.isArray(v) ? v : [v];
        for (const f of toDelete) {
          if (f) formData.append("image-", String(f));
        }
      } else if (v !== undefined && v !== null) {
        formData.append(k, typeof v === "object" ? JSON.stringify(v) : String(v));
      }
    }

    const mimeTypes = {
      ".png": "image/png",
      ".jpg": "image/jpeg",
      ".jpeg": "image/jpeg",
      ".webp": "image/webp",
      ".pdf": "application/pdf",
      ".gif": "image/gif",
    };

    const attachedFilenames = new Set();
    let localPathsAttached = 0;

    if (hasImagePaths) {
      for (let i = 0; i < data.image_paths.length; i++) {
        const p = data.image_paths[i];
        if (p && typeof p === "string" && fs.existsSync(p)) {
          const fileBytes = fs.readFileSync(p);
          const ext = path.extname(p).toLowerCase();
          const mimeType = mimeTypes[ext] || "image/jpeg";
          const blob = new Blob([fileBytes], { type: mimeType });
          const fname = path.basename(p);
          if (!attachedFilenames.has(fname)) {
            formData.append("image", blob, fname);
            attachedFilenames.add(fname);
            localPathsAttached++;
          }
        }
      }
    }

    // Only process base64 images if local paths did not satisfy all images,
    // or to attach images not already attached by filename.
    const allPathsSatisfied = hasImagePaths && localPathsAttached === data.image_paths.length && (!hasBase64Images || data.images.length <= localPathsAttached);

    if (hasBase64Images && !allPathsSatisfied) {
      for (let i = 0; i < data.images.length; i++) {
        const item = data.images[i];
        if (item && item.data_base64) {
          const mimeType = item.mime_type || "image/jpeg";
          const filename = item.filename || `receipt_${i}.${mimeType.split("/")[1] || "jpg"}`;
          if (!attachedFilenames.has(filename)) {
            const buffer = Buffer.from(item.data_base64, "base64");
            const blob = new Blob([buffer], { type: mimeType });
            formData.append("image", blob, filename);
            attachedFilenames.add(filename);
          }
        }
      }
    }

    if (hasSingleBase64 && attachedFilenames.size === 0) {
      const b64 = typeof hasSingleBase64 === "string" ? hasSingleBase64 : hasSingleBase64.data_base64;
      const mime = (typeof hasSingleBase64 === "object" && hasSingleBase64.mime_type) || "image/jpeg";
      const fname = (typeof hasSingleBase64 === "object" && hasSingleBase64.filename) || `receipt.${mime.split("/")[1] || "jpg"}`;
      if (!attachedFilenames.has(fname)) {
        const buffer = Buffer.from(b64, "base64");
        const blob = new Blob([buffer], { type: mime });
        formData.append("image", blob, fname);
        attachedFilenames.add(fname);
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

// ─── Direct Linking & Unlinking Helpers ───
export async function linkReceipt({ receipt_id, tx_id, tx_id_transfer }) {
  if (!receipt_id) throw new Error("receipt_id is required");
  if (!tx_id) throw new Error("tx_id is required");

  return await saveReceipt({
    id: receipt_id,
    tx_id,
    tx_id_transfer: tx_id_transfer || "",
  });
}

export async function unlinkReceipt({ receipt_id }) {
  if (!receipt_id) throw new Error("receipt_id is required");

  return await saveReceipt({
    id: receipt_id,
    tx_id: "",
    tx_id_transfer: "",
  });
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
  "source_ref", "source_hash", "lines", "parsed", "flag", "image", "collection_id", "created", "updated"
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
    orderSql = `ORDER BY date DESC, created DESC`;
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

  return rows.map(formatReceiptOutput);
}

export function parsePbFilterString(filterStr) {
  if (!filterStr || typeof filterStr !== "string") return { whereClause: "", params: [] };
  const orClauses = [];
  const params = [];

  const orParts = filterStr.split("||").map(s => s.trim()).filter(Boolean);
  for (const orPart of orParts) {
    const andParts = orPart.split("&&").map(s => s.trim()).filter(Boolean);
    const andClauses = [];
    for (const term of andParts) {
      // Support all comparison operators: >=, <=, !=, !~, =, <, >, ~
      const match = term.match(/^([a-zA-Z0-9_]+)\s*(>=|<=|!=|!~|=|<|>|~)\s*['"]?(.*?)['"]?$/);
      if (match) {
        const [, field, op, rawVal] = match;
        const val = rawVal.replace(/^['"]|['"]$/g, "").trim();
        const isNullCheck = val === "null" || val === "" || val.toLowerCase() === "null";

        if (op === "=") {
          if (isNullCheck) {
            andClauses.push(`(\`${field}\` IS NULL OR \`${field}\` = '')`);
          } else {
            andClauses.push(`\`${field}\` = ?`);
            params.push(val);
          }
        } else if (op === "!=") {
          if (isNullCheck) {
            andClauses.push(`(\`${field}\` IS NOT NULL AND \`${field}\` != '')`);
          } else {
            andClauses.push(`(\`${field}\` != ? OR \`${field}\` IS NULL)`);
            params.push(val);
          }
        } else if (op === ">=") {
          andClauses.push(`\`${field}\` >= ?`);
          params.push(val);
        } else if (op === "<=") {
          andClauses.push(`\`${field}\` <= ?`);
          params.push(val);
        } else if (op === ">") {
          andClauses.push(`\`${field}\` > ?`);
          params.push(val);
        } else if (op === "<") {
          andClauses.push(`\`${field}\` < ?`);
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
      for (const [op, opVal] of Object.entries(val)) {
        const isNullCheck = opVal === null || opVal === "" || opVal === "null";
        if (op === "$eq") {
          if (isNullCheck) {
            clauses.push(`(\`${key}\` IS NULL OR \`${key}\` = '')`);
          } else {
            clauses.push(`\`${key}\` = ?`);
            params.push(opVal);
          }
        } else if (op === "$ne") {
          if (isNullCheck) {
            clauses.push(`(\`${key}\` IS NOT NULL AND \`${key}\` != '')`);
          } else {
            clauses.push(`(\`${key}\` != ? OR \`${key}\` IS NULL)`);
            params.push(opVal);
          }
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
      if (val === null || val === "" || val === "null") {
        clauses.push(`(\`${key}\` IS NULL OR \`${key}\` = '')`);
      } else {
        clauses.push(`\`${key}\` = ?`);
        params.push(val);
      }
    }
  }

  return {
    whereClause: clauses.join(" AND "),
    params,
  };
}

// ─── Real-Time SSE Synchronization with PocketBase ───
let realtimeAbortController = null;
let realtimeReconnectTimer = null;

export async function startRealtimeListener() {
  if (realtimeAbortController) {
    try { realtimeAbortController.abort(); } catch (e) {}
  }
  realtimeAbortController = new AbortController();
  const { signal } = realtimeAbortController;

  const connect = async () => {
    if (signal.aborted) return;
    try {
      const token = await getAuthToken();
      const headers = {};
      if (token) {
        if (token === PB_API_TOKEN) {
          headers["x-api-token"] = token;
        } else {
          headers["Authorization"] = token;
        }
      }

      const res = await fetch(`${PB_BASE_URL}/api/realtime`, {
        headers,
        signal,
      });

      if (!res.ok || !res.body) {
        throw new Error(`Realtime connection failed: HTTP ${res.status}`);
      }

      const reader = res.body.getReader();
      const decoder = new TextDecoder();
      let buffer = "";
      let clientId = null;

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
              clientId = parsed.clientId;
              // Subscribe to receipts collection
              await fetch(`${PB_BASE_URL}/api/realtime`, {
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
              if (payload.action && payload.record) {
                const action = payload.action;
                const rec = payload.record;
                if (action === "create" || action === "update") {
                  upsertReceiptSqlite(rec);
                  console.error(`[PocketBase Realtime] ${action} cached record ${rec.id}`);
                } else if (action === "delete") {
                  db.prepare("DELETE FROM receipts WHERE id = ?").run(rec.id);
                  console.error(`[PocketBase Realtime] deleted cached record ${rec.id}`);
                }
              }
            } catch (parseErr) {
              // Ignore malformed event
            }
          }
        }
      }
    } catch (err) {
      if (!signal.aborted) {
        console.error(`[PocketBase Realtime] Stream disconnected (${err.message}), reconnecting in 5s...`);
        realtimeReconnectTimer = setTimeout(connect, 5000);
      }
    }
  };

  connect();
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
    case "image": {
      const id = opts._positional && opts._positional[0] ? opts._positional[0] : opts.id;
      const filename = opts.filename;
      const pageIndex = opts["page-index"] !== undefined ? parseInt(opts["page-index"], 10) : (opts.page_index !== undefined ? parseInt(opts.page_index, 10) : 0);
      return await getReceiptImage({ id, filename, page_index: pageIndex });
    }
    case "link": {
      const receipt_id = opts._positional && opts._positional[0] ? opts._positional[0] : (opts.receipt_id || opts.id);
      const tx_id = opts["tx-id"] || opts.tx_id;
      const tx_id_transfer = opts["tx-id-transfer"] || opts.tx_id_transfer;
      return await linkReceipt({ receipt_id, tx_id, tx_id_transfer });
    }
    case "unlink": {
      const receipt_id = opts._positional && opts._positional[0] ? opts._positional[0] : (opts.receipt_id || opts.id);
      return await unlinkReceipt({ receipt_id });
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
    case "for-transactions":
    case "by-transactions": {
      const tx_ids = opts.tx_ids || (opts._positional ? opts._positional : []);
      const ids = Array.isArray(tx_ids) ? tx_ids : String(tx_ids).split(",").map(s => s.trim()).filter(Boolean);
      const map = getReceiptsForTransactions(ids);
      return Object.fromEntries(map);
    }
    default:
      throw new Error(`Unknown receipts subcommand: "${subCmd}". Available: list, get, image, link, unlink, exists, save, delete, sync, for-transactions`);
  }
}

// Start real-time SSE listener in background
startRealtimeListener().catch((err) => {
  console.error(`[PocketBase Realtime] Init error: ${err.message}`);
});

// Periodic incremental sync every 15 minutes as safety net
setInterval(() => {
  syncReceipts({ incremental: true }).catch((err) => {
    console.error(`[PocketBase Background Sync] Error: ${err.message}`);
  });
}, 15 * 60 * 1000);

// Check if initial sync or migration sync is needed on import
(async () => {
  try {
    const row = db.prepare("SELECT COUNT(*) as count FROM receipts").get();
    if (!row || row.count === 0) {
      console.error("[PocketBase] Local cache empty, triggering initial sync...");
      syncReceipts({ full: true }).catch((err) => {
        console.error(`[PocketBase] Background sync error: ${err.message}`);
      });
    } else {
      // Check if image column is populated; if not, trigger a background full sync to backfill images
      const imgRow = db.prepare("SELECT 1 FROM receipts WHERE image IS NOT NULL AND image != '[]' AND image != '' LIMIT 1").get();
      if (!imgRow) {
        console.error("[PocketBase] Image column unpopulated, triggering one-time background full sync...");
        syncReceipts({ full: true }).catch((err) => {
          console.error(`[PocketBase] Background migration sync error: ${err.message}`);
        });
      }
    }
  } catch (e) {
    // Ignore
  }
})();

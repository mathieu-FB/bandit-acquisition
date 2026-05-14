// ============================================================
// CACHE MODULE — SQLite + in-memory two-tier cache
// PR 1: Foundation only (schema, init, preload, full API).
// Call sites in server.js will switch over in subsequent PRs.
// ============================================================

const fs = require('fs');
const path = require('path');
const Database = require('better-sqlite3');

// ------------------------------------------------------------
// Internal state
// ------------------------------------------------------------
let db = null;
let dataDir = null;
let dbPath = null;
let writeErrors = 0;
const ram = {
  daily: new Map(),               // 'YYYY-MM-DD' → row { shopify, meta, google, tiktok, _fetchedAt }
  amazonProducts: new Map(),      // monthKey → { asin → { asin, sku, name, units, ca } }
  amazonProcessedOrders: new Map(), // monthKey → Set<orderId>
  amazonFetchState: new Map(),    // monthKey → ISO string
  amazonAdSpend: { spend: null, lastUpdate: 0 },
  metaAnalysis: new Map(),        // cache_key → { data, createdAt }
  productTypeMap: { map: null, updatedAt: 0 },
  fontaineSKUs: { skus: null, updatedAt: 0 },
  pipedriveFields: { rows: null, updatedAt: 0 },
  recharge: { data: null, fetchedAt: 0 },
};

// Prepared statements (compiled once after init)
let stmts = null;

// ------------------------------------------------------------
// Schema
// ------------------------------------------------------------
const SCHEMA = `
CREATE TABLE IF NOT EXISTS daily_metrics (
  day TEXT PRIMARY KEY,
  shopify_json TEXT NOT NULL,
  meta_json TEXT NOT NULL,
  google_json TEXT NOT NULL,
  tiktok_json TEXT NOT NULL,
  fetched_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_daily_fetched ON daily_metrics(fetched_at);

CREATE TABLE IF NOT EXISTS amazon_products (
  month_key TEXT NOT NULL,
  asin TEXT NOT NULL,
  sku TEXT,
  name TEXT,
  units INTEGER NOT NULL DEFAULT 0,
  ca REAL NOT NULL DEFAULT 0,
  last_updated INTEGER NOT NULL,
  PRIMARY KEY (month_key, asin)
);
CREATE INDEX IF NOT EXISTS idx_amzp_month ON amazon_products(month_key);

CREATE TABLE IF NOT EXISTS amazon_processed_orders (
  month_key TEXT NOT NULL,
  amazon_order_id TEXT NOT NULL,
  processed_at INTEGER NOT NULL,
  PRIMARY KEY (month_key, amazon_order_id)
);

CREATE TABLE IF NOT EXISTS amazon_fetch_state (
  month_key TEXT PRIMARY KEY,
  last_fetch_iso TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS amazon_ad_spend (
  scope TEXT PRIMARY KEY,
  spend REAL,
  last_update INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS meta_analysis (
  cache_key TEXT PRIMARY KEY,
  data_json TEXT NOT NULL,
  created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_meta_created ON meta_analysis(created_at);

CREATE TABLE IF NOT EXISTS product_type_map (
  product_id TEXT PRIMARY KEY,
  product_type TEXT,
  updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS fontaine_skus (
  sku TEXT PRIMARY KEY,
  product_id TEXT,
  updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS pipedrive_fields (
  key TEXT PRIMARY KEY,
  name TEXT,
  field_json TEXT NOT NULL,
  updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS recharge_snapshot (
  scope TEXT PRIMARY KEY,
  data_json TEXT NOT NULL,
  fetched_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS schema_meta (
  key TEXT PRIMARY KEY,
  value TEXT
);
INSERT OR IGNORE INTO schema_meta(key, value) VALUES ('version', '1');
`;

// ------------------------------------------------------------
// Lifecycle
// ------------------------------------------------------------
function init(opts = {}) {
  dataDir = opts.dataDir || process.env.DATA_DIR || path.join(__dirname, 'data');

  if (!process.env.DATA_DIR) {
    console.warn('[Cache] DATA_DIR not set — using ephemeral local dir. Data WILL be lost on redeploy.');
  }

  if (!fs.existsSync(dataDir)) {
    fs.mkdirSync(dataDir, { recursive: true });
  }

  dbPath = path.join(dataDir, 'bandit-cache.db');
  db = new Database(dbPath);
  db.pragma('journal_mode = WAL');
  db.pragma('synchronous = NORMAL');
  db.pragma('temp_store = MEMORY');
  db.pragma('foreign_keys = ON');

  db.exec(SCHEMA);
  prepareStatements();

  // One-shot import of existing JSON files
  try {
    migrateJsonFiles();
  } catch (err) {
    console.error('[Cache] Migration error (non-fatal):', err.message);
  }

  console.log(`[Cache] Initialized at ${dbPath}`);
}

function prepareStatements() {
  stmts = {
    // daily_metrics
    upsertDaily: db.prepare(`
      INSERT INTO daily_metrics (day, shopify_json, meta_json, google_json, tiktok_json, fetched_at)
      VALUES (@day, @shopify, @meta, @google, @tiktok, @fetched_at)
      ON CONFLICT(day) DO UPDATE SET
        shopify_json = excluded.shopify_json,
        meta_json    = excluded.meta_json,
        google_json  = excluded.google_json,
        tiktok_json  = excluded.tiktok_json,
        fetched_at   = excluded.fetched_at
    `),
    selectDailyRange: db.prepare(`SELECT * FROM daily_metrics WHERE day >= ? AND day <= ? ORDER BY day`),
    selectAllDailyDays: db.prepare(`SELECT day FROM daily_metrics ORDER BY day`),
    selectDailyOne: db.prepare(`SELECT * FROM daily_metrics WHERE day = ?`),
    deleteDailyOne: db.prepare(`DELETE FROM daily_metrics WHERE day = ?`),
    deleteAllDaily: db.prepare(`DELETE FROM daily_metrics`),

    // amazon_products
    upsertAmazonProduct: db.prepare(`
      INSERT INTO amazon_products (month_key, asin, sku, name, units, ca, last_updated)
      VALUES (@month_key, @asin, @sku, @name, @units, @ca, @last_updated)
      ON CONFLICT(month_key, asin) DO UPDATE SET
        sku          = excluded.sku,
        name         = excluded.name,
        units        = excluded.units,
        ca           = excluded.ca,
        last_updated = excluded.last_updated
    `),
    selectAmazonMonth: db.prepare(`SELECT * FROM amazon_products WHERE month_key = ?`),
    selectAllAmazon: db.prepare(`SELECT * FROM amazon_products`),
    selectAllAmazonMonths: db.prepare(`SELECT DISTINCT month_key FROM amazon_products`),
    deleteAllAmazonProducts: db.prepare(`DELETE FROM amazon_products`),

    // amazon_processed_orders
    insertProcessedOrder: db.prepare(`
      INSERT OR IGNORE INTO amazon_processed_orders (month_key, amazon_order_id, processed_at)
      VALUES (?, ?, ?)
    `),
    selectAllProcessedOrders: db.prepare(`SELECT month_key, amazon_order_id FROM amazon_processed_orders`),
    deleteAllProcessedOrders: db.prepare(`DELETE FROM amazon_processed_orders`),

    // amazon_fetch_state
    upsertFetchState: db.prepare(`
      INSERT INTO amazon_fetch_state (month_key, last_fetch_iso) VALUES (?, ?)
      ON CONFLICT(month_key) DO UPDATE SET last_fetch_iso = excluded.last_fetch_iso
    `),
    selectAllFetchState: db.prepare(`SELECT month_key, last_fetch_iso FROM amazon_fetch_state`),
    deleteAllFetchState: db.prepare(`DELETE FROM amazon_fetch_state`),

    // amazon_ad_spend
    upsertAdSpend: db.prepare(`
      INSERT INTO amazon_ad_spend (scope, spend, last_update) VALUES (?, ?, ?)
      ON CONFLICT(scope) DO UPDATE SET spend = excluded.spend, last_update = excluded.last_update
    `),
    selectAdSpend: db.prepare(`SELECT spend, last_update FROM amazon_ad_spend WHERE scope = ?`),

    // meta_analysis
    upsertMetaAnalysis: db.prepare(`
      INSERT INTO meta_analysis (cache_key, data_json, created_at) VALUES (?, ?, ?)
      ON CONFLICT(cache_key) DO UPDATE SET data_json = excluded.data_json, created_at = excluded.created_at
    `),
    selectMetaAnalysis: db.prepare(`SELECT data_json, created_at FROM meta_analysis WHERE cache_key = ?`),
    selectAllMetaAnalysis: db.prepare(`SELECT cache_key, data_json, created_at FROM meta_analysis WHERE created_at > ?`),
    deleteOldMetaAnalysis: db.prepare(`DELETE FROM meta_analysis WHERE created_at < ?`),

    // product_type_map
    upsertProductType: db.prepare(`
      INSERT INTO product_type_map (product_id, product_type, updated_at) VALUES (?, ?, ?)
      ON CONFLICT(product_id) DO UPDATE SET product_type = excluded.product_type, updated_at = excluded.updated_at
    `),
    selectAllProductTypes: db.prepare(`SELECT product_id, product_type, updated_at FROM product_type_map`),
    deleteAllProductTypes: db.prepare(`DELETE FROM product_type_map`),

    // fontaine_skus
    upsertFontaineSKU: db.prepare(`
      INSERT INTO fontaine_skus (sku, product_id, updated_at) VALUES (?, ?, ?)
      ON CONFLICT(sku) DO UPDATE SET product_id = excluded.product_id, updated_at = excluded.updated_at
    `),
    selectAllFontaineSKUs: db.prepare(`SELECT sku, product_id, updated_at FROM fontaine_skus`),
    deleteAllFontaineSKUs: db.prepare(`DELETE FROM fontaine_skus`),

    // pipedrive_fields
    upsertPipedriveField: db.prepare(`
      INSERT INTO pipedrive_fields (key, name, field_json, updated_at) VALUES (?, ?, ?, ?)
      ON CONFLICT(key) DO UPDATE SET name = excluded.name, field_json = excluded.field_json, updated_at = excluded.updated_at
    `),
    selectAllPipedriveFields: db.prepare(`SELECT key, name, field_json, updated_at FROM pipedrive_fields`),
    deleteAllPipedriveFields: db.prepare(`DELETE FROM pipedrive_fields`),

    // recharge_snapshot
    upsertRecharge: db.prepare(`
      INSERT INTO recharge_snapshot (scope, data_json, fetched_at) VALUES (?, ?, ?)
      ON CONFLICT(scope) DO UPDATE SET data_json = excluded.data_json, fetched_at = excluded.fetched_at
    `),
    selectRecharge: db.prepare(`SELECT data_json, fetched_at FROM recharge_snapshot WHERE scope = ?`),

    // schema_meta
    selectSchemaMeta: db.prepare(`SELECT value FROM schema_meta WHERE key = ?`),
    upsertSchemaMeta: db.prepare(`
      INSERT INTO schema_meta (key, value) VALUES (?, ?)
      ON CONFLICT(key) DO UPDATE SET value = excluded.value
    `),
  };
}

function migrateJsonFiles() {
  const flag = stmts.selectSchemaMeta.get('migrated_json');
  if (flag && flag.value === '1') return;

  const tx = db.transaction(() => {
    const now = Date.now();

    // 1. amazon-products.json → amazon_products + amazon_processed_orders + amazon_fetch_state
    const ampFile = path.join(dataDir, 'amazon-products.json');
    if (fs.existsSync(ampFile)) {
      try {
        const data = JSON.parse(fs.readFileSync(ampFile, 'utf8'));
        let prodCount = 0, orderCount = 0;

        for (const [key, val] of Object.entries(data.months || {})) {
          if (key.endsWith('_orderIds')) {
            const mk = key.replace('_orderIds', '');
            Object.keys(val || {}).forEach(orderId => {
              stmts.insertProcessedOrder.run(mk, orderId, now);
              orderCount++;
            });
          } else {
            Object.values(val || {}).forEach(p => {
              stmts.upsertAmazonProduct.run({
                month_key: key,
                asin: p.asin,
                sku: p.sku || null,
                name: p.name || null,
                units: p.units || 0,
                ca: p.ca || 0,
                last_updated: now,
              });
              prodCount++;
            });
          }
        }

        Object.entries(data.lastFetch || {}).forEach(([mk, iso]) => {
          stmts.upsertFetchState.run(mk, iso);
        });

        fs.renameSync(ampFile, ampFile + '.migrated');
        console.log(`[Cache] Migrated amazon-products.json: ${prodCount} products, ${orderCount} orders.`);
      } catch (err) {
        console.error('[Cache] amazon-products.json migration failed:', err.message);
      }
    }

    // 2. amazon-adspend.json → amazon_ad_spend
    const adsFile = path.join(dataDir, 'amazon-adspend.json');
    if (fs.existsSync(adsFile)) {
      try {
        const data = JSON.parse(fs.readFileSync(adsFile, 'utf8'));
        stmts.upsertAdSpend.run('default', data.spend != null ? data.spend : null, data.lastUpdate || 0);
        fs.renameSync(adsFile, adsFile + '.migrated');
        console.log(`[Cache] Migrated amazon-adspend.json: spend=${data.spend}, lastUpdate=${data.lastUpdate}`);
      } catch (err) {
        console.error('[Cache] amazon-adspend.json migration failed:', err.message);
      }
    }

    stmts.upsertSchemaMeta.run('migrated_json', '1');
  });

  tx();
  console.log('[Cache] JSON migration complete.');
}

function preload(opts = {}) {
  const days = opts.days != null ? opts.days : 90;
  const t0 = Date.now();

  // 1. daily_metrics: last N days
  const cutoff = formatDate(new Date(Date.now() - days * 86400000));
  const dailyRows = stmts.selectDailyRange.all(cutoff, '9999-12-31');
  ram.daily.clear();
  dailyRows.forEach(r => {
    ram.daily.set(r.day, {
      shopify: safeParse(r.shopify_json),
      meta: safeParse(r.meta_json),
      google: safeParse(r.google_json),
      tiktok: safeParse(r.tiktok_json),
      _fetchedAt: r.fetched_at,
    });
  });

  // 2. amazon_products (all months — small dataset)
  const amzRows = stmts.selectAllAmazon.all();
  ram.amazonProducts.clear();
  amzRows.forEach(r => {
    if (!ram.amazonProducts.has(r.month_key)) ram.amazonProducts.set(r.month_key, {});
    ram.amazonProducts.get(r.month_key)[r.asin] = {
      asin: r.asin, sku: r.sku, name: r.name, units: r.units, ca: r.ca,
    };
  });

  // 3. amazon_processed_orders
  const orderRows = stmts.selectAllProcessedOrders.all();
  ram.amazonProcessedOrders.clear();
  orderRows.forEach(r => {
    if (!ram.amazonProcessedOrders.has(r.month_key)) ram.amazonProcessedOrders.set(r.month_key, new Set());
    ram.amazonProcessedOrders.get(r.month_key).add(r.amazon_order_id);
  });

  // 4. amazon_fetch_state
  const fsRows = stmts.selectAllFetchState.all();
  ram.amazonFetchState.clear();
  fsRows.forEach(r => ram.amazonFetchState.set(r.month_key, r.last_fetch_iso));

  // 5. amazon_ad_spend
  const adRow = stmts.selectAdSpend.get('default');
  ram.amazonAdSpend = adRow
    ? { spend: adRow.spend, lastUpdate: adRow.last_update }
    : { spend: null, lastUpdate: 0 };

  // 6. meta_analysis (last 24h only to avoid loading stale entries)
  const maCutoff = Date.now() - 24 * 60 * 60 * 1000;
  const maRows = stmts.selectAllMetaAnalysis.all(maCutoff);
  ram.metaAnalysis.clear();
  maRows.forEach(r => {
    ram.metaAnalysis.set(r.cache_key, { data: safeParse(r.data_json), createdAt: r.created_at });
  });

  // 7. product_type_map
  const ptRows = stmts.selectAllProductTypes.all();
  if (ptRows.length > 0) {
    const map = {};
    let maxAge = 0;
    ptRows.forEach(r => { map[r.product_id] = r.product_type; if (r.updated_at > maxAge) maxAge = r.updated_at; });
    ram.productTypeMap = { map, updatedAt: maxAge };
  } else {
    ram.productTypeMap = { map: null, updatedAt: 0 };
  }

  // 8. fontaine_skus
  const fsSkuRows = stmts.selectAllFontaineSKUs.all();
  if (fsSkuRows.length > 0) {
    const set = new Set();
    let maxAge = 0;
    fsSkuRows.forEach(r => { set.add(r.sku); if (r.updated_at > maxAge) maxAge = r.updated_at; });
    ram.fontaineSKUs = { skus: set, updatedAt: maxAge };
  } else {
    ram.fontaineSKUs = { skus: null, updatedAt: 0 };
  }

  // 9. pipedrive_fields
  const pdRows = stmts.selectAllPipedriveFields.all();
  if (pdRows.length > 0) {
    const rows = pdRows.map(r => safeParse(r.field_json));
    const maxAge = Math.max(...pdRows.map(r => r.updated_at));
    ram.pipedriveFields = { rows, updatedAt: maxAge };
  } else {
    ram.pipedriveFields = { rows: null, updatedAt: 0 };
  }

  // 10. recharge_snapshot
  const rcRow = stmts.selectRecharge.get('default');
  ram.recharge = rcRow
    ? { data: safeParse(rcRow.data_json), fetchedAt: rcRow.fetched_at }
    : { data: null, fetchedAt: 0 };

  const elapsed = Date.now() - t0;
  return {
    elapsedMs: elapsed,
    days: ram.daily.size,
    amazonMonths: ram.amazonProducts.size,
    amazonProcessedOrders: orderRows.length,
    metaAnalysisEntries: ram.metaAnalysis.size,
  };
}

function close() {
  if (db) {
    try { db.close(); } catch (e) { /* ignore */ }
    db = null;
  }
}

// ------------------------------------------------------------
// Daily metrics API
// ------------------------------------------------------------
function getDailyRow(day) {
  return ram.daily.get(day) || null;
}

function hasDailyRow(day) {
  return ram.daily.has(day);
}

function upsertDailyRow(day, row) {
  const fetchedAt = Date.now();
  ram.daily.set(day, { ...row, _fetchedAt: fetchedAt });
  safeWrite(() => stmts.upsertDaily.run({
    day,
    shopify: JSON.stringify(row.shopify || {}),
    meta: JSON.stringify(row.meta || {}),
    google: JSON.stringify(row.google || {}),
    tiktok: JSON.stringify(row.tiktok || {}),
    fetched_at: fetchedAt,
  }));
}

function upsertDailyRowsBulk(rows) {
  // rows: [{ day, shopify, meta, google, tiktok }]
  const fetchedAt = Date.now();
  const tx = db.transaction((items) => {
    items.forEach(r => {
      ram.daily.set(r.day, { ...r, _fetchedAt: fetchedAt });
      stmts.upsertDaily.run({
        day: r.day,
        shopify: JSON.stringify(r.shopify || {}),
        meta: JSON.stringify(r.meta || {}),
        google: JSON.stringify(r.google || {}),
        tiktok: JSON.stringify(r.tiktok || {}),
        fetched_at: fetchedAt,
      });
    });
  });
  safeWrite(() => tx(rows));
}

function deleteDailyRows(days) {
  if (!Array.isArray(days)) days = [days];
  const tx = db.transaction((items) => {
    items.forEach(d => {
      ram.daily.delete(d);
      stmts.deleteDailyOne.run(d);
    });
  });
  safeWrite(() => tx(days));
  return days.length;
}

function listAllDays() {
  return stmts.selectAllDailyDays.all().map(r => r.day);
}

function getFreshDays() {
  // Today + yesterday in Europe/Paris
  const now = new Date();
  const parisStr = now.toLocaleString('en-US', { timeZone: 'Europe/Paris' });
  const parisNow = new Date(parisStr);
  const yesterday = new Date(parisNow);
  yesterday.setDate(yesterday.getDate() - 1);
  return [formatDate(parisNow), formatDate(yesterday)];
}

function shouldAutoRefresh(day) {
  return getFreshDays().includes(day);
}

// ------------------------------------------------------------
// Amazon products API
// ------------------------------------------------------------
function getAmazonMonth(monthKey) {
  return ram.amazonProducts.get(monthKey) || {};
}

function getAllAmazonMonths() {
  return Array.from(ram.amazonProducts.keys());
}

function upsertAmazonProducts(monthKey, products) {
  // products: array or object of { asin, sku, name, units, ca }
  const list = Array.isArray(products) ? products : Object.values(products);
  if (list.length === 0) return;

  const now = Date.now();
  if (!ram.amazonProducts.has(monthKey)) ram.amazonProducts.set(monthKey, {});
  const monthMap = ram.amazonProducts.get(monthKey);

  const tx = db.transaction((items) => {
    items.forEach(p => {
      monthMap[p.asin] = { asin: p.asin, sku: p.sku || null, name: p.name || null, units: p.units || 0, ca: p.ca || 0 };
      stmts.upsertAmazonProduct.run({
        month_key: monthKey,
        asin: p.asin,
        sku: p.sku || null,
        name: p.name || null,
        units: p.units || 0,
        ca: p.ca || 0,
        last_updated: now,
      });
    });
  });
  safeWrite(() => tx(list));
}

function isAmazonOrderProcessed(monthKey, orderId) {
  const set = ram.amazonProcessedOrders.get(monthKey);
  return set ? set.has(orderId) : false;
}

function markAmazonOrderProcessed(monthKey, orderId) {
  if (!ram.amazonProcessedOrders.has(monthKey)) ram.amazonProcessedOrders.set(monthKey, new Set());
  ram.amazonProcessedOrders.get(monthKey).add(orderId);
  safeWrite(() => stmts.insertProcessedOrder.run(monthKey, orderId, Date.now()));
}

function markAmazonOrdersProcessedBulk(monthKey, orderIds) {
  if (!ram.amazonProcessedOrders.has(monthKey)) ram.amazonProcessedOrders.set(monthKey, new Set());
  const set = ram.amazonProcessedOrders.get(monthKey);
  const now = Date.now();
  const tx = db.transaction((items) => {
    items.forEach(id => {
      set.add(id);
      stmts.insertProcessedOrder.run(monthKey, id, now);
    });
  });
  safeWrite(() => tx(orderIds));
}

function getAmazonFetchWatermark(monthKey) {
  return ram.amazonFetchState.get(monthKey) || null;
}

function setAmazonFetchWatermark(monthKey, iso) {
  ram.amazonFetchState.set(monthKey, iso);
  safeWrite(() => stmts.upsertFetchState.run(monthKey, iso));
}

function resetAmazonProducts() {
  ram.amazonProducts.clear();
  ram.amazonProcessedOrders.clear();
  ram.amazonFetchState.clear();
  const tx = db.transaction(() => {
    stmts.deleteAllAmazonProducts.run();
    stmts.deleteAllProcessedOrders.run();
    stmts.deleteAllFetchState.run();
  });
  safeWrite(() => tx());
}

// ------------------------------------------------------------
// Amazon ad spend API
// ------------------------------------------------------------
function getAmazonAdSpend() {
  return { ...ram.amazonAdSpend };
}

function setAmazonAdSpend(spend) {
  const lastUpdate = Date.now();
  ram.amazonAdSpend = { spend, lastUpdate };
  safeWrite(() => stmts.upsertAdSpend.run('default', spend, lastUpdate));
}

// ------------------------------------------------------------
// Meta analysis cache API
// ------------------------------------------------------------
function getMetaAnalysis(key, maxAgeMs) {
  const entry = ram.metaAnalysis.get(key);
  if (!entry) return null;
  if (maxAgeMs && Date.now() - entry.createdAt > maxAgeMs) return null;
  return entry.data;
}

function setMetaAnalysis(key, data) {
  const createdAt = Date.now();
  ram.metaAnalysis.set(key, { data, createdAt });
  safeWrite(() => stmts.upsertMetaAnalysis.run(key, JSON.stringify(data), createdAt));
}

function deleteMetaAnalysisOlderThan(ageMs) {
  const cutoff = Date.now() - ageMs;
  for (const [k, v] of ram.metaAnalysis) {
    if (v.createdAt < cutoff) ram.metaAnalysis.delete(k);
  }
  safeWrite(() => stmts.deleteOldMetaAnalysis.run(cutoff));
}

// ------------------------------------------------------------
// Product type map API
// ------------------------------------------------------------
function getProductTypeMap(maxAgeMs) {
  if (!ram.productTypeMap.map) return null;
  if (maxAgeMs && Date.now() - ram.productTypeMap.updatedAt > maxAgeMs) return null;
  return ram.productTypeMap.map;
}

function setProductTypeMap(map) {
  const now = Date.now();
  ram.productTypeMap = { map: { ...map }, updatedAt: now };
  const tx = db.transaction((entries) => {
    stmts.deleteAllProductTypes.run();
    entries.forEach(([pid, type]) => stmts.upsertProductType.run(pid, type, now));
  });
  safeWrite(() => tx(Object.entries(map)));
}

// ------------------------------------------------------------
// Fontaine SKUs API
// ------------------------------------------------------------
function getFontaineSKUs(maxAgeMs) {
  if (!ram.fontaineSKUs.skus) return null;
  if (maxAgeMs && Date.now() - ram.fontaineSKUs.updatedAt > maxAgeMs) return null;
  return ram.fontaineSKUs.skus;
}

function setFontaineSKUs(skus, productIdsBySku) {
  // skus: Set<string> or array; productIdsBySku: optional Map/object for product_id linkage
  const set = skus instanceof Set ? skus : new Set(skus);
  const now = Date.now();
  ram.fontaineSKUs = { skus: set, updatedAt: now };
  const tx = db.transaction((arr) => {
    stmts.deleteAllFontaineSKUs.run();
    arr.forEach(sku => {
      const pid = productIdsBySku ? (productIdsBySku.get ? productIdsBySku.get(sku) : productIdsBySku[sku]) : null;
      stmts.upsertFontaineSKU.run(sku, pid || null, now);
    });
  });
  safeWrite(() => tx(Array.from(set)));
}

// ------------------------------------------------------------
// Pipedrive fields API
// ------------------------------------------------------------
function getPipedriveFields(maxAgeMs) {
  if (!ram.pipedriveFields.rows) return null;
  if (maxAgeMs && Date.now() - ram.pipedriveFields.updatedAt > maxAgeMs) return null;
  return ram.pipedriveFields.rows;
}

function setPipedriveFields(fields) {
  const now = Date.now();
  ram.pipedriveFields = { rows: fields, updatedAt: now };
  const tx = db.transaction((items) => {
    stmts.deleteAllPipedriveFields.run();
    items.forEach(f => stmts.upsertPipedriveField.run(f.key || String(f.id || ''), f.name || null, JSON.stringify(f), now));
  });
  safeWrite(() => tx(fields));
}

// ------------------------------------------------------------
// Recharge snapshot API
// ------------------------------------------------------------
function getRechargeSnapshot(maxAgeMs) {
  if (!ram.recharge.data) return null;
  if (maxAgeMs && Date.now() - ram.recharge.fetchedAt > maxAgeMs) return null;
  return ram.recharge.data;
}

function setRechargeSnapshot(data) {
  const now = Date.now();
  ram.recharge = { data, fetchedAt: now };
  safeWrite(() => stmts.upsertRecharge.run('default', JSON.stringify(data), now));
}

// ------------------------------------------------------------
// Diagnostics
// ------------------------------------------------------------
function stats() {
  let dbSizeBytes = 0;
  try {
    if (dbPath && fs.existsSync(dbPath)) dbSizeBytes = fs.statSync(dbPath).size;
  } catch (e) { /* ignore */ }

  const allDays = Array.from(ram.daily.keys()).sort();
  return {
    dbPath,
    dbSizeBytes,
    writeErrors,
    daysInRam: ram.daily.size,
    oldestDay: allDays[0] || null,
    newestDay: allDays[allDays.length - 1] || null,
    amazonMonths: ram.amazonProducts.size,
    amazonProcessedOrders: Array.from(ram.amazonProcessedOrders.values()).reduce((s, set) => s + set.size, 0),
    amazonAdSpend: { ...ram.amazonAdSpend },
    metaAnalysisEntries: ram.metaAnalysis.size,
    productTypeMapEntries: ram.productTypeMap.map ? Object.keys(ram.productTypeMap.map).length : 0,
    fontaineSKUs: ram.fontaineSKUs.skus ? ram.fontaineSKUs.skus.size : 0,
    pipedriveFields: ram.pipedriveFields.rows ? ram.pipedriveFields.rows.length : 0,
    hasRecharge: !!ram.recharge.data,
  };
}

// ------------------------------------------------------------
// Internals
// ------------------------------------------------------------
function safeParse(s) {
  try { return JSON.parse(s); } catch { return null; }
}

function safeWrite(fn) {
  try { fn(); } catch (err) {
    writeErrors++;
    console.error('[Cache] DB write error:', err.message);
  }
}

function formatDate(d) {
  const yyyy = d.getFullYear();
  const mm = String(d.getMonth() + 1).padStart(2, '0');
  const dd = String(d.getDate()).padStart(2, '0');
  return `${yyyy}-${mm}-${dd}`;
}

module.exports = {
  init,
  preload,
  close,
  // daily
  getDailyRow,
  hasDailyRow,
  upsertDailyRow,
  upsertDailyRowsBulk,
  deleteDailyRows,
  listAllDays,
  getFreshDays,
  shouldAutoRefresh,
  // amazon
  getAmazonMonth,
  getAllAmazonMonths,
  upsertAmazonProducts,
  isAmazonOrderProcessed,
  markAmazonOrderProcessed,
  markAmazonOrdersProcessedBulk,
  getAmazonFetchWatermark,
  setAmazonFetchWatermark,
  resetAmazonProducts,
  // amazon ad spend
  getAmazonAdSpend,
  setAmazonAdSpend,
  // meta
  getMetaAnalysis,
  setMetaAnalysis,
  deleteMetaAnalysisOlderThan,
  // product types
  getProductTypeMap,
  setProductTypeMap,
  // fontaine skus
  getFontaineSKUs,
  setFontaineSKUs,
  // pipedrive
  getPipedriveFields,
  setPipedriveFields,
  // recharge
  getRechargeSnapshot,
  setRechargeSnapshot,
  // diagnostics
  stats,
};

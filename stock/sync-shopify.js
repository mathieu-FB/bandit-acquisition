// ============================================================
// SYNC SHOPIFY — three sync entrypoints, each logged in stock_sync_log.
//
//  1. syncShopifyVariants()  → GET /products.json, map SKU → variant_id,
//     product_id, inventory_item_id, title, image_url. Must run BEFORE
//     syncShopifyStock (the stock sync needs inventory_item_ids).
//
//  2. syncShopifyStock()     → GET /inventory_levels.json filtered by
//     the location stored in stock_parametres_globaux.shopify_location_id
//     (default 82726682960 = VETO SANTE). Upserts stock_actuel with
//     source='shopify'.
//
//  3. syncShopifyMonthlySales({ fromYearMonth, toYearMonth })
//     → iterate /orders.json paginated, aggregate net units (qty minus
//     refunded qty) per (sku, YYYY-MM), upsert stock_previsions_mensuelles
//     with is_estimated=0. Filters orders on ALLOWED_SOURCES (DTC only).
// ============================================================

const fetch = require('node-fetch');
const stockDb = require('./db');

const API_VERSION = '2024-01';

// Same filter used by the daily report — keeps stock/sales consistent with the
// DTC channels that actually consume from the VETO SANTE location.
const ALLOWED_SOURCES = new Set([
  'web',
  'JUST',
  '295841693697',
  'subscription_contract',
  'subscription_contract_checkout_one',
]);

function shopifyBase() {
  const store = process.env.SHOPIFY_STORE_URL;
  if (!store) throw new Error('SHOPIFY_STORE_URL non configuré');
  return `https://${store}/admin/api/${API_VERSION}`;
}

function shopifyHeaders() {
  const token = process.env.SHOPIFY_ACCESS_TOKEN;
  if (!token) throw new Error('SHOPIFY_ACCESS_TOKEN non configuré');
  return { 'X-Shopify-Access-Token': token, 'Content-Type': 'application/json' };
}

// Follow Link: <...>; rel="next" REST pagination.
async function* paginate(startUrl) {
  let url = startUrl;
  while (url) {
    const res = await fetch(url, { headers: shopifyHeaders() });
    if (!res.ok) {
      const body = await res.text();
      throw new Error(`Shopify ${res.status} on ${url}: ${body.slice(0, 300)}`);
    }
    const data = await res.json();
    yield data;
    const link = res.headers.get('link');
    if (link && link.includes('rel="next"')) {
      const m = link.match(/<([^>]+)>;\s*rel="next"/);
      url = m ? m[1] : null;
    } else {
      url = null;
    }
  }
}

// ------------------------------------------------------------
// 1. Variants sync
// ------------------------------------------------------------
async function syncShopifyVariants({ onProgress } = {}) {
  const logId = stockDb.startSync('shopify_variants');
  try {
    let updated = 0, unmatched = 0, pages = 0, variantsSeen = 0;
    const startUrl = `${shopifyBase()}/products.json?` + new URLSearchParams({
      limit: '250',
      fields: 'id,title,image,variants',
    }).toString();
    for await (const data of paginate(startUrl)) {
      pages++;
      for (const product of data.products || []) {
        const productImage = product.image && product.image.src ? product.image.src : null;
        for (const variant of product.variants || []) {
          variantsSeen++;
          if (!variant.sku) continue;
          const sku = String(variant.sku).trim();
          if (!sku) continue;
          const existing = stockDb.getReferentielSku(sku);
          if (!existing) { unmatched++; continue; }
          stockDb.updateReferentielShopify(sku, {
            shopify_product_id: String(product.id),
            shopify_variant_id: String(variant.id),
            shopify_inventory_item_id: variant.inventory_item_id != null ? String(variant.inventory_item_id) : null,
            shopify_variant_title: variant.title || null,
            image_url: productImage,
          });
          updated++;
        }
      }
      if (onProgress) onProgress({ pages, variantsSeen, updated, unmatched });
    }
    const stats = { pages, variantsSeen, updated, unmatched };
    stockDb.finishSync(logId, { status: 'ok', message: JSON.stringify(stats) });
    return stats;
  } catch (err) {
    stockDb.finishSync(logId, { status: 'error', message: err.message });
    throw err;
  }
}

// ------------------------------------------------------------
// 2. Stock sync (inventory_levels for our target location only)
// ------------------------------------------------------------
async function syncShopifyStock({ onProgress } = {}) {
  const logId = stockDb.startSync('shopify_stock');
  try {
    const locationId = stockDb.getParametreGlobal('shopify_location_id') || '82726682960';
    const referentiels = stockDb.listReferentielActif().filter(r => r.shopify_inventory_item_id);
    if (referentiels.length === 0) {
      const msg = 'Aucun SKU du référentiel n\'a d\'inventory_item_id — lance /api/stock/sync-shopify?type=variants d\'abord.';
      stockDb.finishSync(logId, { status: 'error', message: msg });
      throw new Error(msg);
    }
    let updated = 0, chunks = 0, unmatched = 0, itemsRequested = 0, levelsReturned = 0;
    const missingItemIds = new Set(referentiels.map(r => r.shopify_inventory_item_id));
    const CHUNK = 50;
    for (let i = 0; i < referentiels.length; i += CHUNK) {
      const chunk = referentiels.slice(i, i + CHUNK);
      const ids = chunk.map(r => r.shopify_inventory_item_id).join(',');
      itemsRequested += chunk.length;
      const url = `${shopifyBase()}/inventory_levels.json?` + new URLSearchParams({
        location_ids: locationId,
        inventory_item_ids: ids,
        limit: '250',
      }).toString();
      const res = await fetch(url, { headers: shopifyHeaders() });
      if (!res.ok) throw new Error(`inventory_levels ${res.status}: ${await res.text()}`);
      const data = await res.json();
      const upserts = [];
      for (const level of data.inventory_levels || []) {
        levelsReturned++;
        const skuRow = stockDb.getSkuByInventoryItemId(level.inventory_item_id);
        if (!skuRow) { unmatched++; continue; }
        upserts.push({
          sku: skuRow.sku,
          stock_dispo: level.available || 0,
          location_id: String(level.location_id),
          source: 'shopify',
        });
        missingItemIds.delete(String(level.inventory_item_id));
      }
      if (upserts.length) stockDb.upsertStockActuelBulk(upserts);
      updated += upserts.length;
      chunks++;
      if (onProgress) onProgress({ chunks, updated, unmatched });
    }
    const stats = {
      chunks, updated, unmatched, locationId,
      itemsRequested,
      levelsReturned,
      itemsWithoutInventoryLevel: missingItemIds.size,
      itemsWithoutInventoryLevelSample: Array.from(missingItemIds).slice(0, 10),
      hint: missingItemIds.size > 0
        ? `${missingItemIds.size} SKU n'ont AUCUN record inventory_level à la location ${locationId}. Vérifie que cette location est bien VETO SANTE — les SKU peuvent être activés à une autre location. GET /api/stock/debug/inventory-check?sku=<sku> pour investiguer.`
        : null,
    };
    stockDb.finishSync(logId, { status: 'ok', message: JSON.stringify(stats) });
    return stats;
  } catch (err) {
    stockDb.finishSync(logId, { status: 'error', message: err.message });
    throw err;
  }
}

// ------------------------------------------------------------
// Debug helper: for a single SKU, return the raw list of locations
// where its inventory_item_id has a record, plus the currently-tracked
// location. Useful when a stock sync leaves a SKU at its seed value.
// ------------------------------------------------------------
async function debugInventoryForSku(sku) {
  const ref = stockDb.getReferentielSku(sku);
  if (!ref) return { error: `SKU introuvable dans le référentiel: ${sku}` };
  if (!ref.shopify_inventory_item_id) {
    return {
      sku, ref: pickRefFields(ref),
      error: 'Pas de shopify_inventory_item_id (lance sync variants d\'abord)',
    };
  }
  const trackedLocation = stockDb.getParametreGlobal('shopify_location_id') || '82726682960';
  // 1. All inventory levels for this inventory_item_id, all locations.
  const levelsUrl = `${shopifyBase()}/inventory_levels.json?` + new URLSearchParams({
    inventory_item_ids: ref.shopify_inventory_item_id,
    limit: '250',
  }).toString();
  const levelsRes = await fetch(levelsUrl, { headers: shopifyHeaders() });
  const levelsPayload = levelsRes.ok ? await levelsRes.json() : { status: levelsRes.status, text: await levelsRes.text() };
  // 2. List all locations to help identify them by name.
  const locsRes = await fetch(`${shopifyBase()}/locations.json`, { headers: shopifyHeaders() });
  const locsPayload = locsRes.ok ? await locsRes.json() : { status: locsRes.status, text: await locsRes.text() };
  return {
    sku, ref: pickRefFields(ref),
    trackedLocationId: trackedLocation,
    levelsForItem: levelsPayload.inventory_levels || levelsPayload,
    availableAtTrackedLocation: (levelsPayload.inventory_levels || []).find(l => String(l.location_id) === String(trackedLocation)) || null,
    locations: (locsPayload.locations || []).map(l => ({
      id: String(l.id), name: l.name, active: l.active,
      matchTracked: String(l.id) === String(trackedLocation),
    })),
  };
}

function pickRefFields(ref) {
  return {
    sku: ref.sku, nom_court: ref.nom_court,
    shopify_product_id: ref.shopify_product_id,
    shopify_variant_id: ref.shopify_variant_id,
    shopify_inventory_item_id: ref.shopify_inventory_item_id,
    stockActuelDb: (stockDb.getStockActuel(ref.sku) || { stock_dispo: null }).stock_dispo,
    stockActuelSource: (stockDb.getStockActuel(ref.sku) || {}).source || null,
  };
}

// ------------------------------------------------------------
// Debug helper: for a SKU, list ALL source_names encountered in Shopify
// orders over a date range, with net-units and gross-units per source.
// Ignores ALLOWED_SOURCES so we can see what's out there. Use this to
// decide which sources should count for stock/réappro.
// ------------------------------------------------------------
async function debugSourcesForSku({ sku, fromYearMonth, toYearMonth }) {
  if (!sku) throw new Error('sku requis');
  const from = fromYearMonth || `${new Date().getUTCFullYear()}-${String(Math.max(1, new Date().getUTCMonth() - 2)).padStart(2, '0')}`;
  const to = toYearMonth || `${new Date().getUTCFullYear()}-${String(new Date().getUTCMonth()).padStart(2, '0')}`;
  const [fromY, fromM] = from.split('-').map(Number);
  const [toY, toM] = to.split('-').map(Number);
  const startISO = new Date(Date.UTC(fromY, fromM - 1, 1)).toISOString();
  const endISO = new Date(Date.UTC(toY, toM, 1)).toISOString();

  const perSource = {}; // source_name → { gross, net, orders, monthlyBreakdown: {ym: {gross, net}} }
  let pages = 0, ordersSeen = 0, ordersWithSku = 0;

  const startUrl = `${shopifyBase()}/orders.json?` + new URLSearchParams({
    created_at_min: startISO,
    created_at_max: endISO,
    status: 'any',
    limit: '250',
    fields: 'id,name,created_at,source_name,line_items,refunds,financial_status',
  }).toString();

  for await (const data of paginate(startUrl)) {
    pages++;
    for (const order of data.orders || []) {
      ordersSeen++;
      const src = order.source_name || '(null)';
      const dt = new Date(order.created_at);
      const ym = `${dt.getUTCFullYear()}-${String(dt.getUTCMonth() + 1).padStart(2, '0')}`;
      const refundedByLI = {};
      for (const ref of order.refunds || []) {
        for (const rli of ref.refund_line_items || []) {
          refundedByLI[rli.line_item_id] = (refundedByLI[rli.line_item_id] || 0) + (rli.quantity || 0);
        }
      }
      let orderMatched = false;
      for (const li of order.line_items || []) {
        if (!li.sku || String(li.sku).trim() !== String(sku).trim()) continue;
        orderMatched = true;
        const gross = li.quantity || 0;
        const refunded = refundedByLI[li.id] || 0;
        const net = Math.max(0, gross - refunded);
        if (!perSource[src]) perSource[src] = { gross: 0, net: 0, refunded: 0, orders: 0, monthly: {} };
        perSource[src].gross += gross;
        perSource[src].net += net;
        perSource[src].refunded += refunded;
        if (!perSource[src].monthly[ym]) perSource[src].monthly[ym] = { gross: 0, net: 0 };
        perSource[src].monthly[ym].gross += gross;
        perSource[src].monthly[ym].net += net;
      }
      if (orderMatched) {
        ordersWithSku++;
        perSource[order.source_name || '(null)'].orders = (perSource[order.source_name || '(null)'].orders || 0) + 1;
      }
    }
  }

  const bySource = Object.entries(perSource).map(([source, agg]) => ({
    source,
    included_in_current_filter: ALLOWED_SOURCES.has(source),
    gross_qty: agg.gross,
    net_qty: agg.net,
    refunded_qty: agg.refunded,
    orders_count: agg.orders,
    monthly: agg.monthly,
  })).sort((a, b) => b.net_qty - a.net_qty);

  const totalNet = bySource.reduce((s, x) => s + x.net_qty, 0);
  const includedNet = bySource.filter(x => x.included_in_current_filter).reduce((s, x) => s + x.net_qty, 0);

  return {
    sku,
    range: `${from} → ${to}`,
    pages, ordersSeen, ordersWithSku,
    totalNetAllSources: totalNet,
    includedNetCurrentFilter: includedNet,
    excludedNet: totalNet - includedNet,
    bySource,
    currentAllowedSources: Array.from(ALLOWED_SOURCES),
  };
}

// ------------------------------------------------------------
// 3. Monthly sales sync (backfill or refresh a range)
// ------------------------------------------------------------
function parseYearMonth(ym) {
  const m = String(ym).match(/^(\d{4})-(\d{1,2})$/);
  if (!m) throw new Error(`Format attendu YYYY-MM, reçu: ${ym}`);
  const y = Number(m[1]);
  const mo = Number(m[2]);
  if (mo < 1 || mo > 12) throw new Error(`Mois invalide: ${mo}`);
  return { year: y, month: mo };
}

async function syncShopifyMonthlySales({ fromYearMonth, toYearMonth, onProgress } = {}) {
  const logId = stockDb.startSync('shopify_sales');
  try {
    if (!fromYearMonth || !toYearMonth) throw new Error('fromYearMonth et toYearMonth requis (YYYY-MM)');
    const from = parseYearMonth(fromYearMonth);
    const to = parseYearMonth(toYearMonth);
    // Bounds: inclusive on the first of `from` month, exclusive on the first of the month AFTER `to`.
    const startISO = new Date(Date.UTC(from.year, from.month - 1, 1)).toISOString();
    const endISO = new Date(Date.UTC(to.year, to.month, 1)).toISOString();

    // aggregates[sku][ym] = net units
    const aggregates = {};
    let pages = 0, ordersSeen = 0, ordersKept = 0, lineItemsProcessed = 0;

    const startUrl = `${shopifyBase()}/orders.json?` + new URLSearchParams({
      created_at_min: startISO,
      created_at_max: endISO,
      status: 'any',
      limit: '250',
      fields: 'id,name,created_at,source_name,line_items,refunds,financial_status',
    }).toString();

    for await (const data of paginate(startUrl)) {
      pages++;
      for (const order of data.orders || []) {
        ordersSeen++;
        if (!ALLOWED_SOURCES.has(order.source_name)) continue;
        ordersKept++;
        const dt = new Date(order.created_at);
        const y = dt.getUTCFullYear();
        const m = dt.getUTCMonth() + 1;
        const ym = `${y}-${String(m).padStart(2, '0')}`;
        // Refunded qty per line_item_id
        const refundedByLI = {};
        for (const ref of order.refunds || []) {
          for (const rli of ref.refund_line_items || []) {
            refundedByLI[rli.line_item_id] = (refundedByLI[rli.line_item_id] || 0) + (rli.quantity || 0);
          }
        }
        for (const li of order.line_items || []) {
          lineItemsProcessed++;
          if (!li.sku) continue;
          const sku = String(li.sku).trim();
          if (!sku) continue;
          const netQty = Math.max(0, (li.quantity || 0) - (refundedByLI[li.id] || 0));
          if (netQty <= 0) continue;
          if (!aggregates[sku]) aggregates[sku] = {};
          aggregates[sku][ym] = (aggregates[sku][ym] || 0) + netQty;
        }
      }
      if (onProgress) onProgress({ pages, ordersSeen, ordersKept });
    }

    // Upsert only SKUs present in referentiel_sku.
    const rows = [];
    let skusMatched = 0, skusUnmatched = 0;
    for (const [sku, byMonth] of Object.entries(aggregates)) {
      if (!stockDb.getReferentielSku(sku)) { skusUnmatched++; continue; }
      skusMatched++;
      for (const [ym, qty] of Object.entries(byMonth)) {
        const [yy, mm] = ym.split('-').map(Number);
        rows.push({ sku, annee: yy, mois: mm, ventes_reelles: qty, is_estimated: 0 });
      }
    }
    stockDb.upsertPrevisionsMensuellesBulk(rows);

    const stats = {
      range: `${fromYearMonth} → ${toYearMonth}`,
      pages, ordersSeen, ordersKept,
      lineItemsProcessed,
      skusMatched, skusUnmatched,
      previsionsUpserted: rows.length,
    };
    stockDb.finishSync(logId, { status: 'ok', message: JSON.stringify(stats) });
    return stats;
  } catch (err) {
    stockDb.finishSync(logId, { status: 'error', message: err.message });
    throw err;
  }
}

module.exports = {
  syncShopifyVariants,
  syncShopifyStock,
  syncShopifyMonthlySales,
  debugInventoryForSku,
  debugSourcesForSku,
  ALLOWED_SOURCES,
  API_VERSION,
};

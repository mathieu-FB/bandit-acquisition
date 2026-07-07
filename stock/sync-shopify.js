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
    let updated = 0, chunks = 0, unmatched = 0;
    const CHUNK = 50;
    for (let i = 0; i < referentiels.length; i += CHUNK) {
      const chunk = referentiels.slice(i, i + CHUNK);
      const ids = chunk.map(r => r.shopify_inventory_item_id).join(',');
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
        const skuRow = stockDb.getSkuByInventoryItemId(level.inventory_item_id);
        if (!skuRow) { unmatched++; continue; }
        upserts.push({
          sku: skuRow.sku,
          stock_dispo: level.available || 0,
          location_id: String(level.location_id),
          source: 'shopify',
        });
      }
      if (upserts.length) stockDb.upsertStockActuelBulk(upserts);
      updated += upserts.length;
      chunks++;
      if (onProgress) onProgress({ chunks, updated, unmatched });
    }
    const stats = { chunks, updated, unmatched, locationId };
    stockDb.finishSync(logId, { status: 'ok', message: JSON.stringify(stats) });
    return stats;
  } catch (err) {
    stockDb.finishSync(logId, { status: 'error', message: err.message });
    throw err;
  }
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
  ALLOWED_SOURCES,
  API_VERSION,
};

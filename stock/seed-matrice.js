// ============================================================
// SEED MATRICE — parse `Matrice produits vXXXX.xlsx` and load
// data into referentiel_sku + stock_actuel + previsions_mensuelles.
//
// Manual trigger only (POST /api/stock/seed-matrice). Dry-run is
// the default; pass ?dryRun=0 to actually write to DB.
// ============================================================

const path = require('path');
const cache = require('../cache');
const stockDb = require('./db');
const xlsxIo = require('./xlsx-io');

const DEFAULT_XLSX_FILENAME = 'Matrice produits v2026-01.xlsx';

// ------------------------------------------------------------
// Column indices (0-based) for the "Matrice" sheet.
// See docs/matrice-schema.md if this ever needs updating.
// ------------------------------------------------------------
const COL = {
  categorie: 0,           // A
  famille: 1,             // B
  animal: 2,              // C
  sku: 3,                 // D
  cip: 4,                 // E
  nom_court: 5,           // F  Designation 20
  nom_long: 9,            // J  Designation 40
  nom_en: 10,             // K  Designation EN 40
  motif: 14,              // O
  taille: 15,             // P
  ean_13: 19,             // T
  ean_12: 20,             // U
  url_produit: 21,        // V
  pa_dernier: 22,         // W  DERNIER PRU
  pa_vs: 23,              // X  PA VS
  pvc_ttc: 24,            // Y  PVC (TTC)
  pays_fabrication: 26,   // AA
  hs_code_10: 27,         // AB
  hs_code_6: 28,          // AC
  poids_brut: 29,         // AD
  poids_net: 30,          // AE
  longueur: 31,           // AF
  largeur: 32,            // AG
  hauteur: 33,            // AH
  // 38..49 = images (skip; enrichissement Shopify)
  lead_time_jours: 55,    // BD  Lead time (jours)
  couverture_visee_jours: 57, // BF Couverture stock visée
  matrice_bg_ref: 58,     // BG  Commande à passer (#)
  matrice_bh_ref: 59,     // BH  Commande à passer (€)
};

const STOCK_COL = { sku: 0, available: 3 };
const VENTES_COL = { skuOrCip: 0, qty: 1 };

// ------------------------------------------------------------
// Parsers — pure, no DB writes.
// ------------------------------------------------------------

function parseMatrice(rows) {
  // rows[0] = header; data starts at rows[1].
  const referentiel = [];
  const skipped = [];
  const cipToSku = {}; // for VENTES 2025 fallback matching

  for (let i = 1; i < rows.length; i++) {
    const r = rows[i];
    if (!r) continue;
    const sku = xlsxIo.toStr(r[COL.sku]);
    if (!sku) {
      // Empty row or footer — skip silently
      continue;
    }
    const cip = xlsxIo.toStr(r[COL.cip]);
    const parsed = {
      sku,
      categorie: xlsxIo.toStr(r[COL.categorie]),
      famille: xlsxIo.toStr(r[COL.famille]),
      animal: xlsxIo.toStr(r[COL.animal]),
      cip,
      nom_court: xlsxIo.toStr(r[COL.nom_court]),
      nom_long: xlsxIo.toStr(r[COL.nom_long]),
      nom_en: xlsxIo.toStr(r[COL.nom_en]),
      motif: xlsxIo.toStr(r[COL.motif]),
      taille: xlsxIo.toStr(r[COL.taille]),
      ean_13: xlsxIo.toStr(r[COL.ean_13]),
      ean_12: xlsxIo.toStr(r[COL.ean_12]),
      url_produit: xlsxIo.toStr(r[COL.url_produit]),
      pa_dernier: xlsxIo.toNumber(r[COL.pa_dernier]),
      pa_vs: xlsxIo.toNumber(r[COL.pa_vs]),
      pvc_ttc: xlsxIo.toNumber(r[COL.pvc_ttc]),
      pays_fabrication: xlsxIo.toStr(r[COL.pays_fabrication]),
      hs_code_10: xlsxIo.toStr(r[COL.hs_code_10]),
      hs_code_6: xlsxIo.toStr(r[COL.hs_code_6]),
      poids_brut: xlsxIo.toNumber(r[COL.poids_brut]),
      poids_net: xlsxIo.toNumber(r[COL.poids_net]),
      longueur: xlsxIo.toNumber(r[COL.longueur]),
      largeur: xlsxIo.toNumber(r[COL.largeur]),
      hauteur: xlsxIo.toNumber(r[COL.hauteur]),
      lead_time_jours: xlsxIo.toInt(r[COL.lead_time_jours]),
      couverture_visee_jours: xlsxIo.toInt(r[COL.couverture_visee_jours]),
      couverture_visee_source: xlsxIo.toInt(r[COL.couverture_visee_jours]) != null ? 'matrice' : null,
      matrice_bg_ref: xlsxIo.toInt(r[COL.matrice_bg_ref]),
      matrice_bh_ref: xlsxIo.toNumber(r[COL.matrice_bh_ref]),
    };
    // Detect duplicates
    if (referentiel.find(x => x.sku === sku)) {
      skipped.push({ sku, row: i + 1, reason: 'SKU dupliqué dans la Matrice' });
      continue;
    }
    referentiel.push(parsed);
    if (cip) cipToSku[cip] = sku;
  }
  return { referentiel, skipped, cipToSku };
}

function parseStock(rows) {
  const items = [];
  const skipped = [];
  for (let i = 1; i < rows.length; i++) {
    const r = rows[i];
    if (!r) continue;
    const sku = xlsxIo.toStr(r[STOCK_COL.sku]);
    if (!sku) continue;
    const stock_dispo = xlsxIo.toInt(r[STOCK_COL.available]);
    if (stock_dispo == null) {
      skipped.push({ sku, row: i + 1, reason: 'stock_dispo manquant' });
      continue;
    }
    items.push({ sku, stock_dispo });
  }
  return { items, skipped };
}

// Ventes 2025: annual total per SKU or CIP → spread evenly across 12 months (marked is_estimated=1).
// The real monthly breakdown will come from the Shopify orders sync (step 1c).
function parseVentesAnnuelles(rows, cipToSku) {
  const items = [];
  const skipped = [];
  const YEAR = 2025;
  for (let i = 1; i < rows.length; i++) {
    const r = rows[i];
    if (!r) continue;
    const raw = xlsxIo.toStr(r[VENTES_COL.skuOrCip]);
    if (!raw) continue;
    const qty = xlsxIo.toInt(r[VENTES_COL.qty]);
    if (qty == null || qty <= 0) continue;

    // Match logic:
    // 1. If raw is a purely numeric CIP → look up cipToSku
    // 2. Else assume it's a SKU
    let sku = null;
    const isPureCip = /^\d+$/.test(raw);
    if (isPureCip) {
      sku = cipToSku[raw] || null;
      if (!sku) {
        skipped.push({ key: raw, row: i + 1, reason: 'CIP sans correspondance SKU' });
        continue;
      }
    } else {
      sku = raw;
    }

    // Spread evenly. Remainder goes into December to keep the annual total intact.
    const base = Math.floor(qty / 12);
    const remainder = qty - base * 12;
    for (let m = 1; m <= 12; m++) {
      const monthQty = m === 12 ? base + remainder : base;
      items.push({ sku, annee: YEAR, mois: m, ventes_reelles: monthQty, is_estimated: 1 });
    }
  }
  return { items, skipped };
}

// ------------------------------------------------------------
// Orchestrator
// ------------------------------------------------------------

function resolveXlsxPath(customPath) {
  if (customPath) return path.isAbsolute(customPath) ? customPath : path.join(cache.getDataDir(), customPath);
  return path.join(cache.getDataDir(), 'seed', DEFAULT_XLSX_FILENAME);
}

function runSeed({ dryRun = true, xlsxPath = null } = {}) {
  const t0 = Date.now();
  const resolvedPath = resolveXlsxPath(xlsxPath);
  const report = {
    xlsxPath: resolvedPath,
    dryRun,
    startedAt: new Date(t0).toISOString(),
    sheets: {},
    warnings: [],
    errors: [],
    stats: {},
    durationMs: 0,
  };

  let logId = null;
  if (!dryRun) logId = stockDb.startSync('seed_matrice');

  try {
    // 1. Matrice sheet → referentiel_sku (INDISPENSABLE)
    const matriceRows = xlsxIo.readSheetRaw(resolvedPath, 'Matrice');
    const matrice = parseMatrice(matriceRows);
    report.sheets.matrice = {
      rowsSeen: matriceRows.length - 1,
      referentielCount: matrice.referentiel.length,
      skipped: matrice.skipped.length,
    };
    if (matrice.skipped.length) {
      report.warnings.push(...matrice.skipped.slice(0, 20).map(s => `[Matrice] Ligne ${s.row} SKU=${s.sku}: ${s.reason}`));
      if (matrice.skipped.length > 20) report.warnings.push(`[Matrice] +${matrice.skipped.length - 20} autres warnings tronqués`);
    }

    // 2. STOCK sheet → stock_actuel (OPTIONNELLE — écrasée par sync Shopify)
    let stockRows = null;
    let stock = { items: [], skipped: [] };
    try {
      stockRows = xlsxIo.readSheetRaw(resolvedPath, 'STOCK');
      stock = parseStock(stockRows);
      report.sheets.stock = {
        rowsSeen: stockRows.length - 1,
        stockCount: stock.items.length,
        skipped: stock.skipped.length,
      };
    } catch (err) {
      report.sheets.stock = { skipped: true, reason: 'feuille absente' };
      report.warnings.push('[STOCK] Feuille absente — ignorée (le stock sera récupéré via sync Shopify)');
    }

    // 3. VENTES 2025 sheet → previsions_mensuelles (OPTIONNELLE — écrasée par sync Shopify sales)
    let ventesRows = null;
    let ventes = { items: [], skipped: [] };
    try {
      ventesRows = xlsxIo.readSheetRaw(resolvedPath, 'VENTES 2025');
      ventes = parseVentesAnnuelles(ventesRows, matrice.cipToSku);
      report.sheets.ventes = {
        rowsSeen: ventesRows.length - 1,
        previsionsCount: ventes.items.length,
        skusMatched: new Set(ventes.items.map(i => i.sku)).size,
        skipped: ventes.skipped.length,
      };
      if (ventes.skipped.length) {
        report.warnings.push(...ventes.skipped.slice(0, 20).map(s => `[VENTES 2025] Ligne ${s.row} clé=${s.key}: ${s.reason}`));
        if (ventes.skipped.length > 20) report.warnings.push(`[VENTES 2025] +${ventes.skipped.length - 20} autres warnings tronqués`);
      }
    } catch (err) {
      report.sheets.ventes = { skipped: true, reason: 'feuille absente' };
      report.warnings.push('[VENTES 2025] Feuille absente — ignorée (les ventes historiques seront récupérées via sync Shopify sales)');
    }

    // 4. Apply — only if !dryRun
    if (!dryRun) {
      const nRef = stockDb.upsertReferentielSKUBulk(matrice.referentiel);
      // Filter stock rows to those with a matching SKU in the referentiel
      const skuSet = new Set(matrice.referentiel.map(r => r.sku));
      const stockFiltered = stock.items.filter(s => skuSet.has(s.sku));
      const stockDropped = stock.items.length - stockFiltered.length;
      const nStock = stockDb.upsertStockActuelBulk(stockFiltered.map(s => ({ ...s, source: 'seed_xlsx' })));
      if (stockDropped > 0) report.warnings.push(`[STOCK] ${stockDropped} SKU dans la feuille STOCK sans correspondance dans le Référentiel — ignorés`);

      // Filter previsions to SKUs present in referentiel
      const previsionsFiltered = ventes.items.filter(v => skuSet.has(v.sku));
      const previsionsDropped = ventes.items.length - previsionsFiltered.length;
      const nPrev = stockDb.upsertPrevisionsMensuellesBulk(previsionsFiltered);
      if (previsionsDropped > 0) report.warnings.push(`[VENTES 2025] ${previsionsDropped} lignes prévisionnelles hors Référentiel — ignorées`);

      report.stats = {
        referentielUpserts: nRef,
        stockUpserts: nStock,
        previsionsUpserts: nPrev,
      };
    } else {
      // Dry-run stats mirror what would happen
      const skuSet = new Set(matrice.referentiel.map(r => r.sku));
      const stockKept = stock.items.filter(s => skuSet.has(s.sku)).length;
      const prevKept = ventes.items.filter(v => skuSet.has(v.sku)).length;
      report.stats = {
        referentielUpserts: matrice.referentiel.length,
        stockUpserts: stockKept,
        stockDroppedNotInReferentiel: stock.items.length - stockKept,
        previsionsUpserts: prevKept,
        previsionsDroppedNotInReferentiel: ventes.items.length - prevKept,
      };
    }

    report.durationMs = Date.now() - t0;
    if (!dryRun) stockDb.finishSync(logId, { status: 'ok', message: JSON.stringify(report.stats) });
    return report;
  } catch (err) {
    report.errors.push(err.message);
    report.durationMs = Date.now() - t0;
    if (!dryRun && logId != null) stockDb.finishSync(logId, { status: 'error', message: err.message });
    throw Object.assign(err, { report });
  }
}

module.exports = {
  runSeed,
  parseMatrice,
  parseStock,
  parseVentesAnnuelles,
  resolveXlsxPath,
  DEFAULT_XLSX_FILENAME,
};

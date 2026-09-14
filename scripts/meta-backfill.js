#!/usr/bin/env node
// scripts/meta-backfill.js
//
// CLI wrapper autour de metaSync.backfill(). Usage :
//   npm run meta:backfill -- --start=2026-03-18 --end=2026-09-13
//   node scripts/meta-backfill.js --start=2026-03-18 --end=yesterday
//
// end=yesterday|today acceptés comme raccourcis.
//
// Écrit dans meta_ad_daily + meta_creative (voir cache.js SCHEMA_V2).
// N'écrase pas — les upserts sont idempotents via PK (day, ad_id) et
// creative_id. Sûr de relancer sur un range déjà partiellement backfilé.

require('dotenv').config();

const cache = require('../cache');
const metaSync = require('../meta-sync');

function parseArgs(argv) {
  const out = {};
  for (const a of argv.slice(2)) {
    const m = a.match(/^--([a-zA-Z_][a-zA-Z0-9_-]*)=(.*)$/);
    if (m) out[m[1]] = m[2];
  }
  return out;
}

function resolveDate(v) {
  if (!v) return null;
  const s = String(v).trim().toLowerCase();
  const today = new Date();
  if (s === 'today') return formatDate(today);
  if (s === 'yesterday') {
    const y = new Date(today);
    y.setDate(y.getDate() - 1);
    return formatDate(y);
  }
  if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return s;
  throw new Error(`Date invalide: "${v}" — attendu YYYY-MM-DD, "yesterday" ou "today"`);
}

function formatDate(d) {
  const yyyy = d.getFullYear();
  const mm = String(d.getMonth() + 1).padStart(2, '0');
  const dd = String(d.getDate()).padStart(2, '0');
  return `${yyyy}-${mm}-${dd}`;
}

async function main() {
  const args = parseArgs(process.argv);
  const start = resolveDate(args.start);
  const end = resolveDate(args.end);
  if (!start || !end) {
    console.error('Usage: node scripts/meta-backfill.js --start=YYYY-MM-DD --end=YYYY-MM-DD');
    console.error('       (end accepte "yesterday" ou "today")');
    process.exit(1);
  }

  if (!process.env.META_ACCESS_TOKEN || !process.env.META_AD_ACCOUNT_ID) {
    console.error('META_ACCESS_TOKEN / META_AD_ACCOUNT_ID non configurés (.env).');
    process.exit(1);
  }

  cache.init();

  console.log(`\n=== Meta backfill ${start} → ${end} ===\n`);
  const t0 = Date.now();
  const stats = await metaSync.backfill(start, end);
  const durationMin = ((Date.now() - t0) / 60000).toFixed(1);

  console.log('\n=== Récap ===');
  console.log(`Range              : ${stats.range.start} → ${stats.range.end}`);
  console.log(`Jours tentés       : ${stats.daysAttempted}`);
  console.log(`Jours OK           : ${stats.daysDone}`);
  console.log(`Jours en erreur    : ${stats.errors.length}`);
  console.log(`Rows insights total: ${stats.totalRows}`);
  console.log(`Ads upsertés       : ${stats.totalAds}`);
  console.log(`Creatives fetched  : ${stats.totalCreativesFetched}`);
  console.log(`Creatives cachés   : ${stats.totalCreativesCached}`);
  console.log(`Durée              : ${durationMin} min`);
  if (stats.errors.length > 0) {
    console.log(`\nErreurs par jour :`);
    for (const e of stats.errors) console.log(`  - ${e.day} : ${e.error}`);
  }

  // Validation croisée : spend total meta_ad_daily vs daily_metrics.meta_json
  console.log('\n=== Validation croisée spend ===');
  const spendAd = cache.getMetaAdDailySpendSum(start, end);
  // Calcul spend depuis daily_metrics.meta_json
  const db = cache.getDb();
  const dailyRows = db.prepare(`SELECT meta_json FROM daily_metrics WHERE day >= ? AND day <= ?`).all(start, end);
  let spendDaily = 0;
  for (const r of dailyRows) {
    try { const m = JSON.parse(r.meta_json || '{}'); spendDaily += Number(m.spend || 0); } catch {}
  }
  const diff = spendAd - spendDaily;
  const diffPct = spendDaily > 0 ? (diff / spendDaily) * 100 : 0;
  console.log(`Spend meta_ad_daily : ${spendAd.toFixed(2)} €`);
  console.log(`Spend daily_metrics : ${spendDaily.toFixed(2)} €`);
  console.log(`Écart               : ${diff.toFixed(2)} € (${diffPct.toFixed(2)} %)`);
  const withinTolerance = Math.abs(diffPct) <= 2;
  console.log(`Tolérance ±2 %      : ${withinTolerance ? 'OK ✓' : 'HORS TOLÉRANCE ✗'}`);

  cache.close();
  process.exit(stats.errors.length > 0 ? 1 : 0);
}

main().catch(err => {
  console.error('[meta-backfill] fatal:', err.stack || err.message);
  process.exit(2);
});

// ============================================================
// META AD-LEVEL SYNC
//
// Persistance ad-level Meta indépendante de daily_metrics (account-level).
// Écrit dans meta_ad_daily + meta_creative (SCHEMA_V2 dans cache.js).
//
// Attribution : action_attribution_windows explicite = ['7d_click','1d_view']
// pour reproducibilité — ne dépend plus des défauts du compte.
//
// Rate limit : backoff exponentiel sur error codes Meta 17, 32, 613
// (voir handleMetaError). Retry jusqu'à 5 fois, delay 2s → 60s.
//
// PAS de creative_id refetch : si meta_creative existe déjà et a un
// thumbnail_url, on ne rappelle pas le creative endpoint (économie de
// quota + latence backfill).
// ============================================================

const fetch = require('node-fetch');
const cache = require('./cache');

const API_VERSION = 'v19.0';
const ATTRIBUTION_WINDOWS = ['7d_click', '1d_view'];
const MAX_RETRIES = 5;
const BACKOFF_BASE_MS = 2000;
const BACKOFF_MAX_MS = 60000;
const PAGE_LIMIT = 500;

// Meta error codes qui justifient un backoff/retry.
// 17  : User request limit reached
// 32  : Page-level throttling
// 613 : Custom throttling
const RATE_LIMIT_CODES = new Set([17, 32, 613]);

function graphBase() {
  return `https://graph.facebook.com/${API_VERSION}`;
}

function requireEnv() {
  const token = process.env.META_ACCESS_TOKEN;
  const accountId = process.env.META_AD_ACCOUNT_ID;
  if (!token || !accountId) {
    throw new Error('META_ACCESS_TOKEN / META_AD_ACCOUNT_ID non configurés');
  }
  return { token, accountId };
}

// ------------------------------------------------------------
// HTTP + backoff
// ------------------------------------------------------------

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

/**
 * Fetch avec retry/backoff sur rate limit Meta.
 * Backoff exponentiel plafonné à BACKOFF_MAX_MS. Respect optionnel de
 * Retry-After si présent.
 */
async function fetchWithBackoff(url, opts = {}) {
  let lastErr = null;
  for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
    const res = await fetch(url, opts);
    if (res.ok) return res.json();
    const status = res.status;
    let body = null;
    try { body = await res.json(); } catch {
      // Not JSON — non-recoverable
      throw new Error(`Meta ${status}: ${await res.text().catch(() => '?')}`);
    }
    const err = body.error || {};
    const code = err.code;
    lastErr = new Error(`Meta ${status} code=${code} type=${err.type || '?'} msg=${err.message || '?'}`);

    const retryable = status === 429
      || status === 500
      || status === 503
      || (typeof code === 'number' && RATE_LIMIT_CODES.has(code));
    if (!retryable || attempt === MAX_RETRIES) throw lastErr;

    // Respect Retry-After si présent, sinon backoff expo.
    const retryAfterHeader = res.headers.get('retry-after');
    let delay;
    if (retryAfterHeader && /^\d+$/.test(retryAfterHeader)) {
      delay = Math.min(Number(retryAfterHeader) * 1000, BACKOFF_MAX_MS);
    } else {
      delay = Math.min(BACKOFF_BASE_MS * Math.pow(2, attempt), BACKOFF_MAX_MS);
      // jitter ±20%
      delay = Math.round(delay * (0.8 + Math.random() * 0.4));
    }
    console.warn(`[meta-sync] rate-limit / retryable (code=${code}, status=${status}) — backoff ${delay}ms (attempt ${attempt + 1}/${MAX_RETRIES})`);
    await sleep(delay);
  }
  throw lastErr || new Error('Meta: retries exhausted');
}

// ------------------------------------------------------------
// Parsing helpers (dérive les métriques d'une row insights)
//
// Sémantique identique à parseMetaInsightRow() de server.js, avec en plus :
//  - video_p25 depuis video_p25_watched_actions
//  - video_views_3s préfère video_3_sec_watched_actions (dedicated) puis
//    fallback sur actions[video_view] (comme server.js)
// ------------------------------------------------------------

function findActionValue(list, actionTypes) {
  if (!Array.isArray(list)) return 0;
  for (const type of actionTypes) {
    const a = list.find(x => x.action_type === type);
    if (a) return parseInt(a.value || 0);
  }
  return 0;
}

function findActionValueFloat(list, actionTypes) {
  if (!Array.isArray(list)) return 0;
  for (const type of actionTypes) {
    const a = list.find(x => x.action_type === type);
    if (a) return parseFloat(a.value || 0);
  }
  return 0;
}

function parseAdInsightRow(row) {
  const spend = parseFloat(row.spend || 0);
  const impressions = parseInt(row.impressions || 0);
  const clicks = parseInt(row.clicks || 0);
  const reach = parseInt(row.reach || 0);
  const frequency = parseFloat(row.frequency || 0);

  const purchases = findActionValue(row.actions, ['purchase', 'omni_purchase']);
  const linkClicks = findActionValue(row.actions, ['link_click']);
  const revenue = findActionValueFloat(row.action_values, ['purchase', 'omni_purchase']);

  // ThruPlays : champ dédié video_thruplay_watched_actions
  const thruplays = findActionValue(row.video_thruplay_watched_actions, ['video_view']);

  // video_views_3s : préférer video_3_sec_watched_actions puis fallback actions[video_view]
  let video_views_3s = findActionValue(row.video_3_sec_watched_actions, ['video_view']);
  if (video_views_3s === 0) {
    video_views_3s = findActionValue(row.actions, ['video_view']);
  }

  const video_p25 = findActionValue(row.video_p25_watched_actions, ['video_view']);

  return {
    spend, impressions, clicks, reach, frequency,
    purchases, revenue, link_clicks: linkClicks,
    video_views_3s, thruplays, video_p25,
  };
}

// ------------------------------------------------------------
// Fetch insights (level=ad, time_increment=1) avec pagination complète
// ------------------------------------------------------------

async function fetchAdDailyInsights(start, end) {
  const { token, accountId } = requireEnv();
  const fields = [
    'ad_id', 'ad_name',
    'adset_id', 'adset_name',
    'campaign_id', 'campaign_name',
    'spend', 'impressions', 'clicks',
    'actions', 'action_values',
    'reach', 'frequency',
    'video_3_sec_watched_actions',
    'video_thruplay_watched_actions',
    'video_p25_watched_actions',
  ].join(',');

  const params = new URLSearchParams({
    access_token: token,
    fields,
    time_range: JSON.stringify({ since: start, until: end }),
    time_increment: '1',
    level: 'ad',
    limit: String(PAGE_LIMIT),
    action_attribution_windows: JSON.stringify(ATTRIBUTION_WINDOWS),
  });

  let url = `${graphBase()}/${accountId}/insights?${params.toString()}`;
  const rows = [];
  let pages = 0;
  while (url) {
    pages++;
    const data = await fetchWithBackoff(url);
    if (Array.isArray(data.data)) rows.push(...data.data);
    url = data.paging && data.paging.next ? data.paging.next : null;
  }
  return { rows, pages };
}

// ------------------------------------------------------------
// Creative fetch — mirrors server.js:fetchAdCreative() 4-fallback logic
// avec en plus : image_hash, video_id, object_type
// ------------------------------------------------------------

/**
 * Récupère le creative d'un ad et upsert meta_creative.
 * Skip le fetch réseau si meta_creative existe déjà ET a thumbnail_url.
 * Retourne { creative_id, cached } — creative_id peut être null si l'ad
 * n'a pas de creative accessible (rare).
 */
async function upsertCreativeForAd(adId, adDay) {
  const { token } = requireEnv();

  // On ne peut pas skip avant de connaître creative_id — on va d'abord
  // récupérer le creative_id via ad → creative{id} (1 hop léger).
  // Ensuite check DB : si présent et thumbnail_url non vide, skip.
  //
  // NOTE : optimisation possible ultérieure = maintenir un ad_id → creative_id
  // map côté DB (nouvelle table meta_ad_to_creative). Pour l'instant on paie
  // le 1 hop par ad car parseAdInsightRow n'a pas le creative_id.

  const adFields = 'creative{id,image_hash,video_id,object_type,title,body,image_url,object_story_spec}';
  const url = `${graphBase()}/${adId}?fields=${encodeURIComponent(adFields + ',preview_shareable_link')}&access_token=${token}`;

  let json;
  try {
    json = await fetchWithBackoff(url);
  } catch (e) {
    console.warn(`[meta-sync] creative fetch failed for ad ${adId}: ${e.message}`);
    return { creative_id: null, cached: false };
  }
  const creative = json.creative || {};
  const creativeId = creative.id ? String(creative.id) : null;
  if (!creativeId) return { creative_id: null, cached: false };

  // Skip si déjà en cache avec un thumbnail présent
  const existing = cache.getMetaCreative(creativeId);
  if (existing && existing.thumbnail_url) {
    // Mise à jour légère first_seen/last_seen si adDay antérieur/postérieur
    cache.upsertMetaCreative({
      creative_id: creativeId,
      first_seen: adDay,
      last_seen: adDay,
    });
    return { creative_id: creativeId, cached: true };
  }

  const spec = creative.object_story_spec || {};
  let imageUrl = null;
  // 1. object_story_spec sources (bonne qualité)
  if (spec.video_data && spec.video_data.image_url) imageUrl = spec.video_data.image_url;
  if (!imageUrl && spec.link_data && spec.link_data.picture) imageUrl = spec.link_data.picture;
  // 2. Creative-level image_url
  if (!imageUrl && creative.image_url) imageUrl = creative.image_url;
  // 3. Thumbnail endpoint fallback (creative endpoint direct)
  if (!imageUrl) {
    try {
      const thumbUrl = `${graphBase()}/${creativeId}?fields=thumbnail_url&thumbnail_width=600&thumbnail_height=600&access_token=${token}`;
      const thumbJson = await fetchWithBackoff(thumbUrl);
      if (thumbJson && thumbJson.thumbnail_url) imageUrl = thumbJson.thumbnail_url;
    } catch (e) {
      // silencieux — on stocke sans thumbnail_url
    }
  }

  cache.upsertMetaCreative({
    creative_id: creativeId,
    image_hash: creative.image_hash || null,
    video_id: creative.video_id ? String(creative.video_id) : null,
    thumbnail_url: imageUrl || null,
    image_url: imageUrl || null,
    title: creative.title || null,
    body: creative.body || null,
    object_type: creative.object_type || null,
    first_seen: adDay,
    last_seen: adDay,
  });
  return { creative_id: creativeId, cached: false };
}

// ------------------------------------------------------------
// syncDay / backfill
// ------------------------------------------------------------

/**
 * Sync un seul jour : fetch ad-level insights + creatives + upsert.
 * Retourne stats { day, rowsFetched, adsUpserted, creativesFetched,
 *   creativesCached, errors }.
 */
async function syncDay(day) {
  if (!day || !/^\d{4}-\d{2}-\d{2}$/.test(day)) {
    throw new Error(`syncDay: format YYYY-MM-DD attendu, reçu "${day}"`);
  }
  const startedAt = Date.now();
  const { rows, pages } = await fetchAdDailyInsights(day, day);

  // Prépare les rows meta_ad_daily. On mappe les IDs / labels + métriques
  // parsées. creative_id est ajouté après le call creative endpoint.
  const adRowsByAdId = new Map();
  for (const row of rows) {
    const parsed = parseAdInsightRow(row);
    const rec = {
      day,
      ad_id: row.ad_id,
      adset_id: row.adset_id || null,
      campaign_id: row.campaign_id || null,
      creative_id: null, // rempli après
      ad_name: row.ad_name || null,
      adset_name: row.adset_name || null,
      campaign_name: row.campaign_name || null,
      ...parsed,
    };
    adRowsByAdId.set(row.ad_id, rec);
  }

  // Fetch creatives pour chaque ad. On paie 1 hop / ad (avec skip si déjà en
  // cache et thumbnail_url présent). Sériel pour rester poli avec Meta.
  let creativesFetched = 0, creativesCached = 0, creativeErrors = 0;
  for (const adId of adRowsByAdId.keys()) {
    try {
      const { creative_id, cached } = await upsertCreativeForAd(adId, day);
      if (creative_id) {
        adRowsByAdId.get(adId).creative_id = creative_id;
      }
      if (cached) creativesCached++; else creativesFetched++;
    } catch (e) {
      creativeErrors++;
      console.warn(`[meta-sync] upsertCreativeForAd(${adId}) failed: ${e.message}`);
    }
  }

  const adRows = Array.from(adRowsByAdId.values());
  const upserted = cache.upsertMetaAdDailyBulk(adRows);
  const durationMs = Date.now() - startedAt;

  const stats = {
    day,
    pages,
    rowsFetched: rows.length,
    adsUpserted: upserted,
    creativesFetched,
    creativesCached,
    creativeErrors,
    durationMs,
  };
  console.log(`[meta-sync] syncDay(${day}) done — ${stats.rowsFetched} rows, ${stats.adsUpserted} ads, ${stats.creativesFetched} new creatives, ${stats.creativesCached} cached, ${stats.creativeErrors} errors (${(durationMs / 1000).toFixed(1)}s)`);
  return stats;
}

/**
 * Backfill sur une plage [start, end] inclusive. Sériel jour par jour pour
 * mieux respecter le rate limit + pouvoir reprendre en cas de crash.
 * L'erreur sur un jour n'interrompt PAS le backfill — collectée dans errors[].
 */
async function backfill(start, end) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(start) || !/^\d{4}-\d{2}-\d{2}$/.test(end)) {
    throw new Error('backfill: format YYYY-MM-DD attendu pour start et end');
  }
  const s = new Date(start + 'T00:00:00Z');
  const e = new Date(end + 'T00:00:00Z');
  if (e < s) throw new Error('backfill: end < start');

  const startedAt = Date.now();
  const daysDone = [];
  const errors = [];
  let totalRows = 0, totalAds = 0, totalCreativesFetched = 0, totalCreativesCached = 0;

  const cursor = new Date(s);
  while (cursor <= e) {
    const y = cursor.getUTCFullYear();
    const m = String(cursor.getUTCMonth() + 1).padStart(2, '0');
    const d = String(cursor.getUTCDate()).padStart(2, '0');
    const day = `${y}-${m}-${d}`;
    try {
      const st = await syncDay(day);
      daysDone.push(day);
      totalRows += st.rowsFetched;
      totalAds += st.adsUpserted;
      totalCreativesFetched += st.creativesFetched;
      totalCreativesCached += st.creativesCached;
    } catch (e2) {
      errors.push({ day, error: e2.message });
      console.error(`[meta-sync] syncDay(${day}) failed: ${e2.message}`);
    }
    cursor.setUTCDate(cursor.getUTCDate() + 1);
  }
  const durationMs = Date.now() - startedAt;
  return {
    range: { start, end },
    daysAttempted: daysDone.length + errors.length,
    daysDone: daysDone.length,
    totalRows, totalAds,
    totalCreativesFetched, totalCreativesCached,
    errors,
    durationMs,
  };
}

module.exports = {
  fetchAdDailyInsights,
  upsertCreativeForAd,
  syncDay,
  backfill,
  parseAdInsightRow, // exposé pour tests
  ATTRIBUTION_WINDOWS,
};

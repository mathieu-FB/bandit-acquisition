// ============================================================
// DAILY REPORT — Email quotidien d'acquisition
// ============================================================

const fetch = require('node-fetch');
const sgMail = require('@sendgrid/mail');
const Anthropic = require('@anthropic-ai/sdk').default;

// ============================================================
// CONFIG
// ============================================================

const ALLOWED_SOURCES = new Set([
  'web', 'JUST', '295841693697',
  'subscription_contract', 'subscription_contract_checkout_one',
]);

function formatDate(d) {
  return d.toISOString().split('T')[0];
}

// ============================================================
// SHOPIFY — fetch + compute HT net sales
// ============================================================

async function fetchShopifyOrders(start, end) {
  const store = process.env.SHOPIFY_STORE_URL;
  const token = process.env.SHOPIFY_ACCESS_TOKEN;
  if (!store || !token) return [];

  const orders = [];
  let url = `https://${store}/admin/api/2024-01/orders.json?` +
    new URLSearchParams({
      created_at_min: `${start}T00:00:00+00:00`,
      created_at_max: `${end}T23:59:59+00:00`,
      status: 'any',
      limit: '250',
      fields: 'id,created_at,total_price,subtotal_price,total_discounts,total_tax,source_name,customer,financial_status,refunds',
    }).toString();

  while (url) {
    const res = await fetch(url, {
      headers: { 'X-Shopify-Access-Token': token, 'Content-Type': 'application/json' },
    });
    if (!res.ok) break;
    const data = await res.json();
    const filtered = (data.orders || []).filter(o => ALLOWED_SOURCES.has(o.source_name));
    orders.push(...filtered);

    const link = res.headers.get('link');
    if (link && link.includes('rel="next"')) {
      const match = link.match(/<([^>]+)>;\s*rel="next"/);
      url = match ? match[1] : null;
    } else {
      url = null;
    }
  }
  return orders;
}

function orderNetSalesHT(order) {
  const grossHT = parseFloat(order.subtotal_price || 0);
  let refundedHT = 0;
  if (order.refunds && order.refunds.length > 0) {
    order.refunds.forEach(refund => {
      if (refund.refund_line_items) {
        refund.refund_line_items.forEach(rli => {
          refundedHT += parseFloat(rli.subtotal || 0);
        });
      }
    });
  }
  return grossHT - refundedHT;
}

function computeShopifyStats(orders) {
  const valid = orders.filter(o => o.financial_status !== 'voided');
  const countable = valid.filter(o => o.financial_status !== 'refunded');

  const totalOrders = countable.length;
  const netSales = valid.reduce((sum, o) => sum + orderNetSalesHT(o), 0);
  const aov = totalOrders > 0 ? netSales / totalOrders : 0;

  return { totalOrders, netSales, aov };
}

// ============================================================
// META ADS — aggregate + best ad
// ============================================================

async function fetchMetaInsights(start, end) {
  const token = process.env.META_ACCESS_TOKEN;
  const accountId = process.env.META_AD_ACCOUNT_ID;
  if (!token || !accountId) return null;

  const fields = 'spend,impressions,clicks,cpm,actions,action_values';
  const url = `https://graph.facebook.com/v19.0/${accountId}/insights?` +
    new URLSearchParams({
      access_token: token,
      fields,
      time_range: JSON.stringify({ since: start, until: end }),
      time_increment: 1,
      level: 'account',
    }).toString();

  const res = await fetch(url);
  if (!res.ok) return null;
  const json = await res.json();
  return json.data || [];
}

async function fetchMetaBestAd(start, end) {
  const token = process.env.META_ACCESS_TOKEN;
  const accountId = process.env.META_AD_ACCOUNT_ID;
  if (!token || !accountId) return null;

  try {
    // Step 1: get ads with spend + purchases (light query — no action_values to avoid 500)
    const url1 = `https://graph.facebook.com/v19.0/${accountId}/insights?` +
      new URLSearchParams({
        access_token: token,
        fields: 'ad_id,ad_name,adset_name,campaign_name,spend,actions',
        time_range: JSON.stringify({ since: start, until: end }),
        level: 'ad',
        limit: '50',
      }).toString();

    const res1 = await fetch(url1);
    if (!res1.ok) return null;
    const json1 = await res1.json();
    if (json1.error) { console.error('Meta best ad error:', json1.error.message); return null; }

    const ads = (json1.data || []).map(ad => {
      const spend = parseFloat(ad.spend || 0);
      let purchases = 0;
      if (ad.actions) {
        const p = ad.actions.find(a => a.action_type === 'purchase' || a.action_type === 'omni_purchase');
        if (p) purchases = parseInt(p.value || 0);
      }
      return { ...ad, spendNum: spend, purchases };
    }).filter(a => a.spendNum >= 5);

    if (ads.length === 0) return null;

    // Step 2: get action_values for top 5 ads by spend (individual calls)
    const topAds = ads.sort((a, b) => b.spendNum - a.spendNum).slice(0, 5);

    let bestAd = null;
    let bestRoas = 0;

    for (const ad of topAds) {
      const url2 = `https://graph.facebook.com/v19.0/${accountId}/insights?` +
        new URLSearchParams({
          access_token: token,
          fields: 'action_values',
          time_range: JSON.stringify({ since: start, until: end }),
          level: 'ad',
          filtering: JSON.stringify([{ field: 'ad.id', operator: 'EQUAL', value: ad.ad_id }]),
        }).toString();

      let revenue = 0;
      try {
        const res2 = await fetch(url2);
        const json2 = await res2.json();
        if (json2.data && json2.data[0] && json2.data[0].action_values) {
          const r = json2.data[0].action_values.find(a => a.action_type === 'purchase' || a.action_type === 'omni_purchase');
          if (r) revenue = parseFloat(r.value || 0);
        }
      } catch (e) { /* skip revenue for this ad */ }

      const roas = ad.spendNum > 0 ? revenue / ad.spendNum : 0;
      if (roas > bestRoas) {
        bestRoas = roas;
        bestAd = {
          name: ad.ad_name,
          adset: ad.adset_name,
          campaign: ad.campaign_name,
          spend: ad.spendNum,
          revenue,
          purchases: ad.purchases,
          roas,
        };
      }
    }

    return bestAd;
  } catch (err) {
    console.error('Meta best ad error:', err.message);
    return null;
  }
}

function aggregateMeta(dailyData) {
  if (!dailyData || !dailyData.length) {
    return { spend: 0, impressions: 0, clicks: 0, purchases: 0, revenue: 0 };
  }

  let spend = 0, impressions = 0, clicks = 0, purchases = 0, revenue = 0;
  dailyData.forEach(row => {
    spend += parseFloat(row.spend || 0);
    impressions += parseInt(row.impressions || 0);
    clicks += parseInt(row.clicks || 0);
    if (row.actions) {
      const p = row.actions.find(a => a.action_type === 'purchase' || a.action_type === 'omni_purchase');
      if (p) purchases += parseInt(p.value || 0);
    }
    if (row.action_values) {
      const r = row.action_values.find(a => a.action_type === 'purchase' || a.action_type === 'omni_purchase');
      if (r) revenue += parseFloat(r.value || 0);
    }
  });

  return {
    spend, impressions, clicks, purchases, revenue,
    roas: spend > 0 ? revenue / spend : 0,
    cpm: impressions > 0 ? (spend / impressions) * 1000 : 0,
  };
}

// ============================================================
// GOOGLE ADS (via gRPC library)
// ============================================================

async function fetchGoogleStats(start, end) {
  try {
    const { GoogleAdsApi, fromMicros } = require('google-ads-api');
    const clientId = process.env.GOOGLE_ADS_CLIENT_ID;
    const clientSecret = process.env.GOOGLE_ADS_CLIENT_SECRET;
    const devToken = process.env.GOOGLE_ADS_DEVELOPER_TOKEN;
    const refreshToken = process.env.GOOGLE_ADS_REFRESH_TOKEN;
    const customerId = (process.env.GOOGLE_ADS_CUSTOMER_ID || '').replace(/-/g, '');
    if (!clientId || !clientSecret || !devToken || !refreshToken || !customerId) return null;

    const client = new GoogleAdsApi({ client_id: clientId, client_secret: clientSecret, developer_token: devToken });
    const customer = client.Customer({ customer_id: customerId, refresh_token: refreshToken });

    const results = await customer.query(`
      SELECT
        metrics.cost_micros,
        metrics.impressions,
        metrics.clicks,
        metrics.conversions,
        metrics.conversions_value
      FROM campaign
      WHERE segments.date BETWEEN '${start}' AND '${end}'
    `);

    let spend = 0, impressions = 0, clicks = 0, conversions = 0, revenue = 0;
    results.forEach(row => {
      spend += fromMicros(row.metrics?.cost_micros || 0);
      impressions += parseInt(row.metrics?.impressions || 0);
      clicks += parseInt(row.metrics?.clicks || 0);
      conversions += parseFloat(row.metrics?.conversions || 0);
      revenue += parseFloat(row.metrics?.conversions_value || 0);
    });

    return {
      spend, impressions, clicks, conversions, revenue,
      roas: spend > 0 ? revenue / spend : 0,
      cpm: impressions > 0 ? (spend / impressions) * 1000 : 0,
    };
  } catch (err) {
    console.error('Google Ads report error:', err.errors?.[0]?.message || err.message);
    return null;
  }
}

// ============================================================
// TIKTOK ADS
// ============================================================

async function fetchTikTokStats(start, end) {
  const token = process.env.TIKTOK_ACCESS_TOKEN;
  const advertiserId = process.env.TIKTOK_ADVERTISER_ID;
  if (!token || !advertiserId) return null;

  try {
    const url = 'https://business-api.tiktok.com/open_api/v1.3/report/integrated/get/?' +
      new URLSearchParams({
        advertiser_id: advertiserId,
        report_type: 'BASIC',
        data_level: 'AUCTION_ADVERTISER',
        dimensions: JSON.stringify(['stat_time_day']),
        metrics: JSON.stringify(['spend', 'impressions', 'clicks', 'complete_payment', 'value_per_complete_payment']),
        start_date: start,
        end_date: end,
      }).toString();

    const res = await fetch(url, { headers: { 'Access-Token': token } });
    if (!res.ok) return null;
    const json = await res.json();
    if (json.code !== 0) return null;

    let spend = 0, impressions = 0, clicks = 0, purchases = 0, revenue = 0;
    (json.data?.list || []).forEach(row => {
      const m = row.metrics || {};
      const s = parseFloat(m.spend || 0);
      const p = parseInt(m.complete_payment || 0);
      spend += s;
      impressions += parseInt(m.impressions || 0);
      clicks += parseInt(m.clicks || 0);
      purchases += p;
      revenue += p * parseFloat(m.value_per_complete_payment || 0);
    });

    return {
      spend, impressions, clicks, purchases, revenue,
      roas: spend > 0 ? revenue / spend : 0,
      cpm: impressions > 0 ? (spend / impressions) * 1000 : 0,
    };
  } catch (err) {
    console.error('TikTok report error:', err.message);
    return null;
  }
}

// ============================================================
// AMAZON — fetch sales metrics
// ============================================================

async function getAmazonAccessToken() {
  const clientId = process.env.AMAZON_SP_CLIENT_ID;
  const clientSecret = process.env.AMAZON_SP_CLIENT_SECRET;
  const refreshToken = process.env.AMAZON_SP_REFRESH_TOKEN;
  if (!clientId || !clientSecret || !refreshToken) return null;

  const res = await fetch('https://api.amazon.com/auth/o2/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({
      grant_type: 'refresh_token',
      refresh_token: refreshToken,
      client_id: clientId,
      client_secret: clientSecret,
    }).toString(),
  });
  if (!res.ok) return null;
  const json = await res.json();
  return json.access_token;
}

async function fetchAmazonStats(start, end) {
  const token = await getAmazonAccessToken();
  const marketplaceId = process.env.AMAZON_MARKETPLACE_ID || 'A13V1IB3VIYZZH';
  if (!token) return null;

  try {
    const url = `https://sellingpartnerapi-eu.amazon.com/sales/v1/orderMetrics?` +
      new URLSearchParams({
        marketplaceIds: marketplaceId,
        interval: `${start}T00:00:00+00:00--${end}T23:59:59+00:00`,
        granularity: 'Day',
      }).toString();

    const res = await fetch(url, {
      headers: { 'x-amz-access-token': token, 'Content-Type': 'application/json' },
    });
    if (!res.ok) return null;
    const json = await res.json();

    let ca = 0, orders = 0;
    (json.payload || []).forEach(day => {
      ca += parseFloat(day.totalSales?.amount || 0);
      orders += parseInt(day.unitCount || day.orderCount || 0);
    });

    return { ca, orders };
  } catch (err) {
    console.error('[Report] Amazon stats error:', err.message);
    return null;
  }
}

// ============================================================
// TIKTOK — best ad
// ============================================================

async function fetchTikTokBestAd(start, end) {
  const token = process.env.TIKTOK_ACCESS_TOKEN;
  const advertiserId = process.env.TIKTOK_ADVERTISER_ID;
  if (!token || !advertiserId) return null;

  try {
    const url = 'https://business-api.tiktok.com/open_api/v1.3/report/integrated/get/?' +
      new URLSearchParams({
        advertiser_id: advertiserId,
        report_type: 'BASIC',
        data_level: 'AUCTION_AD',
        dimensions: JSON.stringify(['ad_id']),
        metrics: JSON.stringify(['ad_name', 'campaign_name', 'adgroup_name', 'spend', 'impressions', 'clicks', 'complete_payment', 'value_per_complete_payment']),
        start_date: start,
        end_date: end,
        page: '1',
        page_size: '50',
        filtering: JSON.stringify([{ field_name: 'spend', filter_type: 'GREATER_THAN', filter_value: '5' }]),
      }).toString();

    const res = await fetch(url, { headers: { 'Access-Token': token } });
    if (!res.ok) return null;
    const json = await res.json();
    if (json.code !== 0) return null;

    const ads = (json.data?.list || []).map(row => {
      const m = row.metrics || {};
      const spend = parseFloat(m.spend || 0);
      const purchases = parseInt(m.complete_payment || 0);
      const revenue = purchases * parseFloat(m.value_per_complete_payment || 0);
      const roas = spend > 0 ? revenue / spend : 0;
      return { name: m.ad_name, campaign: m.campaign_name, adgroup: m.adgroup_name, spend, purchases, revenue, roas };
    }).filter(a => a.spend >= 5);

    if (ads.length === 0) return null;
    return ads.sort((a, b) => b.roas - a.roas)[0];
  } catch (err) {
    console.error('[Report] TikTok best ad error:', err.message);
    return null;
  }
}

// ============================================================
// COLLECT ALL DATA FOR REPORT
// ============================================================

async function collectReportData() {
  // Use Paris timezone to compute "yesterday" — the server runs in UTC,
  // but the cron fires at 00:01 Europe/Paris (= 22:01 UTC the day before).
  const nowParis = new Date(new Date().toLocaleString('en-US', { timeZone: 'Europe/Paris' }));
  const yesterday = new Date(nowParis);
  yesterday.setDate(yesterday.getDate() - 1);
  const dayBefore = new Date(yesterday);
  dayBefore.setDate(dayBefore.getDate() - 1);
  const lastWeekSameDay = new Date(yesterday);
  lastWeekSameDay.setDate(lastWeekSameDay.getDate() - 7);

  const yStr = formatDate(yesterday);
  const dbStr = formatDate(dayBefore);
  const lwStr = formatDate(lastWeekSameDay);

  console.log(`[Report] Collecting data for ${yStr} (vs ${dbStr} and ${lwStr})`);

  // Fetch everything in parallel
  const [
    shopifyYesterday, shopifyDayBefore, shopifyLastWeek,
    metaYesterday, metaDayBefore, metaLastWeek,
    bestAd, bestTiktokAd,
    googleYesterday, googleDayBefore, googleLastWeek,
    tiktokYesterday, tiktokDayBefore, tiktokLastWeek,
    amazonYesterday, amazonDayBefore, amazonLastWeek,
  ] = await Promise.all([
    fetchShopifyOrders(yStr, yStr),
    fetchShopifyOrders(dbStr, dbStr),
    fetchShopifyOrders(lwStr, lwStr),
    fetchMetaInsights(yStr, yStr),
    fetchMetaInsights(dbStr, dbStr),
    fetchMetaInsights(lwStr, lwStr),
    fetchMetaBestAd(yStr, yStr),
    fetchTikTokBestAd(yStr, yStr),
    fetchGoogleStats(yStr, yStr),
    fetchGoogleStats(dbStr, dbStr),
    fetchGoogleStats(lwStr, lwStr),
    fetchTikTokStats(yStr, yStr),
    fetchTikTokStats(dbStr, dbStr),
    fetchTikTokStats(lwStr, lwStr),
    fetchAmazonStats(yStr, yStr),
    fetchAmazonStats(dbStr, dbStr),
    fetchAmazonStats(lwStr, lwStr),
  ]);

  const shopify = {
    yesterday: computeShopifyStats(shopifyYesterday),
    dayBefore: computeShopifyStats(shopifyDayBefore),
    lastWeek: computeShopifyStats(shopifyLastWeek),
  };

  const meta = {
    yesterday: aggregateMeta(metaYesterday),
    dayBefore: aggregateMeta(metaDayBefore),
    lastWeek: aggregateMeta(metaLastWeek),
  };

  const google = {
    yesterday: googleYesterday || { spend: 0, impressions: 0, clicks: 0, conversions: 0, revenue: 0, roas: 0, cpm: 0 },
    dayBefore: googleDayBefore || { spend: 0, impressions: 0, clicks: 0, conversions: 0, revenue: 0, roas: 0, cpm: 0 },
    lastWeek: googleLastWeek || { spend: 0, impressions: 0, clicks: 0, conversions: 0, revenue: 0, roas: 0, cpm: 0 },
  };

  const tiktok = {
    yesterday: tiktokYesterday || { spend: 0, impressions: 0, clicks: 0, purchases: 0, revenue: 0, roas: 0, cpm: 0 },
    dayBefore: tiktokDayBefore || { spend: 0, impressions: 0, clicks: 0, purchases: 0, revenue: 0, roas: 0, cpm: 0 },
    lastWeek: tiktokLastWeek || { spend: 0, impressions: 0, clicks: 0, purchases: 0, revenue: 0, roas: 0, cpm: 0 },
  };

  const defaultAmz = { ca: 0, orders: 0 };
  const amazon = {
    yesterday: amazonYesterday || defaultAmz,
    dayBefore: amazonDayBefore || defaultAmz,
    lastWeek: amazonLastWeek || defaultAmz,
  };

  // Totals (Shopify spend only — Amazon ads not included in daily spend as it's MTD cached)
  const totalSpendY = meta.yesterday.spend + google.yesterday.spend + tiktok.yesterday.spend;
  const totalSpendDB = meta.dayBefore.spend + google.dayBefore.spend + tiktok.dayBefore.spend;
  const totalSpendLW = meta.lastWeek.spend + google.lastWeek.spend + tiktok.lastWeek.spend;

  const percentMarketingY = shopify.yesterday.netSales > 0 ? (totalSpendY / shopify.yesterday.netSales) * 100 : 0;
  const percentMarketingDB = shopify.dayBefore.netSales > 0 ? (totalSpendDB / shopify.dayBefore.netSales) * 100 : 0;
  const percentMarketingLW = shopify.lastWeek.netSales > 0 ? (totalSpendLW / shopify.lastWeek.netSales) * 100 : 0;

  // Blended KPIs
  const blendedCacY = shopify.yesterday.totalOrders > 0 ? totalSpendY / shopify.yesterday.totalOrders : 0;
  const blendedCacDB = shopify.dayBefore.totalOrders > 0 ? totalSpendDB / shopify.dayBefore.totalOrders : 0;
  const blendedCacLW = shopify.lastWeek.totalOrders > 0 ? totalSpendLW / shopify.lastWeek.totalOrders : 0;

  const blendedRoasY = totalSpendY > 0 ? shopify.yesterday.netSales / totalSpendY : 0;
  const blendedRoasDB = totalSpendDB > 0 ? shopify.dayBefore.netSales / totalSpendDB : 0;
  const blendedRoasLW = totalSpendLW > 0 ? shopify.lastWeek.netSales / totalSpendLW : 0;

  return {
    date: yStr,
    dateDayBefore: dbStr,
    dateLastWeek: lwStr,
    dayName: yesterday.toLocaleDateString('fr-FR', { weekday: 'long', day: 'numeric', month: 'long', year: 'numeric' }),
    shopify, meta, google, tiktok, amazon,
    totals: {
      spendYesterday: totalSpendY,
      spendDayBefore: totalSpendDB,
      spendLastWeek: totalSpendLW,
      percentMarketingYesterday: percentMarketingY,
      percentMarketingDayBefore: percentMarketingDB,
      percentMarketingLastWeek: percentMarketingLW,
      blendedCacYesterday: blendedCacY,
      blendedCacDayBefore: blendedCacDB,
      blendedCacLastWeek: blendedCacLW,
      blendedRoasYesterday: blendedRoasY,
      blendedRoasDayBefore: blendedRoasDB,
      blendedRoasLastWeek: blendedRoasLW,
    },
    bestAd,
    bestTiktokAd,
  };
}

// ============================================================
// CLAUDE ANALYSIS
// ============================================================

async function generateAnalysis(data) {
  const apiKey = process.env.ANTHROPIC_API_KEY;
  if (!apiKey) return 'Analyse non disponible (clé API manquante).';

  const anthropic = new Anthropic({ apiKey });

  const prompt = `Tu es un expert senior en acquisition e-commerce / performance marketing. Tu rédiges un brief quotidien pour le CEO d'une marque DTC (French Bandit, accessoires pour chiens et chats).

Voici les données de la veille (${data.dayName}) comparées à J-1 et J-7 :

## Shopify (canaux DTC uniquement, HT, retours déduits)
- CA HT hier : ${data.shopify.yesterday.netSales.toFixed(2)}€ | J-1 : ${data.shopify.dayBefore.netSales.toFixed(2)}€ | J-7 : ${data.shopify.lastWeek.netSales.toFixed(2)}€
- Commandes hier : ${data.shopify.yesterday.totalOrders} | J-1 : ${data.shopify.dayBefore.totalOrders} | J-7 : ${data.shopify.lastWeek.totalOrders}
- AOV hier : ${data.shopify.yesterday.aov.toFixed(2)}€ | J-1 : ${data.shopify.dayBefore.aov.toFixed(2)}€ | J-7 : ${data.shopify.lastWeek.aov.toFixed(2)}€

## Amazon
- CA hier : ${data.amazon.yesterday.ca.toFixed(2)}€ | J-1 : ${data.amazon.dayBefore.ca.toFixed(2)}€ | J-7 : ${data.amazon.lastWeek.ca.toFixed(2)}€
- Commandes hier : ${data.amazon.yesterday.orders} | J-1 : ${data.amazon.dayBefore.orders} | J-7 : ${data.amazon.lastWeek.orders}

## Dépenses publicitaires totales (Meta + Google + TikTok)
- Spend hier : ${data.totals.spendYesterday.toFixed(2)}€ | J-1 : ${data.totals.spendDayBefore.toFixed(2)}€ | J-7 : ${data.totals.spendLastWeek.toFixed(2)}€
- % Marketing hier : ${data.totals.percentMarketingYesterday.toFixed(1)}% | J-1 : ${data.totals.percentMarketingDayBefore.toFixed(1)}% | J-7 : ${data.totals.percentMarketingLastWeek.toFixed(1)}%
- Blended CAC : ${data.totals.blendedCacYesterday.toFixed(2)}€ | J-1 : ${data.totals.blendedCacDayBefore.toFixed(2)}€ | J-7 : ${data.totals.blendedCacLastWeek.toFixed(2)}€
- Blended ROAS : ${data.totals.blendedRoasYesterday.toFixed(2)} | J-1 : ${data.totals.blendedRoasDayBefore.toFixed(2)} | J-7 : ${data.totals.blendedRoasLastWeek.toFixed(2)}

## Meta Ads
- Spend : ${data.meta.yesterday.spend.toFixed(2)}€ | J-1 : ${data.meta.dayBefore.spend.toFixed(2)}€ | J-7 : ${data.meta.lastWeek.spend.toFixed(2)}€
- ROAS : ${data.meta.yesterday.roas.toFixed(2)} | J-1 : ${data.meta.dayBefore.roas.toFixed(2)} | J-7 : ${data.meta.lastWeek.roas.toFixed(2)}
- CPM : ${data.meta.yesterday.cpm.toFixed(2)}€ | J-1 : ${data.meta.dayBefore.cpm.toFixed(2)}€ | J-7 : ${data.meta.lastWeek.cpm.toFixed(2)}€
- Purchases : ${data.meta.yesterday.purchases} | Revenue : ${data.meta.yesterday.revenue.toFixed(2)}€

## Google Ads
- Spend : ${data.google.yesterday.spend.toFixed(2)}€ | J-1 : ${data.google.dayBefore.spend.toFixed(2)}€ | J-7 : ${data.google.lastWeek.spend.toFixed(2)}€
- ROAS : ${data.google.yesterday.roas.toFixed(2)} | J-1 : ${data.google.dayBefore.roas.toFixed(2)} | J-7 : ${data.google.lastWeek.roas.toFixed(2)}

## TikTok Ads
- Spend : ${data.tiktok.yesterday.spend.toFixed(2)}€ | J-1 : ${data.tiktok.dayBefore.spend.toFixed(2)}€ | J-7 : ${data.tiktok.lastWeek.spend.toFixed(2)}€
- ROAS : ${data.tiktok.yesterday.roas.toFixed(2)} | J-1 : ${data.tiktok.dayBefore.roas.toFixed(2)} | J-7 : ${data.tiktok.lastWeek.roas.toFixed(2)}
- Purchases : ${data.tiktok.yesterday.purchases} | Revenue : ${data.tiktok.yesterday.revenue.toFixed(2)}€

${data.bestAd ? `## Best performing ad — Meta
- Nom : ${data.bestAd.name}
- Campaign : ${data.bestAd.campaign} › ${data.bestAd.adset}
- Spend : ${data.bestAd.spend.toFixed(2)}€ | Revenue : ${data.bestAd.revenue.toFixed(2)}€ | ROAS : ${data.bestAd.roas.toFixed(2)}
- Purchases : ${data.bestAd.purchases}` : ''}

${data.bestTiktokAd ? `## Best performing ad — TikTok
- Nom : ${data.bestTiktokAd.name}
- Campaign : ${data.bestTiktokAd.campaign} › ${data.bestTiktokAd.adgroup}
- Spend : ${data.bestTiktokAd.spend.toFixed(2)}€ | Revenue : ${data.bestTiktokAd.revenue.toFixed(2)}€ | ROAS : ${data.bestTiktokAd.roas.toFixed(2)}
- Purchases : ${data.bestTiktokAd.purchases}` : ''}

---

Rédige un brief de 6 à 10 lignes maximum, en français, très professionnel et synthétique. Structure :
1. UNE phrase sur la performance globale (CA Shopify + Amazon combiné, tendance vs J-1 et J-7)
2. Les signaux positifs s'il y en a (1-2 lignes max)
3. Les alertes / points d'attention (spend anormal, ROAS en baisse, CPM en hausse, CAC qui monte...) (1-2 lignes max)
4. Amazon : mentionne la perf Amazon en 1 phrase si les données sont disponibles
5. Best ads : mentionne la ou les best ad(s) (Meta et/ou TikTok) avec ROAS
6. Une recommandation actionnable si pertinent

Ton style : direct, factuel, pas de blabla. Utilise des chiffres. Pas de bullet points, que du texte fluide. Pas de titre ni de signature. Écris comme un head of growth qui brief son CEO sur Slack le matin.`;

  const response = await anthropic.messages.create({
    model: 'claude-sonnet-4-20250514',
    max_tokens: 500,
    messages: [{ role: 'user', content: prompt }],
  });

  return response.content[0].text;
}

// ============================================================
// HTML EMAIL
// ============================================================

function fmtEur(val) {
  return new Intl.NumberFormat('fr-FR', { style: 'currency', currency: 'EUR', maximumFractionDigits: 0 }).format(val);
}

function fmtPct(val) {
  return val.toFixed(1) + '%';
}

function changeColor(current, previous, invert) {
  if (previous === 0) return '#6b7280';
  const pct = ((current - previous) / Math.abs(previous)) * 100;
  if (invert) return pct <= 0 ? '#00c48c' : '#ff5a5f';
  return pct >= 0 ? '#00c48c' : '#ff5a5f';
}

function changeText(current, previous) {
  if (previous === 0) return '—';
  const pct = ((current - previous) / Math.abs(previous)) * 100;
  const sign = pct >= 0 ? '+' : '';
  return `${sign}${pct.toFixed(1)}%`;
}

function buildEmailHTML(data, analysis) {
  const d = data;

  const kpiRow = (label, current, previous, previousLW, formatter, invert) => `
    <tr>
      <td style="padding:12px 16px;font-weight:600;color:#1a1d26;border-bottom:1px solid #f0f0f0;">${label}</td>
      <td style="padding:12px 16px;font-size:20px;font-weight:700;color:#1a1d26;border-bottom:1px solid #f0f0f0;">${formatter(current)}</td>
      <td style="padding:12px 16px;color:${changeColor(current, previous, invert)};font-weight:600;border-bottom:1px solid #f0f0f0;">
        ${changeText(current, previous)} <span style="color:#9ca3af;font-weight:400;">vs J-1</span>
      </td>
      <td style="padding:12px 16px;color:${changeColor(current, previousLW, invert)};font-weight:600;border-bottom:1px solid #f0f0f0;">
        ${changeText(current, previousLW)} <span style="color:#9ca3af;font-weight:400;">vs J-7</span>
      </td>
    </tr>`;

  const channelRow = (name, color, current) => {
    if (current.spend === 0) return '';
    return `
    <tr>
      <td style="padding:8px 16px;border-bottom:1px solid #f0f0f0;">
        <span style="display:inline-block;width:10px;height:10px;border-radius:3px;background:${color};margin-right:6px;vertical-align:middle;"></span>
        <span style="font-weight:600;">${name}</span>
      </td>
      <td style="padding:8px 16px;border-bottom:1px solid #f0f0f0;">${fmtEur(current.spend)}</td>
      <td style="padding:8px 16px;border-bottom:1px solid #f0f0f0;">${current.roas.toFixed(2)}x</td>
      <td style="padding:8px 16px;border-bottom:1px solid #f0f0f0;">${fmtEur(current.cpm)}</td>
    </tr>`;
  };

  const bestAdSection = d.bestAd ? `
    <div style="margin-top:24px;padding:16px 20px;background:#f0f0ff;border-radius:10px;border-left:4px solid #6c5ce7;">
      <div style="font-size:11px;text-transform:uppercase;letter-spacing:0.5px;color:#6c5ce7;font-weight:700;margin-bottom:6px;">Best performing ad — Meta</div>
      <div style="font-weight:700;color:#1a1d26;font-size:15px;margin-bottom:4px;">${d.bestAd.name}</div>
      <div style="font-size:13px;color:#6b7280;margin-bottom:8px;">${d.bestAd.campaign} › ${d.bestAd.adset}</div>
      <div style="display:flex;gap:16px;">
        <span style="font-size:13px;"><strong>ROAS</strong> ${d.bestAd.roas.toFixed(2)}x</span>
        <span style="font-size:13px;margin-left:12px;"><strong>Spend</strong> ${fmtEur(d.bestAd.spend)}</span>
        <span style="font-size:13px;margin-left:12px;"><strong>Revenue</strong> ${fmtEur(d.bestAd.revenue)}</span>
        <span style="font-size:13px;margin-left:12px;"><strong>Purchases</strong> ${d.bestAd.purchases}</span>
      </div>
    </div>` : '';

  return `<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#f5f6fa;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;">
  <div style="max-width:640px;margin:0 auto;padding:24px 16px;">

    <!-- HEADER -->
    <div style="text-align:center;padding:20px 0 16px;">
      <div style="font-size:18px;font-weight:700;color:#1a1d26;">Bandit <span style="color:#6c5ce7;">Acquisition</span></div>
      <div style="font-size:13px;color:#9ca3af;margin-top:4px;">Rapport du ${d.dayName}</div>
    </div>

    <!-- ANALYSIS -->
    <div style="background:#ffffff;border-radius:12px;padding:20px 24px;margin-bottom:20px;border:1px solid #e8eaef;box-shadow:0 1px 3px rgba(0,0,0,0.04);">
      <div style="font-size:11px;text-transform:uppercase;letter-spacing:0.5px;color:#6c5ce7;font-weight:700;margin-bottom:10px;">Analyse</div>
      <div style="font-size:14px;line-height:1.6;color:#1a1d26;">${analysis.replace(/\n/g, '<br>')}</div>
    </div>

    <!-- KPIs -->
    <div style="background:#ffffff;border-radius:12px;overflow:hidden;margin-bottom:20px;border:1px solid #e8eaef;box-shadow:0 1px 3px rgba(0,0,0,0.04);">
      <div style="padding:16px 16px 8px;font-size:11px;text-transform:uppercase;letter-spacing:0.5px;color:#6c5ce7;font-weight:700;">Indicateurs clés</div>
      <table style="width:100%;border-collapse:collapse;font-size:13px;">
        ${kpiRow('CA Shopify HT', d.shopify.yesterday.netSales, d.shopify.dayBefore.netSales, d.shopify.lastWeek.netSales, fmtEur, false)}
        ${kpiRow('Commandes Shopify', d.shopify.yesterday.totalOrders, d.shopify.dayBefore.totalOrders, d.shopify.lastWeek.totalOrders, v => v.toString(), false)}
        ${kpiRow('AOV', d.shopify.yesterday.aov, d.shopify.dayBefore.aov, d.shopify.lastWeek.aov, fmtEur, false)}
        ${kpiRow('CA Amazon', d.amazon.yesterday.ca, d.amazon.dayBefore.ca, d.amazon.lastWeek.ca, fmtEur, false)}
        ${kpiRow('Commandes Amazon', d.amazon.yesterday.orders, d.amazon.dayBefore.orders, d.amazon.lastWeek.orders, v => v.toString(), false)}
        ${kpiRow('Spend total', d.totals.spendYesterday, d.totals.spendDayBefore, d.totals.spendLastWeek, fmtEur, true)}
        ${kpiRow('% Marketing', d.totals.percentMarketingYesterday, d.totals.percentMarketingDayBefore, d.totals.percentMarketingLastWeek, fmtPct, true)}
        ${kpiRow('Blended CAC', d.totals.blendedCacYesterday, d.totals.blendedCacDayBefore, d.totals.blendedCacLastWeek, fmtEur, true)}
        ${kpiRow('Blended ROAS', d.totals.blendedRoasYesterday, d.totals.blendedRoasDayBefore, d.totals.blendedRoasLastWeek, v => v.toFixed(2) + 'x', false)}
      </table>
    </div>

    <!-- CHANNELS -->
    <div style="background:#ffffff;border-radius:12px;overflow:hidden;margin-bottom:20px;border:1px solid #e8eaef;box-shadow:0 1px 3px rgba(0,0,0,0.04);">
      <div style="padding:16px 16px 8px;font-size:11px;text-transform:uppercase;letter-spacing:0.5px;color:#6c5ce7;font-weight:700;">Par canal</div>
      <table style="width:100%;border-collapse:collapse;font-size:13px;">
        <tr style="color:#9ca3af;font-size:11px;">
          <td style="padding:4px 16px;">Canal</td>
          <td style="padding:4px 16px;">Spend</td>
          <td style="padding:4px 16px;">ROAS</td>
          <td style="padding:4px 16px;">CPM</td>
        </tr>
        ${channelRow('Meta', '#1877f2', d.meta.yesterday)}
        ${channelRow('Google', '#ea4335', d.google.yesterday)}
        ${channelRow('TikTok', '#000000', d.tiktok.yesterday)}
      </table>
    </div>

    <!-- BEST ADS -->
    ${bestAdSection}
    ${d.bestTiktokAd ? `
    <div style="margin-top:12px;padding:16px 20px;background:#f0f0f0;border-radius:10px;border-left:4px solid #000000;">
      <div style="font-size:11px;text-transform:uppercase;letter-spacing:0.5px;color:#000;font-weight:700;margin-bottom:6px;">Best performing ad — TikTok</div>
      <div style="font-weight:700;color:#1a1d26;font-size:15px;margin-bottom:4px;">${d.bestTiktokAd.name}</div>
      <div style="font-size:13px;color:#6b7280;margin-bottom:8px;">${d.bestTiktokAd.campaign} › ${d.bestTiktokAd.adgroup}</div>
      <div>
        <span style="font-size:13px;"><strong>ROAS</strong> ${d.bestTiktokAd.roas.toFixed(2)}x</span>
        <span style="font-size:13px;margin-left:12px;"><strong>Spend</strong> ${fmtEur(d.bestTiktokAd.spend)}</span>
        <span style="font-size:13px;margin-left:12px;"><strong>Revenue</strong> ${fmtEur(d.bestTiktokAd.revenue)}</span>
        <span style="font-size:13px;margin-left:12px;"><strong>Purchases</strong> ${d.bestTiktokAd.purchases}</span>
      </div>
    </div>` : ''}

    <!-- FOOTER -->
    <div style="text-align:center;padding:20px 0;font-size:11px;color:#9ca3af;">
      Bandit Acquisition Dashboard
    </div>
  </div>
</body>
</html>`;
}

// ============================================================
// SEND EMAIL
// ============================================================

async function sendReport() {
  console.log('[Report] Starting daily report generation...');

  const data = await collectReportData();
  console.log(`[Report] Data collected. CA HT: ${data.shopify.yesterday.netSales.toFixed(2)}€, Spend: ${data.totals.spendYesterday.toFixed(2)}€`);

  const analysis = await generateAnalysis(data);
  console.log('[Report] Analysis generated.');

  const html = buildEmailHTML(data, analysis);

  const sgApiKey = process.env.SENDGRID_API_KEY;
  const emailTo = process.env.REPORT_EMAIL_TO;
  const emailFrom = process.env.REPORT_EMAIL_FROM;

  if (!sgApiKey || !emailTo || !emailFrom) {
    console.log('[Report] Email not configured. Skipping send.');
    return { data, analysis, html };
  }

  sgMail.setApiKey(sgApiKey);

  const msg = {
    to: emailTo,
    from: emailFrom,
    subject: `Bandit Acquisition — ${data.dayName} — Shopify ${fmtEur(data.shopify.yesterday.netSales)} HT | Amazon ${fmtEur(data.amazon.yesterday.ca)}`,
    html,
  };

  await sgMail.send(msg);
  console.log(`[Report] Email sent to ${emailTo}`);

  return { data, analysis, html };
}

module.exports = { sendReport, collectReportData, generateAnalysis, buildEmailHTML };

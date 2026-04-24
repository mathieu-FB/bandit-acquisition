// ============================================================
// DAILY REPORT — Email quotidien d'acquisition
// ============================================================

const fetch = require('node-fetch');
const sgMail = require('@sendgrid/mail');
const Anthropic = require('@anthropic-ai/sdk').default;
const zlib = require('zlib');

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
// OBJECTIVES (mirrored from server.js)
// ============================================================

const OBJECTIVES = {
  2026: {
    Q2: {
      ca: 424000,
      ratio: 30,
      months: {
        4: { ca: 128000, ratio: 30 },
        5: { ca: 144000, ratio: 30 },
        6: { ca: 152000, ratio: 30 },
      },
    },
  },
};

const AMAZON_OBJECTIVES = {
  2026: {
    Q2: {
      ca: 120000,
      tacos: 20,
      months: {
        4: { ca: 40000, tacos: 20 },
        5: { ca: 40000, tacos: 20 },
        6: { ca: 40000, tacos: 20 },
      },
    },
  },
};

function getObjective(year, month) {
  const yearObj = OBJECTIVES[year];
  if (!yearObj) return null;
  for (const [qKey, q] of Object.entries(yearObj)) {
    if (q.months && q.months[month]) {
      return { quarter: qKey, quarterObj: q, monthObj: q.months[month] };
    }
  }
  return null;
}

function getAmazonObjective(year, month) {
  const yearObj = AMAZON_OBJECTIVES[year];
  if (!yearObj) return null;
  for (const [qKey, q] of Object.entries(yearObj)) {
    if (q.months && q.months[month]) {
      return { quarter: qKey, quarterObj: q, monthObj: q.months[month] };
    }
  }
  return null;
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
      fields: 'id,created_at,total_price,subtotal_price,total_discounts,total_tax,source_name,customer,financial_status,refunds,total_shipping_price_set',
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

function orderShippingHT(order) {
  const shippingTTC = parseFloat(order.total_shipping_price_set?.shop_money?.amount || 0);
  return shippingTTC / 1.20; // 20% TVA
}

function computeShopifyStats(orders) {
  const valid = orders.filter(o => o.financial_status !== 'voided');
  const countable = valid.filter(o => o.financial_status !== 'refunded');

  const totalOrders = countable.length;
  const netSales = valid.reduce((sum, o) => sum + orderNetSalesHT(o), 0);
  const shippingHT = valid.reduce((sum, o) => sum + orderShippingHT(o), 0);
  const totalCA = netSales + shippingHT;
  const aov = totalOrders > 0 ? netSales / totalOrders : 0;

  return { totalOrders, netSales, shippingHT, totalCA, aov };
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
    if (json1.error) return null;

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
        if (json2.data?.[0]?.action_values) {
          const r = json2.data[0].action_values.find(a => a.action_type === 'purchase' || a.action_type === 'omni_purchase');
          if (r) revenue = parseFloat(r.value || 0);
        }
      } catch (e) { /* skip */ }

      const roas = ad.spendNum > 0 ? revenue / ad.spendNum : 0;
      if (roas > bestRoas) {
        bestRoas = roas;
        bestAd = { name: ad.ad_name, adset: ad.adset_name, campaign: ad.campaign_name, spend: ad.spendNum, revenue, purchases: ad.purchases, roas };
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
    return { spend: 0, impressions: 0, clicks: 0, purchases: 0, revenue: 0, roas: 0, cpm: 0 };
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
// GOOGLE ADS
// ============================================================

async function fetchGoogleStats(start, end) {
  try {
    const { GoogleAdsApi, fromMicros } = require('google-ads-api');
    const clientId = process.env.GOOGLE_ADS_CLIENT_ID;
    const clientSecret = process.env.GOOGLE_ADS_CLIENT_SECRET;
    const devToken = process.env.GOOGLE_ADS_DEVELOPER_TOKEN;
    const refreshToken = process.env.GOOGLE_ADS_REFRESH_TOKEN;
    const loginCustomerId = (process.env.GOOGLE_ADS_LOGIN_CUSTOMER_ID || process.env.GOOGLE_ADS_CUSTOMER_ID || '').replace(/-/g, '');
    const customerId = (process.env.GOOGLE_ADS_CLIENT_ACCOUNT_ID || process.env.GOOGLE_ADS_CUSTOMER_ID || '').replace(/-/g, '');
    if (!clientId || !clientSecret || !devToken || !refreshToken || !customerId) return null;

    const client = new GoogleAdsApi({ client_id: clientId, client_secret: clientSecret, developer_token: devToken });
    const opts = { customer_id: customerId, refresh_token: refreshToken };
    if (loginCustomerId && loginCustomerId !== customerId) opts.login_customer_id = loginCustomerId;
    const customer = client.Customer(opts);

    const results = await customer.query(`
      SELECT metrics.cost_micros, metrics.impressions, metrics.clicks, metrics.conversions, metrics.conversions_value
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

    return { spend, impressions, clicks, conversions, revenue, roas: spend > 0 ? revenue / spend : 0, cpm: impressions > 0 ? (spend / impressions) * 1000 : 0 };
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

    return { spend, impressions, clicks, purchases, revenue, roas: spend > 0 ? revenue / spend : 0, cpm: impressions > 0 ? (spend / impressions) * 1000 : 0 };
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
    body: new URLSearchParams({ grant_type: 'refresh_token', refresh_token: refreshToken, client_id: clientId, client_secret: clientSecret }).toString(),
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

    const res = await fetch(url, { headers: { 'x-amz-access-token': token, 'Content-Type': 'application/json' } });
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
// AMAZON ADS — spend for TACOS calculation
// ============================================================

async function getAmazonAdsAccessToken() {
  const clientId = process.env.AMAZON_ADS_CLIENT_ID;
  const clientSecret = process.env.AMAZON_ADS_CLIENT_SECRET;
  const refreshToken = process.env.AMAZON_ADS_REFRESH_TOKEN;
  if (!clientId || !clientSecret || !refreshToken) return null;

  const res = await fetch('https://api.amazon.com/auth/o2/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({ grant_type: 'refresh_token', refresh_token: refreshToken, client_id: clientId, client_secret: clientSecret }).toString(),
  });
  if (!res.ok) return null;
  const json = await res.json();
  return json.access_token;
}

async function fetchAmazonAdsSpendForRange(start, end) {
  const token = await getAmazonAdsAccessToken();
  const profileId = process.env.AMAZON_ADS_PROFILE_ID;
  if (!token || !profileId) return null;

  try {
    const [spSpend, sbSpend, sdSpend] = await Promise.all([
      fetchOneAmazonAdReport(token, profileId, 'SPONSORED_PRODUCTS', 'spCampaigns', start, end),
      fetchOneAmazonAdReport(token, profileId, 'SPONSORED_BRANDS', 'sbCampaigns', start, end),
      fetchOneAmazonAdReport(token, profileId, 'SPONSORED_DISPLAY', 'sdCampaigns', start, end),
    ]);
    const total = spSpend + sbSpend + sdSpend;
    console.log(`[Report] Amazon Ads spend ${start}→${end}: ${total}€ (SP:${spSpend}, SB:${sbSpend}, SD:${sdSpend})`);
    return total;
  } catch (err) {
    console.error('[Report] Amazon Ads spend error:', err.message);
    return null;
  }
}

async function fetchOneAmazonAdReport(token, profileId, adProduct, reportTypeId, start, end) {
  const headers = {
    'Amazon-Advertising-API-ClientId': process.env.AMAZON_ADS_CLIENT_ID,
    'Amazon-Advertising-API-Scope': profileId,
    Authorization: `Bearer ${token}`,
  };

  // Step 1: Request report
  const reportRes = await fetch('https://advertising-api-eu.amazon.com/reporting/reports', {
    method: 'POST',
    headers: { ...headers, 'Content-Type': 'application/vnd.createasyncreportrequest.v3+json' },
    body: JSON.stringify({
      startDate: start,
      endDate: end,
      configuration: { adProduct, groupBy: ['campaign'], columns: [adProduct === 'SPONSORED_PRODUCTS' ? 'spend' : 'cost'], reportTypeId, timeUnit: 'SUMMARY', format: 'GZIP_JSON' },
    }),
  });

  let reportId;
  if (!reportRes.ok) {
    const errBody = await reportRes.text();
    if (reportRes.status === 425) {
      const match = errBody.match(/duplicate of\s*:\s*([a-f0-9-]+)/i);
      if (match) reportId = match[1];
      else return 0;
    } else return 0;
  } else {
    reportId = (await reportRes.json()).reportId;
  }

  // Step 2: Poll (max 3 min for daily report)
  let downloadUrl = null;
  for (let attempt = 0; attempt < 36; attempt++) {
    await new Promise(r => setTimeout(r, 5000));
    const statusRes = await fetch(`https://advertising-api-eu.amazon.com/reporting/reports/${reportId}`, { headers });
    if (!statusRes.ok) continue;
    const statusData = await statusRes.json();
    if (statusData.status === 'COMPLETED') { downloadUrl = statusData.url; break; }
    if (statusData.status === 'FAILURE') return 0;
  }
  if (!downloadUrl) return 0;

  // Step 3: Download + decompress
  const dlRes = await fetch(downloadUrl);
  if (!dlRes.ok) return 0;
  const buffer = await dlRes.buffer();

  let rows;
  try {
    rows = JSON.parse(zlib.gunzipSync(buffer).toString());
  } catch {
    try { rows = JSON.parse(buffer.toString()); } catch { return 0; }
  }

  let spend = 0;
  (Array.isArray(rows) ? rows : []).forEach(row => { spend += parseFloat(row.spend || row.cost || 0); });
  return spend;
}

// ============================================================
// FREELANCE COST (mirrored from server.js)
// ============================================================

const FREELANCE_MONTHLY_COST = 1280;

function getFreelanceDailyCost(dateStr) {
  const d = new Date(dateStr);
  const daysInMonth = new Date(d.getFullYear(), d.getMonth() + 1, 0).getDate();
  return FREELANCE_MONTHLY_COST / daysInMonth;
}

// ============================================================
// COLLECT ALL DATA FOR REPORT
// ============================================================

async function collectReportData() {
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

  // MTD: 1st of month → yesterday
  const mtdStart = `${yesterday.getFullYear()}-${String(yesterday.getMonth() + 1).padStart(2, '0')}-01`;

  // QTD: 1st of quarter → yesterday
  const qMonth = Math.floor(yesterday.getMonth() / 3) * 3;
  const qtdStart = `${yesterday.getFullYear()}-${String(qMonth + 1).padStart(2, '0')}-01`;

  console.log(`[Report] Collecting data for ${yStr} (vs ${dbStr} and ${lwStr}), MTD from ${mtdStart}, QTD from ${qtdStart}`);

  // Fetch everything in parallel
  const [
    shopifyYesterday, shopifyDayBefore, shopifyLastWeek, shopifyMTD,
    metaYesterday, metaDayBefore, metaLastWeek, metaMTD,
    bestAd, bestTiktokAd,
    googleYesterday, googleDayBefore, googleLastWeek, googleMTD,
    tiktokYesterday, tiktokDayBefore, tiktokLastWeek, tiktokMTD,
    amazonYesterday, amazonDayBefore, amazonLastWeek, amazonMTD,
    amazonAdsSpendMTD,
  ] = await Promise.all([
    fetchShopifyOrders(yStr, yStr),
    fetchShopifyOrders(dbStr, dbStr),
    fetchShopifyOrders(lwStr, lwStr),
    fetchShopifyOrders(mtdStart, yStr),
    fetchMetaInsights(yStr, yStr),
    fetchMetaInsights(dbStr, dbStr),
    fetchMetaInsights(lwStr, lwStr),
    fetchMetaInsights(mtdStart, yStr),
    fetchMetaBestAd(yStr, yStr),
    fetchTikTokBestAd(yStr, yStr),
    fetchGoogleStats(yStr, yStr),
    fetchGoogleStats(dbStr, dbStr),
    fetchGoogleStats(lwStr, lwStr),
    fetchGoogleStats(mtdStart, yStr),
    fetchTikTokStats(yStr, yStr),
    fetchTikTokStats(dbStr, dbStr),
    fetchTikTokStats(lwStr, lwStr),
    fetchTikTokStats(mtdStart, yStr),
    fetchAmazonStats(yStr, yStr),
    fetchAmazonStats(dbStr, dbStr),
    fetchAmazonStats(lwStr, lwStr),
    fetchAmazonStats(mtdStart, yStr),
    fetchAmazonAdsSpendForRange(mtdStart, yStr),
  ]);

  const shopify = {
    yesterday: computeShopifyStats(shopifyYesterday),
    dayBefore: computeShopifyStats(shopifyDayBefore),
    lastWeek: computeShopifyStats(shopifyLastWeek),
    mtd: computeShopifyStats(shopifyMTD),
  };

  const meta = {
    yesterday: aggregateMeta(metaYesterday),
    dayBefore: aggregateMeta(metaDayBefore),
    lastWeek: aggregateMeta(metaLastWeek),
    mtd: aggregateMeta(metaMTD),
  };

  const defGoogle = { spend: 0, impressions: 0, clicks: 0, conversions: 0, revenue: 0, roas: 0, cpm: 0 };
  const google = {
    yesterday: googleYesterday || defGoogle,
    dayBefore: googleDayBefore || defGoogle,
    lastWeek: googleLastWeek || defGoogle,
    mtd: googleMTD || defGoogle,
  };

  const defTiktok = { spend: 0, impressions: 0, clicks: 0, purchases: 0, revenue: 0, roas: 0, cpm: 0 };
  const tiktok = {
    yesterday: tiktokYesterday || defTiktok,
    dayBefore: tiktokDayBefore || defTiktok,
    lastWeek: tiktokLastWeek || defTiktok,
    mtd: tiktokMTD || defTiktok,
  };

  const defAmz = { ca: 0, orders: 0 };
  const amazon = {
    yesterday: amazonYesterday || defAmz,
    dayBefore: amazonDayBefore || defAmz,
    lastWeek: amazonLastWeek || defAmz,
    mtd: amazonMTD || defAmz,
  };

  // E-commerce totals (Shopify spend = Meta + Google + TikTok + freelance)
  const freelanceY = getFreelanceDailyCost(yStr);
  const totalSpendY = meta.yesterday.spend + google.yesterday.spend + tiktok.yesterday.spend + freelanceY;
  const totalSpendDB = meta.dayBefore.spend + google.dayBefore.spend + tiktok.dayBefore.spend + getFreelanceDailyCost(dbStr);
  const totalSpendLW = meta.lastWeek.spend + google.lastWeek.spend + tiktok.lastWeek.spend + getFreelanceDailyCost(lwStr);

  // MTD spend (count days for freelance)
  const mtdDays = Math.ceil((new Date(yStr) - new Date(mtdStart)) / (1000 * 60 * 60 * 24)) + 1;
  const totalSpendMTD = meta.mtd.spend + google.mtd.spend + tiktok.mtd.spend + (freelanceY * mtdDays);

  const ecomRoasY = totalSpendY > 0 ? shopify.yesterday.totalCA / totalSpendY : 0;
  const ecomRoasDB = totalSpendDB > 0 ? shopify.dayBefore.totalCA / totalSpendDB : 0;
  const ecomRoasLW = totalSpendLW > 0 ? shopify.lastWeek.totalCA / totalSpendLW : 0;
  const ecomRoasMTD = totalSpendMTD > 0 ? shopify.mtd.totalCA / totalSpendMTD : 0;

  const percentMarketingY = shopify.yesterday.totalCA > 0 ? (totalSpendY / shopify.yesterday.totalCA) * 100 : 0;
  const blendedCacY = shopify.yesterday.totalOrders > 0 ? totalSpendY / shopify.yesterday.totalOrders : 0;

  // Amazon TACOS = Amazon Ads spend MTD / Amazon CA MTD * 100
  const amzAdSpendMTD = amazonAdsSpendMTD || 0;
  const amazonTacosMTD = amazon.mtd.ca > 0 ? (amzAdSpendMTD / amazon.mtd.ca) * 100 : 0;

  // Objectives
  const year = yesterday.getFullYear();
  const month = yesterday.getMonth() + 1;
  const obj = getObjective(year, month);
  const amzObj = getAmazonObjective(year, month);

  let objectives = null;
  if (obj) {
    const daysInMonth = new Date(year, month, 0).getDate();
    const dayOfMonth = yesterday.getDate();
    const expectedPace = (dayOfMonth / daysInMonth) * 100;

    // QTD shopify CA
    // We only have MTD data easily; QTD would need additional fetch if qtdStart != mtdStart
    const mtdProgress = obj.monthObj.ca > 0 ? (shopify.mtd.totalCA / obj.monthObj.ca) * 100 : 0;
    const qtdProgress = obj.quarterObj.ca > 0 ? (shopify.mtd.totalCA / obj.quarterObj.ca) * 100 : 0; // Approximation with MTD only

    objectives = {
      monthly: {
        target: obj.monthObj.ca,
        current: shopify.mtd.totalCA,
        progress: mtdProgress,
        expectedPace,
        onTrack: mtdProgress >= expectedPace * 0.9,
      },
      quarterly: {
        target: obj.quarterObj.ca,
        quarter: obj.quarter,
      },
    };
  }

  let amazonObjectives = null;
  if (amzObj) {
    const daysInMonth = new Date(year, month, 0).getDate();
    const dayOfMonth = yesterday.getDate();
    const expectedPace = (dayOfMonth / daysInMonth) * 100;

    const amzMtdProgress = amzObj.monthObj.ca > 0 ? (amazon.mtd.ca / amzObj.monthObj.ca) * 100 : 0;

    amazonObjectives = {
      monthly: {
        target: amzObj.monthObj.ca,
        current: amazon.mtd.ca,
        progress: amzMtdProgress,
        expectedPace,
        onTrack: amzMtdProgress >= expectedPace * 0.9,
      },
      quarterly: {
        target: amzObj.quarterObj.ca,
        quarter: amzObj.quarter,
      },
    };
  }

  return {
    date: yStr,
    dateDayBefore: dbStr,
    dateLastWeek: lwStr,
    dayName: yesterday.toLocaleDateString('fr-FR', { weekday: 'long', day: 'numeric', month: 'long', year: 'numeric' }),
    shopify, meta, google, tiktok, amazon,
    ecommerce: {
      roas: { yesterday: ecomRoasY, dayBefore: ecomRoasDB, lastWeek: ecomRoasLW, mtd: ecomRoasMTD },
      spend: { yesterday: totalSpendY, dayBefore: totalSpendDB, lastWeek: totalSpendLW, mtd: totalSpendMTD },
      percentMarketing: percentMarketingY,
      blendedCac: blendedCacY,
    },
    objectives,
    amazonObjectives,
    amazonTacosMTD,
    amzAdSpendMTD,
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
  const d = data;

  const objSection = d.objectives ? `
## Objectif mensuel E-commerce
- Target mois : ${d.objectives.monthly.target.toFixed(0)}€ | Réalisé MTD : ${d.objectives.monthly.current.toFixed(0)}€ | Progression : ${d.objectives.monthly.progress.toFixed(1)}% (rythme attendu : ${d.objectives.monthly.expectedPace.toFixed(1)}%)
${d.objectives.monthly.onTrack ? '→ ON TRACK' : '→ EN RETARD'}` : '';

  const amzObjSection = d.amazonObjectives ? `
## Objectif mensuel Amazon
- Target mois : ${d.amazonObjectives.monthly.target.toFixed(0)}€ | Réalisé MTD : ${d.amazonObjectives.monthly.current.toFixed(0)}€ | Progression : ${d.amazonObjectives.monthly.progress.toFixed(1)}% (rythme attendu : ${d.amazonObjectives.monthly.expectedPace.toFixed(1)}%)
${d.amazonObjectives.monthly.onTrack ? '→ ON TRACK' : '→ EN RETARD'}` : '';

  const prompt = `Tu es un expert senior en acquisition e-commerce / performance marketing. Tu rédiges un brief quotidien pour le CEO d'une marque DTC (French Bandit, accessoires pour chiens et chats). La marque vend sur son site Shopify (e-commerce) et sur Amazon (marketplace).

Voici les données de la veille (${d.dayName}) comparées à J-1 et J-7 :

## E-COMMERCE (Shopify — canaux DTC, HT, retours déduits)
- CA HT hier : ${d.shopify.yesterday.totalCA.toFixed(0)}€ | J-1 : ${d.shopify.dayBefore.totalCA.toFixed(0)}€ | J-7 : ${d.shopify.lastWeek.totalCA.toFixed(0)}€
- Commandes hier : ${d.shopify.yesterday.totalOrders} | J-1 : ${d.shopify.dayBefore.totalOrders} | J-7 : ${d.shopify.lastWeek.totalOrders}
- AOV hier : ${d.shopify.yesterday.aov.toFixed(0)}€ | J-1 : ${d.shopify.dayBefore.aov.toFixed(0)}€ | J-7 : ${d.shopify.lastWeek.aov.toFixed(0)}€
- ROAS E-COMMERCE : ${d.ecommerce.roas.yesterday.toFixed(2)}x | J-1 : ${d.ecommerce.roas.dayBefore.toFixed(2)}x | J-7 : ${d.ecommerce.roas.lastWeek.toFixed(2)}x | MTD : ${d.ecommerce.roas.mtd.toFixed(2)}x
- Spend total hier : ${d.ecommerce.spend.yesterday.toFixed(0)}€ | % Marketing : ${d.ecommerce.percentMarketing.toFixed(1)}%
- Blended CAC : ${d.ecommerce.blendedCac.toFixed(0)}€
- CA MTD : ${d.shopify.mtd.totalCA.toFixed(0)}€ | Spend MTD : ${d.ecommerce.spend.mtd.toFixed(0)}€
${objSection}

## AMAZON (marketplace)
- CA hier : ${d.amazon.yesterday.ca.toFixed(0)}€ | J-1 : ${d.amazon.dayBefore.ca.toFixed(0)}€ | J-7 : ${d.amazon.lastWeek.ca.toFixed(0)}€
- Commandes hier : ${d.amazon.yesterday.orders} | J-1 : ${d.amazon.dayBefore.orders} | J-7 : ${d.amazon.lastWeek.orders}
- CA MTD : ${d.amazon.mtd.ca.toFixed(0)}€
- TACOS MTD : ${d.amazonTacosMTD.toFixed(1)}% (Spend Ads MTD : ${d.amzAdSpendMTD.toFixed(0)}€)
${amzObjSection}

## DÉTAIL PAR CANAL (pub e-commerce)
- Meta : Spend ${d.meta.yesterday.spend.toFixed(0)}€ | ROAS ${d.meta.yesterday.roas.toFixed(2)}x | CPM ${d.meta.yesterday.cpm.toFixed(0)}€ | Achats ${d.meta.yesterday.purchases}
- Google : Spend ${d.google.yesterday.spend.toFixed(0)}€ | ROAS ${d.google.yesterday.roas.toFixed(2)}x
- TikTok : Spend ${d.tiktok.yesterday.spend.toFixed(0)}€ | ROAS ${d.tiktok.yesterday.roas.toFixed(2)}x | Achats ${d.tiktok.yesterday.purchases}

${d.bestAd ? `## Best ad Meta : "${d.bestAd.name}" — ROAS ${d.bestAd.roas.toFixed(2)}x, ${d.bestAd.purchases} achats, ${d.bestAd.spend.toFixed(0)}€ spend` : ''}
${d.bestTiktokAd ? `## Best ad TikTok : "${d.bestTiktokAd.name}" — ROAS ${d.bestTiktokAd.roas.toFixed(2)}x, ${d.bestTiktokAd.purchases} achats` : ''}

---

Rédige un brief de 8 à 12 lignes max, en français, très professionnel et synthétique. Structure :
1. Performance E-COMMERCE hier : CA, ROAS, tendance vs J-1 et J-7 (2-3 lignes)
2. Performance AMAZON hier : CA, tendance (1-2 lignes)
3. Objectifs : où on en est sur l'objectif mensuel e-commerce ET Amazon, on track ou pas (2 lignes)
4. Signaux positifs / alertes (ROAS, CPM, CAC) (1-2 lignes)
5. Best ads + recommandation actionnable si pertinent (1-2 lignes)

Ton style : direct, factuel, chiffres. Pas de bullet points, texte fluide. Pas de titre ni de signature.`;

  const response = await anthropic.messages.create({
    model: 'claude-sonnet-4-6',
    max_tokens: 600,
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
      <td style="padding:10px 16px;font-weight:600;color:#1a1d26;border-bottom:1px solid #f0f0f0;">${label}</td>
      <td style="padding:10px 16px;font-size:18px;font-weight:700;color:#1a1d26;border-bottom:1px solid #f0f0f0;">${formatter(current)}</td>
      <td style="padding:10px 16px;color:${changeColor(current, previous, invert)};font-weight:600;border-bottom:1px solid #f0f0f0;">
        ${changeText(current, previous)} <span style="color:#9ca3af;font-weight:400;">vs J-1</span>
      </td>
      <td style="padding:10px 16px;color:${changeColor(current, previousLW, invert)};font-weight:600;border-bottom:1px solid #f0f0f0;">
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

  // Objective progress bar
  const progressBar = (label, current, target, onTrack) => {
    if (!target) return '';
    const pct = Math.min((current / target) * 100, 100);
    const color = onTrack ? '#00c48c' : '#ff5a5f';
    return `
    <div style="margin-bottom:12px;">
      <div style="display:flex;justify-content:space-between;font-size:12px;margin-bottom:4px;">
        <span style="font-weight:600;color:#1a1d26;">${label}</span>
        <span style="color:#6b7280;">${fmtEur(current)} / ${fmtEur(target)} (${pct.toFixed(1)}%)</span>
      </div>
      <div style="background:#f0f0f0;border-radius:4px;height:8px;overflow:hidden;">
        <div style="background:${color};height:100%;width:${pct}%;border-radius:4px;"></div>
      </div>
    </div>`;
  };

  const bestAdSection = d.bestAd ? `
    <div style="margin-top:16px;padding:14px 18px;background:#f0f0ed;border-radius:10px;border-left:4px solid #1877f2;">
      <div style="font-size:11px;text-transform:uppercase;letter-spacing:0.5px;color:#1877f2;font-weight:700;margin-bottom:4px;">Best ad — Meta</div>
      <div style="font-weight:700;color:#1a1d26;font-size:14px;margin-bottom:6px;">${d.bestAd.name}</div>
      <div style="font-size:13px;">
        <strong>ROAS</strong> ${d.bestAd.roas.toFixed(2)}x · <strong>Spend</strong> ${fmtEur(d.bestAd.spend)} · <strong>Revenue</strong> ${fmtEur(d.bestAd.revenue)} · <strong>Achats</strong> ${d.bestAd.purchases}
      </div>
    </div>` : '';

  const bestTiktokSection = d.bestTiktokAd ? `
    <div style="margin-top:8px;padding:14px 18px;background:#f0f0f0;border-radius:10px;border-left:4px solid #000000;">
      <div style="font-size:11px;text-transform:uppercase;letter-spacing:0.5px;color:#000;font-weight:700;margin-bottom:4px;">Best ad — TikTok</div>
      <div style="font-weight:700;color:#1a1d26;font-size:14px;margin-bottom:6px;">${d.bestTiktokAd.name}</div>
      <div style="font-size:13px;">
        <strong>ROAS</strong> ${d.bestTiktokAd.roas.toFixed(2)}x · <strong>Spend</strong> ${fmtEur(d.bestTiktokAd.spend)} · <strong>Revenue</strong> ${fmtEur(d.bestTiktokAd.revenue)} · <strong>Achats</strong> ${d.bestTiktokAd.purchases}
      </div>
    </div>` : '';

  return `<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#f7f7f5;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;">
  <div style="max-width:640px;margin:0 auto;padding:24px 16px;">

    <!-- HEADER -->
    <div style="text-align:center;padding:20px 0 16px;">
      <div style="font-size:18px;font-weight:700;color:#1a1a1a;">Bandit <span style="font-weight:400;letter-spacing:1px;text-transform:uppercase;font-size:13px;color:#555;">Daily Report</span></div>
      <div style="font-size:13px;color:#9ca3af;margin-top:4px;">Rapport du ${d.dayName}</div>
    </div>

    <!-- ANALYSIS -->
    <div style="background:#ffffff;border-radius:12px;padding:20px 24px;margin-bottom:20px;border:1px solid #e5e5e0;box-shadow:0 1px 3px rgba(0,0,0,0.04);">
      <div style="font-size:11px;text-transform:uppercase;letter-spacing:0.5px;color:#1a1a1a;font-weight:700;margin-bottom:10px;">Analyse du jour</div>
      <div style="font-size:14px;line-height:1.6;color:#1a1d26;">${analysis.replace(/\n/g, '<br>')}</div>
    </div>

    <!-- OBJECTIVES -->
    ${d.objectives || d.amazonObjectives ? `
    <div style="background:#ffffff;border-radius:12px;padding:16px 20px;margin-bottom:20px;border:1px solid #e5e5e0;box-shadow:0 1px 3px rgba(0,0,0,0.04);">
      <div style="font-size:11px;text-transform:uppercase;letter-spacing:0.5px;color:#1a1a1a;font-weight:700;margin-bottom:12px;">Objectifs du mois</div>
      ${d.objectives ? progressBar('E-commerce (Shopify)', d.objectives.monthly.current, d.objectives.monthly.target, d.objectives.monthly.onTrack) : ''}
      ${d.amazonObjectives ? progressBar('Amazon', d.amazonObjectives.monthly.current, d.amazonObjectives.monthly.target, d.amazonObjectives.monthly.onTrack) : ''}
    </div>` : ''}

    <!-- E-COMMERCE KPIs -->
    <div style="background:#ffffff;border-radius:12px;overflow:hidden;margin-bottom:20px;border:1px solid #e5e5e0;box-shadow:0 1px 3px rgba(0,0,0,0.04);">
      <div style="padding:16px 16px 8px;font-size:11px;text-transform:uppercase;letter-spacing:0.5px;color:#1a1a1a;font-weight:700;">E-commerce (Shopify)</div>
      <table style="width:100%;border-collapse:collapse;font-size:13px;">
        ${kpiRow('CA HT', d.shopify.yesterday.totalCA, d.shopify.dayBefore.totalCA, d.shopify.lastWeek.totalCA, fmtEur, false)}
        ${kpiRow('Commandes', d.shopify.yesterday.totalOrders, d.shopify.dayBefore.totalOrders, d.shopify.lastWeek.totalOrders, v => v.toString(), false)}
        ${kpiRow('AOV', d.shopify.yesterday.aov, d.shopify.dayBefore.aov, d.shopify.lastWeek.aov, fmtEur, false)}
        ${kpiRow('ROAS E-com', d.ecommerce.roas.yesterday, d.ecommerce.roas.dayBefore, d.ecommerce.roas.lastWeek, v => v.toFixed(2) + 'x', false)}
        ${kpiRow('Spend', d.ecommerce.spend.yesterday, d.ecommerce.spend.dayBefore, d.ecommerce.spend.lastWeek, fmtEur, true)}
        ${kpiRow('% Marketing', d.ecommerce.percentMarketing, 0, 0, fmtPct, true)}
      </table>
    </div>

    <!-- AMAZON KPIs -->
    <div style="background:#ffffff;border-radius:12px;overflow:hidden;margin-bottom:20px;border:1px solid #e5e5e0;box-shadow:0 1px 3px rgba(0,0,0,0.04);">
      <div style="padding:16px 16px 8px;font-size:11px;text-transform:uppercase;letter-spacing:0.5px;color:#f0982d;font-weight:700;">Amazon</div>
      <table style="width:100%;border-collapse:collapse;font-size:13px;">
        ${kpiRow('CA', d.amazon.yesterday.ca, d.amazon.dayBefore.ca, d.amazon.lastWeek.ca, fmtEur, false)}
        ${kpiRow('Commandes', d.amazon.yesterday.orders, d.amazon.dayBefore.orders, d.amazon.lastWeek.orders, v => v.toString(), false)}
        <tr>
          <td style="padding:10px 16px;font-weight:600;color:#1a1d26;border-bottom:1px solid #f0f0f0;">Spend Ads MTD</td>
          <td style="padding:10px 16px;font-size:18px;font-weight:700;color:#1a1d26;border-bottom:1px solid #f0f0f0;" colspan="3">${fmtEur(d.amzAdSpendMTD)}</td>
        </tr>
        <tr>
          <td style="padding:10px 16px;font-weight:600;color:#1a1d26;border-bottom:1px solid #f0f0f0;">TACOS MTD</td>
          <td style="padding:10px 16px;font-size:18px;font-weight:700;color:#${d.amazonTacosMTD > 15 ? 'ff5a5f' : '00c48c'};border-bottom:1px solid #f0f0f0;" colspan="3">${d.amazonTacosMTD.toFixed(1)}%</td>
        </tr>
      </table>
    </div>

    <!-- CHANNELS -->
    <div style="background:#ffffff;border-radius:12px;overflow:hidden;margin-bottom:20px;border:1px solid #e5e5e0;box-shadow:0 1px 3px rgba(0,0,0,0.04);">
      <div style="padding:16px 16px 8px;font-size:11px;text-transform:uppercase;letter-spacing:0.5px;color:#1a1a1a;font-weight:700;">Détail par canal</div>
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
    ${bestTiktokSection}

    <!-- FOOTER -->
    <div style="text-align:center;padding:20px 0;font-size:11px;color:#9ca3af;">
      Bandit Dashboard · Rapport automatique
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
  console.log(`[Report] Data collected. E-com CA: ${data.shopify.yesterday.totalCA.toFixed(0)}€, ROAS: ${data.ecommerce.roas.yesterday.toFixed(2)}x, Amazon CA: ${data.amazon.yesterday.ca.toFixed(0)}€`);

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
    subject: `Bandit — ${data.dayName} — E-com ${fmtEur(data.shopify.yesterday.totalCA)} (ROAS ${data.ecommerce.roas.yesterday.toFixed(2)}x) | Amazon ${fmtEur(data.amazon.yesterday.ca)}`,
    html,
  };

  await sgMail.send(msg);
  console.log(`[Report] Email sent to ${emailTo}`);

  return { data, analysis, html };
}

module.exports = { sendReport, collectReportData, generateAnalysis, buildEmailHTML };

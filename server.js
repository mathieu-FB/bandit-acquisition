require('dotenv').config();
const express = require('express');
const fetch = require('node-fetch');
const path = require('path');
const fs = require('fs');
const zlib = require('zlib');
const cron = require('node-cron');
const { GoogleAdsApi, fromMicros } = require('google-ads-api');
const { sendReport } = require('./daily-report');

const app = express();
const PORT = process.env.PORT || 3001;

app.use(express.static(path.join(__dirname, 'public')));
app.use(express.json());

// ============================================================
// HELPERS
// ============================================================

function formatDate(d) {
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, '0');
  const day = String(d.getDate()).padStart(2, '0');
  return `${y}-${m}-${day}`;
}

// Convert a UTC date string to Paris local date (YYYY-MM-DD)
function toParisDate(utcStr) {
  return new Date(utcStr).toLocaleDateString('en-CA', { timeZone: 'Europe/Paris' });
}

function buildDateRange(query) {
  const now = new Date();
  let start, end, compStart, compEnd;

  if (query.start && query.end) {
    start = new Date(query.start);
    end = new Date(query.end);
  } else {
    // Default: last 7 days
    end = new Date(now);
    end.setDate(end.getDate() - 1); // yesterday
    start = new Date(end);
    start.setDate(start.getDate() - 6);
  }

  if (query.comp_start && query.comp_end) {
    compStart = new Date(query.comp_start);
    compEnd = new Date(query.comp_end);
  } else {
    // Default comparison: previous equivalent period
    const daysDiff = Math.round((end - start) / (1000 * 60 * 60 * 24)) + 1;
    compEnd = new Date(start);
    compEnd.setDate(compEnd.getDate() - 1);
    compStart = new Date(compEnd);
    compStart.setDate(compStart.getDate() - daysDiff + 1);
  }

  return {
    start: formatDate(start),
    end: formatDate(end),
    compStart: formatDate(compStart),
    compEnd: formatDate(compEnd),
    startDate: start,
    endDate: end,
    compStartDate: compStart,
    compEndDate: compEnd,
  };
}

// ============================================================
// SHOPIFY API
// ============================================================

async function shopifyRequest(endpoint, params = {}) {
  const store = process.env.SHOPIFY_STORE_URL;
  const token = process.env.SHOPIFY_ACCESS_TOKEN;
  if (!store || !token) return null;

  const qs = new URLSearchParams(params).toString();
  const url = `https://${store}/admin/api/2024-01/${endpoint}.json${qs ? '?' + qs : ''}`;
  const res = await fetch(url, {
    headers: { 'X-Shopify-Access-Token': token, 'Content-Type': 'application/json' },
  });
  if (!res.ok) {
    console.error(`Shopify error ${res.status}: ${await res.text()}`);
    return null;
  }
  return res.json();
}

// Only include DTC sales channels (exclude Amazon, Faire, draft orders, etc.)
const ALLOWED_SOURCES = new Set([
  'web',                                  // Online Store
  'JUST',                                 // JUST
  '295841693697',                         // Bandit x JUST
  'subscription_contract',                // Subscriptions
  'subscription_contract_checkout_one',   // Recharge Subscriptions
]);

async function fetchAllShopifyOrders(start, end) {
  const orders = [];
  let url = `https://${process.env.SHOPIFY_STORE_URL}/admin/api/2024-01/orders.json?` +
    new URLSearchParams({
      created_at_min: `${start}T00:00:00+00:00`,
      created_at_max: `${end}T23:59:59+00:00`,
      status: 'any',
      limit: '250',
      fields: 'id,created_at,total_price,subtotal_price,total_discounts,total_tax,source_name,customer,financial_status,refunds',
    }).toString();

  const token = process.env.SHOPIFY_ACCESS_TOKEN;
  while (url) {
    const res = await fetch(url, {
      headers: { 'X-Shopify-Access-Token': token, 'Content-Type': 'application/json' },
    });
    if (!res.ok) break;
    const data = await res.json();
    // Filter by allowed sales channels
    const filtered = (data.orders || []).filter(o => ALLOWED_SOURCES.has(o.source_name));
    orders.push(...filtered);

    // Pagination via Link header
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

// Compute HT (excl. tax) net sales for an order, minus refunds
function orderNetSalesHT(order) {
  // subtotal_price = line items after discounts, before tax & shipping = HT
  const grossHT = parseFloat(order.subtotal_price || 0);

  // Subtract refund amounts (HT: refund subtotal, not including tax refunds)
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

function computeShopifyMetrics(orders) {
  // Exclude fully refunded and voided orders
  const validOrders = orders.filter(o =>
    o.financial_status !== 'voided'
  );

  // Exclude fully refunded from order count, but keep partially_refunded
  const countableOrders = validOrders.filter(o => o.financial_status !== 'refunded');
  const totalOrders = countableOrders.length;

  // Net sales HT = subtotal (HT) minus refunds (HT) for all non-voided orders
  const netSales = validOrders.reduce((sum, o) => sum + orderNetSalesHT(o), 0);
  const totalDiscounts = validOrders.reduce((sum, o) => sum + parseFloat(o.total_discounts || 0), 0);
  const aov = totalOrders > 0 ? netSales / totalOrders : 0;

  // Repeat customers
  const customerOrders = {};
  countableOrders.forEach(o => {
    if (o.customer && o.customer.id) {
      if (!customerOrders[o.customer.id]) customerOrders[o.customer.id] = [];
      customerOrders[o.customer.id].push(o);
    }
  });

  let repeatCustomerCount = 0;
  let repeatNetSales = 0;
  Object.values(customerOrders).forEach(custOrders => {
    const isRepeat = custOrders.length > 1 ||
      (custOrders[0]?.customer?.orders_count && custOrders[0].customer.orders_count > 1);
    if (isRepeat) {
      repeatCustomerCount++;
      repeatNetSales += custOrders.reduce((s, o) => s + orderNetSalesHT(o), 0);
    }
  });

  const uniqueCustomers = Object.keys(customerOrders).length;
  const repeatRate = uniqueCustomers > 0 ? (repeatCustomerCount / uniqueCustomers) * 100 : 0;

  return { totalOrders, netSales, totalDiscounts, aov, repeatRate, repeatNetSales };
}

// Compute daily breakdown for charts (HT, minus refunds)
function computeDailyShopifyMetrics(orders) {
  const daily = {};
  const validOrders = orders.filter(o => o.financial_status !== 'voided');
  validOrders.forEach(o => {
    const day = toParisDate(o.created_at);
    if (!daily[day]) daily[day] = { sales: 0, orders: 0 };
    daily[day].sales += orderNetSalesHT(o);
    if (o.financial_status !== 'refunded') {
      daily[day].orders += 1;
    }
  });
  return daily;
}

// ============================================================
// META (FACEBOOK) ADS API
// ============================================================

async function fetchMetaAdsData(start, end) {
  const token = process.env.META_ACCESS_TOKEN;
  const accountId = process.env.META_AD_ACCOUNT_ID;
  if (!token || !accountId) return null;

  const fields = 'spend,impressions,clicks,cpm,cpc,ctr,actions,action_values,reach,frequency';
  const url = `https://graph.facebook.com/v19.0/${accountId}/insights?` +
    new URLSearchParams({
      access_token: token,
      fields,
      time_range: JSON.stringify({ since: start, until: end }),
      time_increment: 1,
      level: 'account',
    }).toString();

  const res = await fetch(url);
  if (!res.ok) {
    console.error(`Meta Ads error ${res.status}: ${await res.text()}`);
    return null;
  }

  const json = await res.json();
  return json.data || [];
}

function aggregateMetaData(dailyData) {
  if (!dailyData || !dailyData.length) {
    return { spend: 0, impressions: 0, clicks: 0, purchases: 0, revenue: 0, daily: {} };
  }

  let totalSpend = 0, totalImpressions = 0, totalClicks = 0, totalPurchases = 0, totalRevenue = 0;
  const daily = {};

  dailyData.forEach(row => {
    const spend = parseFloat(row.spend || 0);
    const impressions = parseInt(row.impressions || 0);
    const clicks = parseInt(row.clicks || 0);

    let purchases = 0, revenue = 0;
    if (row.actions) {
      const purchaseAction = row.actions.find(a => a.action_type === 'purchase' || a.action_type === 'omni_purchase');
      if (purchaseAction) purchases = parseInt(purchaseAction.value || 0);
    }
    if (row.action_values) {
      const revenueAction = row.action_values.find(a => a.action_type === 'purchase' || a.action_type === 'omni_purchase');
      if (revenueAction) revenue = parseFloat(revenueAction.value || 0);
    }

    totalSpend += spend;
    totalImpressions += impressions;
    totalClicks += clicks;
    totalPurchases += purchases;
    totalRevenue += revenue;

    const day = row.date_start;
    daily[day] = { spend, impressions, clicks, purchases, revenue };
  });

  return {
    spend: totalSpend,
    impressions: totalImpressions,
    clicks: totalClicks,
    purchases: totalPurchases,
    revenue: totalRevenue,
    cpm: totalImpressions > 0 ? (totalSpend / totalImpressions) * 1000 : 0,
    roas: totalSpend > 0 ? totalRevenue / totalSpend : 0,
    daily,
  };
}

// ============================================================
// GOOGLE ADS API (gRPC via google-ads-api library)
// ============================================================

function getGoogleAdsCustomer() {
  const clientId = process.env.GOOGLE_ADS_CLIENT_ID;
  const clientSecret = process.env.GOOGLE_ADS_CLIENT_SECRET;
  const devToken = process.env.GOOGLE_ADS_DEVELOPER_TOKEN;
  const refreshToken = process.env.GOOGLE_ADS_REFRESH_TOKEN;
  const customerId = (process.env.GOOGLE_ADS_CUSTOMER_ID || '').replace(/-/g, '');
  if (!clientId || !clientSecret || !devToken || !refreshToken || !customerId) return null;

  const client = new GoogleAdsApi({
    client_id: clientId,
    client_secret: clientSecret,
    developer_token: devToken,
  });

  return client.Customer({
    customer_id: customerId,
    refresh_token: refreshToken,
  });
}

async function fetchGoogleAdsData(start, end) {
  const customer = getGoogleAdsCustomer();
  if (!customer) return null;

  try {
    const results = await customer.query(`
      SELECT
        segments.date,
        metrics.cost_micros,
        metrics.impressions,
        metrics.clicks,
        metrics.conversions,
        metrics.conversions_value
      FROM campaign
      WHERE segments.date BETWEEN '${start}' AND '${end}'
      ORDER BY segments.date
    `);
    return results;
  } catch (err) {
    console.error('Google Ads error:', err.errors?.[0]?.message || err.message);
    return null;
  }
}

function aggregateGoogleData(rawData) {
  if (!rawData || !rawData.length) {
    return { spend: 0, impressions: 0, clicks: 0, conversions: 0, revenue: 0, daily: {} };
  }

  let totalSpend = 0, totalImpressions = 0, totalClicks = 0, totalConversions = 0, totalRevenue = 0;
  const daily = {};

  rawData.forEach(row => {
    const date = row.segments?.date;
    const spend = fromMicros(row.metrics?.cost_micros || 0);
    const impressions = parseInt(row.metrics?.impressions || 0);
    const clicks = parseInt(row.metrics?.clicks || 0);
    const conversions = parseFloat(row.metrics?.conversions || 0);
    const revenue = parseFloat(row.metrics?.conversions_value || 0);

    totalSpend += spend;
    totalImpressions += impressions;
    totalClicks += clicks;
    totalConversions += conversions;
    totalRevenue += revenue;

    if (date) {
      if (!daily[date]) daily[date] = { spend: 0, impressions: 0, clicks: 0, conversions: 0, revenue: 0 };
      daily[date].spend += spend;
      daily[date].impressions += impressions;
      daily[date].clicks += clicks;
      daily[date].conversions += conversions;
      daily[date].revenue += revenue;
    }
  });

  return {
    spend: totalSpend,
    impressions: totalImpressions,
    clicks: totalClicks,
    conversions: totalConversions,
    revenue: totalRevenue,
    cpm: totalImpressions > 0 ? (totalSpend / totalImpressions) * 1000 : 0,
    roas: totalSpend > 0 ? totalRevenue / totalSpend : 0,
    daily,
  };
}

// ============================================================
// TIKTOK ADS API
// ============================================================

async function fetchTikTokAdsData(start, end) {
  const token = process.env.TIKTOK_ACCESS_TOKEN;
  const advertiserId = process.env.TIKTOK_ADVERTISER_ID;
  if (!token || !advertiserId) return null;

  const url = 'https://business-api.tiktok.com/open_api/v1.3/report/integrated/get/';
  const params = new URLSearchParams({
    advertiser_id: advertiserId,
    report_type: 'BASIC',
    data_level: 'AUCTION_ADVERTISER',
    dimensions: JSON.stringify(['stat_time_day']),
    metrics: JSON.stringify([
      'spend', 'impressions', 'clicks', 'cpm', 'cpc', 'ctr',
      'complete_payment', 'total_complete_payment_rate',
      'complete_payment_roas', 'value_per_complete_payment'
    ]),
    start_date: start,
    end_date: end,
    page: '1',
    page_size: '100',
  });

  const res = await fetch(`${url}?${params.toString()}`, {
    headers: { 'Access-Token': token },
  });

  if (!res.ok) {
    console.error(`TikTok Ads error ${res.status}: ${await res.text()}`);
    return null;
  }

  const json = await res.json();
  if (json.code !== 0) {
    console.error('TikTok API error:', json.message);
    return null;
  }

  return json.data?.list || [];
}

function aggregateTikTokData(rawData) {
  if (!rawData || !rawData.length) {
    return { spend: 0, impressions: 0, clicks: 0, purchases: 0, revenue: 0, daily: {} };
  }

  let totalSpend = 0, totalImpressions = 0, totalClicks = 0, totalPurchases = 0, totalRevenue = 0;
  const daily = {};

  rawData.forEach(row => {
    const metrics = row.metrics || {};
    const dims = row.dimensions || {};
    const spend = parseFloat(metrics.spend || 0);
    const impressions = parseInt(metrics.impressions || 0);
    const clicks = parseInt(metrics.clicks || 0);
    const purchases = parseInt(metrics.complete_payment || 0);
    // Revenue = purchases * value_per_purchase or from ROAS
    const valuePerPurchase = parseFloat(metrics.value_per_complete_payment || 0);
    const revenue = purchases * valuePerPurchase;

    totalSpend += spend;
    totalImpressions += impressions;
    totalClicks += clicks;
    totalPurchases += purchases;
    totalRevenue += revenue;

    // TikTok returns "2026-04-16 00:00:00" — normalize to "2026-04-16"
    const day = (dims.stat_time_day || '').split(' ')[0];
    if (day) {
      daily[day] = { spend, impressions, clicks, purchases, revenue };
    }
  });

  return {
    spend: totalSpend,
    impressions: totalImpressions,
    clicks: totalClicks,
    purchases: totalPurchases,
    revenue: totalRevenue,
    cpm: totalImpressions > 0 ? (totalSpend / totalImpressions) * 1000 : 0,
    roas: totalSpend > 0 ? totalRevenue / totalSpend : 0,
    daily,
  };
}

// ============================================================
// DAILY CACHE — stores per-day data to avoid re-fetching
// ============================================================

const dailyCache = {}; // { "2026-01-01": { shopify, meta, google, tiktok } }
let lastCacheInvalidation = null;

function invalidateFreshDays() {
  const today = formatDate(new Date());
  // Always clear today (data still incoming)
  delete dailyCache[today];
  if (lastCacheInvalidation !== today) {
    // New day — also invalidate yesterday (final numbers may have changed)
    const y = new Date();
    y.setDate(y.getDate() - 1);
    delete dailyCache[formatDate(y)];
    lastCacheInvalidation = today;
    console.log(`[Cache] New day: invalidated today + yesterday. ${Object.keys(dailyCache).length} days in cache.`);
  }
}

async function ensureCached(startStr, endStr) {
  invalidateFreshDays();

  const uncached = [];
  const d = new Date(startStr + 'T12:00:00');
  const end = new Date(endStr + 'T12:00:00');

  while (d <= end) {
    const day = formatDate(d);
    if (!dailyCache[day]) {
      uncached.push(day);
    }
    d.setDate(d.getDate() + 1);
  }

  if (uncached.length === 0) return;

  const fetchStart = uncached[0];
  const fetchEnd = uncached[uncached.length - 1];
  console.log(`[Cache] Fetching ${uncached.length} days: ${fetchStart} → ${fetchEnd}`);

  const [shopifyOrders, metaRaw, googleRaw, tiktokRaw] = await Promise.all([
    fetchAllShopifyOrders(fetchStart, fetchEnd),
    fetchMetaAdsData(fetchStart, fetchEnd),
    fetchGoogleAdsData(fetchStart, fetchEnd),
    fetchTikTokAdsData(fetchStart, fetchEnd),
  ]);

  // Split Shopify orders by day
  const shopifyByDay = {};
  (shopifyOrders || []).forEach(order => {
    const day = toParisDate(order.created_at);
    if (!shopifyByDay[day]) shopifyByDay[day] = [];
    shopifyByDay[day].push(order);
  });

  const metaAgg = aggregateMetaData(metaRaw);
  const googleAgg = aggregateGoogleData(googleRaw);
  const tiktokAgg = aggregateTikTokData(tiktokRaw);

  uncached.forEach(day => {
    const dayOrders = shopifyByDay[day] || [];
    const valid = dayOrders.filter(o => o.financial_status !== 'voided');
    const countable = valid.filter(o => o.financial_status !== 'refunded');

    const customers = {};
    countable.forEach(o => {
      if (o.customer?.id) {
        if (!customers[o.customer.id]) {
          customers[o.customer.id] = { orders: 0, netSales: 0, globalCount: o.customer.orders_count || 1 };
        }
        customers[o.customer.id].orders++;
        customers[o.customer.id].netSales += orderNetSalesHT(o);
      }
    });

    dailyCache[day] = {
      shopify: {
        netSales: valid.reduce((s, o) => s + orderNetSalesHT(o), 0),
        orders: countable.length,
        discounts: valid.reduce((s, o) => s + parseFloat(o.total_discounts || 0), 0),
        customers,
      },
      meta: metaAgg.daily[day] || { spend: 0, impressions: 0, clicks: 0, purchases: 0, revenue: 0 },
      google: googleAgg.daily[day] || { spend: 0, impressions: 0, clicks: 0, conversions: 0, revenue: 0 },
      tiktok: tiktokAgg.daily[day] || { spend: 0, impressions: 0, clicks: 0, purchases: 0, revenue: 0 },
    };
  });

  console.log(`[Cache] Done. ${Object.keys(dailyCache).length} days cached.`);
}

function aggregateFromCache(startStr, endStr) {
  const allDates = [];
  const d = new Date(startStr + 'T12:00:00');
  const end = new Date(endStr + 'T12:00:00');
  while (d <= end) {
    allDates.push(formatDate(d));
    d.setDate(d.getDate() + 1);
  }

  let netSales = 0, totalOrders = 0, totalDiscounts = 0;
  const mergedCustomers = {};

  const metaTotals = { spend: 0, impressions: 0, clicks: 0, purchases: 0, revenue: 0 };
  const googleTotals = { spend: 0, impressions: 0, clicks: 0, conversions: 0, revenue: 0 };
  const tiktokTotals = { spend: 0, impressions: 0, clicks: 0, purchases: 0, revenue: 0 };

  const metaDaily = {}, googleDaily = {}, tiktokDaily = {}, shopifyDaily = {};

  allDates.forEach(day => {
    const c = dailyCache[day];
    if (!c) return;

    netSales += c.shopify.netSales;
    totalOrders += c.shopify.orders;
    totalDiscounts += c.shopify.discounts;
    shopifyDaily[day] = { sales: c.shopify.netSales, orders: c.shopify.orders };

    if (c.shopify.customers) {
      Object.entries(c.shopify.customers).forEach(([id, data]) => {
        if (!mergedCustomers[id]) mergedCustomers[id] = { orders: 0, netSales: 0, globalCount: data.globalCount };
        mergedCustomers[id].orders += data.orders;
        mergedCustomers[id].netSales += data.netSales;
      });
    }

    ['spend', 'impressions', 'clicks', 'purchases', 'revenue'].forEach(k => { metaTotals[k] += c.meta[k] || 0; });
    ['spend', 'impressions', 'clicks', 'conversions', 'revenue'].forEach(k => { googleTotals[k] += c.google[k] || 0; });
    ['spend', 'impressions', 'clicks', 'purchases', 'revenue'].forEach(k => { tiktokTotals[k] += c.tiktok[k] || 0; });

    metaDaily[day] = c.meta;
    googleDaily[day] = c.google;
    tiktokDaily[day] = c.tiktok;
  });

  // Repeat rate
  let repeatCount = 0, repeatNetSales = 0;
  const unique = Object.keys(mergedCustomers).length;
  Object.values(mergedCustomers).forEach(cust => {
    if (cust.orders > 1 || cust.globalCount > 1) {
      repeatCount++;
      repeatNetSales += cust.netSales;
    }
  });

  const aov = totalOrders > 0 ? netSales / totalOrders : 0;
  const repeatRate = unique > 0 ? (repeatCount / unique) * 100 : 0;

  const metaCpm = metaTotals.impressions > 0 ? (metaTotals.spend / metaTotals.impressions) * 1000 : 0;
  const metaRoas = metaTotals.spend > 0 ? metaTotals.revenue / metaTotals.spend : 0;
  const googleCpm = googleTotals.impressions > 0 ? (googleTotals.spend / googleTotals.impressions) * 1000 : 0;
  const googleRoas = googleTotals.spend > 0 ? googleTotals.revenue / googleTotals.spend : 0;
  const tiktokCpm = tiktokTotals.impressions > 0 ? (tiktokTotals.spend / tiktokTotals.impressions) * 1000 : 0;
  const tiktokRoas = tiktokTotals.spend > 0 ? tiktokTotals.revenue / tiktokTotals.spend : 0;

  return {
    shopify: { netSales, totalOrders, totalDiscounts, aov, repeatRate, repeatNetSales },
    meta: { ...metaTotals, cpm: metaCpm, roas: metaRoas, daily: metaDaily },
    google: { ...googleTotals, cpm: googleCpm, roas: googleRoas, daily: googleDaily },
    tiktok: { ...tiktokTotals, cpm: tiktokCpm, roas: tiktokRoas, daily: tiktokDaily },
    shopifyDaily,
    allDates,
  };
}

// ============================================================
// MAIN DASHBOARD ENDPOINT
// ============================================================

app.get('/api/dashboard', async (req, res) => {
  try {
    const dates = buildDateRange(req.query);

    // Ensure both periods are cached, then aggregate from cache
    await Promise.all([
      ensureCached(dates.start, dates.end),
      ensureCached(dates.compStart, dates.compEnd),
    ]);

    const current = aggregateFromCache(dates.start, dates.end);
    const comp = aggregateFromCache(dates.compStart, dates.compEnd);

    const { shopify, meta, google, tiktok, shopifyDaily, allDates } = current;
    const shopifyPrev = comp.shopify;

    const totalSpend = meta.spend + google.spend + tiktok.spend;
    const totalSpendPrev = comp.meta.spend + comp.google.spend + comp.tiktok.spend;
    const totalPurchases = (meta.purchases || 0) + (google.conversions || 0) + (tiktok.purchases || 0);
    const totalPurchasesPrev = (comp.meta.purchases || 0) + (comp.google.conversions || 0) + (comp.tiktok.purchases || 0);

    const percentMarketing = shopify.netSales > 0 ? (totalSpend / shopify.netSales) * 100 : 0;
    const percentMarketingPrev = shopifyPrev.netSales > 0 ? (totalSpendPrev / shopifyPrev.netSales) * 100 : 0;
    const blendedCac = totalPurchases > 0 ? totalSpend / totalPurchases : 0;
    const blendedCacPrev = totalPurchasesPrev > 0 ? totalSpendPrev / totalPurchasesPrev : 0;
    const blendedRoas = totalSpend > 0 ? shopify.netSales / totalSpend : 0;
    const blendedRoasPrev = totalSpendPrev > 0 ? shopifyPrev.netSales / totalSpendPrev : 0;

    // Build daily chart series
    const dailySpendByChannel = allDates.map(day => ({
      date: day, meta: meta.daily[day]?.spend || 0, google: google.daily[day]?.spend || 0, tiktok: tiktok.daily[day]?.spend || 0,
    }));

    const dailyRoasByChannel = allDates.map(day => {
      const ms = meta.daily[day]?.spend || 0, mr = meta.daily[day]?.revenue || 0;
      const gs = google.daily[day]?.spend || 0, gr = google.daily[day]?.revenue || 0;
      const ts = tiktok.daily[day]?.spend || 0, tr = tiktok.daily[day]?.revenue || 0;
      return { date: day, meta: ms > 0 ? mr / ms : 0, google: gs > 0 ? gr / gs : 0, tiktok: ts > 0 ? tr / ts : 0 };
    });

    const dailyCpmByChannel = allDates.map(day => {
      const mi = meta.daily[day]?.impressions || 0, gi = google.daily[day]?.impressions || 0, ti = tiktok.daily[day]?.impressions || 0;
      return {
        date: day,
        meta: mi > 0 ? ((meta.daily[day]?.spend || 0) / mi) * 1000 : 0,
        google: gi > 0 ? ((google.daily[day]?.spend || 0) / gi) * 1000 : 0,
        tiktok: ti > 0 ? ((tiktok.daily[day]?.spend || 0) / ti) * 1000 : 0,
      };
    });

    const dailySales = allDates.map(day => ({ date: day, sales: shopifyDaily[day]?.sales || 0, orders: shopifyDaily[day]?.orders || 0 }));
    const dailyMarketingCosts = allDates.map(day => ({
      date: day, total: (meta.daily[day]?.spend || 0) + (google.daily[day]?.spend || 0) + (tiktok.daily[day]?.spend || 0),
    }));
    const dailyPercentMarketing = allDates.map(day => {
      const sales = shopifyDaily[day]?.sales || 0;
      const spend = (meta.daily[day]?.spend || 0) + (google.daily[day]?.spend || 0) + (tiktok.daily[day]?.spend || 0);
      return { date: day, percent: sales > 0 ? (spend / sales) * 100 : 0 };
    });

    res.json({
      dates: { start: dates.start, end: dates.end, compStart: dates.compStart, compEnd: dates.compEnd },
      kpis: {
        netSales: { current: shopify.netSales, previous: shopifyPrev.netSales },
        marketingCosts: { current: totalSpend, previous: totalSpendPrev },
        percentMarketing: { current: percentMarketing, previous: percentMarketingPrev },
        orders: { current: shopify.totalOrders, previous: shopifyPrev.totalOrders },
        aov: { current: shopify.aov, previous: shopifyPrev.aov },
        discountCodes: { current: shopify.totalDiscounts, previous: shopifyPrev.totalDiscounts },
        repeatRate: { current: shopify.repeatRate, previous: shopifyPrev.repeatRate },
        repeatNetSales: { current: shopify.repeatNetSales, previous: shopifyPrev.repeatNetSales },
        blendedCac: { current: blendedCac, previous: blendedCacPrev },
        blendedRoas: { current: blendedRoas, previous: blendedRoasPrev },
        blendedCpm: { current: 0, previous: 0 },
      },
      channels: {
        meta: { spend: meta.spend, roas: meta.roas, cpm: meta.cpm, impressions: meta.impressions, clicks: meta.clicks },
        google: { spend: google.spend, roas: google.roas, cpm: google.cpm, impressions: google.impressions, clicks: google.clicks },
        tiktok: { spend: tiktok.spend, roas: tiktok.roas, cpm: tiktok.cpm, impressions: tiktok.impressions, clicks: tiktok.clicks },
      },
      charts: { dailySpendByChannel, dailyRoasByChannel, dailyCpmByChannel, dailySales, dailyMarketingCosts, dailyPercentMarketing },
    });
  } catch (err) {
    console.error('Dashboard error:', err);
    res.status(500).json({ error: err.message });
  }
});

// ============================================================
// OBJECTIVES — Monthly & Quarterly targets
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

// ============================================================
// PRODUCT CATEGORIES — % of CA objective
// ============================================================

const PRODUCT_CATEGORIES = [
  {
    name: 'Fontaines & Distributeurs',
    pct: 65,
    types: ['Fontaine à eau', 'Distributeur de croquettes', 'Pièce détachée fontaine', 'Consommable'],
    color: '#0984e3',
  },
  {
    name: 'Sellerie',
    pct: 20,
    types: ['Collier pour chien', 'Collier pour chat', 'Bandana pour chien', 'Bandana pour chat', 'Harnais pour chien', 'Harnais pour chat', 'Laisse pour chien', 'Pochettes et sachets ramasse-crottes', 'Kit'],
    color: '#6c5ce7',
  },
  {
    name: 'Box & Litières',
    pct: 10,
    types: ['Jouet pour chien', 'Box Jouet', 'Bac à litière pour chat'],
    color: '#00b894',
  },
  {
    name: 'Médailles',
    pct: 5,
    types: ['Médaille pour chien', 'Médaille pour chat'],
    color: '#fdcb6e',
  },
];

// Product ID → product_type cache
let productTypeMap = null;
let productTypeMapExpiry = 0;

async function getProductTypeMap() {
  if (productTypeMap && Date.now() < productTypeMapExpiry) return productTypeMap;

  const map = {};
  let url = `https://${process.env.SHOPIFY_STORE_URL}/admin/api/2024-01/products.json?` +
    new URLSearchParams({ limit: '250', fields: 'id,product_type' }).toString();
  const token = process.env.SHOPIFY_ACCESS_TOKEN;

  while (url) {
    const res = await fetch(url, {
      headers: { 'X-Shopify-Access-Token': token, 'Content-Type': 'application/json' },
    });
    if (!res.ok) break;
    const data = await res.json();
    (data.products || []).forEach(p => { map[p.id] = (p.product_type || '').trim(); });

    const link = res.headers.get('link');
    if (link && link.includes('rel="next"')) {
      const match = link.match(/<([^>]+)>;\s*rel="next"/);
      url = match ? match[1] : null;
    } else {
      url = null;
    }
  }

  productTypeMap = map;
  productTypeMapExpiry = Date.now() + 3600 * 1000; // cache 1h
  console.log(`[ProductTypeMap] Cached ${Object.keys(map).length} products`);
  return map;
}

async function fetchOrdersWithLineItems(start, end) {
  const orders = [];
  let url = `https://${process.env.SHOPIFY_STORE_URL}/admin/api/2024-01/orders.json?` +
    new URLSearchParams({
      created_at_min: `${start}T00:00:00+00:00`,
      created_at_max: `${end}T23:59:59+00:00`,
      status: 'any',
      limit: '250',
      fields: 'id,created_at,financial_status,source_name,line_items,refunds',
    }).toString();
  const token = process.env.SHOPIFY_ACCESS_TOKEN;

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

app.get('/api/objectives', async (req, res) => {
  try {
    const now = new Date();
    const year = now.getFullYear();
    const month = now.getMonth() + 1;
    const yesterday = new Date(now);
    yesterday.setDate(yesterday.getDate() - 1);

    const obj = getObjective(year, month);
    if (!obj) return res.json({ configured: false });

    const monthStart = `${year}-${String(month).padStart(2, '0')}-01`;
    const qMonths = Object.keys(obj.quarterObj.months).map(Number).sort((a, b) => a - b);
    const quarterStart = `${year}-${String(qMonths[0]).padStart(2, '0')}-01`;

    const todayStr = formatDate(now);
    const daysInMonth = new Date(year, month, 0).getDate();
    const daysElapsedMonth = now.getDate();
    const quarterStartDate = new Date(`${quarterStart}T00:00:00`);
    const quarterEndDate = new Date(year, qMonths[qMonths.length - 1], 0);
    const totalDaysQuarter = Math.round((quarterEndDate - quarterStartDate) / (1000 * 60 * 60 * 24)) + 1;
    const daysElapsedQuarter = Math.round((now - quarterStartDate) / (1000 * 60 * 60 * 24)) + 1;

    // Use cache for QTD (which includes MTD) + today
    await ensureCached(quarterStart, todayStr);

    // Daily stats (today)
    const dayData = aggregateFromCache(todayStr, todayStr);
    const daySpend = dayData.meta.spend + dayData.google.spend + dayData.tiktok.spend;
    const dayCA = dayData.shopify.netSales;
    const dayRatio = dayCA > 0 ? (daySpend / dayCA) * 100 : 0;
    const dailyCATarget = obj.monthObj.ca / daysInMonth;
    const dayProgressCA = dailyCATarget > 0 ? (dayCA / dailyCATarget) * 100 : 0;

    const mtd = aggregateFromCache(monthStart, todayStr);
    const spendMTD = mtd.meta.spend + mtd.google.spend + mtd.tiktok.spend;
    const ratioMTD = mtd.shopify.netSales > 0 ? (spendMTD / mtd.shopify.netSales) * 100 : 0;
    const projectedCA_month = daysElapsedMonth > 0 ? (mtd.shopify.netSales / daysElapsedMonth) * daysInMonth : 0;
    const projectedSpend_month = daysElapsedMonth > 0 ? (spendMTD / daysElapsedMonth) * daysInMonth : 0;
    const projectedRatio_month = projectedCA_month > 0 ? (projectedSpend_month / projectedCA_month) * 100 : 0;

    const qtd = aggregateFromCache(quarterStart, todayStr);
    const spendQTD = qtd.meta.spend + qtd.google.spend + qtd.tiktok.spend;
    const ratioQTD = qtd.shopify.netSales > 0 ? (spendQTD / qtd.shopify.netSales) * 100 : 0;
    const projectedCA_quarter = daysElapsedQuarter > 0 ? (qtd.shopify.netSales / daysElapsedQuarter) * totalDaysQuarter : 0;
    const projectedSpend_quarter = daysElapsedQuarter > 0 ? (spendQTD / daysElapsedQuarter) * totalDaysQuarter : 0;
    const projectedRatio_quarter = projectedCA_quarter > 0 ? (projectedSpend_quarter / projectedCA_quarter) * 100 : 0;

    const monthNames = ['', 'Janvier', 'Février', 'Mars', 'Avril', 'Mai', 'Juin', 'Juillet', 'Août', 'Septembre', 'Octobre', 'Novembre', 'Décembre'];

    const dayNames = ['Dimanche', 'Lundi', 'Mardi', 'Mercredi', 'Jeudi', 'Vendredi', 'Samedi'];
    const dayLabel = `${dayNames[now.getDay()]} ${now.getDate()} ${monthNames[month]}`;

    res.json({
      configured: true,
      day: {
        label: dayLabel,
        currentCA: dayCA,
        currentSpend: daySpend,
        currentRatio: dayRatio,
        dailyCATarget,
        progressCA: dayProgressCA,
        objectiveRatio: obj.monthObj.ratio,
      },
      month: {
        label: monthNames[month] + ' ' + year,
        objectiveCA: obj.monthObj.ca, objectiveRatio: obj.monthObj.ratio,
        currentCA: mtd.shopify.netSales, currentSpend: spendMTD, currentRatio: ratioMTD,
        projectedCA: projectedCA_month, projectedRatio: projectedRatio_month,
        progressCA: (mtd.shopify.netSales / obj.monthObj.ca) * 100,
        daysElapsed: daysElapsedMonth, daysTotal: daysInMonth,
      },
      quarter: {
        label: obj.quarter + ' ' + year,
        objectiveCA: obj.quarterObj.ca, objectiveRatio: obj.quarterObj.ratio,
        currentCA: qtd.shopify.netSales, currentSpend: spendQTD, currentRatio: ratioQTD,
        projectedCA: projectedCA_quarter, projectedRatio: projectedRatio_quarter,
        progressCA: (qtd.shopify.netSales / obj.quarterObj.ca) * 100,
        daysElapsed: daysElapsedQuarter, daysTotal: totalDaysQuarter,
      },
    });
  } catch (err) {
    console.error('Objectives error:', err);
    res.status(500).json({ error: err.message });
  }
});

// ============================================================
// PRODUCT BREAKDOWN BY CATEGORY
// ============================================================

app.get('/api/product-breakdown', async (req, res) => {
  try {
    if (!process.env.SHOPIFY_STORE_URL || !process.env.SHOPIFY_ACCESS_TOKEN) {
      return res.json({ configured: false });
    }

    const now = new Date();
    const year = now.getFullYear();
    const month = now.getMonth() + 1;
    const todayStr = formatDate(now);
    const daysInMonth = new Date(year, month, 0).getDate();
    const daysElapsedMonth = now.getDate();
    const monthStart = `${year}-${String(month).padStart(2, '0')}-01`;

    // Quarter dates
    const obj = getObjective(year, month);
    let quarterStart = monthStart;
    let totalDaysQuarter = daysInMonth;
    let daysElapsedQuarter = daysElapsedMonth;

    if (obj) {
      const qMonths = Object.keys(obj.quarterObj.months).map(Number).sort((a, b) => a - b);
      quarterStart = `${year}-${String(qMonths[0]).padStart(2, '0')}-01`;
      const quarterStartDate = new Date(`${quarterStart}T00:00:00`);
      const quarterEndDate = new Date(year, qMonths[qMonths.length - 1], 0);
      totalDaysQuarter = Math.round((quarterEndDate - quarterStartDate) / (1000 * 60 * 60 * 24)) + 1;
      daysElapsedQuarter = Math.round((now - quarterStartDate) / (1000 * 60 * 60 * 24)) + 1;
    }

    const [typeMap, orders] = await Promise.all([
      getProductTypeMap(),
      fetchOrdersWithLineItems(monthStart, todayStr),
    ]);

    // Build refund map: line_item_id → refunded quantity
    const refundMap = {};
    orders.forEach(order => {
      if (order.refunds) {
        order.refunds.forEach(refund => {
          (refund.refund_line_items || []).forEach(rli => {
            refundMap[rli.line_item_id] = (refundMap[rli.line_item_id] || 0) + rli.quantity;
          });
        });
      }
    });

    // Aggregate by product type
    const typeAgg = {}; // { product_type: { units, ca } }
    let totalUnits = 0, totalCA = 0;

    orders.forEach(order => {
      if (order.financial_status === 'voided') return;
      (order.line_items || []).forEach(li => {
        const productType = typeMap[li.product_id] || 'Autre';
        const refunded = refundMap[li.id] || 0;
        const netQty = li.quantity - refunded;
        if (netQty <= 0) return;

        // CA HT = price × qty (price is already after discount per item)
        const ca = parseFloat(li.price || 0) * netQty;

        if (!typeAgg[productType]) typeAgg[productType] = { units: 0, ca: 0 };
        typeAgg[productType].units += netQty;
        typeAgg[productType].ca += ca;
        totalUnits += netQty;
        totalCA += ca;
      });
    });

    // Aggregate into categories
    const categories = PRODUCT_CATEGORIES.map(cat => {
      let units = 0, ca = 0;
      cat.types.forEach(t => {
        if (typeAgg[t]) {
          units += typeAgg[t].units;
          ca += typeAgg[t].ca;
        }
      });
      const objectiveCA = obj ? obj.monthObj.ca * (cat.pct / 100) : 0;
      const progressCA = objectiveCA > 0 ? (ca / objectiveCA) * 100 : 0;
      const projectedCA = daysElapsedMonth > 0 ? (ca / daysElapsedMonth) * daysInMonth : 0;
      return {
        name: cat.name,
        color: cat.color,
        pct: cat.pct,
        units,
        ca,
        pctOfTotal: totalCA > 0 ? (ca / totalCA) * 100 : 0,
        objectiveCA,
        progressCA,
        projectedCA,
      };
    });

    // "Autres" — everything not in a category
    const categorizedTypes = new Set(PRODUCT_CATEGORIES.flatMap(c => c.types));
    let autresUnits = 0, autresCA = 0;
    Object.entries(typeAgg).forEach(([type, data]) => {
      if (!categorizedTypes.has(type)) {
        autresUnits += data.units;
        autresCA += data.ca;
      }
    });
    if (autresCA > 0) {
      categories.push({
        name: 'Autres',
        color: '#b2bec3',
        pct: 0,
        units: autresUnits,
        ca: autresCA,
        pctOfTotal: totalCA > 0 ? (autresCA / totalCA) * 100 : 0,
        objectiveCA: 0,
        progressCA: 0,
        projectedCA: 0,
      });
    }

    // All product types for the detailed breakdown
    const allTypes = Object.entries(typeAgg)
      .map(([type, data]) => ({
        type,
        units: data.units,
        ca: data.ca,
        pctOfTotal: totalCA > 0 ? (data.ca / totalCA) * 100 : 0,
      }))
      .sort((a, b) => b.ca - a.ca);

    const monthNames = ['', 'Janvier', 'Février', 'Mars', 'Avril', 'Mai', 'Juin', 'Juillet', 'Août', 'Septembre', 'Octobre', 'Novembre', 'Décembre'];

    res.json({
      configured: true,
      period: {
        label: `${monthNames[month]} ${year}`,
        start: monthStart,
        end: todayStr,
        daysElapsed: daysElapsedMonth,
        daysTotal: daysInMonth,
      },
      totalUnits,
      totalCA,
      categories,
      allTypes,
    });
  } catch (err) {
    console.error('Product breakdown error:', err);
    res.status(500).json({ error: err.message });
  }
});

// ============================================================
// AMAZON SP-API + ADS API
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
  if (!res.ok) { console.error('[Amazon] Token error:', await res.text()); return null; }
  const json = await res.json();
  return json.access_token;
}

async function fetchAmazonSalesMetrics(start, end) {
  const token = await getAmazonAccessToken();
  const marketplaceId = process.env.AMAZON_MARKETPLACE_ID || 'A13V1IB3VIYZZH';
  if (!token) return null;

  const url = `https://sellingpartnerapi-eu.amazon.com/sales/v1/orderMetrics?` +
    new URLSearchParams({
      marketplaceIds: marketplaceId,
      interval: `${start}T00:00:00+00:00--${end}T23:59:59+00:00`,
      granularity: 'Day',
    }).toString();

  const res = await fetch(url, {
    headers: { 'x-amz-access-token': token, 'Content-Type': 'application/json' },
  });
  if (!res.ok) { console.error('[Amazon] Sales error:', res.status, await res.text()); return null; }
  const json = await res.json();
  return json.payload || [];
}

// ============================================================
// AMAZON PRODUCT STATS — Persistent JSON storage + incremental fetch
// ============================================================

const AMAZON_DATA_DIR = process.env.AMAZON_DATA_DIR || path.join(__dirname, 'data');
const AMAZON_PRODUCTS_FILE = path.join(AMAZON_DATA_DIR, 'amazon-products.json');

function loadAmazonProductData() {
  try {
    if (fs.existsSync(AMAZON_PRODUCTS_FILE)) {
      return JSON.parse(fs.readFileSync(AMAZON_PRODUCTS_FILE, 'utf8'));
    }
  } catch (err) {
    console.error('[Amazon] Error loading product data:', err.message);
  }
  return { months: {}, lastFetch: {} };
}

function saveAmazonProductData(data) {
  try {
    if (!fs.existsSync(AMAZON_DATA_DIR)) fs.mkdirSync(AMAZON_DATA_DIR, { recursive: true });
    fs.writeFileSync(AMAZON_PRODUCTS_FILE, JSON.stringify(data, null, 2));
  } catch (err) {
    console.error('[Amazon] Error saving product data:', err.message);
  }
}

// In-memory cache loaded from file on startup
let amazonProductData = loadAmazonProductData();

async function fetchAmazonTopProducts(start, end) {
  const token = await getAmazonAccessToken();
  const marketplaceId = process.env.AMAZON_MARKETPLACE_ID || 'A13V1IB3VIYZZH';
  if (!token) return [];

  // Month key for storage (e.g. "2026-04")
  const monthKey = start.substring(0, 7);

  // Determine where to start fetching from (incremental)
  const lastFetchTime = amazonProductData.lastFetch[monthKey] || null;
  const createdAfter = lastFetchTime || `${start}T00:00:00Z`;
  const twoMinAgo = new Date(Date.now() - 3 * 60 * 1000).toISOString();
  const createdBefore = `${end}T23:59:59Z` < twoMinAgo ? `${end}T23:59:59Z` : twoMinAgo;

  // Skip if last fetch was less than 10 min ago
  if (lastFetchTime) {
    const lastFetchDate = new Date(lastFetchTime);
    if (Date.now() - lastFetchDate.getTime() < 10 * 60 * 1000) {
      const monthData = amazonProductData.months[monthKey] || {};
      return Object.values(monthData).sort((a, b) => b.ca - a.ca).slice(0, 5);
    }
  }

  // Fetch all new orders since last fetch (paginate)
  const allOrders = [];
  let nextToken = null;
  let page = 0;
  do {
    const params = nextToken
      ? { NextToken: nextToken }
      : {
          MarketplaceIds: marketplaceId,
          CreatedAfter: createdAfter,
          CreatedBefore: createdBefore,
          OrderStatuses: 'Shipped,Unshipped',
          MaxResultsPerPage: '100',
        };
    const url = `https://sellingpartnerapi-eu.amazon.com/orders/v0/orders?` +
      new URLSearchParams(params).toString();
    const res = await fetch(url, {
      headers: { 'x-amz-access-token': token, 'Content-Type': 'application/json' },
    });
    if (!res.ok) { console.error('[Amazon] Orders error:', res.status, await res.text()); break; }
    const json = await res.json();
    allOrders.push(...(json.payload?.Orders || []));
    nextToken = json.payload?.NextToken || null;
    page++;
  } while (nextToken && page < 20); // max 2000 orders per fetch

  console.log(`[Amazon] Incremental fetch: ${allOrders.length} new orders since ${createdAfter}`);

  if (allOrders.length === 0) {
    amazonProductData.lastFetch[monthKey] = createdBefore;
    saveAmazonProductData(amazonProductData);
    const monthData = amazonProductData.months[monthKey] || {};
    return Object.values(monthData).sort((a, b) => b.ca - a.ca).slice(0, 5);
  }

  // Fetch order items in parallel batches of 5
  const productAgg = amazonProductData.months[monthKey] || {};
  const processedOrderIds = new Set(Object.keys(amazonProductData.months[monthKey + '_orderIds'] || {}));

  for (let i = 0; i < allOrders.length; i += 5) {
    const batch = allOrders.slice(i, i + 5).filter(o => !processedOrderIds.has(o.AmazonOrderId));
    if (batch.length === 0) continue;

    const results = await Promise.all(batch.map(async (order) => {
      const itemsUrl = `https://sellingpartnerapi-eu.amazon.com/orders/v0/orders/${order.AmazonOrderId}/orderItems`;
      try {
        const itemsRes = await fetch(itemsUrl, {
          headers: { 'x-amz-access-token': token, 'Content-Type': 'application/json' },
        });
        if (itemsRes.status === 429) {
          await new Promise(r => setTimeout(r, 3000));
          const retry = await fetch(itemsUrl, {
            headers: { 'x-amz-access-token': token, 'Content-Type': 'application/json' },
          });
          if (!retry.ok) return { orderId: order.AmazonOrderId, items: null };
          return { orderId: order.AmazonOrderId, items: await retry.json() };
        }
        if (!itemsRes.ok) return { orderId: order.AmazonOrderId, items: null };
        return { orderId: order.AmazonOrderId, items: await itemsRes.json() };
      } catch { return { orderId: order.AmazonOrderId, items: null }; }
    }));

    results.forEach(r => {
      if (!r.items) return;
      processedOrderIds.add(r.orderId);
      (r.items.payload?.OrderItems || []).forEach(item => {
        const asin = item.ASIN || 'unknown';
        const name = item.Title || asin;
        const qty = parseInt(item.QuantityOrdered || 0);
        const price = parseFloat(item.ItemPrice?.Amount || 0);
        if (!productAgg[asin]) productAgg[asin] = { name, asin, units: 0, ca: 0 };
        productAgg[asin].units += qty;
        productAgg[asin].ca += price;
      });
    });

    // Pause between batches for rate limits
    if (i + 5 < allOrders.length) await new Promise(r => setTimeout(r, 1200));
  }

  // Save to persistent storage
  amazonProductData.months[monthKey] = productAgg;
  amazonProductData.months[monthKey + '_orderIds'] = Object.fromEntries([...processedOrderIds].map(id => [id, 1]));
  amazonProductData.lastFetch[monthKey] = createdBefore;
  saveAmazonProductData(amazonProductData);

  console.log(`[Amazon] Saved ${Object.keys(productAgg).length} products, ${processedOrderIds.size} orders processed for ${monthKey}`);

  return Object.values(productAgg).sort((a, b) => b.ca - a.ca).slice(0, 5);
}

async function getAmazonAdsAccessToken() {
  const clientId = process.env.AMAZON_ADS_CLIENT_ID;
  const clientSecret = process.env.AMAZON_ADS_CLIENT_SECRET;
  const refreshToken = process.env.AMAZON_ADS_REFRESH_TOKEN;
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

// Amazon Ads spend — background fetch + persistent cache
let amazonAdSpendCache = { spend: null, lastUpdate: 0, reportId: null, fetching: false };

// Load cached ad spend from file
function loadAdSpendCache() {
  try {
    const file = path.join(AMAZON_DATA_DIR, 'amazon-adspend.json');
    if (fs.existsSync(file)) {
      const data = JSON.parse(fs.readFileSync(file, 'utf8'));
      amazonAdSpendCache.spend = data.spend;
      amazonAdSpendCache.lastUpdate = data.lastUpdate || 0;
      console.log(`[Amazon Ads] Loaded cached spend: ${data.spend}`);
    }
  } catch (err) { console.error('[Amazon Ads] Cache load error:', err.message); }
}
loadAdSpendCache();

function saveAdSpendCache() {
  try {
    if (!fs.existsSync(AMAZON_DATA_DIR)) fs.mkdirSync(AMAZON_DATA_DIR, { recursive: true });
    fs.writeFileSync(path.join(AMAZON_DATA_DIR, 'amazon-adspend.json'), JSON.stringify({
      spend: amazonAdSpendCache.spend,
      lastUpdate: amazonAdSpendCache.lastUpdate,
    }));
  } catch (err) { console.error('[Amazon Ads] Cache save error:', err.message); }
}

// Background: request report, poll, download, update cache
async function refreshAmazonAdSpend() {
  if (amazonAdSpendCache.fetching) return;
  amazonAdSpendCache.fetching = true;

  try {
    const token = await getAmazonAdsAccessToken();
    const profileId = process.env.AMAZON_ADS_PROFILE_ID;
    if (!token || !profileId) { amazonAdSpendCache.fetching = false; return; }

    const now = new Date();
    const year = now.getFullYear();
    const month = now.getMonth() + 1;
    const start = `${year}-${String(month).padStart(2, '0')}-01`;
    const yesterday = formatDate(new Date(Date.now() - 86400000));

    // Step 1: Request report
    const reportRes = await fetch('https://advertising-api-eu.amazon.com/reporting/reports', {
      method: 'POST',
      headers: {
        'Amazon-Advertising-API-ClientId': process.env.AMAZON_ADS_CLIENT_ID,
        'Amazon-Advertising-API-Scope': profileId,
        Authorization: `Bearer ${token}`,
        'Content-Type': 'application/vnd.createasyncreportrequest.v3+json',
      },
      body: JSON.stringify({
        startDate: start,
        endDate: yesterday,
        configuration: {
          adProduct: 'SPONSORED_PRODUCTS',
          groupBy: ['campaign'],
          columns: ['spend'],
          reportTypeId: 'spCampaigns',
          timeUnit: 'SUMMARY',
          format: 'GZIP_JSON',
        },
      }),
    });

    let reportId;
    if (!reportRes.ok) {
      const errBody = await reportRes.text();
      // Handle 425 "duplicate" — extract existing report ID
      if (reportRes.status === 425) {
        const match = errBody.match(/duplicate of\s*:\s*([a-f0-9-]+)/i);
        if (match) {
          reportId = match[1];
          console.log('[Amazon Ads] Using existing report:', reportId);
        } else {
          console.error('[Amazon Ads] 425 duplicate but no ID found:', errBody);
          amazonAdSpendCache.fetching = false;
          return;
        }
      } else {
        console.error('[Amazon Ads] Report request error:', reportRes.status, errBody);
        amazonAdSpendCache.fetching = false;
        return;
      }
    } else {
      const reportData = await reportRes.json();
      reportId = reportData.reportId;
    }
    console.log('[Amazon Ads] Report ID:', reportId);

    // Step 2: Poll (max 2 min)
    let downloadUrl = null;
    for (let attempt = 0; attempt < 24; attempt++) {
      await new Promise(r => setTimeout(r, 5000));
      const statusRes = await fetch(`https://advertising-api-eu.amazon.com/reporting/reports/${reportId}`, {
        headers: {
          'Amazon-Advertising-API-ClientId': process.env.AMAZON_ADS_CLIENT_ID,
          'Amazon-Advertising-API-Scope': profileId,
          Authorization: `Bearer ${token}`,
        },
      });
      if (!statusRes.ok) continue;
      const statusData = await statusRes.json();
      console.log('[Amazon Ads] Report status:', statusData.status);
      if (statusData.status === 'COMPLETED') { downloadUrl = statusData.url; break; }
      if (statusData.status === 'FAILURE') { console.error('[Amazon Ads] Report failed:', statusData.failureReason); break; }
    }

    if (!downloadUrl) { console.error('[Amazon Ads] Report timed out'); amazonAdSpendCache.fetching = false; return; }

    // Step 3: Download + decompress
    const dlRes = await fetch(downloadUrl);
    if (!dlRes.ok) { amazonAdSpendCache.fetching = false; return; }
    const buffer = await dlRes.buffer();

    let reportJson;
    try {
      const decompressed = zlib.gunzipSync(buffer);
      reportJson = JSON.parse(decompressed.toString());
    } catch {
      try { reportJson = JSON.parse(buffer.toString()); } catch { reportJson = []; }
    }

    let totalSpend = 0;
    (Array.isArray(reportJson) ? reportJson : []).forEach(row => {
      totalSpend += parseFloat(row.spend || row.cost || 0);
    });

    amazonAdSpendCache.spend = totalSpend;
    amazonAdSpendCache.lastUpdate = Date.now();
    saveAdSpendCache();
    console.log(`[Amazon Ads] Spend updated: ${totalSpend}€`);
  } catch (err) {
    console.error('[Amazon Ads] Refresh error:', err.message);
  } finally {
    amazonAdSpendCache.fetching = false;
  }
}

// Returns cached spend immediately, triggers background refresh if stale (>30 min)
function fetchAmazonAdSpend() {
  if (isAmazonAdsConfigured()) {
    const stale = Date.now() - amazonAdSpendCache.lastUpdate > 30 * 60 * 1000;
    if (stale && !amazonAdSpendCache.fetching) {
      refreshAmazonAdSpend(); // fire and forget
    }
  }
  return amazonAdSpendCache.spend || 0;
}

function isAmazonConfigured() {
  return !!(process.env.AMAZON_SP_CLIENT_ID && process.env.AMAZON_SP_CLIENT_SECRET && process.env.AMAZON_SP_REFRESH_TOKEN);
}

function isAmazonAdsConfigured() {
  return !!(process.env.AMAZON_ADS_CLIENT_ID && process.env.AMAZON_ADS_REFRESH_TOKEN && process.env.AMAZON_ADS_PROFILE_ID);
}

// Amazon objectives helper
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

app.get('/api/amazon/dashboard', async (req, res) => {
  try {
    if (!isAmazonConfigured()) {
      return res.json({ configured: false });
    }

    const now = new Date();
    const year = now.getFullYear();
    const month = now.getMonth() + 1;
    const todayStr = formatDate(now);
    const daysInMonth = new Date(year, month, 0).getDate();
    const daysElapsedMonth = now.getDate();

    const obj = getAmazonObjective(year, month);
    const monthStart = `${year}-${String(month).padStart(2, '0')}-01`;

    // Quarter dates
    let quarterStart = monthStart;
    let totalDaysQuarter = daysInMonth;
    let daysElapsedQuarter = daysElapsedMonth;
    let qMonths = [month];

    if (obj) {
      qMonths = Object.keys(obj.quarterObj.months).map(Number).sort((a, b) => a - b);
      quarterStart = `${year}-${String(qMonths[0]).padStart(2, '0')}-01`;
      const quarterStartDate = new Date(`${quarterStart}T00:00:00`);
      const quarterEndDate = new Date(year, qMonths[qMonths.length - 1], 0);
      totalDaysQuarter = Math.round((quarterEndDate - quarterStartDate) / (1000 * 60 * 60 * 24)) + 1;
      daysElapsedQuarter = Math.round((now - quarterStartDate) / (1000 * 60 * 60 * 24)) + 1;
    }

    // Fetch sales data + get cached ad spend
    const salesQTD = await fetchAmazonSalesMetrics(quarterStart, todayStr);
    const adSpend = isAmazonAdsConfigured() ? fetchAmazonAdSpend() : 0;

    // Aggregate sales by day
    let totalCA = 0, totalOrders = 0;
    let mtdCA = 0, mtdOrders = 0;
    let todayCA = 0, todayOrders = 0;
    const dailyCA = {};

    if (salesQTD && Array.isArray(salesQTD)) {
      salesQTD.forEach(day => {
        const date = (day.interval || '').split('T')[0] || (day.date || '');
        const amount = parseFloat(day.totalSales?.amount || day.orderItemCount || 0);
        const orders = parseInt(day.orderCount || day.unitCount || 0);
        dailyCA[date] = { ca: amount, orders };
        totalCA += amount;
        totalOrders += orders;

        if (date >= monthStart) { mtdCA += amount; mtdOrders += orders; }
        if (date === todayStr) { todayCA = amount; todayOrders = orders; }
      });
    }

    // Ad spend total (fetchAmazonAdSpend now returns a number)
    const totalAdSpend = adSpend || 0;

    const tacosMTD = mtdCA > 0 ? (totalAdSpend / mtdCA) * 100 : 0;
    const tacosQTD = totalCA > 0 ? (totalAdSpend / totalCA) * 100 : 0;
    const tacosDay = todayCA > 0 ? 0 : 0; // Can't compute daily ads spend easily

    // Projections
    const projectedCA_month = daysElapsedMonth > 0 ? (mtdCA / daysElapsedMonth) * daysInMonth : 0;
    const projectedCA_quarter = daysElapsedQuarter > 0 ? (totalCA / daysElapsedQuarter) * totalDaysQuarter : 0;

    // Top 5 products by CA
    const topProducts = await fetchAmazonTopProducts(monthStart, todayStr);

    const monthNames = ['', 'Janvier', 'Février', 'Mars', 'Avril', 'Mai', 'Juin', 'Juillet', 'Août', 'Septembre', 'Octobre', 'Novembre', 'Décembre'];
    const dayNames = ['Dimanche', 'Lundi', 'Mardi', 'Mercredi', 'Jeudi', 'Vendredi', 'Samedi'];

    res.json({
      configured: true,
      adsConfigured: isAmazonAdsConfigured(),
      objectives: obj ? {
        day: {
          label: `${dayNames[now.getDay()]} ${now.getDate()} ${monthNames[month]}`,
          currentCA: todayCA,
          dailyCATarget: obj.monthObj.ca / daysInMonth,
          progressCA: (todayCA / (obj.monthObj.ca / daysInMonth)) * 100,
          tacos: tacosDay,
          tacosTarget: obj.monthObj.tacos,
        },
        month: {
          label: `${monthNames[month]} ${year}`,
          objectiveCA: obj.monthObj.ca,
          currentCA: mtdCA,
          progressCA: (mtdCA / obj.monthObj.ca) * 100,
          projectedCA: projectedCA_month,
          tacos: tacosMTD,
          tacosTarget: obj.monthObj.tacos,
          projectedTacos: tacosMTD, // linear approx
          daysElapsed: daysElapsedMonth,
          daysTotal: daysInMonth,
        },
        quarter: {
          label: `${obj.quarter} ${year}`,
          objectiveCA: obj.quarterObj.ca,
          currentCA: totalCA,
          progressCA: (totalCA / obj.quarterObj.ca) * 100,
          projectedCA: projectedCA_quarter,
          tacos: tacosQTD,
          tacosTarget: obj.quarterObj.tacos,
          projectedTacos: tacosQTD,
          daysElapsed: daysElapsedQuarter,
          daysTotal: totalDaysQuarter,
        },
      } : null,
      kpis: {
        ca: mtdCA,
        orders: mtdOrders,
        tacos: tacosMTD,
      },
      topProducts,
    });
  } catch (err) {
    console.error('Amazon dashboard error:', err);
    res.status(500).json({ error: err.message });
  }
});

// ============================================================
// META ANALYSIS — Deep ad & adset analysis with Claude
// ============================================================

const Anthropic = require('@anthropic-ai/sdk').default;

async function fetchMetaAdInsights(start, end) {
  const token = process.env.META_ACCESS_TOKEN;
  const accountId = process.env.META_AD_ACCOUNT_ID;
  if (!token || !accountId) return [];

  // Fetch in smaller batches — first get spend + actions (light query)
  const url = `https://graph.facebook.com/v19.0/${accountId}/insights?` +
    new URLSearchParams({
      access_token: token,
      fields: 'ad_id,ad_name,adset_name,campaign_name,spend,impressions,clicks,actions,action_values,reach,frequency',
      time_range: JSON.stringify({ since: start, until: end }),
      level: 'ad',
      limit: '100',
      filtering: JSON.stringify([{ field: 'spend', operator: 'GREATER_THAN', value: '5' }]),
    }).toString();

  const res = await fetch(url);
  if (!res.ok) { console.error('Meta ad insights error:', await res.text()); return []; }
  const json = await res.json();
  return json.data || [];
}

async function fetchMetaAdsetInsights(start, end) {
  const token = process.env.META_ACCESS_TOKEN;
  const accountId = process.env.META_AD_ACCOUNT_ID;
  if (!token || !accountId) return [];

  const url = `https://graph.facebook.com/v19.0/${accountId}/insights?` +
    new URLSearchParams({
      access_token: token,
      fields: 'adset_id,adset_name,campaign_name,spend,impressions,clicks,cpm,cpc,ctr,actions,action_values,reach,frequency',
      time_range: JSON.stringify({ since: start, until: end }),
      level: 'adset',
      limit: '50',
    }).toString();

  const res = await fetch(url);
  if (!res.ok) { console.error('Meta adset insights error:', await res.text()); return []; }
  const json = await res.json();
  return json.data || [];
}

async function fetchAdCreative(adId) {
  const token = process.env.META_ACCESS_TOKEN;
  try {
    // Get ad with creative fields including image_hash-based URL
    const url = `https://graph.facebook.com/v19.0/${adId}?fields=creative{id,title,body,thumbnail_url,image_url,object_story_spec,asset_feed_spec}&access_token=${token}`;
    const res = await fetch(url);
    if (!res.ok) { console.error(`[Meta] Ad creative ${adId}: ${res.status}`); return null; }
    const json = await res.json();
    const creative = json.creative || {};
    let imageUrl = creative.image_url || creative.thumbnail_url || null;

    // Try extracting image from object_story_spec
    if (!imageUrl && creative.object_story_spec) {
      const spec = creative.object_story_spec;
      if (spec.link_data?.image_hash || spec.link_data?.picture) {
        imageUrl = spec.link_data.picture || null;
      }
      if (!imageUrl && spec.video_data?.image_url) {
        imageUrl = spec.video_data.image_url;
      }
    }

    // Fallback: fetch the creative ID directly for thumbnail
    if (!imageUrl && creative.id) {
      const crUrl = `https://graph.facebook.com/v19.0/${creative.id}?fields=thumbnail_url,image_url,effective_instagram_media_id&access_token=${token}`;
      const crRes = await fetch(crUrl);
      if (crRes.ok) {
        const crJson = await crRes.json();
        imageUrl = crJson.thumbnail_url || crJson.image_url || null;
      }
    }

    return {
      thumbnail_url: imageUrl,
      image_url: imageUrl,
      title: creative.title || '',
      body: creative.body || '',
    };
  } catch (e) { console.error(`[Meta] Creative error ${adId}:`, e.message); return null; }
}

function parseMetaInsightRow(row) {
  const spend = parseFloat(row.spend || 0);
  const impressions = parseInt(row.impressions || 0);
  const clicks = parseInt(row.clicks || 0);
  const cpm = parseFloat(row.cpm || 0);
  const cpc = parseFloat(row.cpc || 0);
  const ctr = parseFloat(row.ctr || 0);
  const reach = parseInt(row.reach || 0);
  const frequency = parseFloat(row.frequency || 0);
  let purchases = 0, revenue = 0;
  if (row.actions) {
    const pa = row.actions.find(a => a.action_type === 'purchase' || a.action_type === 'omni_purchase');
    if (pa) purchases = parseInt(pa.value || 0);
  }
  if (row.action_values) {
    const ra = row.action_values.find(a => a.action_type === 'purchase' || a.action_type === 'omni_purchase');
    if (ra) revenue = parseFloat(ra.value || 0);
  }
  const roas = spend > 0 ? revenue / spend : 0;
  const cpa = purchases > 0 ? spend / purchases : 0;
  return { spend, impressions, clicks, cpm, cpc, ctr, reach, frequency, purchases, revenue, roas, cpa };
}

app.get('/api/meta/analysis', async (req, res) => {
  try {
    const token = process.env.META_ACCESS_TOKEN;
    const apiKey = process.env.ANTHROPIC_API_KEY;
    if (!token) return res.status(400).json({ error: 'Meta non configuré' });

    const days = parseInt(req.query.days) || 15;
    const end = new Date();
    end.setDate(end.getDate() - 1);
    const start = new Date(end);
    start.setDate(start.getDate() - days + 1);
    const startStr = formatDate(start);
    const endStr = formatDate(end);

    // Fetch ads (top by spend) and adsets in parallel
    const [topAdsRaw, adsetRaw] = await Promise.all([
      fetchMetaAdInsights(startStr, endStr),
      fetchMetaAdsetInsights(startStr, endStr),
    ]);

    // Parse ads, sort by ROAS, pick top 5
    const allAds = topAdsRaw.map(row => ({
      id: row.ad_id,
      name: row.ad_name,
      adsetName: row.adset_name,
      campaignName: row.campaign_name,
      ...parseMetaInsightRow(row),
    })).filter(a => a.spend >= 5);

    // Top 5 by ROAS (with minimum spend threshold)
    const topAds = [...allAds].sort((a, b) => b.roas - a.roas).slice(0, 5);

    // Fetch creatives for top 5 (with thumbnail)
    const creatives = await Promise.all(topAds.map(ad => fetchAdCreative(ad.id)));
    topAds.forEach((ad, i) => {
      const c = creatives[i];
      ad.thumbnailUrl = c?.thumbnail_url || null;
      ad.imageUrl = c?.image_url || null;
      ad.creativeTitle = c?.title || '';
      ad.creativeBody = c?.body || '';
    });

    // Parse adsets
    const allAdsets = adsetRaw.map(row => ({
      id: row.adset_id,
      name: row.adset_name,
      campaignName: row.campaign_name,
      ...parseMetaInsightRow(row),
    })).filter(a => a.spend >= 5);

    const topAdsets = [...allAdsets].sort((a, b) => b.roas - a.roas).slice(0, 5);
    const worstAdsets = [...allAdsets].sort((a, b) => a.roas - b.roas).slice(0, 5);

    // Account-level summary
    const accountTotals = {
      spend: allAds.reduce((s, a) => s + a.spend, 0),
      revenue: allAds.reduce((s, a) => s + a.revenue, 0),
      purchases: allAds.reduce((s, a) => s + a.purchases, 0),
      impressions: allAds.reduce((s, a) => s + a.impressions, 0),
      clicks: allAds.reduce((s, a) => s + a.clicks, 0),
      reach: allAdsets.reduce((s, a) => s + a.reach, 0),
    };
    accountTotals.roas = accountTotals.spend > 0 ? accountTotals.revenue / accountTotals.spend : 0;
    accountTotals.cpa = accountTotals.purchases > 0 ? accountTotals.spend / accountTotals.purchases : 0;
    accountTotals.cpm = accountTotals.impressions > 0 ? (accountTotals.spend / accountTotals.impressions) * 1000 : 0;
    accountTotals.ctr = accountTotals.impressions > 0 ? (accountTotals.clicks / accountTotals.impressions) * 100 : 0;

    // Claude analysis
    let analysis = { topAdsAnalysis: '', newAdsProposals: '', scalingAnalysis: '', globalAnalysis: '' };
    if (apiKey) {
      const anthropic = new Anthropic({ apiKey });

      const fmtAd = (ad, i) => `#${i+1} "${ad.name}" — Spend: ${ad.spend.toFixed(0)}€, Revenue: ${ad.revenue.toFixed(0)}€, ROAS: ${ad.roas.toFixed(2)}, CPA: ${ad.cpa.toFixed(0)}€, CPM: ${ad.cpm.toFixed(1)}€, CTR: ${ad.ctr.toFixed(2)}%, Purchases: ${ad.purchases}, Impressions: ${ad.impressions}, Reach: ${ad.reach}, Frequency: ${ad.frequency.toFixed(1)}`;
      const fmtAdset = (as, i) => `#${i+1} "${as.name}" (${as.campaignName}) — Spend: ${as.spend.toFixed(0)}€, Revenue: ${as.revenue.toFixed(0)}€, ROAS: ${as.roas.toFixed(2)}, CPA: ${as.cpa.toFixed(0)}€, CPM: ${as.cpm.toFixed(1)}€, CTR: ${as.ctr.toFixed(2)}%, Purchases: ${as.purchases}, Reach: ${as.reach}, Frequency: ${as.frequency.toFixed(1)}`;

      const prompt = `Tu es un senior data analyst / growth strategist spécialisé Meta Ads pour des marques DTC e-commerce. Tu analyses le compte Meta de French Bandit (accessoires premium pour chiens).

Période : ${startStr} → ${endStr} (14 derniers jours)

=== DONNÉES COMPTE ===
Spend total: ${accountTotals.spend.toFixed(0)}€ | Revenue: ${accountTotals.revenue.toFixed(0)}€ | ROAS: ${accountTotals.roas.toFixed(2)} | CPA: ${accountTotals.cpa.toFixed(0)}€ | CPM: ${accountTotals.cpm.toFixed(1)}€ | CTR: ${accountTotals.ctr.toFixed(2)}% | Purchases: ${accountTotals.purchases} | Reach: ${accountTotals.reach}

=== TOP 5 ADS (par ROAS) ===
${topAds.map(fmtAd).join('\n')}

=== TOP 5 ADSETS (par ROAS) ===
${topAdsets.map(fmtAdset).join('\n')}

=== WORST 5 ADSETS (pire ROAS) ===
${worstAdsets.map(fmtAdset).join('\n')}

=== INSTRUCTIONS ===
Réponds en JSON strict avec ces 4 champs (valeurs = strings avec du Markdown) :

{
  "topAdsAnalysis": "Analyse détaillée de chaque top ad. Pour chaque ad : pourquoi elle performe, quels signaux regarder (CTR vs CPM, fréquence, saturation). Identifie les patterns communs aux top performers. Donne des recommandations actionnables pour chaque ad (augmenter budget, dupliquer, tester variantes).",

  "newAdsProposals": "Propose 3 nouvelles publicités basées sur les patterns des top performers. Pour chaque proposition donne :\\n- Concept et angle\\n- Hook (première phrase / accroche)\\n- Body text complet\\n- Format recommandé (vidéo/image/carrousel)\\n- Audience suggérée\\n- Budget test recommandé",

  "scalingAnalysis": "Pour chaque top adset, donne un plan de scaling précis : augmentation de budget recommandée (%), fréquence actuelle vs seuil de saturation, stratégie de duplication, audiences lookalike à tester, signaux de fatigue à surveiller. Sois très concret avec des chiffres.",

  "globalAnalysis": "Analyse macro du compte : santé globale, tendances, répartition du budget, efficacité par campagne, risques identifiés (dépendance à un seul ad, saturation audience, CPM en hausse...). Donne 3 quick wins et 3 recommandations stratégiques à moyen terme."
}

IMPORTANT: Réponds UNIQUEMENT avec le JSON, pas de texte avant/après. Le contenu doit être en français. Utilise du Markdown (## pour les titres, **gras**, - pour les listes). Sois précis, factuel, avec des chiffres.`;

      try {
        const response = await anthropic.messages.create({
          model: 'claude-sonnet-4-20250514',
          max_tokens: 4000,
          messages: [{ role: 'user', content: prompt }],
        });
        const text = response.content[0].text.trim();
        // Parse JSON (might be wrapped in ```json```)
        const jsonStr = text.replace(/^```json\s*/i, '').replace(/\s*```$/, '');
        analysis = JSON.parse(jsonStr);
      } catch (e) {
        console.error('Claude analysis error:', e.message);
        analysis.globalAnalysis = 'Erreur lors de la génération de l\'analyse.';
      }
    }

    res.json({
      period: { start: startStr, end: endStr },
      topAds,
      topAdsets,
      worstAdsets,
      accountTotals,
      analysis,
    });
  } catch (err) {
    console.error('Meta analysis error:', err);
    res.status(500).json({ error: err.message });
  }
});

// ============================================================
// STATUS / HEALTH CHECK
// ============================================================

// Debug Ads endpoint
app.get('/api/amazon/debug-ads', async (req, res) => {
  try {
    const token = await getAmazonAdsAccessToken();
    if (!token) return res.json({ error: 'Failed to get Ads access token', hasClientId: !!process.env.AMAZON_ADS_CLIENT_ID, hasSecret: !!process.env.AMAZON_ADS_CLIENT_SECRET, hasRefresh: !!process.env.AMAZON_ADS_REFRESH_TOKEN });

    const profileId = process.env.AMAZON_ADS_PROFILE_ID;

    // Test: list profiles
    const profilesRes = await fetch('https://advertising-api-eu.amazon.com/v2/profiles', {
      headers: {
        Authorization: `Bearer ${token}`,
        'Amazon-Advertising-API-ClientId': process.env.AMAZON_ADS_CLIENT_ID,
      },
    });
    const profiles = await profilesRes.json();

    // Test: request a report
    const now = new Date();
    const start = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}-01`;
    const end = formatDate(new Date(now.getTime() - 86400000)); // yesterday

    const reportRes = await fetch('https://advertising-api-eu.amazon.com/reporting/reports', {
      method: 'POST',
      headers: {
        'Amazon-Advertising-API-ClientId': process.env.AMAZON_ADS_CLIENT_ID,
        'Amazon-Advertising-API-Scope': profileId,
        Authorization: `Bearer ${token}`,
        'Content-Type': 'application/vnd.createasyncreportrequest.v3+json',
      },
      body: JSON.stringify({
        startDate: start,
        endDate: end,
        configuration: {
          adProduct: 'SPONSORED_PRODUCTS',
          groupBy: ['campaign'],
          columns: ['spend'],
          reportTypeId: 'spCampaigns',
          timeUnit: 'SUMMARY',
          format: 'GZIP_JSON',
        },
      }),
    });
    const reportStatus = reportRes.status;
    const reportBody = await reportRes.text();

    res.json({ tokenOk: true, profileId, profilesCount: profiles.length, profiles: profiles.map(p => ({ id: p.profileId, name: p.accountInfo?.name, marketplace: p.accountInfo?.marketplaceStringId })), reportStatus, reportBody: reportBody.substring(0, 1000) });
  } catch (err) {
    res.json({ error: err.message });
  }
});

// Debug: try creating a report with a specific profile ID
app.get('/api/amazon/debug-report', async (req, res) => {
  try {
    const token = await getAmazonAdsAccessToken();
    if (!token) return res.json({ error: 'No token' });

    const profileId = req.query.profile || process.env.AMAZON_ADS_PROFILE_ID;
    const now = new Date();
    const start = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}-01`;
    const yesterday = formatDate(new Date(Date.now() - 86400000));

    // Try to create report
    const reportRes = await fetch('https://advertising-api-eu.amazon.com/reporting/reports', {
      method: 'POST',
      headers: {
        'Amazon-Advertising-API-ClientId': process.env.AMAZON_ADS_CLIENT_ID,
        'Amazon-Advertising-API-Scope': profileId,
        Authorization: `Bearer ${token}`,
        'Content-Type': 'application/vnd.createasyncreportrequest.v3+json',
      },
      body: JSON.stringify({
        startDate: start,
        endDate: yesterday,
        configuration: {
          adProduct: 'SPONSORED_PRODUCTS',
          groupBy: ['campaign'],
          columns: ['spend'],
          reportTypeId: 'spCampaigns',
          timeUnit: 'SUMMARY',
          format: 'GZIP_JSON',
        },
      }),
    });

    const status = reportRes.status;
    const body = await reportRes.text();

    let reportId = null;
    if (status === 200) {
      reportId = JSON.parse(body).reportId;
    } else if (status === 425) {
      const match = body.match(/duplicate of\s*:\s*([a-f0-9-]+)/i);
      if (match) reportId = match[1];
    }

    // If we got a reportId, poll it
    let reportStatus = null, reportData = null;
    if (reportId) {
      for (let i = 0; i < 12; i++) {
        await new Promise(r => setTimeout(r, 5000));
        const sRes = await fetch(`https://advertising-api-eu.amazon.com/reporting/reports/${reportId}`, {
          headers: {
            'Amazon-Advertising-API-ClientId': process.env.AMAZON_ADS_CLIENT_ID,
            'Amazon-Advertising-API-Scope': profileId,
            Authorization: `Bearer ${token}`,
          },
        });
        const sData = await sRes.json();
        reportStatus = sData.status;
        if (sData.status === 'COMPLETED' && sData.url) {
          const dlRes = await fetch(sData.url);
          const buffer = await dlRes.buffer();
          try {
            reportData = JSON.parse(zlib.gunzipSync(buffer).toString());
          } catch {
            try { reportData = JSON.parse(buffer.toString()); } catch { reportData = 'parse error'; }
          }
          break;
        }
        if (sData.status === 'FAILURE') { reportStatus = 'FAILURE: ' + sData.failureReason; break; }
      }
    }

    res.json({ profileId, createStatus: status, createBody: body.substring(0, 500), reportId, reportStatus, reportData: reportData ? JSON.stringify(reportData).substring(0, 2000) : null });
  } catch (err) {
    res.json({ error: err.message });
  }
});

// Debug endpoint — temporary
app.get('/api/amazon/debug', async (req, res) => {
  try {
    const token = await getAmazonAccessToken();
    if (!token) return res.json({ error: 'Failed to get access token' });

    const marketplaceId = process.env.AMAZON_MARKETPLACE_ID || 'A13V1IB3VIYZZH';
    const now = new Date();
    const todayStr = formatDate(now);
    const start = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}-01`;

    // Test orders list
    const twoMinAgo = new Date(Date.now() - 3 * 60 * 1000).toISOString();
    const ordersUrl = `https://sellingpartnerapi-eu.amazon.com/orders/v0/orders?` +
      new URLSearchParams({
        MarketplaceIds: marketplaceId,
        CreatedAfter: `${start}T00:00:00Z`,
        CreatedBefore: twoMinAgo,
        OrderStatuses: 'Shipped,Unshipped',
        MaxResultsPerPage: '5',
      }).toString();

    const ordersRes = await fetch(ordersUrl, {
      headers: { 'x-amz-access-token': token, 'Content-Type': 'application/json' },
    });
    const ordersStatus = ordersRes.status;
    const ordersBody = await ordersRes.text();

    // If orders work, test order items for first order
    let itemsTest = null;
    if (ordersStatus === 200) {
      const ordersData = JSON.parse(ordersBody);
      const firstOrder = ordersData.payload?.Orders?.[0];
      if (firstOrder) {
        const itemsUrl = `https://sellingpartnerapi-eu.amazon.com/orders/v0/orders/${firstOrder.AmazonOrderId}/orderItems`;
        const itemsRes = await fetch(itemsUrl, {
          headers: { 'x-amz-access-token': token, 'Content-Type': 'application/json' },
        });
        itemsTest = { status: itemsRes.status, body: (await itemsRes.text()).substring(0, 1000) };
      }
    }

    res.json({ tokenOk: true, ordersStatus, ordersBody: ordersBody.substring(0, 500), itemsTest });
  } catch (err) {
    res.json({ error: err.message });
  }
});

app.get('/api/status', (req, res) => {
  const configured = {
    shopify: !!(process.env.SHOPIFY_STORE_URL && process.env.SHOPIFY_ACCESS_TOKEN),
    meta: !!(process.env.META_ACCESS_TOKEN && process.env.META_AD_ACCOUNT_ID),
    google: !!(process.env.GOOGLE_ADS_CLIENT_ID && process.env.GOOGLE_ADS_DEVELOPER_TOKEN && process.env.GOOGLE_ADS_CUSTOMER_ID && process.env.GOOGLE_ADS_REFRESH_TOKEN),
    tiktok: !!(process.env.TIKTOK_ACCESS_TOKEN && process.env.TIKTOK_ADVERTISER_ID),
    amazon: isAmazonConfigured(),
  };
  res.json({ configured });
});

// ============================================================
// TIKTOK OAUTH CALLBACK
// ============================================================

app.get('/api/tiktok/callback', (req, res) => {
  const authCode = req.query.auth_code || req.query.code;
  res.send(`<h2>TikTok Auth Code</h2><p>Copie ce code et lance :</p><pre>node tiktok-auth.js ${authCode}</pre>`);
});

// Amazon Ads OAuth callback — exchanges code for refresh token
app.get('/api/amazon/callback', async (req, res) => {
  const code = req.query.code;
  if (!code) return res.send('<h2>Erreur</h2><p>Pas de code reçu.</p>');

  const clientId = process.env.AMAZON_ADS_CLIENT_ID;
  const clientSecret = process.env.AMAZON_ADS_CLIENT_SECRET;

  if (!clientId || !clientSecret) {
    return res.send(`<h2>Amazon Ads Auth Code</h2><p>Code reçu : <code>${code}</code></p><p>Configure AMAZON_ADS_CLIENT_ID et AMAZON_ADS_CLIENT_SECRET dans Railway, puis relance le flow OAuth.</p>`);
  }

  try {
    const tokenRes = await fetch('https://api.amazon.com/auth/o2/token', {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: new URLSearchParams({
        grant_type: 'authorization_code',
        code,
        redirect_uri: 'https://web-production-1b6dc.up.railway.app/api/amazon/callback',
        client_id: clientId,
        client_secret: clientSecret,
      }).toString(),
    });
    const tokenData = await tokenRes.json();

    if (tokenData.refresh_token) {
      // Also fetch profile ID
      let profileInfo = '';
      try {
        const profileRes = await fetch('https://advertising-api-eu.amazon.com/v2/profiles', {
          headers: {
            'Authorization': `Bearer ${tokenData.access_token}`,
            'Amazon-Advertising-API-ClientId': clientId,
          },
        });
        const profiles = await profileRes.json();
        profileInfo = '<h3>Profiles trouvés :</h3><ul>' +
          profiles.map(p => `<li><strong>${p.accountInfo?.name || p.profileId}</strong> — Profile ID: <code>${p.profileId}</code> (${p.accountInfo?.marketplaceStringId || ''})</li>`).join('') +
          '</ul>';
      } catch (e) { profileInfo = `<p>Erreur profiles: ${e.message}</p>`; }

      res.send(`<h2>Amazon Ads — Authentification réussie</h2>
        <p><strong>Refresh Token :</strong></p><pre style="background:#f0f0f0;padding:12px;border-radius:8px;word-break:break-all;">${tokenData.refresh_token}</pre>
        <p>Ajoute ce refresh token dans Railway comme <code>AMAZON_ADS_REFRESH_TOKEN</code></p>
        ${profileInfo}
        <p>Ajoute le Profile ID souhaité dans Railway comme <code>AMAZON_ADS_PROFILE_ID</code></p>`);
    } else {
      res.send(`<h2>Erreur token</h2><pre>${JSON.stringify(tokenData, null, 2)}</pre>`);
    }
  } catch (err) {
    res.send(`<h2>Erreur</h2><pre>${err.message}</pre>`);
  }
});

// ============================================================
// DAILY REPORT — manual trigger + preview
// ============================================================

app.get('/api/report/send', async (req, res) => {
  try {
    const result = await sendReport();
    res.json({ success: true, analysis: result.analysis });
  } catch (err) {
    console.error('Report send error:', err);
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/report/preview', async (req, res) => {
  try {
    const result = await sendReport();
    res.setHeader('Content-Type', 'text/html');
    res.send(result.html);
  } catch (err) {
    console.error('Report preview error:', err);
    res.status(500).json({ error: err.message });
  }
});

// ============================================================
// CRON — Daily report at 00:01
// ============================================================

cron.schedule('1 0 * * *', async () => {
  console.log('[Cron] Triggering daily report...');
  try {
    await sendReport();
    console.log('[Cron] Daily report sent successfully.');
  } catch (err) {
    console.error('[Cron] Daily report failed:', err);
  }
}, { timezone: 'Europe/Paris' });

// ============================================================
// START
// ============================================================

app.listen(PORT, () => {
  console.log(`Bandit Acquisition Dashboard running on http://localhost:${PORT}`);
  console.log(`Daily report scheduled at 00:01 (Europe/Paris)`);

  // Refresh Amazon Ad spend on startup (background)
  if (isAmazonAdsConfigured()) {
    console.log('[Amazon Ads] Starting initial ad spend refresh...');
    refreshAmazonAdSpend();
  }
});

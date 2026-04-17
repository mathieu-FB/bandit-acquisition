require('dotenv').config();
const express = require('express');
const fetch = require('node-fetch');
const path = require('path');
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
        4: { ca: 128000, ratio: 30 },  // April
        5: { ca: 144000, ratio: 30 },  // May
        6: { ca: 152000, ratio: 30 },  // June
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
// STATUS / HEALTH CHECK
// ============================================================

app.get('/api/status', (req, res) => {
  const configured = {
    shopify: !!(process.env.SHOPIFY_STORE_URL && process.env.SHOPIFY_ACCESS_TOKEN),
    meta: !!(process.env.META_ACCESS_TOKEN && process.env.META_AD_ACCOUNT_ID),
    google: !!(process.env.GOOGLE_ADS_CLIENT_ID && process.env.GOOGLE_ADS_DEVELOPER_TOKEN && process.env.GOOGLE_ADS_CUSTOMER_ID && process.env.GOOGLE_ADS_REFRESH_TOKEN),
    tiktok: !!(process.env.TIKTOK_ACCESS_TOKEN && process.env.TIKTOK_ADVERTISER_ID),
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
});

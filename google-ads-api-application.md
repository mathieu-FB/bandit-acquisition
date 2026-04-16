# Google Ads API — Basic Access Application

**Company Name:** French Bandit (Bandit SAS)

**Business Model:** Our company operates an e-commerce store selling pet accessories (collars, leashes, toys) online through our website french-bandit.com, powered by Shopify. We advertise exclusively for our own brand through Meta Ads, Google Ads, and TikTok Ads. We do not manage ads for any third party.

**Tool Access/Use:** Our tool is an internal acquisition dashboard used exclusively by our marketing team to monitor and analyze ad performance across all our advertising channels (Meta, Google, TikTok) alongside our Shopify sales data. The tool is read-only — it pulls reporting data from the Google Ads API to display KPIs and charts. No employee or external party will use this tool to create, modify, or delete any ads or campaigns. The tool is hosted on our internal server and accessible only to our team.

**Tool Design:** Our internal dashboard pulls daily ad performance metrics from the Google Ads API into our Node.js application. The UI displays:

- Key performance indicators: spend, impressions, clicks, conversions, conversion value
- Daily breakdown charts comparing Google Ads performance alongside Meta and TikTok
- Computed metrics: ROAS, CPM, Blended CAC (combining all channels with Shopify order data)
- Period-over-period comparison (e.g., this week vs. last week)

The dashboard fetches data on-demand when a user loads the page or changes the date range. There is no automated syncing, no data storage in a database, and no ad management functionality. All API calls are read-only reporting queries.

**API Services Called:**

* Pull campaign performance reports using GoogleAdsService.Search (GAQL queries on the `campaign` resource)
* Metrics retrieved: `metrics.cost_micros`, `metrics.impressions`, `metrics.clicks`, `metrics.conversions`, `metrics.conversions_value`
* Segmented by: `segments.date`
* Read-only access — no write operations

**Tool Mockups:**

The dashboard displays:

1. **Top KPI cards** — Net sales, Marketing costs (sum of all channels), % Marketing (costs/sales ratio)
2. **Channel charts** — Line charts showing daily spend, ROAS, and CPM broken down by channel (Meta in blue, Google in red, TikTok in black)
3. **Blended metrics** — Blended CAC, AOV, order count, repeat customer rate
4. **Date picker** — Select custom periods and comparison ranges (7d, 14d, 30d, 90d presets)

*[Attach a screenshot of the dashboard running at http://localhost:3001 with the mockup data]*

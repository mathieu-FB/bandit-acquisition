// ============================================================
// BANDIT ACQUISITION DASHBOARD — Frontend Logic
// ============================================================

const CHANNEL_COLORS = {
  meta: '#1877f2',
  google: '#ea4335',
  tiktok: '#000000',
};
const CHANNEL_LABELS = { meta: 'Meta', google: 'Google', tiktok: 'TikTok' };

let chartInstances = {};

// ============================================================
// DATE HELPERS
// ============================================================

function formatDateISO(d) {
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, '0');
  const day = String(d.getDate()).padStart(2, '0');
  return `${y}-${m}-${day}`;
}

function setDefaultDates() {
  // Default: today
  const now = new Date();
  const start = new Date(now);
  const end = new Date(now);

  document.getElementById('dateStart').value = formatDateISO(start);
  document.getElementById('dateEnd').value = formatDateISO(end);

  // Comparison: yesterday
  const compEnd = new Date(now);
  compEnd.setDate(compEnd.getDate() - 1);
  const compStart = new Date(compEnd);

  document.getElementById('compStart').value = formatDateISO(compStart);
  document.getElementById('compEnd').value = formatDateISO(compEnd);
}

function getSelectedDates() {
  return {
    start: document.getElementById('dateStart').value,
    end: document.getElementById('dateEnd').value,
    comp_start: document.getElementById('compStart').value,
    comp_end: document.getElementById('compEnd').value,
  };
}

function setQuickRange(range) {
  const now = new Date();
  const end = new Date(now); // today

  let start;
  if (range === 'mtd') {
    start = new Date(now.getFullYear(), now.getMonth(), 1);
  } else if (range === 'qtd') {
    const qMonth = Math.floor(now.getMonth() / 3) * 3;
    start = new Date(now.getFullYear(), qMonth, 1);
  } else if (range === 'ytd') {
    start = new Date(now.getFullYear(), 0, 1);
  } else {
    // numeric days (e.g. 7)
    const days = parseInt(range);
    start = new Date(end);
    start.setDate(start.getDate() - days + 1);
  }

  document.getElementById('dateStart').value = formatDateISO(start);
  document.getElementById('dateEnd').value = formatDateISO(end);

  // Comparison: same duration ending the day before start
  const duration = Math.round((end - start) / (1000 * 60 * 60 * 24)) + 1;
  const compEnd = new Date(start);
  compEnd.setDate(compEnd.getDate() - 1);
  const compStart = new Date(compEnd);
  compStart.setDate(compStart.getDate() - duration + 1);

  document.getElementById('compStart').value = formatDateISO(compStart);
  document.getElementById('compEnd').value = formatDateISO(compEnd);

  // Highlight active button
  document.querySelectorAll('.quick-ranges .btn').forEach(b => b.classList.remove('active'));
  const btn = document.querySelector(`.quick-ranges .btn[data-range="${range}"]`);
  if (btn) btn.classList.add('active');

  loadDashboard();
}

// ============================================================
// FORMATTING
// ============================================================

function fmtCurrency(val) {
  return new Intl.NumberFormat('fr-FR', {
    style: 'currency',
    currency: 'EUR',
    minimumFractionDigits: 0,
    maximumFractionDigits: val >= 100 ? 0 : 2,
  }).format(val);
}

function fmtNumber(val) {
  return new Intl.NumberFormat('fr-FR', {
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  }).format(val);
}

function fmtPercent(val) {
  return val.toFixed(val >= 10 ? 0 : 1) + '%';
}

function fmtMultiplier(val) {
  return val.toFixed(1) + 'X';
}

function computeChange(current, previous) {
  if (previous === 0 && current === 0) return 0;
  if (previous === 0) return current > 0 ? 100 : -100;
  return ((current - previous) / Math.abs(previous)) * 100;
}

// ============================================================
// SHARED TOOLTIP CONFIG
// ============================================================

const TOOLTIP_CONFIG = {
  backgroundColor: '#1a1d26',
  titleFont: { size: 12, family: 'Inter', weight: '600' },
  bodyFont: { size: 11, family: 'Inter' },
  padding: 12,
  cornerRadius: 8,
  displayColors: true,
  boxPadding: 4,
};

// ============================================================
// RENDER KPI CARD
// ============================================================

function renderKPI(id, current, previous, formatter, invertColors) {
  const valEl = document.getElementById(`val-${id}`);
  const compEl = document.getElementById(`comp-${id}`);

  if (valEl) valEl.textContent = formatter(current);

  if (compEl) {
    const change = computeChange(current, previous);
    const isPositive = invertColors ? change <= 0 : change >= 0;
    const arrow = change >= 0 ? '\u2191' : '\u2193';
    const sign = change >= 0 ? '+' : '';

    compEl.innerHTML = `
      <span class="comp-previous">From ${formatter(previous)}</span>
      <span class="comp-change ${isPositive ? 'positive' : 'negative'}">
        <span class="arrow">${arrow}</span>
        ${sign}${change.toFixed(2)}%
      </span>
    `;
  }
}

// ============================================================
// MINI SPARKLINE CHARTS (with tooltip on hover)
// ============================================================

function createSparkline(canvasId, dailyData, color, formatter) {
  const ctx = document.getElementById(canvasId);
  if (!ctx) return;

  if (chartInstances[canvasId]) chartInstances[canvasId].destroy();

  const labels = dailyData.map(d => {
    const date = new Date(d.date);
    return date.toLocaleDateString('fr-FR', { weekday: 'short', day: 'numeric', month: 'short' });
  });
  const values = dailyData.map(d => d.value);

  chartInstances[canvasId] = new Chart(ctx, {
    type: 'line',
    data: {
      labels,
      datasets: [{
        data: values,
        borderColor: color || '#6c5ce7',
        borderWidth: 2,
        fill: {
          target: 'origin',
          above: (color || '#6c5ce7') + '15',
        },
        tension: 0.4,
        pointRadius: 0,
        pointHoverRadius: 4,
        pointHoverBackgroundColor: color || '#6c5ce7',
        pointHitRadius: 20,
      }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: 'index', intersect: false },
      plugins: {
        legend: { display: false },
        tooltip: {
          ...TOOLTIP_CONFIG,
          displayColors: false,
          callbacks: {
            label: function(ctx) {
              return formatter ? formatter(ctx.parsed.y) : ctx.parsed.y;
            },
          },
        },
      },
      scales: {
        x: { display: false },
        y: { display: false },
      },
      animation: { duration: 600 },
    },
  });
}

// ============================================================
// LINE CHARTS (MULTI-CHANNEL) — always show all channels
// ============================================================

function createChannelLineChart(canvasId, legendId, dailyData, channelTotals, formatter, showPercent) {
  const ctx = document.getElementById(canvasId);
  if (!ctx) return;

  if (chartInstances[canvasId]) chartInstances[canvasId].destroy();

  const labels = dailyData.map(d => {
    const date = new Date(d.date);
    return date.toLocaleDateString('fr-FR', { weekday: 'short', day: 'numeric', month: 'short' });
  });

  // Always create a dataset for each channel (even if 0)
  const datasets = [];
  Object.entries(CHANNEL_COLORS).forEach(([channel, color]) => {
    const data = dailyData.map(d => d[channel] || 0);
    datasets.push({
      label: CHANNEL_LABELS[channel],
      data,
      borderColor: color,
      backgroundColor: color + '20',
      borderWidth: 2.5,
      tension: 0.3,
      pointRadius: dailyData.length === 1 ? 5 : 3,
      pointHoverRadius: 6,
      pointBackgroundColor: color,
      pointBorderColor: '#fff',
      pointBorderWidth: 1,
    });
  });

  chartInstances[canvasId] = new Chart(ctx, {
    type: 'line',
    data: { labels, datasets },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: {
        mode: 'index',
        intersect: false,
      },
      plugins: {
        legend: { display: false },
        tooltip: {
          ...TOOLTIP_CONFIG,
          callbacks: {
            title: function(items) { return items[0]?.label || ''; },
            label: function(ctx) {
              return ` ${ctx.dataset.label}: ${formatter(ctx.parsed.y)}`;
            },
          },
        },
      },
      scales: {
        x: {
          grid: { display: false },
          ticks: { font: { size: 10, family: 'Inter' }, color: '#9ca3af', maxTicksLimit: 7 },
        },
        y: {
          grid: { color: '#f0f0f0' },
          ticks: {
            font: { size: 10, family: 'Inter' },
            color: '#9ca3af',
            callback: function(val) { return formatter(val); },
            maxTicksLimit: 5,
          },
        },
      },
      animation: { duration: 800 },
    },
  });

  // Legend with totals (+ optional %)
  const legendEl = document.getElementById(legendId);
  if (legendEl && channelTotals) {
    const grandTotal = showPercent ? Object.values(channelTotals).reduce((s, v) => s + (v || 0), 0) : 0;
    legendEl.innerHTML = Object.entries(CHANNEL_COLORS)
      .map(([ch, color]) => {
        const val = channelTotals[ch] || 0;
        const pct = showPercent && grandTotal > 0 ? ` (${(val / grandTotal * 100).toFixed(0)}%)` : '';
        return `
          <div class="legend-item">
            <div class="legend-dot" style="background:${color}"></div>
            <span class="legend-label">${CHANNEL_LABELS[ch]}</span>
            <span class="legend-value">${formatter(val)}${pct}</span>
          </div>
        `;
      }).join('');
  }
}

// ============================================================
// BAR CHART (Marketing costs daily) — scale adapts to data
// ============================================================

function createBarChart(canvasId, dailyData, color) {
  const ctx = document.getElementById(canvasId);
  if (!ctx) return;

  if (chartInstances[canvasId]) chartInstances[canvasId].destroy();

  const labels = dailyData.map(d => {
    const date = new Date(d.date);
    return date.toLocaleDateString('fr-FR', { weekday: 'short', day: 'numeric', month: 'short' });
  });

  const values = dailyData.map(d => d.total);
  const maxVal = Math.max(...values);
  const minVal = Math.min(...values);
  // Give 10% padding below min so bars aren't squished
  const suggestedMin = minVal > 0 ? Math.floor(minVal * 0.85) : 0;

  chartInstances[canvasId] = new Chart(ctx, {
    type: 'bar',
    data: {
      labels,
      datasets: [{
        data: values,
        backgroundColor: color || '#6c5ce7',
        borderRadius: 4,
        borderSkipped: false,
      }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: 'index', intersect: false },
      plugins: {
        legend: { display: false },
        tooltip: {
          ...TOOLTIP_CONFIG,
          displayColors: false,
          callbacks: {
            label: function(ctx) { return fmtCurrency(ctx.parsed.y); },
          },
        },
      },
      scales: {
        x: {
          grid: { display: false },
          ticks: { font: { size: 10, family: 'Inter' }, color: '#9ca3af', maxTicksLimit: 7 },
        },
        y: {
          grid: { color: '#f0f0f0' },
          suggestedMin: suggestedMin,
          ticks: {
            font: { size: 10, family: 'Inter' },
            color: '#9ca3af',
            callback: function(val) { return fmtCurrency(val); },
            maxTicksLimit: 5,
          },
        },
      },
      animation: { duration: 800 },
    },
  });
}

// ============================================================
// OBJECTIVES
// ============================================================

function fmtK(val) {
  if (val >= 1000) return (val / 1000).toFixed(val >= 10000 ? 0 : 1) + 'K €';
  return fmtCurrency(val);
}

function renderObjectivePeriod(prefix, data) {
  document.getElementById(`obj-${prefix}-label`).textContent = data.label;
  const timePct = Math.round((data.daysElapsed / data.daysTotal) * 100);
  document.getElementById(`obj-${prefix}-days`).textContent = `J${data.daysElapsed}/${data.daysTotal} (${timePct}%)`;

  // CA progress
  document.getElementById(`obj-${prefix}-ca`).textContent = fmtK(data.currentCA);
  document.getElementById(`obj-${prefix}-ca-target`).textContent = fmtK(data.objectiveCA);
  const pctCA = Math.min(data.progressCA, 100);
  document.getElementById(`obj-${prefix}-ca-bar`).style.width = pctCA + '%';

  // % badge
  const pctBadge = document.getElementById(`obj-${prefix}-ca-pct`);
  pctBadge.textContent = data.progressCA.toFixed(0) + '%';
  pctBadge.style.color = data.progressCA >= 100 ? '#00c48c' : data.progressCA >= (data.daysElapsed / data.daysTotal * 100) * 0.85 ? 'var(--text-secondary)' : '#ff5a5f';

  const projPct = Math.min((data.projectedCA / data.objectiveCA) * 100, 100);
  const projBar = document.getElementById(`obj-${prefix}-ca-proj`);
  projBar.style.width = projPct + '%';
  projBar.style.opacity = '0.25';

  const projEl = document.getElementById(`obj-${prefix}-ca-proj-val`);
  projEl.textContent = fmtK(data.projectedCA);
  projEl.style.color = data.projectedCA >= data.objectiveCA ? '#00c48c' : '#ff5a5f';

  // Ratio
  document.getElementById(`obj-${prefix}-ratio`).textContent = data.currentRatio.toFixed(1) + '%';
  document.getElementById(`obj-${prefix}-ratio-target`).textContent = data.objectiveRatio + '%';

  const ratioIndicator = document.getElementById(`obj-${prefix}-ratio-indicator`);
  const ratioPct = Math.min((data.currentRatio / 50) * 100, 100); // scale 0-50%
  ratioIndicator.style.left = ratioPct + '%';
  ratioIndicator.style.borderColor = data.currentRatio <= data.objectiveRatio ? '#00c48c' : '#ff5a5f';

  const ratioLine = document.getElementById(`obj-${prefix}-ratio-line`);
  ratioLine.style.left = (data.objectiveRatio / 50 * 100) + '%';

  const projRatioEl = document.getElementById(`obj-${prefix}-ratio-proj-val`);
  projRatioEl.textContent = data.projectedRatio.toFixed(1) + '%';
  projRatioEl.style.color = data.projectedRatio <= data.objectiveRatio ? '#00c48c' : '#ff5a5f';
}

async function loadObjectives() {
  try {
    const res = await fetch('/api/objectives');
    const data = await res.json();
    const section = document.getElementById('objectivesSection');

    if (!data.configured) {
      section.style.display = 'none';
      return;
    }

    section.style.display = 'block';

    // Day card
    if (data.day) {
      const d = data.day;
      document.getElementById('obj-day-label').textContent = d.label;
      const nowH = new Date();
      const hours = nowH.getHours();
      const mins = String(nowH.getMinutes()).padStart(2, '0');
      const dayTimePct = Math.round(((hours * 60 + nowH.getMinutes()) / 1440) * 100);
      document.getElementById('obj-day-time').textContent = `${hours}h${mins} (${dayTimePct}%)`;
      document.getElementById('obj-day-ca').textContent = fmtK(d.currentCA);
      document.getElementById('obj-day-ca-target').textContent = fmtK(d.dailyCATarget);
      const dayPct = Math.min(d.progressCA, 100);
      document.getElementById('obj-day-ca-bar').style.width = dayPct + '%';
      const dayBadge = document.getElementById('obj-day-ca-pct');
      dayBadge.textContent = d.progressCA.toFixed(0) + '%';
      dayBadge.style.color = d.progressCA >= 100 ? '#00c48c' : '#ff5a5f';

      document.getElementById('obj-day-ratio').textContent = d.currentRatio.toFixed(1) + '%';
      document.getElementById('obj-day-ratio-target').textContent = d.objectiveRatio + '%';
      const dayRatioIndicator = document.getElementById('obj-day-ratio-indicator');
      const dayRatioPct = Math.min((d.currentRatio / 50) * 100, 100);
      dayRatioIndicator.style.left = dayRatioPct + '%';
      dayRatioIndicator.style.borderColor = d.currentRatio <= d.objectiveRatio ? '#00c48c' : '#ff5a5f';
      document.getElementById('obj-day-ratio-line').style.left = (d.objectiveRatio / 50 * 100) + '%';
    }

    renderObjectivePeriod('month', data.month);
    renderObjectivePeriod('quarter', data.quarter);
  } catch (e) {
    console.error('Objectives load failed:', e);
  }
}

// ============================================================
// STATUS DOTS
// ============================================================

async function loadStatus() {
  try {
    const res = await fetch('/api/status');
    const data = await res.json();
    const container = document.getElementById('statusDots');
    container.innerHTML = '';

    const sources = [
      { key: 'shopify', label: 'Shopify' },
      { key: 'meta', label: 'Meta' },
      { key: 'google', label: 'Google' },
      { key: 'tiktok', label: 'TikTok' },
      { key: 'amazon', label: 'Amazon' },
    ];

    sources.forEach(s => {
      const active = data.configured[s.key];
      const dot = document.createElement('div');
      dot.className = `status-dot ${active ? 'active' : ''}`;
      dot.innerHTML = `<span class="dot"></span>${s.label}`;
      container.appendChild(dot);
    });
  } catch (e) {
    console.error('Status check failed:', e);
  }
}

// ============================================================
// LOAD DASHBOARD
// ============================================================

async function loadDashboard() {
  const overlay = document.getElementById('loadingOverlay');
  overlay.classList.remove('hidden');

  const dates = getSelectedDates();
  const qs = new URLSearchParams(dates).toString();

  try {
    const res = await fetch(`/api/dashboard?${qs}`);
    const data = await res.json();

    if (data.error) {
      console.error('Dashboard error:', data.error);
      overlay.classList.add('hidden');
      return;
    }

    const { kpis, channels, charts } = data;

    // Render KPIs
    renderKPI('netSales', kpis.netSales.current, kpis.netSales.previous, fmtCurrency, false);
    renderKPI('marketingCosts', kpis.marketingCosts.current, kpis.marketingCosts.previous, fmtCurrency, true);
    renderKPI('percentMarketing', kpis.percentMarketing.current, kpis.percentMarketing.previous, fmtPercent, true);
    renderKPI('orders', kpis.orders.current, kpis.orders.previous, fmtNumber, false);
    renderKPI('aov', kpis.aov.current, kpis.aov.previous, fmtCurrency, false);
    renderKPI('discountCodes', kpis.discountCodes.current, kpis.discountCodes.previous, fmtCurrency, false);
    renderKPI('repeatRate', kpis.repeatRate.current, kpis.repeatRate.previous, fmtPercent, false);
    renderKPI('repeatNetSales', kpis.repeatNetSales.current, kpis.repeatNetSales.previous, fmtCurrency, false);
    renderKPI('blendedCac', kpis.blendedCac.current, kpis.blendedCac.previous, fmtCurrency, true);

    // Sparklines (with date + value for tooltips)
    const salesData = charts.dailySales.map(d => ({ date: d.date, value: d.sales }));
    const costData = charts.dailyMarketingCosts.map(d => ({ date: d.date, value: d.total }));
    const pctData = charts.dailyPercentMarketing.map(d => ({ date: d.date, value: d.percent }));

    createSparkline('chart-netSales', salesData, '#6c5ce7', fmtCurrency);
    createBarChart('chart-marketingCosts', charts.dailyMarketingCosts, '#3b82f6');
    createSparkline('chart-percentMarketing', pctData, '#ff5a5f', fmtPercent);

    // Channel charts — always show all 3 channels
    createChannelLineChart(
      'chart-spendByChannel', 'legend-spend',
      charts.dailySpendByChannel,
      { meta: channels.meta.spend, google: channels.google.spend, tiktok: channels.tiktok.spend },
      fmtCurrency,
      true
    );

    createChannelLineChart(
      'chart-roasByChannel', 'legend-roas',
      charts.dailyRoasByChannel,
      { meta: channels.meta.roas, google: channels.google.roas, tiktok: channels.tiktok.roas },
      fmtMultiplier
    );

    createChannelLineChart(
      'chart-cpmByChannel', 'legend-cpm',
      charts.dailyCpmByChannel,
      { meta: channels.meta.cpm, google: channels.google.cpm, tiktok: channels.tiktok.cpm },
      fmtCurrency
    );

  } catch (err) {
    console.error('Failed to load dashboard:', err);
  } finally {
    overlay.classList.add('hidden');
  }
}

// ============================================================
// PRODUCT BREAKDOWN
// ============================================================

let productPeriod = 'mtd';

async function loadProductBreakdown(period) {
  if (period) productPeriod = period;

  // Show loader overlay
  const page = document.querySelector('.product-page');
  let loader = document.getElementById('productLoader');
  if (!loader) {
    loader = document.createElement('div');
    loader.id = 'productLoader';
    loader.className = 'product-loading-overlay';
    loader.innerHTML = '<div class="spinner"></div><p>Chargement des données produits...</p>';
    page.appendChild(loader);
  }
  loader.style.display = 'flex';

  try {
    const res = await fetch(`/api/product-breakdown?period=${productPeriod}`);
    const data = await res.json();
    const section = document.getElementById('productBreakdownSection');

    if (!data.configured || !data.categories || data.categories.length === 0) {
      section.style.display = 'none';
      loader.style.display = 'none';
      return;
    }

    section.style.display = 'block';
    document.getElementById('productPeriodLabel').textContent = `${data.period.label} — J${data.period.daysElapsed}/${data.period.daysTotal}`;

    // Doughnut chart
    const ctx = document.getElementById('chart-productBreakdown');
    if (ctx) {
      if (chartInstances['chart-productBreakdown']) chartInstances['chart-productBreakdown'].destroy();

      chartInstances['chart-productBreakdown'] = new Chart(ctx, {
        type: 'doughnut',
        data: {
          labels: data.categories.map(c => c.name),
          datasets: [{
            data: data.categories.map(c => c.ca),
            backgroundColor: data.categories.map(c => c.color),
            borderWidth: 2,
            borderColor: '#fff',
          }],
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          cutout: '60%',
          plugins: {
            legend: { display: false },
            tooltip: {
              ...TOOLTIP_CONFIG,
              callbacks: {
                label: function(ctx) {
                  return ` ${ctx.label}: ${fmtCurrency(ctx.parsed)} (${data.categories[ctx.dataIndex].pctOfTotal.toFixed(1)}%)`;
                },
              },
            },
          },
          animation: { duration: 800 },
        },
      });
    }

    // Legend
    const legendEl = document.getElementById('legend-productBreakdown');
    legendEl.innerHTML = data.categories.map(c => `
      <div class="legend-item">
        <div class="legend-dot" style="background:${c.color}"></div>
        <span class="legend-label">${c.name}</span>
        <span class="legend-value">${fmtCurrency(c.ca)} (${c.pctOfTotal.toFixed(0)}%)</span>
      </div>
    `).join('');

    // Category cards with objectives
    const listEl = document.getElementById('productCategoriesList');
    listEl.innerHTML = data.categories.map(c => {
      const hasObj = c.objectiveCA > 0;
      const pct = Math.min(c.progressCA, 100);
      const timePct = data.period.daysElapsed / data.period.daysTotal * 100;
      const onTrack = c.progressCA >= timePct * 0.85;
      const projPct = hasObj ? Math.min((c.projectedCA / c.objectiveCA) * 100, 100) : 0;

      return `
        <div class="product-cat-card">
          <div class="product-cat-header">
            <div class="product-cat-dot" style="background:${c.color}"></div>
            <h4>${c.name}</h4>
            ${hasObj ? `<span class="product-cat-obj-pct">obj ${c.pct}% du total</span>` : ''}
          </div>
          <div class="product-cat-metrics">
            <div class="product-cat-metric">
              <span class="product-cat-val">${fmtCurrency(c.ca)}</span>
              <span class="product-cat-label">CA</span>
            </div>
            <div class="product-cat-metric">
              <span class="product-cat-val">${fmtNumber(c.units)}</span>
              <span class="product-cat-label">Unités</span>
            </div>
            <div class="product-cat-metric">
              <span class="product-cat-val">${c.pctOfTotal.toFixed(1)}%</span>
              <span class="product-cat-label">% du CA</span>
            </div>
          </div>
          ${hasObj ? `
            <div class="product-cat-progress">
              <div class="obj-progress-row">
                <div class="obj-progress-bar">
                  <div class="obj-progress-fill" style="width:${pct}%"></div>
                  <div class="obj-progress-projected" style="width:${projPct}%;opacity:0.25"></div>
                </div>
                <span class="obj-pct" style="color:${c.progressCA >= 100 ? '#00c48c' : onTrack ? 'var(--text-secondary)' : '#ff5a5f'}">${c.progressCA.toFixed(0)}%</span>
              </div>
              <div class="product-cat-obj-details">
                <span>Objectif : ${fmtK(c.objectiveCA)}</span>
                <span>Projection : <strong style="color:${c.projectedCA >= c.objectiveCA ? '#00c48c' : '#ff5a5f'}">${fmtK(c.projectedCA)}</strong></span>
              </div>
            </div>
          ` : ''}
        </div>
      `;
    }).join('');

    // Detailed types table
    const tableEl = document.getElementById('productTypesTable');
    tableEl.innerHTML = `
      <table>
        <thead>
          <tr>
            <th>Type de produit</th>
            <th>Unités</th>
            <th>CA</th>
            <th>% du total</th>
          </tr>
        </thead>
        <tbody>
          ${data.allTypes.map(t => `
            <tr>
              <td class="product-name">${t.type || 'Non défini'}</td>
              <td>${fmtNumber(t.units)}</td>
              <td>${fmtCurrency(t.ca)}</td>
              <td>${t.pctOfTotal.toFixed(1)}%</td>
            </tr>
          `).join('')}
        </tbody>
      </table>
    `;

  } catch (err) {
    console.error('Product breakdown error:', err);
  } finally {
    const loader = document.getElementById('productLoader');
    if (loader) loader.style.display = 'none';
  }
}

// ============================================================
// META ANALYSIS
// ============================================================

let metaAnalysisDays = 15;
let metaAnalysisLoadedDays = null;

async function loadMetaAnalysis(forceDays) {
  const days = forceDays || metaAnalysisDays;
  if (metaAnalysisLoadedDays === days) return;

  const loading = document.getElementById('metaLoading');
  const results = document.getElementById('metaResults');
  loading.style.display = 'flex';
  loading.innerHTML = '<div class="spinner"></div><p>Analyse Meta en cours... (peut prendre 30s)</p>';
  results.style.display = 'none';

  try {
    const res = await fetch(`/api/meta/analysis?days=${days}`);
    const data = await res.json();

    if (data.error) {
      loading.innerHTML = `<p style="color:var(--red)">Erreur: ${data.error}</p>`;
      return;
    }

    // 1. Top 5 Ads
    const topAdsEl = document.getElementById('metaTopAds');
    topAdsEl.innerHTML = data.topAds.map(ad => `
      <div class="meta-ad-card">
        <div class="meta-ad-visual">
          ${ad.thumbnailUrl || ad.imageUrl
            ? `<img src="${ad.thumbnailUrl || ad.imageUrl}" alt="${ad.name}" />`
            : `<div class="meta-ad-no-img">Pas de visuel</div>`}
        </div>
        <div class="meta-ad-info">
          <h4 class="meta-ad-name">${ad.name}</h4>
          <div class="meta-ad-campaign">${ad.campaignName}</div>
          <div class="meta-ad-metrics">
            <div class="meta-metric"><span class="meta-metric-val">${ad.roas.toFixed(2)}x</span><span class="meta-metric-label">ROAS</span></div>
            <div class="meta-metric"><span class="meta-metric-val">${ad.spend.toFixed(0)}€</span><span class="meta-metric-label">Spend</span></div>
            <div class="meta-metric"><span class="meta-metric-val">${ad.revenue.toFixed(0)}€</span><span class="meta-metric-label">Revenue</span></div>
            <div class="meta-metric"><span class="meta-metric-val">${ad.purchases}</span><span class="meta-metric-label">Achats</span></div>
            <div class="meta-metric"><span class="meta-metric-val">${ad.cpa.toFixed(0)}€</span><span class="meta-metric-label">CPA</span></div>
            <div class="meta-metric"><span class="meta-metric-val">${ad.ctr.toFixed(2)}%</span><span class="meta-metric-label">CTR</span></div>
            <div class="meta-metric"><span class="meta-metric-val">${ad.cpm.toFixed(1)}€</span><span class="meta-metric-label">CPM</span></div>
            <div class="meta-metric"><span class="meta-metric-val">${ad.frequency.toFixed(1)}</span><span class="meta-metric-label">Freq.</span></div>
          </div>
        </div>
      </div>
    `).join('');

    // Analysis for top ads
    if (data.analysis.topAdsAnalysis) {
      const analysisDiv = document.createElement('div');
      analysisDiv.className = 'meta-analysis-content';
      analysisDiv.innerHTML = renderMarkdown(data.analysis.topAdsAnalysis);
      topAdsEl.after(analysisDiv);
    }

    // 2. New Ads Proposals
    document.getElementById('metaNewAdsProposals').innerHTML = renderMarkdown(data.analysis.newAdsProposals || 'Analyse non disponible.');

    // 3. Top Adsets
    document.getElementById('metaTopAdsets').innerHTML = renderAdsetCards(data.topAdsets);
    document.getElementById('metaScalingAnalysis').innerHTML = renderMarkdown(data.analysis.scalingAnalysis || 'Analyse non disponible.');

    // 4. Worst Adsets
    document.getElementById('metaWorstAdsets').innerHTML = renderAdsetCards(data.worstAdsets);

    // 5. Global Analysis
    document.getElementById('metaGlobalAnalysis').innerHTML = renderMarkdown(data.analysis.globalAnalysis || 'Analyse non disponible.');

    // Show period
    document.getElementById('metaPeriodLabel').textContent = `${data.period.start} → ${data.period.end}`;

    loading.style.display = 'none';
    results.style.display = 'block';
    metaAnalysisLoadedDays = days;

  } catch (err) {
    console.error('Meta analysis error:', err);
    loading.innerHTML = `<p style="color:var(--red)">Erreur de chargement</p>`;
  }
}

function renderAdsetCards(adsets) {
  return adsets.map(as => `
    <div class="meta-adset-card">
      <div class="meta-adset-header">
        <h4>${as.name}</h4>
        <span class="meta-adset-campaign">${as.campaignName}</span>
      </div>
      <div class="meta-ad-metrics">
        <div class="meta-metric"><span class="meta-metric-val">${as.roas.toFixed(2)}x</span><span class="meta-metric-label">ROAS</span></div>
        <div class="meta-metric"><span class="meta-metric-val">${as.spend.toFixed(0)}€</span><span class="meta-metric-label">Spend</span></div>
        <div class="meta-metric"><span class="meta-metric-val">${as.revenue.toFixed(0)}€</span><span class="meta-metric-label">Revenue</span></div>
        <div class="meta-metric"><span class="meta-metric-val">${as.purchases}</span><span class="meta-metric-label">Achats</span></div>
        <div class="meta-metric"><span class="meta-metric-val">${as.cpa.toFixed(0)}€</span><span class="meta-metric-label">CPA</span></div>
        <div class="meta-metric"><span class="meta-metric-val">${as.ctr.toFixed(2)}%</span><span class="meta-metric-label">CTR</span></div>
        <div class="meta-metric"><span class="meta-metric-val">${as.cpm.toFixed(1)}€</span><span class="meta-metric-label">CPM</span></div>
        <div class="meta-metric"><span class="meta-metric-val">${as.frequency.toFixed(1)}</span><span class="meta-metric-label">Freq.</span></div>
      </div>
    </div>
  `).join('');
}

function renderMarkdown(md) {
  if (!md) return '';
  return md
    .replace(/## (.*)/g, '<h3>$1</h3>')
    .replace(/### (.*)/g, '<h4>$1</h4>')
    .replace(/\*\*([^*]+)\*\*/g, '<strong>$1</strong>')
    .replace(/\n- /g, '\n<li>')
    .replace(/<li>([^<]*?)(?=\n|$)/g, '<li>$1</li>')
    .replace(/(<li>.*<\/li>)/gs, '<ul>$1</ul>')
    .replace(/<\/ul>\s*<ul>/g, '')
    .replace(/\n{2,}/g, '</p><p>')
    .replace(/\n/g, '<br>')
    .replace(/^/, '<p>').replace(/$/, '</p>')
    .replace(/<p><h/g, '<h').replace(/<\/h([34])><\/p>/g, '</h$1>')
    .replace(/<p><ul>/g, '<ul>').replace(/<\/ul><\/p>/g, '</ul>');
}

function renderTiktokAdgroupCards(adgroups, type) {
  return adgroups.map(ag => {
    const budgetDisplay = ag.dailyBudget ? `${parseFloat(ag.dailyBudget).toFixed(0)}€/j` : '—';
    const statusBadge = ag.status ? `<span class="budget-status budget-status-${ag.status === 'ENABLE' ? 'active' : 'paused'}">${ag.status === 'ENABLE' ? 'Actif' : ag.status}</span>` : '';

    let actionBtns = '';
    if (type === 'top' && ag.id) {
      const b = parseFloat(ag.dailyBudget || 0);
      actionBtns = `
        <div class="budget-actions" data-adgroup-id="${ag.id}" data-current-budget="${b}">
          <button class="btn-budget btn-budget-up" onclick="applyTiktokBudget('${ag.id}', ${(b * 1.2).toFixed(2)}, this)" title="+20%">+20%</button>
          <button class="btn-budget btn-budget-up" onclick="applyTiktokBudget('${ag.id}', ${(b * 1.5).toFixed(2)}, this)" title="+50%">+50%</button>
          <button class="btn-budget btn-budget-up" onclick="applyTiktokBudget('${ag.id}', ${(b * 2).toFixed(2)}, this)" title="x2">x2</button>
        </div>`;
    } else if (type === 'worst' && ag.id) {
      const b = parseFloat(ag.dailyBudget || 0);
      actionBtns = `
        <div class="budget-actions" data-adgroup-id="${ag.id}" data-current-budget="${b}">
          <button class="btn-budget btn-budget-down" onclick="applyTiktokBudget('${ag.id}', ${(b * 0.5).toFixed(2)}, this)" title="-50%">-50%</button>
          <button class="btn-budget btn-budget-pause" onclick="applyTiktokAction('${ag.id}', 'pause', this)" title="Pause">Pause</button>
        </div>`;
    }

    return `
      <div class="meta-adset-card">
        <div class="meta-adset-header">
          <h4>${ag.name}</h4>
          <span class="meta-adset-campaign">${ag.campaignName}</span>
        </div>
        <div class="meta-ad-metrics">
          <div class="meta-metric"><span class="meta-metric-val">${ag.roas.toFixed(2)}x</span><span class="meta-metric-label">ROAS</span></div>
          <div class="meta-metric"><span class="meta-metric-val">${ag.spend.toFixed(0)}€</span><span class="meta-metric-label">Spend</span></div>
          <div class="meta-metric"><span class="meta-metric-val">${ag.revenue.toFixed(0)}€</span><span class="meta-metric-label">Revenue</span></div>
          <div class="meta-metric"><span class="meta-metric-val">${ag.purchases}</span><span class="meta-metric-label">Achats</span></div>
          <div class="meta-metric"><span class="meta-metric-val">${ag.cpa.toFixed(0)}€</span><span class="meta-metric-label">CPA</span></div>
          <div class="meta-metric"><span class="meta-metric-val">${ag.ctr.toFixed(2)}%</span><span class="meta-metric-label">CTR</span></div>
          <div class="meta-metric"><span class="meta-metric-val">${ag.cpm.toFixed(1)}€</span><span class="meta-metric-label">CPM</span></div>
          <div class="meta-metric"><span class="meta-metric-val">${budgetDisplay}</span><span class="meta-metric-label">Budget ${statusBadge}</span></div>
        </div>
        ${actionBtns}
      </div>`;
  }).join('');
}

async function applyTiktokBudget(adgroupId, newBudget, btnEl) {
  if (!confirm(`Modifier le budget à ${newBudget.toFixed(0)}€/jour ?`)) return;
  const card = btnEl.closest('.meta-adset-card');
  const actions = card.querySelector('.budget-actions');
  const originalHtml = actions.innerHTML;
  actions.innerHTML = '<span class="budget-loading">Mise à jour...</span>';

  try {
    const res = await fetch('/api/tiktok/update-budget', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ adgroupId, budget: newBudget }),
    });
    const data = await res.json();
    if (data.success) {
      actions.innerHTML = `<span class="budget-success">Budget mis à jour : ${newBudget.toFixed(0)}€/j</span>`;
      // Update the budget display in the metrics
      const metrics = card.querySelectorAll('.meta-metric');
      const budgetMetric = metrics[metrics.length - 1];
      budgetMetric.querySelector('.meta-metric-val').textContent = `${newBudget.toFixed(0)}€/j`;
    } else {
      actions.innerHTML = `<span class="budget-error">Erreur: ${data.error}</span>`;
      setTimeout(() => { actions.innerHTML = originalHtml; }, 3000);
    }
  } catch (err) {
    actions.innerHTML = `<span class="budget-error">Erreur réseau</span>`;
    setTimeout(() => { actions.innerHTML = originalHtml; }, 3000);
  }
}

async function applyTiktokAction(adgroupId, action, btnEl) {
  const label = action === 'pause' ? 'Mettre en pause' : 'Activer';
  if (!confirm(`${label} cet adgroup ?`)) return;
  const card = btnEl.closest('.meta-adset-card');
  const actions = card.querySelector('.budget-actions');
  const originalHtml = actions.innerHTML;
  actions.innerHTML = '<span class="budget-loading">Mise à jour...</span>';

  try {
    const res = await fetch('/api/tiktok/update-budget', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ adgroupId, action }),
    });
    const data = await res.json();
    if (data.success) {
      actions.innerHTML = `<span class="budget-success">${action === 'pause' ? 'Mis en pause' : 'Activé'}</span>`;
    } else {
      actions.innerHTML = `<span class="budget-error">Erreur: ${data.error}</span>`;
      setTimeout(() => { actions.innerHTML = originalHtml; }, 3000);
    }
  } catch (err) {
    actions.innerHTML = `<span class="budget-error">Erreur réseau</span>`;
    setTimeout(() => { actions.innerHTML = originalHtml; }, 3000);
  }
}

// ============================================================
// TIKTOK ANALYSIS
// ============================================================

let tiktokAnalysisDays = 15;
let tiktokAnalysisLoadedDays = null;

async function loadTiktokAnalysis(forceDays) {
  const days = forceDays || tiktokAnalysisDays;
  if (tiktokAnalysisLoadedDays === days) return;

  const loading = document.getElementById('tiktokLoading');
  const results = document.getElementById('tiktokResults');
  loading.style.display = 'flex';
  loading.innerHTML = '<div class="spinner"></div><p>Analyse TikTok en cours... (peut prendre 30s)</p>';
  results.style.display = 'none';

  try {
    const res = await fetch(`/api/tiktok/analysis?days=${days}`);
    const data = await res.json();

    if (data.error) {
      loading.innerHTML = `<p style="color:var(--red)">Erreur: ${data.error}</p>`;
      return;
    }

    // 1. Top 5 Ads (TikTok — no thumbnails, use placeholder)
    const topAdsEl = document.getElementById('tiktokTopAds');
    topAdsEl.innerHTML = data.topAds.map(ad => `
      <div class="meta-ad-card">
        <div class="meta-ad-visual">
          <div class="meta-ad-no-img" style="font-size:24px;">🎵</div>
        </div>
        <div class="meta-ad-info">
          <h4 class="meta-ad-name">${ad.name}</h4>
          <div class="meta-ad-campaign">${ad.campaignName}</div>
          <div class="meta-ad-metrics">
            <div class="meta-metric"><span class="meta-metric-val">${ad.roas.toFixed(2)}x</span><span class="meta-metric-label">ROAS</span></div>
            <div class="meta-metric"><span class="meta-metric-val">${ad.spend.toFixed(0)}€</span><span class="meta-metric-label">Spend</span></div>
            <div class="meta-metric"><span class="meta-metric-val">${ad.revenue.toFixed(0)}€</span><span class="meta-metric-label">Revenue</span></div>
            <div class="meta-metric"><span class="meta-metric-val">${ad.purchases}</span><span class="meta-metric-label">Achats</span></div>
            <div class="meta-metric"><span class="meta-metric-val">${ad.cpa.toFixed(0)}€</span><span class="meta-metric-label">CPA</span></div>
            <div class="meta-metric"><span class="meta-metric-val">${ad.ctr.toFixed(2)}%</span><span class="meta-metric-label">CTR</span></div>
            <div class="meta-metric"><span class="meta-metric-val">${ad.cpm.toFixed(1)}€</span><span class="meta-metric-label">CPM</span></div>
            <div class="meta-metric"><span class="meta-metric-val">${ad.frequency.toFixed(1)}</span><span class="meta-metric-label">Freq.</span></div>
          </div>
        </div>
      </div>
    `).join('');

    // Analysis for top ads
    if (data.analysis.topAdsAnalysis) {
      // Remove any previously injected analysis
      const existingAnalysis = topAdsEl.nextElementSibling;
      if (existingAnalysis && existingAnalysis.classList.contains('meta-analysis-content') && !existingAnalysis.id) {
        existingAnalysis.remove();
      }
      const analysisDiv = document.createElement('div');
      analysisDiv.className = 'meta-analysis-content';
      analysisDiv.innerHTML = renderMarkdown(data.analysis.topAdsAnalysis);
      topAdsEl.after(analysisDiv);
    }

    // 2. New Ads Proposals
    document.getElementById('tiktokNewAdsProposals').innerHTML = renderMarkdown(data.analysis.newAdsProposals || 'Analyse non disponible.');

    // 3. Top Adgroups (with budget actions)
    document.getElementById('tiktokTopAdgroups').innerHTML = renderTiktokAdgroupCards(data.topAdgroups, 'top');
    document.getElementById('tiktokScalingAnalysis').innerHTML = renderMarkdown(data.analysis.scalingAnalysis || 'Analyse non disponible.');

    // 4. Worst Adgroups (with pause/reduce actions)
    document.getElementById('tiktokWorstAdgroups').innerHTML = renderTiktokAdgroupCards(data.worstAdgroups, 'worst');

    // 5. Global Analysis
    document.getElementById('tiktokGlobalAnalysis').innerHTML = renderMarkdown(data.analysis.globalAnalysis || 'Analyse non disponible.');

    // Show period
    document.getElementById('tiktokPeriodLabel').textContent = `${data.period.start} → ${data.period.end}`;

    loading.style.display = 'none';
    results.style.display = 'block';
    tiktokAnalysisLoadedDays = days;

  } catch (err) {
    console.error('TikTok analysis error:', err);
    loading.innerHTML = `<p style="color:var(--red)">Erreur de chargement</p>`;
  }
}

// ============================================================
// AMAZON DASHBOARD
// ============================================================

let amazonLoaded = false;
let amazonKpiDays = 0; // 0 = MTD, 15, 30

async function loadAmazonDashboard(force, days) {
  if (days !== undefined) amazonKpiDays = days;
  if (amazonLoaded && !force && days === undefined) return;

  const loading = document.getElementById('amazonLoading');
  const results = document.getElementById('amazonResults');
  const notConfigured = document.getElementById('amazonNotConfigured');

  loading.style.display = 'flex';
  results.style.display = 'none';
  notConfigured.style.display = 'none';

  try {
    const amzQs = amazonKpiDays > 0 ? `?days=${amazonKpiDays}` : '';
    const res = await fetch(`/api/amazon/dashboard${amzQs}`);
    const data = await res.json();

    if (!data.configured) {
      loading.style.display = 'none';
      notConfigured.style.display = 'block';
      amazonLoaded = true;
      return;
    }

    // Objectives
    if (data.objectives) {
      const obj = data.objectives;

      // Day card
      if (obj.day) {
        const d = obj.day;
        document.getElementById('amz-obj-day-label').textContent = d.label;
        const nowH = new Date();
        const hours = nowH.getHours();
        const mins = String(nowH.getMinutes()).padStart(2, '0');
        const dayTimePct = Math.round(((hours * 60 + nowH.getMinutes()) / 1440) * 100);
        document.getElementById('amz-obj-day-time').textContent = `${hours}h${mins} (${dayTimePct}%)`;
        document.getElementById('amz-obj-day-ca').textContent = fmtK(d.currentCA);
        document.getElementById('amz-obj-day-ca-target').textContent = fmtK(d.dailyCATarget);
        const dayPct = Math.min(d.progressCA, 100);
        document.getElementById('amz-obj-day-ca-bar').style.width = dayPct + '%';
        const dayBadge = document.getElementById('amz-obj-day-ca-pct');
        dayBadge.textContent = d.progressCA.toFixed(0) + '%';
        dayBadge.style.color = d.progressCA >= 100 ? '#00c48c' : '#ff5a5f';

        document.getElementById('amz-obj-day-tacos').textContent = d.tacos.toFixed(1) + '%';
        document.getElementById('amz-obj-day-tacos-target').textContent = d.tacosTarget + '%';
        const dayTacosIndicator = document.getElementById('amz-obj-day-tacos-indicator');
        const dayTacosPct = Math.min((d.tacos / 50) * 100, 100);
        dayTacosIndicator.style.left = dayTacosPct + '%';
        dayTacosIndicator.style.borderColor = d.tacos <= d.tacosTarget ? '#00c48c' : '#ff5a5f';
        document.getElementById('amz-obj-day-tacos-line').style.left = (d.tacosTarget / 50 * 100) + '%';
      }

      // Month card
      if (obj.month) {
        renderAmzObjectivePeriod('month', obj.month);
      }

      // Quarter card
      if (obj.quarter) {
        renderAmzObjectivePeriod('quarter', obj.quarter);
      }
    }

    // KPIs
    document.getElementById('amz-val-ca').textContent = fmtCurrency(data.kpis.ca);
    document.getElementById('amz-val-orders').textContent = fmtNumber(data.kpis.orders);
    document.getElementById('amz-val-tacos').textContent = data.kpis.tacos.toFixed(1) + '%';

    // KPI period label
    const amzPeriodLabel = document.getElementById('amzKpiPeriodLabel');
    if (amzPeriodLabel) amzPeriodLabel.textContent = data.kpis.label || '';

    // Top Products
    const productsEl = document.getElementById('amzTopProducts');
    if (data.topProducts && data.topProducts.length > 0) {
      const totalCA = data.topProducts.reduce((s, p) => s + (p.ca || 0), 0);
      productsEl.innerHTML = `
        <table>
          <thead>
            <tr>
              <th>Produit</th>
              <th>Units sold</th>
              <th>CA</th>
              <th>% du total</th>
            </tr>
          </thead>
          <tbody>
            ${data.topProducts.map(p => `
              <tr>
                <td class="product-name">${p.name || p.asin || '—'}</td>
                <td>${fmtNumber(p.units || 0)}</td>
                <td>${fmtCurrency(p.ca || 0)}</td>
                <td>${totalCA > 0 ? ((p.ca / totalCA) * 100).toFixed(1) : '0'}%</td>
              </tr>
            `).join('')}
          </tbody>
        </table>
      `;
    } else {
      productsEl.innerHTML = '<div class="card" style="text-align:center;padding:24px;color:var(--text-muted);">Données produits non disponibles</div>';
    }

    loading.style.display = 'none';
    results.style.display = 'block';
    amazonLoaded = true;

  } catch (err) {
    console.error('Amazon dashboard error:', err);
    loading.style.display = 'none';
    notConfigured.style.display = 'block';
  }
}

function renderAmzObjectivePeriod(prefix, data) {
  document.getElementById(`amz-obj-${prefix}-label`).textContent = data.label;
  const timePct = Math.round((data.daysElapsed / data.daysTotal) * 100);
  document.getElementById(`amz-obj-${prefix}-days`).textContent = `J${data.daysElapsed}/${data.daysTotal} (${timePct}%)`;

  // CA
  document.getElementById(`amz-obj-${prefix}-ca`).textContent = fmtK(data.currentCA);
  document.getElementById(`amz-obj-${prefix}-ca-target`).textContent = fmtK(data.objectiveCA);
  const pctCA = Math.min(data.progressCA, 100);
  document.getElementById(`amz-obj-${prefix}-ca-bar`).style.width = pctCA + '%';

  const pctBadge = document.getElementById(`amz-obj-${prefix}-ca-pct`);
  pctBadge.textContent = data.progressCA.toFixed(0) + '%';
  pctBadge.style.color = data.progressCA >= 100 ? '#00c48c' : data.progressCA >= (data.daysElapsed / data.daysTotal * 100) * 0.85 ? 'var(--text-secondary)' : '#ff5a5f';

  const projPct = Math.min((data.projectedCA / data.objectiveCA) * 100, 100);
  const projBar = document.getElementById(`amz-obj-${prefix}-ca-proj`);
  projBar.style.width = projPct + '%';
  projBar.style.opacity = '0.25';

  const projEl = document.getElementById(`amz-obj-${prefix}-ca-proj-val`);
  projEl.textContent = fmtK(data.projectedCA);
  projEl.style.color = data.projectedCA >= data.objectiveCA ? '#00c48c' : '#ff5a5f';

  // TACOS
  document.getElementById(`amz-obj-${prefix}-tacos`).textContent = data.tacos.toFixed(1) + '%';
  document.getElementById(`amz-obj-${prefix}-tacos-target`).textContent = data.tacosTarget + '%';

  const tacosIndicator = document.getElementById(`amz-obj-${prefix}-tacos-indicator`);
  const tacosPct = Math.min((data.tacos / 50) * 100, 100);
  tacosIndicator.style.left = tacosPct + '%';
  tacosIndicator.style.borderColor = data.tacos <= data.tacosTarget ? '#00c48c' : '#ff5a5f';

  const tacosLine = document.getElementById(`amz-obj-${prefix}-tacos-line`);
  tacosLine.style.left = (data.tacosTarget / 50 * 100) + '%';

  const projTacosEl = document.getElementById(`amz-obj-${prefix}-tacos-proj-val`);
  projTacosEl.textContent = data.projectedTacos.toFixed(1) + '%';
  projTacosEl.style.color = data.projectedTacos <= data.tacosTarget ? '#00c48c' : '#ff5a5f';
}

// ============================================================
// TABS + SUB-TABS
// ============================================================

function switchTab(tabId) {
  document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
  document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));

  document.querySelector(`.tab-btn[data-tab="${tabId}"]`).classList.add('active');
  document.getElementById(`tab-${tabId}`).classList.add('active');

  if (tabId === 'amazon') loadAmazonDashboard();

  // Show/hide toolbar based on context
  updateToolbarVisibility();
}

function switchSubTab(subtabId) {
  document.querySelectorAll('.subtab-btn').forEach(b => b.classList.remove('active'));
  document.querySelectorAll('.subtab-content').forEach(c => c.classList.remove('active'));

  document.querySelector(`.subtab-btn[data-subtab="${subtabId}"]`).classList.add('active');
  document.getElementById(`subtab-${subtabId}`).classList.add('active');

  if (subtabId === 'data-produits') loadProductBreakdown();
  if (subtabId === 'acquisition') loadMetaAnalysis();

  updateToolbarVisibility();
}

function switchAcqTab(acqId) {
  document.querySelectorAll('.subsubtab-btn').forEach(b => b.classList.remove('active'));
  document.querySelectorAll('.acq-content').forEach(c => c.classList.remove('active'));

  document.querySelector(`.subsubtab-btn[data-acq-tab="${acqId}"]`).classList.add('active');
  document.getElementById(`acq-${acqId}`).classList.add('active');

  if (acqId === 'meta') loadMetaAnalysis();
  if (acqId === 'tiktok') loadTiktokAnalysis();
}

function updateToolbarVisibility() {
  const toolbar = document.querySelector('.toolbar');
  const activeTab = document.querySelector('.tab-btn.active');
  const activeSubTab = document.querySelector('.subtab-btn.active');

  if (activeTab && activeTab.dataset.tab === 'ecommerce' && activeSubTab && activeSubTab.dataset.subtab === 'reporting') {
    toolbar.style.display = 'flex';
  } else {
    toolbar.style.display = 'none';
  }
}

// ============================================================
// EVENT LISTENERS
// ============================================================

document.addEventListener('DOMContentLoaded', () => {
  setDefaultDates();
  loadStatus();
  loadObjectives();
  loadDashboard();

  // Auto-refresh dashboard every 5 minutes
  setInterval(() => {
    const onEcommerce = document.querySelector('.tab-btn[data-tab="ecommerce"]').classList.contains('active');
    const onReporting = document.querySelector('.subtab-btn[data-subtab="reporting"]')?.classList.contains('active');
    if (onEcommerce && onReporting) {
      loadObjectives();
      loadDashboard();
    }
  }, 5 * 60 * 1000);

  document.getElementById('btnApply').addEventListener('click', () => {
    document.querySelectorAll('.quick-ranges .btn').forEach(b => b.classList.remove('active'));
    loadDashboard();
  });

  document.getElementById('btnRefresh').addEventListener('click', () => {
    const activeTab = document.querySelector('.tab-btn.active')?.dataset.tab;
    const activeSubTab = document.querySelector('.subtab-btn.active')?.dataset.subtab;

    if (activeTab === 'amazon') {
      amazonLoaded = false;
      loadAmazonDashboard(true);
    } else if (activeTab === 'ecommerce') {
      if (activeSubTab === 'acquisition') {
        metaAnalysisLoadedDays = null;
        loadMetaAnalysis();
      } else if (activeSubTab === 'data-produits') {
        loadProductBreakdown();
      } else {
        loadDashboard();
      }
    }
  });

  document.querySelectorAll('.quick-ranges .btn').forEach(btn => {
    btn.addEventListener('click', () => {
      setQuickRange(btn.dataset.range);
    });
  });

  // Rank 1 tabs
  document.querySelectorAll('.tab-btn').forEach(btn => {
    btn.addEventListener('click', () => switchTab(btn.dataset.tab));
  });

  // E-commerce sub-tabs
  document.querySelectorAll('.subtab-btn').forEach(btn => {
    btn.addEventListener('click', () => switchSubTab(btn.dataset.subtab));
  });

  // Acquisition sub-sub-tabs (META / TIKTOK)
  document.querySelectorAll('.subsubtab-btn').forEach(btn => {
    btn.addEventListener('click', () => switchAcqTab(btn.dataset.acqTab));
  });

  // Product period buttons (MTD / QTD / YTD)
  document.querySelectorAll('[data-product-period]').forEach(btn => {
    btn.addEventListener('click', () => {
      document.querySelectorAll('[data-product-period]').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      loadProductBreakdown(btn.dataset.productPeriod);
    });
  });

  // Amazon KPI period buttons (MTD / 15J / 30J)
  document.querySelectorAll('[data-amz-days]').forEach(btn => {
    btn.addEventListener('click', () => {
      document.querySelectorAll('[data-amz-days]').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      loadAmazonDashboard(true, parseInt(btn.dataset.amzDays));
    });
  });

  // Meta period buttons (15j / 30j)
  document.querySelectorAll('[data-meta-days]').forEach(btn => {
    btn.addEventListener('click', () => {
      const days = parseInt(btn.dataset.metaDays);
      metaAnalysisDays = days;
      metaAnalysisLoadedDays = null;
      document.querySelectorAll('[data-meta-days]').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      loadMetaAnalysis(days);
    });
  });

  // TikTok period buttons (15j / 30j)
  document.querySelectorAll('[data-tiktok-days]').forEach(btn => {
    btn.addEventListener('click', () => {
      const days = parseInt(btn.dataset.tiktokDays);
      tiktokAnalysisDays = days;
      tiktokAnalysisLoadedDays = null;
      document.querySelectorAll('[data-tiktok-days]').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      loadTiktokAnalysis(days);
    });
  });

  document.querySelectorAll('.date-input').forEach(input => {
    input.addEventListener('click', function() { this.showPicker(); });
  });
});

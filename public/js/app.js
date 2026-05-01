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
  if (range === 'yesterday') {
    end.setDate(end.getDate() - 1);
    start = new Date(end);
  } else if (range === 'mtd') {
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
        borderColor: color || '#1a1a1a',
        borderWidth: 2,
        fill: {
          target: 'origin',
          above: (color || '#1a1a1a') + '15',
        },
        tension: 0.4,
        pointRadius: 0,
        pointHoverRadius: 4,
        pointHoverBackgroundColor: color || '#1a1a1a',
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
        backgroundColor: color || '#1a1a1a',
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

function createCountryPieChart(canvasId, legendId, countryData) {
  const ctx = document.getElementById(canvasId);
  if (!ctx) return;
  if (chartInstances[canvasId]) chartInstances[canvasId].destroy();

  // Sort by count descending, group small countries into "Autres"
  const entries = Object.entries(countryData).sort((a, b) => b[1] - a[1]);
  const total = entries.reduce((s, e) => s + e[1], 0);
  const labels = [], values = [];
  let autresCount = 0;
  entries.forEach(([country, count]) => {
    if (count / total < 0.02 && entries.length > 6) {
      autresCount += count;
    } else {
      labels.push(country);
      values.push(count);
    }
  });
  if (autresCount > 0) { labels.push('Autres'); values.push(autresCount); }

  const colors = ['#1a1a1a', '#3b82f6', '#ef4444', '#10b981', '#f59e0b', '#8b5cf6', '#ec4899', '#6366f1', '#14b8a6', '#f97316', '#9ca3af'];

  chartInstances[canvasId] = new Chart(ctx, {
    type: 'doughnut',
    data: {
      labels,
      datasets: [{
        data: values,
        backgroundColor: colors.slice(0, labels.length),
        borderWidth: 2,
        borderColor: '#fff',
      }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: true,
      plugins: {
        legend: { display: false },
        tooltip: {
          ...TOOLTIP_CONFIG,
          callbacks: {
            label: function(ctx) {
              const pct = total > 0 ? ((ctx.parsed / total) * 100).toFixed(1) : 0;
              return `${ctx.label}: ${ctx.parsed} commandes (${pct}%)`;
            },
          },
        },
      },
      animation: { duration: 800 },
    },
  });

  // Custom legend
  const legendEl = document.getElementById(legendId);
  if (legendEl) {
    legendEl.innerHTML = labels.map((label, i) => {
      const pct = total > 0 ? ((values[i] / total) * 100).toFixed(1) : 0;
      return `<span style="display:inline-flex;align-items:center;gap:4px;margin-right:12px;font-size:12px;">
        <span style="width:10px;height:10px;border-radius:50%;background:${colors[i]};display:inline-block;"></span>
        ${label} ${pct}%
      </span>`;
    }).join('');
  }
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
    // Append blended ROAS inside the % Marketing card
    if (kpis.blendedRoas) {
      const roasEl = document.getElementById('val-percentMarketing');
      if (roasEl) {
        roasEl.innerHTML += `<span class="kpi-roas-inline">ROAS ${kpis.blendedRoas.current.toFixed(2)}x</span>`;
      }
    }
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

    createSparkline('chart-netSales', salesData, '#1a1a1a', fmtCurrency);
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

    // Country pie chart
    if (data.ordersByCountry && Object.keys(data.ordersByCountry).length > 0) {
      createCountryPieChart('chart-ordersByCountry', 'legend-countries', data.ordersByCountry);
    }

    // Load Recharge data (non-blocking)
    loadRechargeData();

  } catch (err) {
    console.error('Failed to load dashboard:', err);
  } finally {
    overlay.classList.add('hidden');
  }
}

// ============================================================
// RECHARGE — Subscription tracking
// ============================================================

async function loadRechargeData() {
  try {
    const res = await fetch('/api/recharge/subscriptions');
    if (!res.ok) return; // Recharge not configured — hide section silently
    const data = await res.json();
    if (data.error) return;

    // Show sections
    document.getElementById('section-recharge').style.display = '';
    document.getElementById('section-recharge-chart').style.display = '';

    // KPI values
    document.getElementById('val-rechargeActive').textContent = data.activeSubscriptions.toLocaleString('fr-FR');
    document.getElementById('val-rechargeNew').textContent = `+${data.newLast30d}`;
    document.getElementById('val-rechargeCancelled').textContent = data.cancelledSubscriptions.toLocaleString('fr-FR');

    // Charts
    renderRechargeTrendChart(data.dailyTrend);
    if (data.byProduct) renderRechargeByProductChart(data.byProduct);
    if (data.byCategory) renderRechargeByCategoryChart(data.byCategory);
  } catch (e) {
    console.error('[Recharge] Load error:', e);
  }
}

function renderRechargeTrendChart(dailyTrend) {
  const ctx = document.getElementById('chart-rechargetrend');
  if (!ctx || !dailyTrend?.length) return;
  if (chartInstances['chart-rechargetrend']) chartInstances['chart-rechargetrend'].destroy();

  const labels = dailyTrend.map(d => {
    const date = new Date(d.date);
    return date.toLocaleDateString('fr-FR', { day: 'numeric', month: 'short' });
  });

  chartInstances['chart-rechargetrend'] = new Chart(ctx, {
    type: 'bar',
    data: {
      labels,
      datasets: [
        {
          label: 'Nouveaux',
          data: dailyTrend.map(d => d.created),
          backgroundColor: '#5c6ac4',
          borderRadius: 4,
        },
        {
          label: 'Résiliés',
          data: dailyTrend.map(d => -d.cancelled),
          backgroundColor: '#ef4444',
          borderRadius: 4,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: 'index', intersect: false },
      plugins: {
        legend: { display: true, position: 'top', labels: { font: { size: 11 }, usePointStyle: true } },
        tooltip: {
          callbacks: {
            label: (ctx) => `${ctx.dataset.label}: ${Math.abs(ctx.raw)}`,
          },
        },
      },
      scales: {
        x: { stacked: true, grid: { display: false }, ticks: { font: { size: 10 }, maxTicksLimit: 15 } },
        y: { stacked: true, grid: { color: '#f0f0f0' }, ticks: { font: { size: 10 } } },
      },
    },
  });
}

function renderRechargeByProductChart(byProduct) {
  const ctx = document.getElementById('chart-rechargeByProduct');
  if (!ctx || !byProduct?.length) return;
  if (chartInstances['chart-rechargeByProduct']) chartInstances['chart-rechargeByProduct'].destroy();

  const colors = ['#5c6ac4', '#3b82f6', '#06b6d4', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6', '#ec4899', '#6b7280', '#14b8a6'];

  chartInstances['chart-rechargeByProduct'] = new Chart(ctx, {
    type: 'doughnut',
    data: {
      labels: byProduct.map(p => p.product),
      datasets: [{
        data: byProduct.map(p => p.count),
        backgroundColor: byProduct.map((_, i) => colors[i % colors.length]),
        borderWidth: 2,
        borderColor: '#fff',
      }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: true, position: 'bottom', labels: { font: { size: 10 }, padding: 8, boxWidth: 12 } },
        tooltip: {
          callbacks: {
            label: (ctx) => {
              const total = ctx.dataset.data.reduce((a, b) => a + b, 0);
              const pct = total > 0 ? ((ctx.raw / total) * 100).toFixed(1) : 0;
              return `${ctx.label}: ${ctx.raw} (${pct}%)`;
            },
          },
        },
      },
    },
  });
}

function renderRechargeByCategoryChart(byCategory) {
  const ctx = document.getElementById('chart-rechargeByCategory');
  if (!ctx) return;
  if (chartInstances['chart-rechargeByCategory']) chartInstances['chart-rechargeByCategory'].destroy();

  const entries = Object.entries(byCategory).filter(([, v]) => v > 0);
  if (!entries.length) return;

  const catColors = { 'Litière': '#5c6ac4', 'Box Jouet': '#f59e0b', 'Autre': '#6b7280' };

  chartInstances['chart-rechargeByCategory'] = new Chart(ctx, {
    type: 'doughnut',
    data: {
      labels: entries.map(([k]) => k),
      datasets: [{
        data: entries.map(([, v]) => v),
        backgroundColor: entries.map(([k]) => catColors[k] || '#6b7280'),
        borderWidth: 2,
        borderColor: '#fff',
      }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: true, position: 'bottom', labels: { font: { size: 11 }, padding: 10, boxWidth: 14 } },
        tooltip: {
          callbacks: {
            label: (ctx) => {
              const total = ctx.dataset.data.reduce((a, b) => a + b, 0);
              const pct = total > 0 ? ((ctx.raw / total) * 100).toFixed(1) : 0;
              return `${ctx.label}: ${ctx.raw} (${pct}%)`;
            },
          },
        },
      },
    },
  });
}

// ============================================================
// PRODUCT BREAKDOWN
// ============================================================

let productPeriod = 'mtd';
let productDateRange = null; // { start, end } for custom range

async function loadProductBreakdown(period) {
  if (period) { productPeriod = period; productDateRange = null; }

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
    const qs = productDateRange
      ? `start=${productDateRange.start}&end=${productDateRange.end}`
      : `period=${productPeriod}`;
    const res = await fetch(`/api/product-breakdown?${qs}`);
    const data = await res.json();
    const section = document.getElementById('productBreakdownSection');

    if (!data.configured || !data.categories || data.categories.length === 0) {
      section.style.display = 'none';
      loader.style.display = 'none';
      return;
    }

    section.style.display = 'block';
    document.getElementById('productPeriodLabel').textContent = `${data.period.label} — J${data.period.daysElapsed}/${data.period.daysTotal}`;
    document.getElementById('productDateStart').value = data.period.start;
    document.getElementById('productDateEnd').value = data.period.end;

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
let metaAnalysisDateRange = null; // { start: 'YYYY-MM-DD', end: 'YYYY-MM-DD' } for custom range
let metaAnalysisLoadedRange = null;

function renderMetaKpiChange(current, previous, invert) {
  if (!previous || previous === 0) return '';
  const pct = ((current - previous) / Math.abs(previous)) * 100;
  const isGood = invert ? pct < 0 : pct > 0;
  const color = isGood ? 'var(--green)' : 'var(--red)';
  const arrow = pct > 0 ? '&#9650;' : '&#9660;';
  return `<div class="meta-kpi-comp" style="color:${color}">${arrow} ${Math.abs(pct).toFixed(1)}%</div>`;
}

function renderMetaCpaTrendChart(dailyTrend) {
  const ctx = document.getElementById('chart-metaCpaTrend');
  if (!ctx || !dailyTrend?.length) return;
  if (chartInstances['chart-metaCpaTrend']) chartInstances['chart-metaCpaTrend'].destroy();

  const labels = dailyTrend.map(d => {
    const date = new Date(d.date);
    return date.toLocaleDateString('fr-FR', { day: 'numeric', month: 'short' });
  });

  chartInstances['chart-metaCpaTrend'] = new Chart(ctx, {
    type: 'line',
    data: {
      labels,
      datasets: [
        { label: 'CPA (€)', data: dailyTrend.map(d => d.cpa), borderColor: '#ef4444', backgroundColor: 'rgba(239,68,68,0.08)', fill: true, tension: 0.3, pointRadius: 3, yAxisID: 'y' },
        { label: 'CPM (€)', data: dailyTrend.map(d => d.cpm), borderColor: '#f59e0b', borderDash: [4, 3], fill: false, tension: 0.3, pointRadius: 2, yAxisID: 'y' },
        { label: 'ROAS', data: dailyTrend.map(d => d.roas), borderColor: '#3b82f6', fill: false, tension: 0.3, pointRadius: 3, yAxisID: 'y1' },
        { label: 'CTR Link (%)', data: dailyTrend.map(d => d.ctrLink), borderColor: '#10b981', borderDash: [6, 3], fill: false, tension: 0.3, pointRadius: 2, yAxisID: 'y2' },
      ],
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      interaction: { mode: 'index', intersect: false },
      plugins: { legend: { display: true, position: 'top', labels: { font: { size: 11 }, usePointStyle: true } }, tooltip: { ...TOOLTIP_CONFIG } },
      scales: {
        x: { grid: { display: false }, ticks: { font: { size: 10 }, maxTicksLimit: 10 } },
        y: { position: 'left', grid: { color: '#f0f0f0' }, title: { display: true, text: 'CPA / CPM (€)', font: { size: 10 } }, ticks: { font: { size: 10 }, callback: v => v.toFixed(0) + '€' } },
        y1: { position: 'right', grid: { display: false }, title: { display: true, text: 'ROAS', font: { size: 10 } }, ticks: { font: { size: 10 }, callback: v => v.toFixed(1) + 'x' } },
        y2: { position: 'right', grid: { display: false }, title: { display: true, text: 'CTR Link', font: { size: 10 } }, ticks: { font: { size: 10 }, callback: v => v.toFixed(1) + '%' } },
      },
    },
  });
}

function renderMetaCampaignPieChart(campaigns) {
  const ctx = document.getElementById('chart-metaCampaignSpend');
  if (!ctx || !campaigns?.length) return;
  if (chartInstances['chart-metaCampaignSpend']) chartInstances['chart-metaCampaignSpend'].destroy();

  const colors = ['#1a1a1a', '#3b82f6', '#ef4444', '#10b981', '#f59e0b', '#8b5cf6', '#ec4899', '#6366f1', '#14b8a6', '#f97316'];
  const total = campaigns.reduce((s, c) => s + c.spend, 0);
  // Group small campaigns into "Autres"
  const labels = [], values = [];
  let autresSpend = 0;
  campaigns.forEach(c => {
    if (c.spend / total < 0.03 && campaigns.length > 6) { autresSpend += c.spend; }
    else { labels.push(c.name.length > 25 ? c.name.substring(0, 25) + '...' : c.name); values.push(c.spend); }
  });
  if (autresSpend > 0) { labels.push('Autres'); values.push(autresSpend); }

  chartInstances['chart-metaCampaignSpend'] = new Chart(ctx, {
    type: 'doughnut',
    data: { labels, datasets: [{ data: values, backgroundColor: colors.slice(0, labels.length), borderWidth: 2, borderColor: '#fff' }] },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: {
        legend: { display: false },
        tooltip: { ...TOOLTIP_CONFIG, callbacks: { label: ctx => `${ctx.label}: ${ctx.parsed.toFixed(0)}€ (${(ctx.parsed / total * 100).toFixed(1)}%)` } },
      },
    },
  });

  const legendEl = document.getElementById('legend-metaCampaigns');
  if (legendEl) {
    legendEl.innerHTML = labels.map((l, i) => {
      const pct = (values[i] / total * 100).toFixed(1);
      return `<span style="display:inline-flex;align-items:center;gap:4px;margin-right:10px;margin-bottom:4px;font-size:11px;">
        <span style="width:8px;height:8px;border-radius:50%;background:${colors[i]};display:inline-block;"></span>
        ${l} ${pct}%
      </span>`;
    }).join('');
  }
}

let _metaAllTopAds = [];

function renderMetaAdCards(ads) {
  return ads.map(ad => `
    <div class="meta-ad-card">
      <div class="meta-ad-visual">
        ${ad.thumbnailUrl || ad.imageUrl
          ? `<img src="${ad.imageUrl || ad.thumbnailUrl}" alt="${ad.name}" />`
          : `<div class="meta-ad-no-img">Pas de visuel</div>`}
      </div>
      <div class="meta-ad-info">
        <h4 class="meta-ad-name">${ad.name}</h4>
        <div class="meta-ad-campaign">${ad.campaignName}${ad.isVideo ? ' <span style="color:#3b82f6;font-size:10px;">VIDEO</span>' : ''}</div>
        <a class="meta-ad-link" href="${ad.adsManagerUrl || '#'}" target="_blank" rel="noopener">Voir dans Ads Manager &#8599;</a>
        <div class="meta-ad-metrics">
          <div class="meta-metric"><span class="meta-metric-val">${ad.roas.toFixed(2)}x</span><span class="meta-metric-label">ROAS</span></div>
          <div class="meta-metric"><span class="meta-metric-val">${ad.spend.toFixed(0)}€</span><span class="meta-metric-label">Spend</span></div>
          <div class="meta-metric"><span class="meta-metric-val">${ad.revenue.toFixed(0)}€</span><span class="meta-metric-label">Revenue</span></div>
          <div class="meta-metric"><span class="meta-metric-val">${ad.purchases}</span><span class="meta-metric-label">Achats</span></div>
          <div class="meta-metric"><span class="meta-metric-val">${ad.cpa.toFixed(0)}€</span><span class="meta-metric-label">CPA</span></div>
          <div class="meta-metric"><span class="meta-metric-val">${ad.hookRate !== null ? ad.hookRate.toFixed(1) + '%' : '—'}</span><span class="meta-metric-label">Hook Rate</span></div>
          <div class="meta-metric"><span class="meta-metric-val">${ad.holdRate !== null ? ad.holdRate.toFixed(1) + '%' : '—'}</span><span class="meta-metric-label">Hold Rate</span></div>
          <div class="meta-metric"><span class="meta-metric-val">${ad.ctrLink.toFixed(2)}%</span><span class="meta-metric-label">CTR Link</span></div>
          <div class="meta-metric"><span class="meta-metric-val">${ad.cpcLink.toFixed(2)}€</span><span class="meta-metric-label">CPC Link</span></div>
          <div class="meta-metric"><span class="meta-metric-val">${ad.cpm.toFixed(1)}€</span><span class="meta-metric-label">CPM</span></div>
          <div class="meta-metric"><span class="meta-metric-val">${ad.frequency.toFixed(1)}</span><span class="meta-metric-label">Freq.</span></div>
          <div class="meta-metric"><span class="meta-metric-val">${ad.ctr.toFixed(2)}%</span><span class="meta-metric-label">CTR All</span></div>
        </div>
      </div>
    </div>
  `).join('');
}

function filterMetaAds(type) {
  let filtered = _metaAllTopAds;
  if (type === 'video') filtered = _metaAllTopAds.filter(a => a.isVideo);
  else if (type === 'static') filtered = _metaAllTopAds.filter(a => !a.isVideo);
  else if (type === 'acquisition') filtered = _metaAllTopAds.filter(a => a.campaignType === 'acquisition');
  else if (type === 'retargeting') filtered = _metaAllTopAds.filter(a => a.campaignType === 'retargeting');
  document.getElementById('metaTopAds').innerHTML = renderMetaAdCards(filtered.slice(0, 24));
}

async function loadMetaAnalysis(forceDays, forceRefresh) {
  const range = metaAnalysisDateRange;
  const days = forceDays || metaAnalysisDays;

  // Cache check: skip if already loaded with same params
  if (!forceRefresh) {
    if (range && metaAnalysisLoadedRange &&
        metaAnalysisLoadedRange.start === range.start && metaAnalysisLoadedRange.end === range.end) return;
    if (!range && metaAnalysisLoadedDays === days) return;
  }

  const loading = document.getElementById('metaLoading');
  const results = document.getElementById('metaResults');
  loading.style.display = 'flex';
  loading.innerHTML = '<div class="spinner"></div><p>Analyse Meta en cours... (peut prendre 30s)</p>';
  results.style.display = 'none';

  try {
    let qs = range ? `start=${range.start}&end=${range.end}` : `days=${days}`;
    if (forceRefresh) qs += '&refresh=1';
    const res = await fetch(`/api/meta/analysis?${qs}`);
    const data = await res.json();

    if (data.error) {
      loading.innerHTML = `<p style="color:var(--red)">Erreur: ${data.error}</p>`;
      return;
    }

    const t = data.accountTotals;
    const c = data.compTotals || {};

    // 0. KPI Summary Cards
    document.getElementById('metaKpiGrid').innerHTML = [
      { label: 'Spend', val: `${t.spend.toFixed(0)}€`, comp: renderMetaKpiChange(t.spend, c.spend, true) },
      { label: 'Revenue', val: `${t.revenue.toFixed(0)}€`, comp: renderMetaKpiChange(t.revenue, c.revenue, false) },
      { label: 'ROAS', val: `${t.roas.toFixed(2)}x`, comp: renderMetaKpiChange(t.roas, c.roas, false) },
      { label: 'CPA', val: `${t.cpa.toFixed(0)}€`, comp: renderMetaKpiChange(t.cpa, c.cpa, true) },
      { label: 'Achats', val: `${t.purchases}`, comp: renderMetaKpiChange(t.purchases, c.purchases, false) },
      { label: 'CPM', val: `${t.cpm.toFixed(1)}€`, comp: renderMetaKpiChange(t.cpm, c.cpm, true) },
    ].map(kpi => `
      <div class="meta-kpi-card">
        <div class="meta-kpi-label">${kpi.label}</div>
        <div class="meta-kpi-val">${kpi.val}</div>
        ${kpi.comp}
      </div>
    `).join('');

    // 1. Charts
    renderMetaCpaTrendChart(data.dailyTrend);
    renderMetaCampaignPieChart(data.campaignBreakdown);

    // 2. Top Ads (with filter support)
    _metaAllTopAds = data.topAds;
    filterMetaAds('all');

    // Analysis for top ads
    if (data.analysis.topAdsAnalysis) {
      const analysisDiv = document.createElement('div');
      analysisDiv.className = 'meta-analysis-content';
      analysisDiv.innerHTML = renderMarkdown(data.analysis.topAdsAnalysis);
      topAdsEl.after(analysisDiv);
    }

    // 3. New Ads Proposals
    document.getElementById('metaNewAdsProposals').innerHTML = renderMarkdown(data.analysis.newAdsProposals || 'Analyse non disponible.');

    // 4. Top Adsets
    document.getElementById('metaTopAdsets').innerHTML = renderAdsetCards(data.topAdsets);
    document.getElementById('metaScalingAnalysis').innerHTML = renderMarkdown(data.analysis.scalingAnalysis || 'Analyse non disponible.');

    // 5. Worst Adsets
    document.getElementById('metaWorstAdsets').innerHTML = renderAdsetCards(data.worstAdsets);

    // 6. Global Analysis
    document.getElementById('metaGlobalAnalysis').innerHTML = renderMarkdown(data.analysis.globalAnalysis || 'Analyse non disponible.');

    // Show period
    document.getElementById('metaPeriodLabel').textContent = `${data.period.start} → ${data.period.end}`;

    // Update date picker to reflect loaded period
    document.getElementById('metaDateStart').value = data.period.start;
    document.getElementById('metaDateEnd').value = data.period.end;

    loading.style.display = 'none';
    results.style.display = 'block';
    metaAnalysisLoadedDays = range ? null : days;
    metaAnalysisLoadedRange = range ? { ...range } : null;

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
// TIKTOK SPARK ADS CREATOR
// ============================================================

let sparkSelectedPosts = [];

async function searchSparkPosts(showAll) {
  const keywords = showAll ? '' : document.getElementById('sparkKeywords').value.trim();

  const status = document.getElementById('sparkSearchStatus');
  const grid = document.getElementById('sparkPostsGrid');
  const selBar = document.getElementById('sparkSelectionBar');

  status.style.display = 'flex';
  status.innerHTML = '<div class="spinner" style="width:20px;height:20px;"></div><span style="margin-left:8px;">Chargement des posts...</span>';
  grid.style.display = 'none';
  selBar.style.display = 'none';
  sparkSelectedPosts = [];

  try {
    const url = keywords
      ? `/api/tiktok/spark-posts?keywords=${encodeURIComponent(keywords)}`
      : `/api/tiktok/spark-posts`;
    const res = await fetch(url);
    const data = await res.json();

    if (data.error) {
      status.innerHTML = `<p style="color:var(--red)">Erreur: ${data.error}</p>`;
      return;
    }

    if (data.posts.length === 0) {
      status.innerHTML = `<p style="color:var(--text-muted)">Aucun post trouvé${keywords ? ` pour "${keywords}"` : ''} (${data.total} posts autorisés au total)</p>`;
      return;
    }

    status.innerHTML = keywords
      ? `<span style="color:var(--text-secondary)">${data.filtered} post(s) trouvé(s) sur ${data.total} autorisés</span>`
      : `<span style="color:var(--text-secondary)">${data.total} posts autorisés</span>`;

    grid.innerHTML = data.posts.map(post => {
      const durationStr = post.duration ? `${Math.floor(post.duration)}s` : '';
      const captionShort = (post.caption || '').replace(/"/g, '&quot;').substring(0, 60);
      return `
      <label class="spark-post-card" data-item-id="${post.itemId}">
        <input type="checkbox" class="spark-post-check" value="${post.itemId}"
          data-caption="${captionShort}"
          data-identity-id="${post.identityId || ''}"
          data-auth-code="${post.authCode || ''}"
          data-cover="${post.coverUrl || ''}"
          onchange="updateSparkSelection()" />
        <div class="spark-post-visual">
          ${post.coverUrl ? `<img src="${post.coverUrl}" alt="" />` : '<div class="meta-ad-no-img">Pas de cover</div>'}
        </div>
        <div class="spark-post-info">
          <p class="spark-post-caption">${post.caption || 'Sans caption'}</p>
          <div class="spark-post-stats">
            ${durationStr ? `<span>${durationStr}</span>` : ''}
            ${post.authStatus === 'AUTHORIZED' ? '<span style="color:var(--green)">Autorisé</span>' : ''}
          </div>
          <div class="spark-post-bottom">
            ${post.identityName ? `<span class="spark-post-author">${post.identityName}</span>` : ''}
            <a href="https://www.tiktok.com/@/video/${post.itemId}" target="_blank" rel="noopener" class="spark-post-link" onclick="event.stopPropagation()" title="Voir sur TikTok">
              <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"/><polyline points="15 3 21 3 21 9"/><line x1="10" y1="14" x2="21" y2="3"/></svg>
            </a>
          </div>
        </div>
        <div class="spark-post-check-icon"></div>
      </label>
    `;}).join('');

    grid.style.display = 'grid';
    selBar.style.display = 'flex';
    updateSparkSelection();

  } catch (err) {
    status.innerHTML = `<p style="color:var(--red)">Erreur réseau</p>`;
  }
}

function formatNumber(n) {
  if (n >= 1000000) return (n / 1000000).toFixed(1) + 'M';
  if (n >= 1000) return (n / 1000).toFixed(1) + 'K';
  return n;
}

function updateSparkSelection() {
  const checks = document.querySelectorAll('.spark-post-check:checked');
  sparkSelectedPosts = Array.from(checks).map(cb => ({
    itemId: cb.value,
    caption: cb.dataset.caption,
    identityId: cb.dataset.identityId,
    coverUrl: cb.dataset.cover,
  }));
  document.getElementById('sparkSelectedCount').textContent = `${sparkSelectedPosts.length} post(s) sélectionné(s)`;

  // Toggle card styling
  document.querySelectorAll('.spark-post-card').forEach(card => {
    card.classList.toggle('selected', card.querySelector('.spark-post-check').checked);
  });
}

function goToSparkStep2() {
  if (sparkSelectedPosts.length === 0) return;
  document.getElementById('sparkStep1').style.display = 'none';
  document.getElementById('sparkStep2').style.display = 'block';

  // Show selected posts summary
  document.getElementById('sparkSelectedPosts').innerHTML = `
    <h4>${sparkSelectedPosts.length} post(s) sélectionné(s)</h4>
    <div class="spark-selected-grid">
      ${sparkSelectedPosts.map(p => `
        <div class="spark-selected-thumb">
          ${p.coverUrl ? `<img src="${p.coverUrl}" alt="" />` : ''}
          <span>${p.caption || 'Post'}</span>
        </div>
      `).join('')}
    </div>`;
}

function backToSparkStep1() {
  document.getElementById('sparkStep2').style.display = 'none';
  document.getElementById('sparkStep1').style.display = 'block';
}

async function createSparkCampaign() {
  const name = document.getElementById('sparkCampName').value.trim();
  const budget = parseFloat(document.getElementById('sparkBudget').value);
  if (!name) return alert('Nom de campagne requis');
  if (!budget || budget < 5) return alert('Budget minimum : 5€/jour');
  if (!confirm(`Créer la campagne "${name}" avec ${sparkSelectedPosts.length} post(s) et un budget de ${budget}€/jour ?`)) return;

  const btn = document.getElementById('sparkCreateBtn');
  btn.disabled = true;
  btn.textContent = 'Création en cours...';

  try {
    const res = await fetch('/api/tiktok/create-spark-campaign', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        campaignName: name,
        dailyBudget: budget,
        posts: sparkSelectedPosts,
      }),
    });
    const data = await res.json();

    document.getElementById('sparkStep2').style.display = 'none';
    document.getElementById('sparkStep3').style.display = 'block';

    if (data.success) {
      const adsOk = data.ads.filter(a => a.success).length;
      const adsFail = data.ads.filter(a => !a.success).length;
      document.getElementById('sparkResult').innerHTML = `
        <div class="spark-result-success">
          <h4>Campagne créée avec succès</h4>
          <div class="spark-result-details">
            <p><strong>Campagne :</strong> ${data.campaignName} (ID: ${data.campaignId})</p>
            <p><strong>Budget :</strong> ${data.dailyBudget}€/jour</p>
            <p><strong>Ads créées :</strong> ${adsOk}/${data.ads.length}</p>
            ${adsFail > 0 ? `<p style="color:var(--red)"><strong>${adsFail} ad(s) en erreur :</strong></p>
              <ul>${data.ads.filter(a => !a.success).map(a => `<li>${a.error}</li>`).join('')}</ul>` : ''}
          </div>
          <button class="btn btn-primary" onclick="resetSparkCreator()" style="margin-top:16px;">Créer une autre campagne</button>
        </div>`;
    } else {
      document.getElementById('sparkResult').innerHTML = `
        <div class="spark-result-error">
          <h4>Erreur lors de la création</h4>
          <p>${data.error}</p>
          <p>Étape : ${data.step || 'inconnue'}</p>
          <button class="btn btn-ghost" onclick="resetSparkCreator()" style="margin-top:16px;">Réessayer</button>
        </div>`;
    }
  } catch (err) {
    document.getElementById('sparkResult').innerHTML = `<div class="spark-result-error"><h4>Erreur réseau</h4><p>${err.message}</p></div>`;
  } finally {
    btn.disabled = false;
    btn.textContent = 'Lancer la campagne';
  }
}

function resetSparkCreator() {
  document.getElementById('sparkStep1').style.display = 'block';
  document.getElementById('sparkStep2').style.display = 'none';
  document.getElementById('sparkStep3').style.display = 'none';
  sparkSelectedPosts = [];
  document.querySelectorAll('.spark-post-check').forEach(cb => { cb.checked = false; });
  updateSparkSelection();
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
// B2B — Pipedrive Reporting
// ============================================================

let b2bSourceChart = null;
let b2bLoaded = false;

function getB2BDateRange(range) {
  const now = new Date();
  const end = new Date(now);
  end.setDate(end.getDate() - 1); // yesterday
  let start;

  switch (range) {
    case '7d':
      start = new Date(end);
      start.setDate(start.getDate() - 6);
      break;
    case 'mtd':
      start = new Date(end.getFullYear(), end.getMonth(), 1);
      break;
    case 'qtd': {
      const qMonth = Math.floor(end.getMonth() / 3) * 3;
      start = new Date(end.getFullYear(), qMonth, 1);
      break;
    }
    case 'ytd':
      start = new Date(end.getFullYear(), 0, 1);
      break;
    default:
      start = new Date(end.getFullYear(), end.getMonth(), 1);
  }

  return {
    start: start.toISOString().split('T')[0],
    end: end.toISOString().split('T')[0],
  };
}

function fmtB2BCurrency(v) {
  return v.toLocaleString('fr-FR', { style: 'currency', currency: 'EUR', maximumFractionDigits: 0 });
}

async function loadB2BReport(range) {
  const loading = document.getElementById('b2bLoading');
  const kpis = document.getElementById('b2bKpis');

  loading.style.display = 'block';
  kpis.style.opacity = '0.3';

  let start, end;
  if (range) {
    const dates = getB2BDateRange(range);
    start = dates.start;
    end = dates.end;
    document.getElementById('b2bDateStart').value = start;
    document.getElementById('b2bDateEnd').value = end;
  } else {
    start = document.getElementById('b2bDateStart').value;
    end = document.getElementById('b2bDateEnd').value;
  }

  try {
    const res = await fetch(`/api/pipedrive/b2b-report?start=${start}&end=${end}`);
    const data = await res.json();

    if (data.error) {
      loading.style.display = 'none';
      kpis.style.opacity = '1';
      document.getElementById('b2b-ca').textContent = 'Erreur';
      console.error('[B2B]', data.error);
      return;
    }

    console.log(`[B2B] ${data.nbDeals} deals dans la période (${data.totalWonDeals} won deals au total)`);

    // KPI cards
    document.getElementById('b2b-ca').textContent = fmtB2BCurrency(data.ca);
    document.getElementById('b2b-clients').textContent = data.nbClients;
    document.getElementById('b2b-deals').textContent = data.nbDeals;
    document.getElementById('b2b-panier').textContent = fmtB2BCurrency(data.panierMoyen);

    // Objectives
    renderB2BObjectives(data.ca, data.objectives);

    // Pie chart — CA par source
    renderB2BSourceChart(data.sources);

    // Top 5 clients
    renderB2BTopClients(data.top5);

    loading.style.display = 'none';
    kpis.style.opacity = '1';
    b2bLoaded = true;
  } catch (err) {
    console.error('[B2B] Load error:', err);
    loading.style.display = 'none';
    kpis.style.opacity = '1';
  }
}

function renderB2BObjectives(ca, objectives) {
  const container = document.getElementById('b2bObjectives');
  if (!objectives) { container.innerHTML = ''; return; }

  const cards = [];

  const MONTH_NAMES = { '01': 'Janvier', '02': 'Février', '03': 'Mars', '04': 'Avril', '05': 'Mai', '06': 'Juin', '07': 'Juillet', '08': 'Août', '09': 'Septembre', '10': 'Octobre', '11': 'Novembre', '12': 'Décembre' };

  const makeCard = (label, current, target) => {
    const pct = target > 0 ? (current / target) * 100 : 0;
    const barWidth = Math.min(pct, 100);
    const status = pct >= 80 ? 'on-track' : 'behind';
    return `<div class="b2b-obj-card">
      <div class="b2b-obj-header">
        <span class="b2b-obj-label">${label}</span>
        <span class="b2b-obj-pct ${status}">${pct.toFixed(0)}%</span>
      </div>
      <div class="b2b-obj-values">
        <span class="b2b-obj-current">${fmtB2BCurrency(current)}</span>
        <span class="b2b-obj-target">/ ${fmtB2BCurrency(target)}</span>
      </div>
      <div class="b2b-obj-bar-bg">
        <div class="b2b-obj-bar ${status}" style="width:${barWidth}%"></div>
      </div>
    </div>`;
  };

  // Each objective uses its own CA (MTD, QTD, YTD), not the selected period CA
  if (objectives.monthly) {
    const m = objectives.monthly.label.split('-')[1];
    cards.push(makeCard(`Objectif ${MONTH_NAMES[m] || m}`, objectives.monthly.ca, objectives.monthly.target));
  }
  if (objectives.quarterly) {
    cards.push(makeCard(`Objectif ${objectives.quarterly.label}`, objectives.quarterly.ca, objectives.quarterly.target));
  }
  if (objectives.annual) {
    cards.push(makeCard(`Objectif ${objectives.annual.label}`, objectives.annual.ca, objectives.annual.target));
  }

  // Avg orders per client
  if (objectives.avgOrders) {
    const avg = objectives.avgOrders;
    const pct = avg.target > 0 ? (avg.current / avg.target) * 100 : 0;
    const barWidth = Math.min(pct, 100);
    const status = pct >= 80 ? 'on-track' : 'behind';
    cards.push(`<div class="b2b-obj-card">
      <div class="b2b-obj-header">
        <span class="b2b-obj-label">Commandes / client (YTD)</span>
        <span class="b2b-obj-pct ${status}">${pct.toFixed(0)}%</span>
      </div>
      <div class="b2b-obj-values">
        <span class="b2b-obj-current">${avg.current.toFixed(1)}</span>
        <span class="b2b-obj-target">/ ${avg.target} objectif</span>
      </div>
      <div class="b2b-obj-bar-bg">
        <div class="b2b-obj-bar ${status}" style="width:${barWidth}%"></div>
      </div>
    </div>`);
  }

  container.innerHTML = cards.join('');
}

const B2B_COLORS = ['#1a1a1a', '#2d9d5c', '#d94040', '#0984e3', '#f39c12', '#8b5cf6', '#00b894', '#e17055', '#636e72'];

function renderB2BSourceChart(sources) {
  const ctx = document.getElementById('b2bSourceChart').getContext('2d');

  if (b2bSourceChart) b2bSourceChart.destroy();

  const labels = sources.map(s => s.name);
  const values = sources.map(s => s.value);
  const colors = sources.map((_, i) => B2B_COLORS[i % B2B_COLORS.length]);

  b2bSourceChart = new Chart(ctx, {
    type: 'doughnut',
    data: {
      labels,
      datasets: [{
        data: values,
        backgroundColor: colors,
        borderWidth: 2,
        borderColor: '#ffffff',
      }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      cutout: '55%',
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            label: ctx => {
              const val = ctx.parsed;
              const total = ctx.dataset.data.reduce((a, b) => a + b, 0);
              const pct = total > 0 ? ((val / total) * 100).toFixed(1) : 0;
              return `${ctx.label}: ${fmtB2BCurrency(val)} (${pct}%)`;
            },
          },
        },
      },
    },
  });

  // Custom legend
  const legendEl = document.getElementById('b2bSourceLegend');
  const total = values.reduce((a, b) => a + b, 0);
  legendEl.innerHTML = sources.map((s, i) => {
    const pct = total > 0 ? ((s.value / total) * 100).toFixed(1) : 0;
    return `<div class="b2b-legend-item">
      <span class="b2b-legend-dot" style="background:${colors[i]}"></span>
      ${s.name} — ${pct}%
    </div>`;
  }).join('');
}

function renderB2BTopClients(top5) {
  const tbody = document.getElementById('b2bTopClients');
  if (!top5 || !top5.length) {
    tbody.innerHTML = '<tr><td colspan="4" style="text-align:center;color:var(--text-muted);padding:24px;">Aucun client sur cette période</td></tr>';
    return;
  }
  tbody.innerHTML = top5.map((c, i) => `
    <tr>
      <td><span class="client-rank">${i + 1}</span><span class="client-name">${c.name}</span></td>
      <td>${fmtB2BCurrency(c.ca)}</td>
      <td>${c.commandes}</td>
      <td>${fmtB2BCurrency(c.panierMoyen)}</td>
    </tr>
  `).join('');
}

// ============================================================
// LINKEDIN — Knowledge Base + AI Post Generation
// ============================================================

let linkedinLoaded = false;
let linkedinIdeasLoaded = false;

// --- Knowledge Base ---

async function loadLinkedinKB() {
  try {
    const res = await fetch('/api/linkedin/posts');
    const posts = await res.json();
    renderKBList(posts);
    document.getElementById('liKbCount').textContent = `${posts.length} post${posts.length !== 1 ? 's' : ''}`;
    return posts;
  } catch (err) {
    console.error('[LinkedIn] KB load error:', err);
    return [];
  }
}

function renderKBList(posts) {
  const list = document.getElementById('liKbList');
  if (!posts.length) {
    list.innerHTML = '<div style="text-align:center;padding:16px;color:var(--text-muted);font-size:13px;">Aucun post enregistré</div>';
    return;
  }
  list.innerHTML = posts.slice().reverse().map(p => `
    <div class="li-kb-item">
      <div class="li-kb-item-content">${escapeHtml(p.content)}</div>
      <span class="li-kb-item-date">${p.date || ''}</span>
      <button class="li-kb-item-del" onclick="deleteLinkedinPost('${p.id}')" title="Supprimer">&times;</button>
    </div>
  `).join('');
}

function escapeHtml(str) {
  return str.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

async function addLinkedinPost() {
  const textarea = document.getElementById('liNewPost');
  const dateInput = document.getElementById('liPostDate');
  const content = textarea.value.trim();
  if (!content) return;

  try {
    await fetch('/api/linkedin/posts', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ content, date: dateInput.value || undefined }),
    });
    textarea.value = '';
    dateInput.value = '';
    await loadLinkedinKB();
    // Reset ideas since KB changed
    linkedinIdeasLoaded = false;
  } catch (err) {
    console.error('[LinkedIn] Add post error:', err);
  }
}

async function deleteLinkedinPost(id) {
  try {
    await fetch(`/api/linkedin/posts/${id}`, { method: 'DELETE' });
    await loadLinkedinKB();
    linkedinIdeasLoaded = false;
  } catch (err) {
    console.error('[LinkedIn] Delete error:', err);
  }
}

// --- Ideas ---

async function loadLinkedinIdeas() {
  const grid = document.getElementById('liIdeasGrid');
  const loading = document.getElementById('liIdeasLoading');
  const empty = document.getElementById('liIdeasEmpty');

  grid.innerHTML = '';
  empty.style.display = 'none';
  loading.style.display = 'block';

  try {
    const res = await fetch('/api/linkedin/ideas');
    const data = await res.json();
    loading.style.display = 'none';

    if (data.error === 'no_posts' || !data.ideas || data.ideas.length === 0) {
      empty.style.display = 'block';
      return;
    }

    grid.innerHTML = data.ideas.map((idea, i) => `
      <div class="li-idea-card" onclick="openIdeaCompose(${i})">
        <div class="li-idea-title">${escapeHtml(idea.title)}</div>
        <div class="li-idea-desc">${escapeHtml(idea.description)}</div>
      </div>
    `).join('');

    window._linkedinIdeas = data.ideas;
    linkedinIdeasLoaded = true;
  } catch (err) {
    loading.style.display = 'none';
    console.error('[LinkedIn] Ideas error:', err);
  }
}

// --- Compose from idea ---

function openIdeaCompose(index) {
  const idea = window._linkedinIdeas[index];
  if (!idea) return;

  document.getElementById('liComposeIdeaTitle').textContent = idea.title;
  document.getElementById('liComposeIdeaDesc').textContent = idea.description;
  document.getElementById('liIdeaContext').value = '';
  document.getElementById('liIdeaUrls').value = '';
  document.getElementById('liIdeaFiles').value = '';
  document.getElementById('liIdeaFileNames').textContent = '';
  document.getElementById('liIdeaResult').style.display = 'none';
  document.getElementById('liComposeFromIdea').style.display = 'block';
  document.getElementById('liComposeFromIdea').scrollIntoView({ behavior: 'smooth' });
}

function closeIdeaCompose() {
  document.getElementById('liComposeFromIdea').style.display = 'none';
}

async function generateFromIdea() {
  const idea = document.getElementById('liComposeIdeaTitle').textContent + ' — ' + document.getElementById('liComposeIdeaDesc').textContent;
  const context = document.getElementById('liIdeaContext').value;
  const urls = document.getElementById('liIdeaUrls').value;
  const files = document.getElementById('liIdeaFiles').files;

  const resultDiv = document.getElementById('liIdeaResult');
  const resultPost = document.getElementById('liIdeaResultPost');
  resultDiv.style.display = 'none';

  const btn = document.querySelector('#liComposeFromIdea .li-generate-btn');
  btn.disabled = true;
  btn.textContent = 'Génération en cours...';

  try {
    const formData = new FormData();
    formData.append('idea', idea);
    if (context) formData.append('context', context);
    if (urls) formData.append('urls', urls);
    for (const f of files) formData.append('files', f);

    const res = await fetch('/api/linkedin/generate', { method: 'POST', body: formData });
    const data = await res.json();

    if (data.error) throw new Error(data.error);

    resultPost.textContent = data.post;
    resultDiv.style.display = 'block';
    resultDiv.scrollIntoView({ behavior: 'smooth' });
  } catch (err) {
    console.error('[LinkedIn] Generate error:', err);
    resultPost.textContent = 'Erreur : ' + err.message;
    resultDiv.style.display = 'block';
  } finally {
    btn.disabled = false;
    btn.textContent = 'Générer le post';
  }
}

// --- Free-form post ---

async function generateFreePost() {
  const context = document.getElementById('liFreeContext').value.trim();
  const urls = document.getElementById('liFreeUrls').value;
  const files = document.getElementById('liFreeFiles').files;

  if (!context && files.length === 0) return;

  const loading = document.getElementById('liFreeLoading');
  const resultDiv = document.getElementById('liFreeResult');
  const resultPost = document.getElementById('liFreeResultPost');
  resultDiv.style.display = 'none';
  loading.style.display = 'block';

  try {
    const formData = new FormData();
    if (context) formData.append('context', context);
    if (urls) formData.append('urls', urls);
    for (const f of files) formData.append('files', f);

    const res = await fetch('/api/linkedin/generate', { method: 'POST', body: formData });
    const data = await res.json();

    loading.style.display = 'none';
    if (data.error) throw new Error(data.error);

    resultPost.textContent = data.post;
    resultDiv.style.display = 'block';
    resultDiv.scrollIntoView({ behavior: 'smooth' });
  } catch (err) {
    loading.style.display = 'none';
    console.error('[LinkedIn] Free generate error:', err);
    resultPost.textContent = 'Erreur : ' + err.message;
    resultDiv.style.display = 'block';
  }
}

// --- Refine post ---

async function refinePost(type) {
  const prefix = type === 'idea' ? 'liIdea' : 'liFree';
  const postEl = document.getElementById(`${prefix}ResultPost`);
  const inputEl = document.getElementById(`${prefix}RefineInput`);
  const btn = inputEl.parentElement.querySelector('.li-refine-btn');

  const currentPost = postEl.textContent.trim();
  const feedback = inputEl.value.trim();
  if (!feedback) return;

  btn.disabled = true;
  btn.textContent = 'Modification...';

  try {
    const res = await fetch('/api/linkedin/refine', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ post: currentPost, feedback }),
    });
    const data = await res.json();
    if (data.error) throw new Error(data.error);

    postEl.textContent = data.post;
    inputEl.value = '';
  } catch (err) {
    console.error('[LinkedIn] Refine error:', err);
    alert('Erreur : ' + err.message);
  } finally {
    btn.disabled = false;
    btn.textContent = 'Modifier';
  }
}

// --- Copy post ---

function copyPost(elementId) {
  const el = document.getElementById(elementId);
  const text = el.textContent;
  navigator.clipboard.writeText(text).then(() => {
    const btn = el.parentElement.querySelector('.btn');
    const orig = btn.textContent;
    btn.textContent = 'Copié !';
    setTimeout(() => btn.textContent = orig, 2000);
  });
}

// --- LinkedIn File Import ---

async function importLinkedinFile() {
  const fileInput = document.getElementById('liImportFile');
  const btn = document.getElementById('liImportBtn');
  const resultEl = document.getElementById('liImportResult');

  if (!fileInput.files.length) return;

  btn.disabled = true;
  btn.textContent = 'Import en cours...';
  resultEl.textContent = '';

  try {
    const formData = new FormData();
    formData.append('file', fileInput.files[0]);

    const res = await fetch('/api/linkedin/import-file', { method: 'POST', body: formData });
    const data = await res.json();

    if (data.error) {
      resultEl.textContent = data.error;
      resultEl.style.color = 'var(--red)';
    } else {
      resultEl.textContent = `${data.imported} posts importés (${data.total} au total)`;
      resultEl.style.color = 'var(--green)';
      await loadLinkedinKB();
      linkedinIdeasLoaded = false;
      if (data.total > 0) loadLinkedinIdeas();
    }
  } catch (err) {
    resultEl.textContent = 'Erreur: ' + err.message;
    resultEl.style.color = 'var(--red)';
  } finally {
    btn.disabled = false;
    btn.textContent = 'Importer';
    fileInput.value = '';
  }
}

// --- Init LinkedIn tab ---

function initLinkedinTab() {
  if (linkedinLoaded) return;
  linkedinLoaded = true;

  loadLinkedinKB().then(posts => {
    if (posts.length > 0 && !linkedinIdeasLoaded) {
      loadLinkedinIdeas();
    } else if (posts.length === 0) {
      document.getElementById('liIdeasEmpty').style.display = 'block';
    }
  });

  // KB toggle
  const toggle = document.getElementById('liKbToggle');
  const body = document.getElementById('liKbBody');
  toggle.addEventListener('click', () => {
    const open = body.style.display !== 'none';
    body.style.display = open ? 'none' : 'block';
    toggle.classList.toggle('open', !open);
  });

  // Import file input — show import button when file selected
  document.getElementById('liImportFile').addEventListener('change', function() {
    const btn = document.getElementById('liImportBtn');
    btn.style.display = this.files.length ? 'inline-flex' : 'none';
  });

  // File input display names
  document.getElementById('liIdeaFiles').addEventListener('change', function() {
    document.getElementById('liIdeaFileNames').textContent = Array.from(this.files).map(f => f.name).join(', ');
  });
  document.getElementById('liFreeFiles').addEventListener('change', function() {
    document.getElementById('liFreeFileNames').textContent = Array.from(this.files).map(f => f.name).join(', ');
  });
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
  if (tabId === 'b2b' && !b2bLoaded) loadB2BReport('mtd');
  if (tabId === 'linkedin') initLinkedinTab();

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

// ============================================================
// AUTH — Tab restrictions
// ============================================================

let currentUserRole = null;
let allowedTabs = ['all'];

async function checkAuth() {
  try {
    const params = new URLSearchParams(window.location.search);
    const token = params.get('token');
    const url = token ? `/api/auth/me?token=${token}` : '/api/auth/me';
    const res = await fetch(url);
    const data = await res.json();

    if (!data.role) {
      window.location.href = '/login.html';
      return false;
    }

    currentUserRole = data.role;
    allowedTabs = data.tabs || [];

    applyTabRestrictions();
    return true;
  } catch (err) {
    console.error('Auth check failed:', err);
    return true; // fail open if auth endpoint is down
  }
}

function applyTabRestrictions() {
  if (allowedTabs.includes('all')) return;

  // Hide unauthorized tabs
  document.querySelectorAll('.tab-btn').forEach(btn => {
    const tab = btn.dataset.tab;
    if (!allowedTabs.includes(tab)) {
      btn.style.display = 'none';
      const content = document.getElementById(`tab-${tab}`);
      if (content) content.style.display = 'none';
    }
  });

  // Switch to first allowed tab if current is hidden
  const activeTab = document.querySelector('.tab-btn.active');
  if (activeTab && !allowedTabs.includes(activeTab.dataset.tab)) {
    const firstAllowed = document.querySelector(`.tab-btn[data-tab="${allowedTabs[0]}"]`);
    if (firstAllowed) switchTab(allowedTabs[0]);
  }

  // Hide logout for viewers, show for admin
  const logoutBtn = document.getElementById('btnLogout');
  if (logoutBtn) {
    logoutBtn.style.display = currentUserRole === 'admin' ? 'inline-flex' : 'none';
  }
}

async function logout() {
  await fetch('/api/auth/logout', { method: 'POST' });
  window.location.href = '/login.html';
}

document.addEventListener('DOMContentLoaded', async () => {
  const authed = await checkAuth();
  if (!authed) return;

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
        metaAnalysisLoadedRange = null;
        loadMetaAnalysis(null, true);
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

  // Helper: clear all product period button active states
  function clearProductPeriodBtns() {
    document.querySelectorAll('[data-product-period], [data-product-quick]').forEach(b => b.classList.remove('active'));
  }

  // Product period buttons (MTD / QTD / YTD)
  document.querySelectorAll('[data-product-period]').forEach(btn => {
    btn.addEventListener('click', () => {
      clearProductPeriodBtns();
      btn.classList.add('active');
      loadProductBreakdown(btn.dataset.productPeriod);
    });
  });

  // Product quick buttons (Aujourd'hui / Hier / 7J)
  document.querySelectorAll('[data-product-quick]').forEach(btn => {
    btn.addEventListener('click', () => {
      const quick = btn.dataset.productQuick;
      const today = new Date();
      const fmt = d => d.toISOString().split('T')[0];
      let start, end;
      if (quick === 'today') {
        start = end = fmt(today);
      } else if (quick === 'yesterday') {
        const y = new Date(today); y.setDate(y.getDate() - 1);
        start = end = fmt(y);
      } else {
        const days = parseInt(quick);
        end = fmt(new Date(today.getTime() - 86400000));
        const s = new Date(today); s.setDate(s.getDate() - days);
        start = fmt(s);
      }
      productDateRange = { start, end };
      clearProductPeriodBtns();
      btn.classList.add('active');
      loadProductBreakdown();
    });
  });

  // Product custom date picker
  document.getElementById('productDateApply').addEventListener('click', () => {
    const start = document.getElementById('productDateStart').value;
    const end = document.getElementById('productDateEnd').value;
    if (!start || !end) return;
    productDateRange = { start, end };
    clearProductPeriodBtns();
    loadProductBreakdown();
  });

  // Amazon KPI period buttons (MTD / 15J / 30J)
  document.querySelectorAll('[data-amz-days]').forEach(btn => {
    btn.addEventListener('click', () => {
      document.querySelectorAll('[data-amz-days]').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      loadAmazonDashboard(true, parseInt(btn.dataset.amzDays));
    });
  });

  // Helper: clear all meta period button active states
  function clearMetaPeriodBtns() {
    document.querySelectorAll('[data-meta-days], [data-meta-quick]').forEach(b => b.classList.remove('active'));
  }

  // Meta period buttons (15J / 30J)
  document.querySelectorAll('[data-meta-days]').forEach(btn => {
    btn.addEventListener('click', () => {
      const days = parseInt(btn.dataset.metaDays);
      metaAnalysisDays = days;
      metaAnalysisDateRange = null;
      metaAnalysisLoadedDays = null;
      clearMetaPeriodBtns();
      btn.classList.add('active');
      loadMetaAnalysis(days);
    });
  });

  // Meta quick buttons (Aujourd'hui / Hier / 7J)
  document.querySelectorAll('[data-meta-quick]').forEach(btn => {
    btn.addEventListener('click', () => {
      const quick = btn.dataset.metaQuick;
      const today = new Date();
      const fmt = d => d.toISOString().split('T')[0];
      let start, end;
      if (quick === 'today') {
        start = end = fmt(today);
      } else if (quick === 'yesterday') {
        const y = new Date(today); y.setDate(y.getDate() - 1);
        start = end = fmt(y);
      } else {
        const days = parseInt(quick);
        end = fmt(new Date(today.getTime() - 86400000));
        const s = new Date(today); s.setDate(s.getDate() - days);
        start = fmt(s);
      }
      metaAnalysisDateRange = { start, end };
      metaAnalysisLoadedRange = null;
      clearMetaPeriodBtns();
      btn.classList.add('active');
      loadMetaAnalysis();
    });
  });

  // Meta custom date picker
  document.getElementById('metaDateApply').addEventListener('click', () => {
    const start = document.getElementById('metaDateStart').value;
    const end = document.getElementById('metaDateEnd').value;
    if (!start || !end) return;
    metaAnalysisDateRange = { start, end };
    metaAnalysisLoadedRange = null;
    clearMetaPeriodBtns();
    loadMetaAnalysis();
  });

  // Meta refresh button (bypass server cache)
  document.getElementById('metaRefreshBtn').addEventListener('click', () => {
    metaAnalysisLoadedDays = null;
    metaAnalysisLoadedRange = null;
    loadMetaAnalysis(null, true);
  });

  // Meta ad filter buttons (Tout / Videos / Static)
  document.querySelectorAll('[data-ad-filter]').forEach(btn => {
    btn.addEventListener('click', () => {
      document.querySelectorAll('[data-ad-filter]').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      filterMetaAds(btn.dataset.adFilter);
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

  // B2B quick range buttons
  document.querySelectorAll('[data-b2b-range]').forEach(btn => {
    btn.addEventListener('click', () => {
      document.querySelectorAll('[data-b2b-range]').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      b2bLoaded = false;
      loadB2BReport(btn.dataset.b2bRange);
    });
  });

  // B2B custom date change
  const b2bStart = document.getElementById('b2bDateStart');
  const b2bEnd = document.getElementById('b2bDateEnd');
  if (b2bStart && b2bEnd) {
    const onB2BDateChange = () => {
      if (b2bStart.value && b2bEnd.value) {
        document.querySelectorAll('[data-b2b-range]').forEach(b => b.classList.remove('active'));
        b2bLoaded = false;
        loadB2BReport();
      }
    };
    b2bStart.addEventListener('change', onB2BDateChange);
    b2bEnd.addEventListener('change', onB2BDateChange);
  }

  document.querySelectorAll('.date-input').forEach(input => {
    input.addEventListener('click', function() { this.showPicker(); });
  });
});

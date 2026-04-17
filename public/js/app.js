// ============================================================
// BANDIT ACQUISITION DASHBOARD — Frontend Logic
// ============================================================

const CHANNEL_COLORS = {
  meta: '#1877f2',
  google: '#ea4335',
  tiktok: '#00b8a9',
};
const CHANNEL_LABELS = { meta: 'Meta', google: 'Google', tiktok: 'TikTok' };

let chartInstances = {};

// ============================================================
// DATE HELPERS
// ============================================================

function formatDateISO(d) {
  return d.toISOString().split('T')[0];
}

function setDefaultDates() {
  // Default: yesterday only
  const now = new Date();
  const end = new Date(now);
  end.setDate(end.getDate() - 1);
  const start = new Date(end); // same day = yesterday

  document.getElementById('dateStart').value = formatDateISO(start);
  document.getElementById('dateEnd').value = formatDateISO(end);

  // Comparison: day before yesterday
  const compEnd = new Date(start);
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

function setQuickRange(days) {
  const now = new Date();
  const end = new Date(now);
  end.setDate(end.getDate() - 1);
  const start = new Date(end);
  start.setDate(start.getDate() - days + 1);

  document.getElementById('dateStart').value = formatDateISO(start);
  document.getElementById('dateEnd').value = formatDateISO(end);

  const compEnd = new Date(start);
  compEnd.setDate(compEnd.getDate() - 1);
  const compStart = new Date(compEnd);
  compStart.setDate(compStart.getDate() - days + 1);

  document.getElementById('compStart').value = formatDateISO(compStart);
  document.getElementById('compEnd').value = formatDateISO(compEnd);

  // Highlight active button
  document.querySelectorAll('.quick-ranges .btn').forEach(b => b.classList.remove('active'));
  const btn = document.querySelector(`.quick-ranges .btn[data-range="${days}"]`);
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

function createChannelLineChart(canvasId, legendId, dailyData, channelTotals, formatter) {
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

  // Legend with totals
  const legendEl = document.getElementById(legendId);
  if (legendEl && channelTotals) {
    legendEl.innerHTML = Object.entries(CHANNEL_COLORS)
      .map(([ch, color]) => `
        <div class="legend-item">
          <div class="legend-dot" style="background:${color}"></div>
          <span class="legend-label">${CHANNEL_LABELS[ch]}</span>
          <span class="legend-value">${formatter(channelTotals[ch] || 0)}</span>
        </div>
      `).join('');
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
      fmtCurrency
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
// EVENT LISTENERS
// ============================================================

document.addEventListener('DOMContentLoaded', () => {
  setDefaultDates();
  loadStatus();

  // No quick range highlighted by default (yesterday mode)
  loadDashboard();

  document.getElementById('btnApply').addEventListener('click', () => {
    document.querySelectorAll('.quick-ranges .btn').forEach(b => b.classList.remove('active'));
    loadDashboard();
  });

  document.getElementById('btnRefresh').addEventListener('click', () => {
    loadDashboard();
  });

  document.querySelectorAll('.quick-ranges .btn').forEach(btn => {
    btn.addEventListener('click', () => {
      const days = parseInt(btn.dataset.range);
      setQuickRange(days);
    });
  });

  // Make date inputs fully clickable (not just the calendar icon)
  document.querySelectorAll('.date-input').forEach(input => {
    input.addEventListener('click', function() { this.showPicker(); });
  });
});

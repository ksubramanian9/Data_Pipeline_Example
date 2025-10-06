const statusEl = document.getElementById('status');
const emptyStateEl = document.getElementById('emptyState');
const lastUpdatedEl = document.getElementById('lastUpdated');
const metricsEl = document.getElementById('metrics');
const leaderboardEl = document.getElementById('leaderboard');
const healthBodyEl = document.getElementById('healthBody');

const palette = [
  '#38bdf8',
  '#c084fc',
  '#f472b6',
  '#facc15',
  '#4ade80',
  '#fb7185',
  '#f97316',
  '#60a5fa',
];

const ctx = document.getElementById('timelineChart').getContext('2d');
const timelineChart = new Chart(ctx, {
  type: 'line',
  data: { datasets: [] },
  options: {
    responsive: true,
    maintainAspectRatio: false,
    interaction: { mode: 'nearest', intersect: false },
    animation: false,
    scales: {
      x: {
        type: 'time',
        time: { unit: 'minute', tooltipFormat: 'MMM d, HH:mm' },
        grid: { color: 'rgba(148, 163, 184, 0.18)' },
        ticks: { color: '#cbd5f5' },
      },
      y: {
        beginAtZero: true,
        grid: { color: 'rgba(148, 163, 184, 0.18)' },
        ticks: {
          color: '#cbd5f5',
          callback: (value) => `$${Number(value).toLocaleString('en-US')}`,
        },
      },
    },
    plugins: {
      legend: {
        labels: {
          color: '#cbd5f5',
          usePointStyle: true,
          pointStyle: 'circle',
        },
      },
      tooltip: {
        callbacks: {
          title: (items) => items.length ? formatTimestamp(items[0].parsed.x) : '',
          label: (ctx) => `${ctx.dataset.label}: ${formatCurrency(ctx.parsed.y)}`,
        },
      },
    },
  },
});

function formatCurrency(value) {
  return `$${Number(value).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
}

function formatTimestamp(isoString) {
  if (!isoString) return '—';
  const date = new Date(isoString);
  return date.toLocaleString(undefined, {
    hour: '2-digit',
    minute: '2-digit',
    month: 'short',
    day: 'numeric',
    timeZoneName: 'short',
  });
}

function setStatus(state, message) {
  statusEl.dataset.state = state;
  statusEl.textContent = message;
}

function toggleEmptyState(show, message) {
  emptyStateEl.hidden = !show;
  emptyStateEl.textContent = message;
}

function updateChart(timeline) {
  if (!Array.isArray(timeline) || !timeline.length) {
    timelineChart.data.datasets = [];
    timelineChart.update('none');
    return;
  }

  timelineChart.data.datasets = timeline.map((series, index) => ({
    label: series.product,
    data: series.points.map((point) => ({ x: point.window_end, y: point.revenue })),
    borderColor: palette[index % palette.length],
    backgroundColor: palette[index % palette.length],
    pointRadius: 2.5,
    pointHoverRadius: 5,
    tension: 0.35,
    fill: false,
  }));
  timelineChart.update('none');
}

function updateMetrics(summary = {}, leaderboard = []) {
  metricsEl.innerHTML = '';
  const items = [];
  if (summary.unique_products !== undefined) {
    items.push({
      label: 'Active products',
      value: summary.unique_products,
      hint: 'Distinct SKUs seen in recent windows',
    });
  }
  if (summary.windows !== undefined) {
    items.push({
      label: 'Tracked windows',
      value: summary.windows,
      hint: 'Total windows available in dataset',
    });
  }
  if (summary.latest_window_end) {
    items.push({
      label: 'Latest window',
      value: formatTimestamp(summary.latest_window_end),
      hint: 'Ending timestamp of freshest aggregate',
    });
  }
  if (leaderboard.length) {
    const total = leaderboard.reduce((acc, row) => acc + Number(row.revenue || 0), 0);
    items.push({
      label: 'Revenue (latest window)',
      value: formatCurrency(total),
      hint: `${leaderboard.length} products contributed`,
    });
  }

  if (!items.length) {
    metricsEl.innerHTML = '<p class="muted">Waiting for metrics…</p>';
    return;
  }

  for (const item of items) {
    const wrapper = document.createElement('div');
    wrapper.className = 'metric';

    const label = document.createElement('span');
    label.className = 'label';
    label.textContent = item.label;

    const value = document.createElement('span');
    value.className = 'value';
    value.textContent = item.value;

    wrapper.appendChild(label);
    wrapper.appendChild(value);

    if (item.hint) {
      const hint = document.createElement('span');
      hint.className = 'hint';
      hint.textContent = item.hint;
      wrapper.appendChild(hint);
    }

    metricsEl.appendChild(wrapper);
  }
}

function updateLeaderboard(rows = []) {
  leaderboardEl.innerHTML = '';
  if (!rows.length) {
    leaderboardEl.innerHTML = '<li class="muted">No products yet in the latest window.</li>';
    return;
  }

  rows.slice(0, 12).forEach((row, index) => {
    const item = document.createElement('li');

    const product = document.createElement('span');
    product.className = 'product';
    product.textContent = `${index + 1}. ${row.product}`;

    const revenue = document.createElement('span');
    revenue.className = 'revenue';
    revenue.textContent = formatCurrency(row.revenue);

    item.appendChild(product);
    item.appendChild(revenue);
    leaderboardEl.appendChild(item);
  });
}

function updateWindowHealth(rows = []) {
  healthBodyEl.innerHTML = '';
  if (!rows.length) {
    const row = document.createElement('tr');
    const cell = document.createElement('td');
    cell.colSpan = 4;
    cell.textContent = 'Waiting for streaming output…';
    cell.className = 'muted';
    row.appendChild(cell);
    healthBodyEl.appendChild(row);
    return;
  }

  rows.forEach((row) => {
    const tr = document.createElement('tr');
    const cells = [
      formatTimestamp(row.window_start),
      formatTimestamp(row.window_end),
      formatCurrency(row.total_revenue),
      `${row.product_count}`,
    ];

    cells.forEach((value) => {
      const td = document.createElement('td');
      td.textContent = value;
      tr.appendChild(td);
    });

    healthBodyEl.appendChild(tr);
  });
}

async function fetchStreamData() {
  try {
    setStatus('loading', 'Refreshing…');
    const response = await fetch('/api/stream');
    if (!response.ok) {
      throw new Error(`Request failed (${response.status})`);
    }
    const payload = await response.json();

    if (payload.status === 'no_data') {
      setStatus('no_data', 'Waiting for streaming output');
      toggleEmptyState(true, 'No Parquet output detected yet. Keep the stream running and aggregates will appear here.');
      updateChart([]);
      updateMetrics();
      updateLeaderboard();
      updateWindowHealth();
      lastUpdatedEl.textContent = 'Last updated —';
      return;
    }

    if (payload.status === 'error') {
      const message = (payload.summary && payload.summary.message) || 'Unexpected error while reading Parquet output.';
      setStatus('error', 'Dashboard error');
      toggleEmptyState(true, message);
      updateChart([]);
      updateMetrics();
      updateLeaderboard();
      updateWindowHealth();
      lastUpdatedEl.textContent = 'Last updated —';
      return;
    }

    setStatus('ok', 'Live stream connected');
    toggleEmptyState(false, '');

    updateChart(payload.timeline || []);
    updateMetrics(payload.summary, payload.leaderboard);
    updateLeaderboard(payload.leaderboard || []);
    updateWindowHealth(payload.window_health || []);

    if (payload.last_updated) {
      lastUpdatedEl.textContent = `Last updated ${formatTimestamp(payload.last_updated)}`;
    }
  } catch (error) {
    console.error(error);
    setStatus('error', 'Failed to refresh');
    toggleEmptyState(true, error && error.message ? error.message : 'Unable to contact API.');
  }
}

fetchStreamData();
setInterval(fetchStreamData, 15000);

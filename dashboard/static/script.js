Chart.defaults.font.family = "Inter, system-ui, -apple-system, Segoe UI, Roboto";
Chart.defaults.color = '#1f2937';
Chart.defaults.plugins.tooltip.backgroundColor = 'rgba(17,24,39,0.92)';
Chart.defaults.plugins.tooltip.titleColor = '#fff';
Chart.defaults.plugins.tooltip.bodyColor = '#e5e7eb';

function createNumberFormatter(meta){
  if(!meta.primary_metric){
    return new Intl.NumberFormat('en-US', { maximumFractionDigits: 0 });
  }
  if(meta.currency){
    try{
      return new Intl.NumberFormat('en-US', {
        style: 'currency',
        currency: meta.currency,
        maximumFractionDigits: 2
      });
    }catch(err){
      console.warn('Falling back to numeric format for currency', err);
    }
  }
  return new Intl.NumberFormat('en-US', { maximumFractionDigits: 2 });
}

const integerFormatter = new Intl.NumberFormat('en-US', { maximumFractionDigits: 0 });

function buildTable(rows, columns, valueColumn, formatter){
  if(!rows?.length){
    return '<div style="padding:24px;text-align:center" class="muted">No rows to display.</div>';
  }
  const header = columns.map(col => `<th>${col}</th>`).join('');
  const rowsHtml = rows.map(r => {
    const cells = columns.map(col => {
      const val = r[col];
      if(val === null || val === undefined) return '<td></td>';
      if(col === valueColumn){
        return `<td>${formatter.format(Number(val))}</td>`;
      }
      if(col === 'record_count' || col.endsWith('_count')){
        return `<td>${integerFormatter.format(Number(val))}</td>`;
      }
      return `<td>${val}</td>`;
    }).join('');
    return `<tr>${cells}</tr>`;
  }).join('');
  return `
    <table>
      <thead><tr>${header}</tr></thead>
      <tbody>${rowsHtml}</tbody>
    </table>
  `;
}

async function loadData(){
  const res = await fetch('/api/daily');
  const data = await res.json();

  if(data.status !== 'ok'){
    document.getElementById('table').innerHTML = '<div style="padding:24px;text-align:center" class="muted">No data yet. Run the Spark job.</div>';
    return;
  }

  const meta = data.meta;
  const valueColumn = meta.value_column;
  const dateColumn = meta.date_column;
  const formatter = createNumberFormatter(meta);

  document.getElementById('datasetTitle').textContent = meta.dataset_name;
  document.getElementById('chartDailyTitle').textContent = `${meta.primary_metric ? meta.primary_metric : 'Records'} by ${dateColumn}`;
  document.getElementById('chartSegmentsTitle').textContent = meta.dimensions.length ? `Top ${meta.dimensions.slice(0,2).join(' · ')}` : 'Top Segments';
  document.getElementById('chartDailySubtitle').textContent = meta.dimensions.length ? `Aggregated by ${meta.dimensions.join(', ')}` : 'Daily totals across the configured dataset';
  document.getElementById('chartSegmentsSubtitle').textContent = meta.dimensions.length ? `Highest values across ${meta.dimensions.slice(0,2).join(' · ')}` : 'Highest values by day';
  document.getElementById('tableSubtitle').textContent = `Recent aggregated slices sorted by ${dateColumn}`;
  document.getElementById('tableTitle').textContent = `Sample ${meta.dataset_name} Rows`;

  const days = data.daily.map(d => d[dateColumn]);
  const totals = data.daily.map(d => d[valueColumn]);

  const ctxDaily = document.getElementById('chartDaily').getContext('2d');
  new Chart(ctxDaily, {
    type: 'line',
    data: {
      labels: days,
      datasets: [{
        label: meta.primary_metric ? meta.primary_metric : 'Records',
        data: totals,
        tension: 0.35,
        borderWidth: 3,
        borderColor: 'rgba(59,130,246,0.9)',
        backgroundColor: 'rgba(59,130,246,0.15)',
        fill: true,
        pointRadius: 4,
        pointHoverRadius: 6,
        pointBackgroundColor: '#1d4ed8'
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            label: ctx => `${ctx.dataset.label}: ${formatter.format(ctx.parsed.y)}`
          }
        }
      },
      scales: {
        y: {
          ticks: {
            callback: value => formatter.format(value)
          },
          grid: { color: 'rgba(148,163,184,0.2)' }
        },
        x: {
          grid: { display: false }
        }
      }
    }
  });

  const segments = data.top_segments.map(d => d.segment ?? d[dateColumn] ?? 'segment');
  const segTotals = data.top_segments.map(d => d[valueColumn]);
  const ctxSegments = document.getElementById('chartProducts').getContext('2d');
  new Chart(ctxSegments, {
    type: 'bar',
    data: {
      labels: segments,
      datasets: [{
        label: meta.primary_metric ? meta.primary_metric : 'Records',
        data: segTotals,
        backgroundColor: segments.map((_, idx) => `rgba(14,165,233,${0.4 + (idx/segments.length)*0.5})`),
        borderRadius: 10,
        borderSkipped: false
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            label: ctx => `${ctx.label}: ${formatter.format(ctx.parsed.y)}`
          }
        }
      },
      scales: {
        y: {
          ticks: {
            callback: value => formatter.format(value)
          },
          grid: { color: 'rgba(148,163,184,0.2)' }
        },
        x: {
          ticks: { maxRotation: 0, minRotation: 0, autoSkip: true, maxTicksLimit: 6 },
          grid: { display: false }
        }
      }
    }
  });

  document.getElementById('table').innerHTML = buildTable(data.sample, meta.table_columns, valueColumn, formatter);
}

loadData();

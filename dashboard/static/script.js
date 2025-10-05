const currencyFormatter = new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 0 });

Chart.defaults.font.family = "Inter, system-ui, -apple-system, Segoe UI, Roboto";
Chart.defaults.color = '#1f2937';
Chart.defaults.plugins.tooltip.backgroundColor = 'rgba(17,24,39,0.92)';
Chart.defaults.plugins.tooltip.titleColor = '#fff';
Chart.defaults.plugins.tooltip.bodyColor = '#e5e7eb';

function buildTable(rows){
  if(!rows?.length){
    return '<div style="padding:24px;text-align:center" class="muted">No rows to display.</div>';
  }
  const cells = rows.map(r => `
    <tr>
      <td>${r.order_date}</td>
      <td>${r.product}</td>
      <td>${currencyFormatter.format(r.total_amount)}</td>
    </tr>
  `);
  return `
    <table>
      <thead>
        <tr><th>order_date</th><th>product</th><th>total_amount</th></tr>
      </thead>
      <tbody>${cells.join('')}</tbody>
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

  const days = data.daily.map(d => d.order_date);
  const totals = data.daily.map(d => d.total_amount);

  const ctxDaily = document.getElementById('chartDaily').getContext('2d');
  new Chart(ctxDaily, {
    type: 'line',
    data: {
      labels: days,
      datasets: [{
        label: 'Total Revenue',
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
            label: ctx => `${ctx.dataset.label}: ${currencyFormatter.format(ctx.parsed.y)}`
          }
        }
      },
      scales: {
        y: {
          ticks: {
            callback: value => currencyFormatter.format(value)
          },
          grid: { color: 'rgba(148,163,184,0.2)' }
        },
        x: {
          grid: { display: false }
        }
      }
    }
  });

  const products = data.top_products.map(d => d.product);
  const pTotals = data.top_products.map(d => d.total_amount);
  const ctxProducts = document.getElementById('chartProducts').getContext('2d');
  new Chart(ctxProducts, {
    type: 'bar',
    data: {
      labels: products,
      datasets: [{
        label: 'Total Revenue',
        data: pTotals,
        backgroundColor: products.map((_, idx) => `rgba(14,165,233,${0.4 + (idx/products.length)*0.5})`),
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
            label: ctx => `${ctx.label}: ${currencyFormatter.format(ctx.parsed.y)}`
          }
        }
      },
      scales: {
        y: {
          ticks: {
            callback: value => currencyFormatter.format(value)
          },
          grid: { color: 'rgba(148,163,184,0.2)' }
        },
        x: {
          ticks: { maxRotation: 0, minRotation: 0, autoSkip: true, maxTicksLimit: 5 },
          grid: { display: false }
        }
      }
    }
  });

  document.getElementById('table').innerHTML = buildTable(data.sample);
}

loadData();

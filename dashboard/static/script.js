async function loadData(){
  const res = await fetch('/api/daily');
  const data = await res.json();
  if(data.status !== 'ok'){
    document.getElementById('table').innerHTML = '<p class="muted">No data yet. Run the Spark job.</p>';
    return;
  }
  const days = data.daily.map(d => d.order_date);
  const totals = data.daily.map(d => d.total_amount);

  const ctxDaily = document.getElementById('chartDaily').getContext('2d');
  new Chart(ctxDaily, {
    type: 'line',
    data: {
      labels: days,
      datasets: [{ label: 'Total Revenue', data: totals }]
    },
    options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { display: true } } }
  });

  const products = data.top_products.map(d => d.product);
  const pTotals = data.top_products.map(d => d.total_amount);
  const ctxProducts = document.getElementById('chartProducts').getContext('2d');
  new Chart(ctxProducts, {
    type: 'bar',
    data: { labels: products, datasets: [{ label: 'Total Revenue', data: pTotals }]},
    options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false } } }
  });

  // Table
  const rows = data.sample;
  const table = [
    '<table>',
    '<thead><tr><th>order_date</th><th>product</th><th>total_amount</th></tr></thead>',
    '<tbody>',
    ...rows.map(r => `<tr><td>${r.order_date}</td><td>${r.product}</td><td>${r.total_amount}</td></tr>`),
    '</tbody>',
    '</table>'
  ].join('');
  document.getElementById('table').innerHTML = table;
}
loadData();

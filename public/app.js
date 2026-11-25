(() => {
  const API = window.API_BASE || '';
  const $ = (s) => document.querySelector(s);
  const esc = (v) => String(v == null ? '' : v).replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const money = (c) => '$' + (Number(c || 0) / 100).toFixed(2);
  const time = (t) => new Date(t).toLocaleTimeString();
  const DEMO = { acme: ['admin@acme.test', 'TenantA@123'], globex: ['admin@globex.test', 'TenantB@123'] };

  let session = JSON.parse(sessionStorage.getItem('tf_session') || 'null');
  let products = [];
  let mode = 'optimized';
  let seenOrders = new Set();
  let timer;

  async function call(method, path, body, extraHeaders = {}) {
    const headers = { 'content-type': 'application/json', ...extraHeaders };
    if (session) headers.authorization = 'Bearer ' + session.token;
    const r = await fetch(API + path, { method, headers, body: body ? JSON.stringify(body) : undefined });
    if (r.status === 401 && session) { logout(); throw new Error('Session expired'); }
    const ct = r.headers.get('content-type') || '';
    const data = ct.includes('json') ? await r.json() : await r.blob();
    return { status: r.status, data, headers: r.headers };
  }

  // ---------- login ----------
  async function initLogin() {
    const sel = $('#tenantSelect');
    try {
      const { data } = await call('GET', '/api/v1/auth/tenants');
      sel.innerHTML = data.map((t) => `<option value="${esc(t.slug)}">${esc(t.name)} (${esc(t.slug)})</option>`).join('');
    } catch { sel.innerHTML = '<option value="acme">acme</option><option value="globex">globex</option>'; }
    const fill = () => { const d = DEMO[sel.value]; if (d) { $('#email').value = d[0]; $('#password').value = d[1]; } };
    sel.onchange = fill; fill();
    $('#loginForm').onsubmit = async (e) => {
      e.preventDefault(); $('#loginErr').textContent = '';
      const r = await call('POST', '/api/v1/auth/login', { email: $('#email').value, password: $('#password').value }, { 'x-tenant-id': sel.value });
      if (r.status !== 200) { $('#loginErr').textContent = r.data.error || 'Login failed'; return; }
      session = r.data; sessionStorage.setItem('tf_session', JSON.stringify(session)); boot();
    };
  }

  function logout() { session = null; sessionStorage.removeItem('tf_session'); clearInterval(timer); location.reload(); }

  // ---------- app ----------
  function lineRow() {
    const div = document.createElement('div'); div.className = 'line';
    div.innerHTML = `<label>Product<select class="p">${products.map((p) => `<option value="${esc(p.id)}">${esc(p.name)} · ${money(p.price_cents)}</option>`).join('')}</select></label>
      <label>Qty<input class="q" type="number" min="1" max="100" value="1" /></label><button type="button" class="ghost">✕</button>`;
    div.querySelector('button').onclick = () => { if ($('#lines').children.length > 1) div.remove(); };
    $('#lines').appendChild(div);
  }

  async function loadProducts() {
    const { data } = await call('GET', '/api/v1/products');
    products = data;
    $('#productsTable tbody').innerHTML = data.map((p) => `<tr><td>${esc(p.sku)}</td><td>${esc(p.name)}</td><td>${money(p.price_cents)}</td><td><b>${p.stock}</b></td></tr>`).join('');
  }

  async function placeOrder(items, customerName) {
    const r = await call('POST', '/api/v1/orders', { customerName, items });
    return r;
  }

  async function loadOrders() {
    const { data } = await call('GET', `/api/v1/orders?mode=${mode}&limit=25`);
    $('#queryStats').textContent = `${data.queries} SQL ${data.queries === 1 ? 'query' : 'queries'} · ${data.ms} ms`;
    $('#ordersTable tbody').innerHTML = data.orders.map((o) => {
      const isNew = !seenOrders.has(o.id); seenOrders.add(o.id);
      const items = o.items.map((i) => `${esc(i.name)}×${i.quantity}`).join(', ');
      const kafka = o.kafka_partition != null ? `p${o.kafka_partition}@${o.kafka_offset}` : '<span class="muted">seed</span>';
      return `<tr class="${isNew && seenOrders.size > 25 ? 'new' : ''}"><td><code>${esc(o.id.slice(0, 8))}</code></td><td>${esc(o.customer_name)}</td><td>${items}</td><td>${money(o.total_cents)}</td>
        <td><span class="st ${esc(o.status)}">${esc(o.status)}</span></td><td>${kafka}</td><td>${time(o.created_at)}</td>
        <td>${o.status === 'CONFIRMED' ? `<a class="link" data-inv="${esc(o.id)}">Invoice</a>` : ''}</td></tr>`;
    }).join('');
  }

  async function loadEvents() {
    const { data } = await call('GET', '/api/v1/insights/events');
    $('#eventsTable tbody').innerHTML = data.slice(0, 10).map((e) => `<tr><td>p${e.partition}</td><td>${e.kafka_offset}</td><td><span class="st ${esc(e.outcome)}">${esc(e.outcome)}</span></td><td>${e.latency_ms} ms</td><td>${time(e.processed_at)}</td></tr>`).join('') || '<tr><td colspan="5" class="muted">No events consumed yet</td></tr>';
  }

  async function loadInvoices() {
    const { data } = await call('GET', '/api/v1/insights/invoices');
    const s = (cold) => data.summary.find((x) => x.cold_start === cold);
    const c = s(true), w = s(false);
    $('#invoiceSummary').innerHTML = `<div class="stat"><span class="muted">Cold starts</span><b>${c ? c.n : 0}</b>${c ? `avg ${c.avg_roundtrip_ms} ms` : ''}</div>
      <div class="stat"><span class="muted">Warm starts</span><b>${w ? w.n : 0}</b>${w ? `avg ${w.avg_roundtrip_ms} ms` : ''}</div>`;
    $('#invoiceTable tbody').innerHTML = data.recent.slice(0, 8).map((r) => `<tr><td><code>${esc(r.order_id.slice(0, 8))}</code></td><td><span class="st ${r.cold_start ? 'cold' : 'warm'}">${r.cold_start ? 'COLD' : 'WARM'}</span></td><td>${r.function_ms} ms</td><td>${r.roundtrip_ms} ms</td><td><code>${esc(r.instance_id)}</code></td></tr>`).join('') || '<tr><td colspan="5" class="muted">No invoices yet (set INVOICE_FUNCTION_URL)</td></tr>';
  }

  async function refresh() {
    try { await Promise.all([loadOrders(), loadProducts(), loadEvents(), loadInvoices()]); } catch (e) { console.warn(e); }
  }

  function boot() {
    $('#loginView').classList.add('hidden'); $('#appView').classList.remove('hidden'); $('#who').classList.remove('hidden');
    const b = $('#tenantBadge'); b.textContent = session.tenant.name; b.className = 'badge ' + session.tenant.slug;
    $('#userEmail').textContent = session.user.email;
    $('#logoutBtn').onclick = logout;

    loadProducts().then(() => { $('#lines').innerHTML = ''; lineRow(); });
    $('#addLine').onclick = lineRow;
    $('#orderForm').onsubmit = async (e) => {
      e.preventDefault();
      const items = [...document.querySelectorAll('#lines .line')].map((l) => ({ productId: l.querySelector('.p').value, quantity: Number(l.querySelector('.q').value) }));
      const r = await placeOrder(items, $('#customer').value);
      $('#orderResult').textContent = `HTTP ${r.status}\n` + JSON.stringify(r.data, null, 2);
      setTimeout(refresh, 800); setTimeout(refresh, 2500);
    };
    $('#burst').onclick = async () => {
      const t0 = performance.now();
      const res = await Promise.all(Array.from({ length: 10 }, (_, i) => placeOrder([{ productId: products[i % products.length].id, quantity: 1 }], `Burst #${i + 1}`)));
      const parts = [...new Set(res.map((r) => r.data.kafka && r.data.kafka.partition))];
      $('#orderResult').textContent = `10 orders → ${res.filter((r) => r.status === 202).length}× 202 Accepted in ${(performance.now() - t0).toFixed(0)} ms\npartition(s) used by this tenant: ${parts.join(', ')} (key = tenant_id → same partition → ordered)\noffsets: ${res.map((r) => r.data.kafka && r.data.kafka.offset).join(', ')}`;
      setTimeout(refresh, 1000); setTimeout(refresh, 3000);
    };
    document.querySelectorAll('.segbtn').forEach((btn) => btn.onclick = () => {
      document.querySelectorAll('.segbtn').forEach((x) => x.classList.remove('active')); btn.classList.add('active'); mode = btn.dataset.mode; loadOrders();
    });
    $('#ordersTable').onclick = async (e) => {
      const id = e.target.dataset && e.target.dataset.inv; if (!id) return;
      e.target.textContent = '…';
      const r = await call('GET', `/api/v1/orders/${id}/invoice`);
      e.target.textContent = 'Invoice';
      if (r.status !== 200) { alert(r.data.error || 'Invoice failed'); return; }
      window.open(URL.createObjectURL(r.data), '_blank');
      $('#labOut').textContent = `Invoice served by serverless function: ${r.headers.get('x-cold-start') === 'true' ? 'COLD' : 'WARM'} start · function ${r.headers.get('x-function-ms')} ms · round-trip ${r.headers.get('x-roundtrip-ms')} ms`;
      loadInvoices();
    };
    $('#n1Btn').onclick = async () => { $('#labOut').textContent = 'running…'; const { data } = await call('GET', '/api/v1/insights/n1?limit=50'); $('#labOut').textContent = `N+1 (naive):  ${data.naive.queries} queries · ${data.naive.ms} ms\nJOIN:         ${data.optimized.queries} query   · ${data.optimized.ms} ms\nSpeed-up:     ${data.speedup}×`; };
    $('#explainBtn').onclick = async () => { $('#labOut').textContent = 'running…'; const { data } = await call('GET', '/api/v1/insights/explain'); $('#labOut').textContent = `Composite index present: ${data.indexPresent}\n${data.indexDef || ''}\n\n${data.plan.join('\n')}`; };
    $('#poolBtn').onclick = async () => { const { data } = await call('GET', '/api/v1/insights/pool'); $('#labOut').textContent = 'pg.Pool → ' + JSON.stringify(data, null, 2); };

    refresh(); timer = setInterval(refresh, 3000);
  }

  if (session) boot(); else initLogin();
})();

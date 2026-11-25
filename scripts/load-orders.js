// Fires N orders for both tenants through the API (high-throughput demo)
//   API_URL=http://localhost:4000 ORDERS=40 npm run load
// Shows: every order gets 202 immediately; each tenant always lands on the SAME partition.
const { color } = require('./_db');

const API = (process.env.API_URL || 'http://localhost:4000').replace(/\/$/, '');
const N = Number(process.env.ORDERS || 40);
const CONC = Number(process.env.CONCURRENCY || 8);

async function login(tenant, email, password) {
  const r = await fetch(`${API}/api/v1/auth/login`, { method: 'POST', headers: { 'content-type': 'application/json', 'x-tenant-id': tenant }, body: JSON.stringify({ email, password }) });
  const { token } = await r.json();
  const products = await (await fetch(`${API}/api/v1/products`, { headers: { authorization: `Bearer ${token}` } })).json();
  return { tenant, token, products };
}

(async () => {
  const tenants = [await login('acme', 'admin@acme.test', 'TenantA@123'), await login('globex', 'admin@globex.test', 'TenantB@123')];
  const partitions = { acme: new Set(), globex: new Set() };
  const times = [];
  let i = 0; let ok = 0;
  const t0 = Date.now();
  await Promise.all(Array.from({ length: CONC }, async () => {
    while (i < N) {
      const n = i++;
      const t = tenants[n % 2];
      const p = t.products[n % t.products.length];
      const s = Date.now();
      const r = await fetch(`${API}/api/v1/orders`, {
        method: 'POST', headers: { 'content-type': 'application/json', authorization: `Bearer ${t.token}` },
        body: JSON.stringify({ customerName: `Load #${n}`, items: [{ productId: p.id, quantity: 1 + (n % 3) }] }),
      });
      const d = await r.json();
      times.push(Date.now() - s);
      if (r.status === 202) { ok++; partitions[t.tenant].add(d.kafka.partition); }
      console.log(`${r.status === 202 ? color.g(r.status) : color.r(r.status)} ${t.tenant.padEnd(6)} order=${(d.orderId || '').slice(0, 8)} partition=${d.kafka ? d.kafka.partition : '-'} offset=${d.kafka ? d.kafka.offset : '-'} ${Date.now() - s}ms`);
    }
  }));
  times.sort((a, b) => a - b);
  const secs = (Date.now() - t0) / 1000;
  console.log(color.b(`\n${ok}/${N} accepted in ${secs.toFixed(2)}s → ${(ok / secs).toFixed(1)} orders/s · p50 ${times[Math.floor(times.length / 2)]}ms · p95 ${times[Math.floor(times.length * 0.95)]}ms`));
  for (const [k, v] of Object.entries(partitions)) console.log(`${k} → partition(s) ${[...v].join(',')} ${v.size === 1 ? color.g('✔ single partition = ordered') : color.r('✘')}`);
})().catch((e) => { console.error(color.r('✘'), e.message); process.exit(1); });

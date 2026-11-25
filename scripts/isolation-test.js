// Tenant isolation test suite (API level + database RLS level)
//   API_URL=http://localhost:4000 npm run test:isolation
//   add --with-kafka to also test cross-tenant productId injection through the consumer
const jwt = require('jsonwebtoken');
const { adminPool, color, tenantBySlug } = require('./_db');
const { withTenant } = require('../src/db/tenantDb');
const { pool } = require('../src/db/pool');

const API = (process.env.API_URL || 'http://localhost:4000').replace(/\/$/, '');
const WITH_KAFKA = process.argv.includes('--with-kafka');
const results = [];
const check = (name, ok, detail = '') => { results.push({ name, ok }); console.log(`${ok ? color.g('PASS') : color.r('FAIL')}  ${name} ${color.d(detail)}`); };

async function api(method, p, { token, tenant, body } = {}) {
  const headers = { 'content-type': 'application/json' };
  if (token) headers.authorization = `Bearer ${token}`;
  if (tenant) headers['x-tenant-id'] = tenant;
  const r = await fetch(API + p, { method, headers, body: body ? JSON.stringify(body) : undefined });
  let data = null; try { data = await r.json(); } catch { /* pdf etc */ }
  return { status: r.status, data };
}
const login = async (tenant, email, password) => (await api('POST', '/api/v1/auth/login', { tenant, body: { email, password } })).data.token;

(async () => {
  console.log(color.b(`Tenant isolation tests → ${API}\n`));
  const A = await tenantBySlug('acme');
  const B = await tenantBySlug('globex');
  const tokA = await login('acme', 'admin@acme.test', 'TenantA@123');
  const tokB = await login('globex', 'admin@globex.test', 'TenantB@123');
  check('Both tenants can log in', !!tokA && !!tokB);

  const wrongTenantLogin = await api('POST', '/api/v1/auth/login', { tenant: 'globex', body: { email: 'admin@acme.test', password: 'TenantA@123' } });
  check('Tenant A credentials rejected on Tenant B login', wrongTenantLogin.status === 401, `status=${wrongTenantLogin.status}`);

  const listA = (await api('GET', '/api/v1/orders?limit=200', { token: tokA })).data.orders;
  const listB = (await api('GET', '/api/v1/orders?limit=200', { token: tokB })).data.orders;
  const idsB = new Set(listB.map((o) => o.id));
  check('Tenant A order list contains zero Tenant B orders', listA.length > 0 && !listA.some((o) => idsB.has(o.id)), `A=${listA.length} B=${listB.length}`);

  const naiveA = (await api('GET', '/api/v1/orders?mode=naive&limit=50', { token: tokA })).data.orders;
  check('N+1 (naive) path is also tenant-scoped', !naiveA.some((o) => idsB.has(o.id)));

  const bOrder = listB[0].id;
  const r1 = await api('GET', `/api/v1/orders/${bOrder}`, { token: tokA });
  check('Tenant A GET Tenant B order by id → 404', r1.status === 404, `status=${r1.status}`);

  const r2 = await api('GET', '/api/v1/orders', { token: tokA, tenant: 'globex' });
  check('Tenant A token + x-tenant-id: globex → 403 TENANT_MISMATCH', r2.status === 403, `status=${r2.status}`);

  const r3 = await api('GET', '/api/v1/orders');
  check('No token → 401', r3.status === 401, `status=${r3.status}`);

  const forged = jwt.sign({ sub: 'x', tid: B.id }, 'wrong-secret');
  const r4 = await api('GET', '/api/v1/orders', { token: forged });
  check('Forged JWT (tid = Tenant B, wrong secret) → 401', r4.status === 401, `status=${r4.status}`);

  const prodA = (await api('GET', '/api/v1/products', { token: tokA })).data;
  const prodB = (await api('GET', '/api/v1/products', { token: tokB })).data;
  check('Products are tenant-scoped', prodA.every((p) => !prodB.some((q) => q.id === p.id)) && prodA.length > 0);

  // ---- Database layer (RLS) ----
  const rls = await withTenant(A.id, async (db) => {
    const leak = await db.query('SELECT count(*)::int n FROM orders WHERE tenant_id = $1', [B.id]);
    const all = await db.query('SELECT count(DISTINCT tenant_id)::int n FROM orders');
    return { leak: leak.rows[0].n, tenants: all.rows[0].n };
  });
  check('RLS: Tenant A context, SELECT ... WHERE tenant_id = B → 0 rows', rls.leak === 0, `rows=${rls.leak}`);
  check('RLS: Tenant A context, unfiltered SELECT sees only 1 tenant', rls.tenants === 1, `tenants=${rls.tenants}`);

  let insertBlocked = false;
  try {
    await withTenant(A.id, (db) => db.query(
      `INSERT INTO orders (id, tenant_id, customer_name) VALUES (gen_random_uuid(), $1, 'hacker')`, [B.id]));
  } catch (e) { insertBlocked = /row-level security/i.test(e.message); }
  check('RLS: Tenant A context cannot INSERT a row for Tenant B', insertBlocked);

  if (WITH_KAFKA) {
    const r5 = await api('POST', '/api/v1/orders', { token: tokA, body: { customerName: 'Injection test', tenantId: B.id, items: [{ productId: prodB[0].id, quantity: 1 }] } });
    check('POST with Tenant B productId + body.tenantId → 202 (event keyed to A)', r5.status === 202 && r5.data.kafka.key === A.id);
    let final = null;
    for (let i = 0; i < 20 && !final; i++) {
      await new Promise((r) => setTimeout(r, 1000));
      const g = await api('GET', `/api/v1/orders/${r5.data.orderId}`, { token: tokA });
      if (g.status === 200) final = g.data;
    }
    check('Consumer REJECTS cross-tenant product (Unknown product)', final && final.status === 'REJECTED', final ? final.reject_reason : 'not processed');
    const stockB = (await api('GET', '/api/v1/products', { token: tokB })).data.find((p) => p.id === prodB[0].id).stock;
    check('Tenant B inventory untouched', stockB === prodB[0].stock, `stock=${stockB}`);
  }

  const failed = results.filter((r) => !r.ok).length;
  console.log('\n' + (failed ? color.r(`${failed} FAILED`) : color.g(`ALL ${results.length} ISOLATION CHECKS PASSED — violations: 0`)));
  process.exitCode = failed ? 1 : 0;
})()
  .catch((e) => { console.error(color.r('✘'), e.message); process.exitCode = 1; })
  .finally(async () => { await pool.end(); await adminPool.end(); });

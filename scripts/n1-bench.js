// N+1 vs JOIN benchmark using the SAME functions as the API
//   npm run bench:n1
const fs = require('fs');
const path = require('path');
const { color, median, tenantBySlug, adminPool } = require('./_db');
const { withTenant } = require('../src/db/tenantDb');
const { pool } = require('../src/db/pool');
const { listOrdersNaive, listOrdersOptimized } = require('../src/services/orderQueries');

const LIMIT = Number(process.env.N1_LIMIT || 50);
const RUNS = Number(process.env.N1_RUNS || 10);

async function bench(fn, tenantId) {
  const times = []; let queries = 0; let count = 0;
  await withTenant(tenantId, (db) => fn(db, { limit: LIMIT })); // warm-up
  for (let i = 0; i < RUNS; i++) {
    const t0 = process.hrtime.bigint();
    const r = await withTenant(tenantId, async (db) => { const o = await fn(db, { limit: LIMIT }); return { q: db.queryCount, n: o.length }; });
    times.push(Number(process.hrtime.bigint() - t0) / 1e6);
    queries = r.q; count = r.n;
  }
  return { median: median(times), queries, count };
}

(async () => {
  const tenant = await tenantBySlug(process.env.N1_TENANT || 'acme');
  console.log(color.b(`N+1 benchmark · tenant=${tenant.slug} · ${LIMIT} orders · ${RUNS} runs`));
  const naive = await bench(listOrdersNaive, tenant.id);
  console.log(color.y(`❌ naive (N+1):  ${naive.queries} queries · median ${naive.median.toFixed(2)} ms`));
  const opt = await bench(listOrdersOptimized, tenant.id);
  console.log(color.g(`✅ JOIN + json_agg: ${opt.queries} query  · median ${opt.median.toFixed(2)} ms`));
  const md = `| Strategy | SQL queries | Median latency (ms) |\n|---|---:|---:|\n` +
    `| Naive loop (N+1) | ${naive.queries} | ${naive.median.toFixed(2)} |\n` +
    `| Single LEFT JOIN + json_agg | ${opt.queries} | ${opt.median.toFixed(2)} |\n` +
    `\n**${(naive.median / opt.median).toFixed(1)}× faster**, ${naive.queries - opt.queries} fewer round-trips. ` +
    `_${LIMIT} orders, median of ${RUNS} runs, ${new Date().toISOString()}_\n`;
  fs.mkdirSync(path.join(__dirname, '..', 'docs', 'benchmarks'), { recursive: true });
  fs.writeFileSync(path.join(__dirname, '..', 'docs', 'benchmarks', 'n1.md'), md);
  console.log('\n' + md);
})()
  .catch((e) => { console.error(color.r('✘'), e.message); process.exitCode = 1; })
  .finally(async () => { await pool.end(); await adminPool.end(); });

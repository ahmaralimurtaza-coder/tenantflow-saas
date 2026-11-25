// EXPLAIN ANALYZE before vs after the composite index (tenant_id, created_at DESC)
//   npm run bench:explain            -> before + create index + after, writes docs/benchmarks/
//   npm run index:drop / index:create -> toggle manually during viva
//   node scripts/explain.js --once   -> just run the plan with the current state
const fs = require('fs');
const path = require('path');
const { adminPool, color, median, tenantBySlug } = require('./_db');

const OUT = path.join(__dirname, '..', 'docs', 'benchmarks');
const RUNS = Number(process.env.EXPLAIN_RUNS || 7);
const INDEX_SQL = 'CREATE INDEX IF NOT EXISTS idx_orders_tenant_created ON orders (tenant_id, created_at DESC)';

const QUERIES = {
  'Q1 latest 50 orders of a tenant':
    `SELECT id, customer_name, status, total_cents, created_at
       FROM orders WHERE tenant_id = $1
      ORDER BY created_at DESC LIMIT 50`,
  'Q2 tenant orders in last 7 days':
    `SELECT id, total_cents, created_at
       FROM orders WHERE tenant_id = $1 AND created_at >= now() - interval '7 days'
      ORDER BY created_at DESC`,
};

async function plan(sql, tenantId) {
  const { rows } = await adminPool.query(`EXPLAIN (ANALYZE, BUFFERS, COSTS, TIMING) ${sql}`, [tenantId]);
  const lines = rows.map((r) => r['QUERY PLAN']);
  const exec = Number((lines.find((l) => l.startsWith('Execution Time')) || '').match(/([\d.]+)/)[1]);
  const planning = Number((lines.find((l) => l.startsWith('Planning Time')) || 'x 0').match(/([\d.]+)/)[1]);
  return { lines, exec, planning };
}

async function measure(label, tenantId) {
  const out = {};
  for (const [name, sql] of Object.entries(QUERIES)) {
    await plan(sql, tenantId); // warm cache
    const runs = [];
    let last;
    for (let i = 0; i < RUNS; i++) { last = await plan(sql, tenantId); runs.push(last.exec); }
    out[name] = { median: median(runs), min: Math.min(...runs), plan: last.lines, planning: last.planning };
    console.log(color.b(`\n── ${label} · ${name} ──`));
    console.log(last.lines.map((l) => (/Seq Scan|Sort/.test(l) ? color.y(l) : /Index/.test(l) ? color.g(l) : l)).join('\n'));
    console.log(color.c(`median execution over ${RUNS} runs: ${median(runs).toFixed(3)} ms`));
  }
  return out;
}

(async () => {
  const arg = process.argv[2];
  const tenant = await tenantBySlug(process.env.EXPLAIN_TENANT || 'acme');
  const { rows: [{ n }] } = await adminPool.query('SELECT count(*)::int n FROM orders');

  if (arg === '--drop') { await adminPool.query('DROP INDEX IF EXISTS idx_orders_tenant_created'); await adminPool.query('ANALYZE orders'); console.log(color.y('index dropped')); return; }
  if (arg === '--create') { await adminPool.query(INDEX_SQL); await adminPool.query('ANALYZE orders'); console.log(color.g('index created: ' + INDEX_SQL)); return; }
  if (arg === '--once') { await measure('CURRENT', tenant.id); return; }

  console.log(color.b(`Table orders: ${n.toLocaleString()} rows · tenant=${tenant.slug}`));
  await adminPool.query('DROP INDEX IF EXISTS idx_orders_tenant_created');
  await adminPool.query('ANALYZE orders');
  const before = await measure('BEFORE (no composite index)', tenant.id);

  console.log(color.b(`\n>>> ${INDEX_SQL}`));
  const t0 = Date.now();
  await adminPool.query(INDEX_SQL);
  await adminPool.query('ANALYZE orders');
  console.log(color.d(`index built in ${Date.now() - t0} ms`));
  const after = await measure('AFTER (composite index)', tenant.id);

  fs.mkdirSync(OUT, { recursive: true });
  const dump = (o) => Object.entries(o).map(([k, v]) => `### ${k}\n${v.plan.join('\n')}\n`).join('\n');
  fs.writeFileSync(path.join(OUT, 'explain-before.txt'), dump(before));
  fs.writeFileSync(path.join(OUT, 'explain-after.txt'), dump(after));
  let md = `| Query | Before (ms) | After (ms) | Speed-up |\n|---|---:|---:|---:|\n`;
  for (const k of Object.keys(QUERIES)) {
    const b = before[k].median, a = after[k].median;
    md += `| ${k} | ${b.toFixed(3)} | ${a.toFixed(3)} | **${(b / a).toFixed(1)}×** |\n`;
  }
  md += `\n_${n.toLocaleString()} rows in \`orders\`, median of ${RUNS} runs, tenant \`${tenant.slug}\`, ${new Date().toISOString()}_\n`;
  fs.writeFileSync(path.join(OUT, 'explain-summary.md'), md);
  console.log('\n' + color.b('SUMMARY') + '\n' + md);
  console.log(color.d(`written: docs/benchmarks/explain-before.txt, explain-after.txt, explain-summary.md`));
})()
  .catch((e) => { console.error(color.r('✘'), e.message); process.exitCode = 1; })
  .finally(() => adminPool.end());

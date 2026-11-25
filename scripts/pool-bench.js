// Connection pooling benchmark: new connection per query vs pg.Pool
//   npm run bench:pool
const fs = require('fs');
const path = require('path');
const { Client, Pool } = require('pg');
const config = require('../src/config');
const { color } = require('./_db');

const TOTAL = Number(process.env.POOL_QUERIES || 200);
const CONCURRENCY = Number(process.env.POOL_CONCURRENCY || 10);
const ssl = config.dbSsl ? { rejectUnauthorized: false } : undefined;
const SQL = 'SELECT count(*) FROM products';

async function runConcurrent(task) {
  let i = 0; const lat = [];
  const t0 = Date.now();
  await Promise.all(Array.from({ length: CONCURRENCY }, async () => {
    while (i++ < TOTAL) { const s = Date.now(); await task(); lat.push(Date.now() - s); }
  }));
  lat.sort((a, b) => a - b);
  return { totalMs: Date.now() - t0, p50: lat[Math.floor(lat.length * 0.5)], p95: lat[Math.floor(lat.length * 0.95)] };
}

(async () => {
  console.log(color.b(`${TOTAL} queries, concurrency ${CONCURRENCY}`));
  const noPool = await runConcurrent(async () => {
    const c = new Client({ connectionString: config.databaseUrl, ssl });
    await c.connect(); await c.query(SQL); await c.end();
  });
  console.log(color.y(`no pool : total ${noPool.totalMs} ms  p50 ${noPool.p50} ms  p95 ${noPool.p95} ms`));
  const pool = new Pool({ connectionString: config.databaseUrl, ssl, max: CONCURRENCY });
  const pooled = await runConcurrent(() => pool.query(SQL));
  console.log(color.g(`pg.Pool : total ${pooled.totalMs} ms  p50 ${pooled.p50} ms  p95 ${pooled.p95} ms  (connections opened: ${pool.totalCount})`));
  await pool.end();
  const md = `| Mode | Total (ms) | p50 (ms) | p95 (ms) |\n|---|---:|---:|---:|\n` +
    `| New connection per query | ${noPool.totalMs} | ${noPool.p50} | ${noPool.p95} |\n` +
    `| pg.Pool (max=${CONCURRENCY}) | ${pooled.totalMs} | ${pooled.p50} | ${pooled.p95} |\n` +
    `\n_${TOTAL} queries, concurrency ${CONCURRENCY}, ${new Date().toISOString()}_\n`;
  fs.mkdirSync(path.join(__dirname, '..', 'docs', 'benchmarks'), { recursive: true });
  fs.writeFileSync(path.join(__dirname, '..', 'docs', 'benchmarks', 'pool.md'), md);
  console.log('\n' + md);
})().catch((e) => { console.error(color.r('✘'), e.message); process.exit(1); });

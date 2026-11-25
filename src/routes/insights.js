// Read-only endpoints used by the dashboard + viva
const router = require('express').Router();
const { withTenant } = require('../db/tenantDb');
const { pool, poolStats } = require('../db/pool');
const { listOrdersNaive, listOrdersOptimized } = require('../services/orderQueries');

router.get('/products', async (req, res, next) => {
  try {
    const rows = await withTenant(req.tenant.id, async (db) =>
      (await db.query('SELECT id, sku, name, price_cents, stock FROM products WHERE tenant_id = $1 ORDER BY sku', [db.tenantId])).rows);
    res.json(rows);
  } catch (e) { next(e); }
});

// Last processed Kafka offsets for this tenant
router.get('/insights/events', async (req, res, next) => {
  try {
    const rows = await withTenant(req.tenant.id, async (db) => (await db.query(
      `SELECT event_id, topic, partition, kafka_offset, outcome, latency_ms, processed_at
         FROM processed_events WHERE tenant_id = $1 ORDER BY processed_at DESC LIMIT 25`, [db.tenantId])).rows);
    res.json(rows);
  } catch (e) { next(e); }
});

// Serverless cold vs warm stats
router.get('/insights/invoices', async (req, res, next) => {
  try {
    const out = await withTenant(req.tenant.id, async (db) => {
      const summary = (await db.query(
        `SELECT cold_start, count(*)::int AS n,
                round(avg(function_ms))::int AS avg_function_ms,
                round(avg(roundtrip_ms))::int AS avg_roundtrip_ms,
                max(roundtrip_ms)::int AS max_roundtrip_ms
           FROM invoice_logs WHERE tenant_id = $1 GROUP BY cold_start`, [db.tenantId])).rows;
      const recent = (await db.query(
        `SELECT order_id, cold_start, instance_id, function_ms, roundtrip_ms, created_at
           FROM invoice_logs WHERE tenant_id = $1 ORDER BY created_at DESC LIMIT 15`, [db.tenantId])).rows;
      return { summary, recent };
    });
    res.json(out);
  } catch (e) { next(e); }
});

// Live N+1 comparison
router.get('/insights/n1', async (req, res, next) => {
  try {
    const limit = Math.min(parseInt(req.query.limit, 10) || 50, 200);
    const run = async (fn) => {
      const t0 = process.hrtime.bigint();
      const r = await withTenant(req.tenant.id, async (db) => { const o = await fn(db, { limit }); return { n: o.length, q: db.queryCount }; });
      return { orders: r.n, queries: r.q, ms: Number((Number(process.hrtime.bigint() - t0) / 1e6).toFixed(1)) };
    };
    const naive = await run(listOrdersNaive);
    const optimized = await run(listOrdersOptimized);
    res.json({ limit, naive, optimized, speedup: Number((naive.ms / Math.max(optimized.ms, 0.1)).toFixed(1)) });
  } catch (e) { next(e); }
});

// Live EXPLAIN ANALYZE of the hot query (scoped to current tenant)
router.get('/insights/explain', async (req, res, next) => {
  try {
    const out = await withTenant(req.tenant.id, async (db) => {
      const idx = await db.query(`SELECT indexdef FROM pg_indexes WHERE indexname = 'idx_orders_tenant_created'`);
      const plan = await db.query(
        `EXPLAIN (ANALYZE, BUFFERS, COSTS, TIMING)
         SELECT id, customer_name, status, total_cents, created_at
           FROM orders WHERE tenant_id = $1 ORDER BY created_at DESC LIMIT 50`, [db.tenantId]);
      return { indexPresent: idx.rowCount > 0, indexDef: idx.rows[0] && idx.rows[0].indexdef, plan: plan.rows.map((r) => r['QUERY PLAN']) };
    });
    const exec = out.plan.find((l) => l.startsWith('Execution Time'));
    res.json({ ...out, executionTime: exec || null });
  } catch (e) { next(e); }
});

router.get('/insights/pool', (_req, res) => res.json(poolStats()));

module.exports = { router, pool };

// N+1 demo: two ways to load orders WITH their items.
// Both receive a tenant-scoped client from withTenant().

// ❌ NAIVE: 1 query for orders + 1 query PER order for items  => N+1 round-trips
async function listOrdersNaive(db, { limit = 50 } = {}) {
  const { rows: orders } = await db.query(
    `SELECT id, customer_name, status, total_cents, created_at, kafka_partition, kafka_offset
       FROM orders
      WHERE tenant_id = $1
      ORDER BY created_at DESC
      LIMIT $2`,
    [db.tenantId, limit]
  );
  for (const o of orders) {
    const { rows: items } = await db.query(
      `SELECT oi.product_id, p.name, oi.quantity, oi.unit_price_cents
         FROM order_items oi JOIN products p ON p.id = oi.product_id
        WHERE oi.order_id = $1 AND oi.tenant_id = $2`,
      [o.id, db.tenantId]
    );
    o.items = items;
  }
  return orders;
}

// ✅ OPTIMIZED: ONE query — LEFT JOIN + json_agg (eager loading)
async function listOrdersOptimized(db, { limit = 50 } = {}) {
  const { rows } = await db.query(
    `SELECT o.id, o.customer_name, o.status, o.total_cents, o.created_at,
            o.kafka_partition, o.kafka_offset,
            COALESCE(
              json_agg(json_build_object(
                'product_id', oi.product_id, 'name', p.name,
                'quantity', oi.quantity, 'unit_price_cents', oi.unit_price_cents)
              ) FILTER (WHERE oi.id IS NOT NULL), '[]') AS items
       FROM (SELECT * FROM orders
              WHERE tenant_id = $1
              ORDER BY created_at DESC
              LIMIT $2) o
       LEFT JOIN order_items oi ON oi.order_id = o.id AND oi.tenant_id = $1
       LEFT JOIN products p     ON p.id = oi.product_id AND p.tenant_id = $1
      GROUP BY o.id, o.customer_name, o.status, o.total_cents, o.created_at, o.kafka_partition, o.kafka_offset
      ORDER BY o.created_at DESC`,
    [db.tenantId, limit]
  );
  return rows;
}

async function getOrder(db, orderId) {
  const { rows } = await db.query(
    `SELECT o.*, COALESCE(json_agg(json_build_object(
              'product_id', oi.product_id, 'name', p.name,
              'quantity', oi.quantity, 'unit_price_cents', oi.unit_price_cents))
              FILTER (WHERE oi.id IS NOT NULL), '[]') AS items
       FROM orders o
       LEFT JOIN order_items oi ON oi.order_id = o.id AND oi.tenant_id = $2
       LEFT JOIN products p     ON p.id = oi.product_id AND p.tenant_id = $2
      WHERE o.id = $1 AND o.tenant_id = $2
      GROUP BY o.id`,
    [orderId, db.tenantId]
  );
  return rows[0] || null;
}

module.exports = { listOrdersNaive, listOrdersOptimized, getOrder };

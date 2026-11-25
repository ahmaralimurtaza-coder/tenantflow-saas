// Seeds 2 tenants, 1 admin user each, products, and bulk historical orders
// so EXPLAIN ANALYZE has enough rows to show a real difference.
//   SEED_ORDERS=100000 npm run db:seed
const bcrypt = require('bcryptjs');
const { adminPool, color } = require('./_db');

const N = Number(process.env.SEED_ORDERS || 100000);

const TENANTS = [
  { slug: 'acme', name: 'Acme Traders (Tenant A)', email: 'admin@acme.test', password: 'TenantA@123',
    products: [['ACM-001', 'Steel Bolts (box)', 1250], ['ACM-002', 'Copper Wire 10m', 3400], ['ACM-003', 'Safety Helmet', 2100],
               ['ACM-004', 'LED Panel', 5600], ['ACM-005', 'Cement Bag 50kg', 1800], ['ACM-006', 'PVC Pipe 3m', 950]] },
  { slug: 'globex', name: 'Globex Supplies (Tenant B)', email: 'admin@globex.test', password: 'TenantB@123',
    products: [['GLX-001', 'Office Chair', 15900], ['GLX-002', 'A4 Paper Ream', 750], ['GLX-003', 'Laser Toner', 8900],
               ['GLX-004', 'Whiteboard', 6400], ['GLX-005', 'Desk Lamp', 2900], ['GLX-006', 'USB-C Hub', 4500]] },
];

(async () => {
  const db = await adminPool.connect();
  try {
    await db.query('BEGIN');
    await db.query('TRUNCATE invoice_logs, processed_events, order_items, orders, products, users, tenants RESTART IDENTITY CASCADE');
    const ids = [];
    for (const t of TENANTS) {
      const { rows: [tenant] } = await db.query('INSERT INTO tenants (slug, name) VALUES ($1,$2) RETURNING id', [t.slug, t.name]);
      ids.push(tenant.id);
      await db.query('INSERT INTO users (tenant_id, email, password_hash) VALUES ($1,$2,$3)', [tenant.id, t.email, await bcrypt.hash(t.password, 10)]);
      for (const [sku, name, price] of t.products) {
        await db.query('INSERT INTO products (tenant_id, sku, name, price_cents, stock) VALUES ($1,$2,$3,$4,$5)', [tenant.id, sku, name, price, 1000]);
      }
      console.log(color.g(`✔ tenant ${t.slug} (${tenant.id})  login: ${t.email} / ${t.password}`));
    }

    console.log(color.d(`… generating ${N.toLocaleString()} historical orders`));
    await db.query(
      `INSERT INTO orders (id, tenant_id, customer_name, status, total_cents, created_at, processed_at)
       SELECT gen_random_uuid(), CASE WHEN g % 2 = 0 THEN $1::uuid ELSE $2::uuid END,
              'Seed Customer ' || g, 'CONFIRMED', 0,
              now() - (random() * interval '365 days'), now()
         FROM generate_series(1, $3) g`,
      [ids[0], ids[1], N]
    );
    await db.query(
      `INSERT INTO order_items (order_id, tenant_id, product_id, quantity, unit_price_cents)
       SELECT o.id, o.tenant_id, p.id, 1 + (abs(hashtext(o.id::text || p.id::text)) % 5), p.price_cents
         FROM orders o
         JOIN LATERAL (
           SELECT id, price_cents FROM products
            WHERE tenant_id = o.tenant_id
            ORDER BY md5(id::text || o.id::text) LIMIT 2
         ) p ON true`
    );
    await db.query(
      `UPDATE orders o SET total_cents = s.total
         FROM (SELECT order_id, sum(quantity * unit_price_cents) AS total FROM order_items GROUP BY order_id) s
        WHERE s.order_id = o.id`
    );
    await db.query('COMMIT');
    await db.query('DROP INDEX IF EXISTS idx_orders_tenant_created');
    await db.query('ANALYZE');
    const { rows } = await db.query('SELECT (SELECT count(*) FROM orders) o, (SELECT count(*) FROM order_items) i');
    console.log(color.g(`✔ seeded ${rows[0].o} orders, ${rows[0].i} order_items (composite index dropped -> run npm run bench:explain)`));
  } catch (e) {
    await db.query('ROLLBACK').catch(() => {});
    throw e;
  } finally {
    db.release();
    await adminPool.end();
  }
})().catch((e) => { console.error(color.r('✘ seed failed:'), e.message); process.exit(1); });

// Inventory consumer (Consumer Group: inventory-service)
// - Reads OrderCreated events from tenant.orders
// - Inside a tenant-scoped transaction: idempotency check, stock check (FOR UPDATE),
//   decrement inventory, persist order + items, record the processed offset
// - Then triggers the serverless invoice worker (fire-and-forget)
const { buildKafka } = require('./client');
const config = require('../config');
const { withTenant } = require('../db/tenantDb');
const { requestInvoice } = require('../services/invoiceClient');

const c = { dim: '\x1b[2m', green: '\x1b[32m', red: '\x1b[31m', cyan: '\x1b[36m', yellow: '\x1b[33m', reset: '\x1b[0m' };

async function processOrderEvent(event, meta) {
  const started = Date.now();
  return withTenant(event.tenantId, async (db) => {
    const dup = await db.query('SELECT 1 FROM processed_events WHERE event_id = $1', [event.eventId]);
    if (dup.rowCount) return { outcome: 'DUPLICATE_SKIPPED', latency: Date.now() - started };

    let status = 'CONFIRMED';
    let reason = null;
    const lines = [];
    // Lock product rows in a stable order to avoid deadlocks
    const items = [...event.items].sort((a, b) => a.productId.localeCompare(b.productId));
    for (const it of items) {
      const { rows } = await db.query(
        'SELECT id, name, price_cents, stock FROM products WHERE id = $1 AND tenant_id = $2 FOR UPDATE',
        [it.productId, event.tenantId]
      );
      const p = rows[0];
      if (!p) { status = 'REJECTED'; reason = `Unknown product ${it.productId}`; break; }
      if (p.stock < it.quantity) { status = 'REJECTED'; reason = `Insufficient stock for ${p.name} (have ${p.stock}, need ${it.quantity})`; break; }
      lines.push({ ...it, price: p.price_cents, name: p.name });
    }

    const total = lines.reduce((s, l) => s + l.price * l.quantity, 0);
    await db.query(
      `INSERT INTO orders (id, tenant_id, customer_name, status, reject_reason, total_cents,
                           kafka_partition, kafka_offset, created_at, processed_at)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9, now())
       ON CONFLICT (id) DO NOTHING`,
      [event.orderId, event.tenantId, event.customerName, status, reason,
       status === 'CONFIRMED' ? total : 0, meta.partition, meta.offset, event.createdAt]
    );
    if (status === 'CONFIRMED') {
      for (const l of lines) {
        await db.query('UPDATE products SET stock = stock - $1 WHERE id = $2 AND tenant_id = $3', [l.quantity, l.productId, event.tenantId]);
        await db.query(
          'INSERT INTO order_items (order_id, tenant_id, product_id, quantity, unit_price_cents) VALUES ($1,$2,$3,$4,$5)',
          [event.orderId, event.tenantId, l.productId, l.quantity, l.price]
        );
      }
    }
    const latency = Date.now() - started;
    await db.query(
      `INSERT INTO processed_events (tenant_id, event_id, topic, partition, kafka_offset, message_key, outcome, latency_ms)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
      [event.tenantId, event.eventId, meta.topic, meta.partition, meta.offset, meta.key, status, latency]
    );
    return { outcome: status, reason, total, lines, latency };
  });
}

async function triggerInvoice(event, result) {
  try {
    const order = {
      id: event.orderId, customerName: event.customerName, createdAt: event.createdAt,
      totalCents: result.total, items: result.lines.map((l) => ({ name: l.name, quantity: l.quantity, unitPriceCents: l.price })),
    };
    const inv = await requestInvoice(order, { tenantSlug: event.tenantSlug });
    if (!inv) return;
    await withTenant(event.tenantId, (db) => db.query(
      `INSERT INTO invoice_logs (tenant_id, order_id, cold_start, instance_id, function_ms, roundtrip_ms, pdf_bytes)
       VALUES ($1,$2,$3,$4,$5,$6,$7)`,
      [event.tenantId, event.orderId, inv.coldStart, inv.instanceId, inv.functionMs, inv.roundtripMs, inv.pdf.length]
    ));
    console.log(`${c.cyan}[invoice]${c.reset} order=${event.orderId.slice(0, 8)} ${inv.coldStart ? c.yellow + 'COLD' : c.green + 'WARM'}${c.reset} fn=${inv.functionMs}ms roundtrip=${inv.roundtripMs}ms pdf=${inv.pdf.length}B`);
  } catch (e) {
    console.error(`${c.red}[invoice] failed${c.reset}`, e.message);
  }
}

function validEvent(e) {
  return e && e.type === 'OrderCreated' && e.eventId && e.orderId && e.tenantId &&
    Array.isArray(e.items) && e.items.length > 0;
}

async function startConsumer({ log = true } = {}) {
  const kafka = buildKafka(`${config.kafka.clientId}-consumer`);
  const consumer = kafka.consumer({ groupId: config.kafka.groupId, sessionTimeout: 30000 });
  await consumer.connect();
  await consumer.subscribe({ topic: config.kafka.topic, fromBeginning: true });

  const { GROUP_JOIN, COMMIT_OFFSETS, CRASH } = consumer.events;
  consumer.on(GROUP_JOIN, ({ payload }) => {
    console.log(`${c.green}[consumer] joined group="${payload.groupId}" member=${payload.memberId.slice(-12)} assigned=${JSON.stringify(payload.memberAssignment)}${c.reset}`);
  });
  consumer.on(COMMIT_OFFSETS, ({ payload }) => {
    for (const t of payload.topics) for (const p of t.partitions) {
      console.log(`${c.dim}[consumer] committed ${t.topic}[p${p.partition}] -> next offset ${p.offset}${c.reset}`);
    }
  });
  consumer.on(CRASH, ({ payload }) => console.error('[consumer] crash', payload.error && payload.error.message));

  await consumer.run({
    autoCommit: true,
    eachMessage: async ({ topic, partition, message }) => {
      const meta = { topic, partition, offset: Number(message.offset), key: message.key && message.key.toString() };
      let event;
      try { event = JSON.parse(message.value.toString()); } catch { event = null; }
      if (!validEvent(event)) {
        console.warn(`${c.red}[consumer] p${partition} offset=${meta.offset} invalid event skipped${c.reset}`);
        return;
      }
      const result = await processOrderEvent(event, meta); // throws -> kafkajs retries, offset not committed
      const color = result.outcome === 'CONFIRMED' ? c.green : result.outcome === 'REJECTED' ? c.red : c.yellow;
      if (log) {
        console.log(`${color}[consumer] ${topic} p${partition} offset=${meta.offset} key(tenant)=${event.tenantSlug || meta.key} order=${event.orderId.slice(0, 8)} -> ${result.outcome}${c.reset} ${result.reason ? '(' + result.reason + ') ' : ''}${result.latency}ms`);
      }
      if (result.outcome === 'CONFIRMED') triggerInvoice(event, result);
    },
  });
  return consumer;
}

module.exports = { startConsumer, processOrderEvent };

const path = require('path');
const express = require('express');
const helmet = require('helmet');
const cors = require('cors');
const config = require('./config');
const { pool, poolStats } = require('./db/pool');
const { requireTenantAuth } = require('./middleware/tenant');
const { disconnectProducer, getProducer } = require('./kafka/producer');

const app = express();
app.set('trust proxy', 1);
app.use(helmet({ crossOriginResourcePolicy: { policy: 'cross-origin' } }));
app.use(cors({
  origin: process.env.CORS_ORIGIN ? process.env.CORS_ORIGIN.split(',') : true,
  exposedHeaders: ['x-resolved-tenant', 'x-query-count', 'x-query-ms', 'location', 'x-cold-start', 'x-function-ms', 'x-roundtrip-ms'],
}));
app.use(express.json({ limit: '100kb' }));

// request log
app.use((req, res, next) => {
  const t0 = Date.now();
  res.on('finish', () => {
    if (req.path.startsWith('/api')) console.log(`[api] ${req.method} ${req.originalUrl} ${res.statusCode} ${Date.now() - t0}ms tenant=${req.tenant ? req.tenant.slug : '-'}`);
  });
  next();
});

app.get('/api/v1/health', async (_req, res) => {
  const out = { status: 'ok', db: 'down', pool: poolStats(), kafka: { brokers: config.kafka.brokers.length, topic: config.kafka.topic } };
  try { await pool.query('SELECT 1'); out.db = 'up'; } catch (e) { out.status = 'degraded'; out.dbError = e.message; }
  res.status(out.status === 'ok' ? 200 : 503).json(out);
});

app.use('/api/v1/auth', require('./routes/auth'));
// Everything below is tenant-scoped: JWT required, tenant comes from the token
app.use('/api/v1', requireTenantAuth);
app.use('/api/v1/orders', require('./routes/orders'));
app.use('/api/v1', require('./routes/insights').router);

app.use('/api', (_req, res) => res.status(404).json({ error: 'NOT_FOUND' }));
app.use(express.static(path.join(__dirname, '..', 'public')));

// eslint-disable-next-line no-unused-vars
app.use((err, _req, res, _next) => {
  console.error('[error]', err.message);
  res.status(500).json({ error: 'INTERNAL_ERROR' });
});

process.on('unhandledRejection', (e) => console.error('[unhandledRejection]', e && e.message));

if (require.main === module) {
  const server = app.listen(config.port, () => {
    console.log(`[api] listening on :${config.port}  pool.max=${config.pool.max}  topic=${config.kafka.topic}`);
  });
  getProducer().then(() => console.log('[kafka] producer connected')).catch((e) => console.error('[kafka] producer not connected yet:', e.message));
  let consumer;
  if (config.runConsumerInApi) {
    require('./kafka/consumer').startConsumer().then((c) => { consumer = c; })
      .catch((e) => console.error('[consumer] failed to start', e.message));
  }
  const shutdown = async () => {
    server.close();
    await disconnectProducer();
    if (consumer) await consumer.disconnect().catch(() => {});
    await pool.end().catch(() => {});
    process.exit(0);
  };
  process.on('SIGTERM', shutdown);
  process.on('SIGINT', shutdown);
}

module.exports = app;

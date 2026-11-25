// Connection pooling with node-postgres (pg.Pool).
// One pool per process; connections are reused across requests instead of
// opening a new TCP + TLS + auth handshake for every query.
const { Pool } = require('pg');
const config = require('../config');

const pool = new Pool({
  connectionString: config.databaseUrl,
  ssl: config.dbSsl ? { rejectUnauthorized: false } : undefined,
  max: config.pool.max,                                   // max open connections
  idleTimeoutMillis: config.pool.idleTimeoutMillis,       // close idle clients after N ms
  connectionTimeoutMillis: config.pool.connectionTimeoutMillis, // fail fast if pool exhausted
  application_name: 'tenantflow',
});

pool.on('error', (err) => console.error('[pg-pool] idle client error', err.message));

function poolStats() {
  return {
    max: config.pool.max,
    total: pool.totalCount,
    idle: pool.idleCount,
    waiting: pool.waitingCount,
  };
}

module.exports = { pool, poolStats };

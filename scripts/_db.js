// Admin connection for scripts (table owner; runs migrations/seed/benchmarks)
const { Pool } = require('pg');
const config = require('../src/config');

const adminPool = new Pool({
  connectionString: process.env.ADMIN_DATABASE_URL || config.databaseUrl,
  ssl: config.dbSsl ? { rejectUnauthorized: false } : undefined,
  max: 4,
});

const color = { g: (s) => `\x1b[32m${s}\x1b[0m`, r: (s) => `\x1b[31m${s}\x1b[0m`, y: (s) => `\x1b[33m${s}\x1b[0m`, c: (s) => `\x1b[36m${s}\x1b[0m`, b: (s) => `\x1b[1m${s}\x1b[0m`, d: (s) => `\x1b[2m${s}\x1b[0m` };
const median = (a) => { const s = [...a].sort((x, y) => x - y); const m = Math.floor(s.length / 2); return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2; };

async function tenantBySlug(slug) {
  const { rows } = await adminPool.query('SELECT id, slug, name FROM tenants WHERE slug = $1', [slug]);
  if (!rows[0]) throw new Error(`Tenant "${slug}" not found — run npm run db:seed first`);
  return rows[0];
}

module.exports = { adminPool, color, median, tenantBySlug };

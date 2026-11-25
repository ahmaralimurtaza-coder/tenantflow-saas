// Tenant-scoped database access.
// Every request/event runs inside withTenant(tenantId, fn):
//   BEGIN
//   SET LOCAL ROLE app_tenant              -> non-owner role, so RLS is enforced
//   set_config('app.tenant_id', id, true)  -> RLS policy reads this
//   ... queries (which ALSO filter by tenant_id explicitly) ...
//   COMMIT
// The client passed to fn is wrapped so it counts queries (used by the N+1 demo).
const { pool } = require('./pool');
const config = require('../config');

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

async function withTenant(tenantId, fn) {
  if (!tenantId || !UUID_RE.test(tenantId)) throw new Error('withTenant: invalid tenant id');
  const client = await pool.connect();
  let queryCount = 0;
  const scoped = {
    tenantId,
    get queryCount() { return queryCount; },
    query: (text, params) => { queryCount += 1; return client.query(text, params); },
  };
  try {
    await client.query('BEGIN');
    if (config.rlsRole && config.rlsRole !== 'none') {
      await client.query(`SET LOCAL ROLE ${config.rlsRole}`);
    }
    await client.query("SELECT set_config('app.tenant_id', $1, true)", [tenantId]);
    const result = await fn(scoped);
    await client.query('COMMIT');
    return result;
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {});
    throw err;
  } finally {
    client.release();
  }
}

module.exports = { withTenant, UUID_RE };

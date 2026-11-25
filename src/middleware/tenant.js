// Tenant Resolution Middleware
// Sources (in order of trust):
//   1. JWT claim `tid`            -> authoritative for every protected route
//   2. `x-tenant-id` header       -> slug or UUID (used for login + cross-check)
//   3. Subdomain                  -> acme.<ROOT_DOMAIN>
// If a header/subdomain is present on a protected route and it disagrees with
// the JWT claim, the request is rejected with 403 (prevents tenant hopping).
const jwt = require('jsonwebtoken');
const { pool } = require('../db/pool');
const config = require('../config');
const { UUID_RE } = require('../db/tenantDb');

const cache = new Map(); // key: slug|id -> { tenant, at }
const TTL = 60_000;

async function findTenant(ref) {
  if (!ref) return null;
  const key = String(ref).toLowerCase();
  const hit = cache.get(key);
  if (hit && Date.now() - hit.at < TTL) return hit.tenant;
  const isUuid = UUID_RE.test(key);
  const { rows } = await pool.query(
    `SELECT id, slug, name FROM tenants WHERE ${isUuid ? 'id = $1::uuid' : 'slug = $1'}`,
    [key]
  );
  const tenant = rows[0] || null;
  if (tenant) {
    cache.set(tenant.id, { tenant, at: Date.now() });
    cache.set(tenant.slug, { tenant, at: Date.now() });
  }
  return tenant;
}

function tenantHint(req) {
  const header = req.get('x-tenant-id');
  if (header) return { source: 'header', ref: header.trim() };
  if (config.rootDomain) {
    const host = (req.hostname || '').toLowerCase();
    const root = config.rootDomain.toLowerCase();
    if (host.endsWith('.' + root)) {
      const sub = host.slice(0, -(root.length + 1)).split('.').pop();
      if (sub && sub !== 'www' && sub !== 'api') return { source: 'subdomain', ref: sub };
    }
  }
  return null;
}

// Public routes (login): tenant comes from header/subdomain/body
async function resolvePublicTenant(req, res, next) {
  try {
    const hint = tenantHint(req) || (req.body && req.body.tenant ? { source: 'body', ref: req.body.tenant } : null);
    if (!hint) return res.status(400).json({ error: 'TENANT_REQUIRED', message: 'Send x-tenant-id header or use tenant subdomain' });
    const tenant = await findTenant(hint.ref);
    if (!tenant) return res.status(404).json({ error: 'TENANT_NOT_FOUND' });
    req.tenant = { ...tenant, source: hint.source };
    next();
  } catch (e) { next(e); }
}

// Protected routes: JWT required, tenant = JWT claim
async function requireTenantAuth(req, res, next) {
  try {
    const auth = req.get('authorization') || '';
    const token = auth.startsWith('Bearer ') ? auth.slice(7) : null;
    if (!token) return res.status(401).json({ error: 'UNAUTHENTICATED' });
    let claims;
    try {
      claims = jwt.verify(token, config.jwtSecret, { algorithms: ['HS256'] });
    } catch {
      return res.status(401).json({ error: 'INVALID_TOKEN' });
    }
    const tenant = await findTenant(claims.tid);
    if (!tenant) return res.status(401).json({ error: 'TENANT_NOT_FOUND' });

    const hint = tenantHint(req);
    if (hint) {
      const r = hint.ref.toLowerCase();
      if (r !== tenant.id && r !== tenant.slug) {
        return res.status(403).json({ error: 'TENANT_MISMATCH', message: 'Token tenant does not match requested tenant' });
      }
    }
    req.user = { id: claims.sub, email: claims.email, role: claims.role };
    req.tenant = { ...tenant, source: 'jwt' };
    res.set('x-resolved-tenant', tenant.slug);
    next();
  } catch (e) { next(e); }
}

module.exports = { resolvePublicTenant, requireTenantAuth, findTenant };

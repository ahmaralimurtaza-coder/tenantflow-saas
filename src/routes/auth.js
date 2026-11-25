const router = require('express').Router();
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const config = require('../config');
const { pool } = require('../db/pool');
const { withTenant } = require('../db/tenantDb');
const { resolvePublicTenant } = require('../middleware/tenant');

// Public: list tenants for the demo login dropdown
router.get('/tenants', async (_req, res, next) => {
  try {
    const { rows } = await pool.query('SELECT slug, name FROM tenants ORDER BY slug');
    res.json(rows);
  } catch (e) { next(e); }
});

// POST /api/v1/auth/login   headers: x-tenant-id: acme   body: { email, password }
router.post('/login', resolvePublicTenant, async (req, res, next) => {
  try {
    const { email, password } = req.body || {};
    if (!email || !password) return res.status(400).json({ error: 'EMAIL_AND_PASSWORD_REQUIRED' });
    const user = await withTenant(req.tenant.id, async (db) => {
      const { rows } = await db.query(
        'SELECT id, email, password_hash, role FROM users WHERE tenant_id = $1 AND lower(email) = lower($2)',
        [db.tenantId, email]
      );
      return rows[0];
    });
    if (!user || !(await bcrypt.compare(password, user.password_hash))) {
      return res.status(401).json({ error: 'INVALID_CREDENTIALS' });
    }
    const token = jwt.sign(
      { sub: user.id, tid: req.tenant.id, tslug: req.tenant.slug, email: user.email, role: user.role },
      config.jwtSecret,
      { algorithm: 'HS256', expiresIn: config.jwtExpiresIn }
    );
    res.json({ token, tenant: { id: req.tenant.id, slug: req.tenant.slug, name: req.tenant.name }, user: { email: user.email, role: user.role } });
  } catch (e) { next(e); }
});

module.exports = router;

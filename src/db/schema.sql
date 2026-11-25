-- =====================================================================
-- TenantFlow SaaS schema
-- Strategy: Shared Database + Shared Schema + tenant_id discriminator
-- Isolation: (1) app-level WHERE tenant_id = $1 in every query
--            (2) PostgreSQL Row-Level Security (defense in depth)
-- =====================================================================
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS tenants (
  id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  slug        TEXT UNIQUE NOT NULL,          -- used in subdomain / x-tenant-id
  name        TEXT NOT NULL,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS users (
  id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id     UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  email         TEXT NOT NULL,
  password_hash TEXT NOT NULL,
  role          TEXT NOT NULL DEFAULT 'admin',
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, email)
);

CREATE TABLE IF NOT EXISTS products (
  id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id   UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  sku         TEXT NOT NULL,
  name        TEXT NOT NULL,
  price_cents INTEGER NOT NULL CHECK (price_cents >= 0),
  stock       INTEGER NOT NULL CHECK (stock >= 0),
  UNIQUE (tenant_id, sku)
);

CREATE TABLE IF NOT EXISTS orders (
  id               UUID PRIMARY KEY,
  tenant_id        UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  customer_name    TEXT NOT NULL,
  status           TEXT NOT NULL DEFAULT 'CONFIRMED',   -- CONFIRMED | REJECTED
  reject_reason    TEXT,
  total_cents      INTEGER NOT NULL DEFAULT 0,
  kafka_partition  INTEGER,
  kafka_offset     BIGINT,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  processed_at     TIMESTAMPTZ
);
-- NOTE: the composite index idx_orders_tenant_created is intentionally NOT created here.
-- `npm run bench:explain` runs EXPLAIN ANALYZE before it, creates it, then runs again.

CREATE TABLE IF NOT EXISTS order_items (
  id               BIGSERIAL PRIMARY KEY,
  order_id         UUID NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
  tenant_id        UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  product_id       UUID NOT NULL REFERENCES products(id),
  quantity         INTEGER NOT NULL CHECK (quantity > 0),
  unit_price_cents INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_order_items_order ON order_items (order_id);

-- Kafka consumer audit log: every processed offset is recorded here
CREATE TABLE IF NOT EXISTS processed_events (
  id             BIGSERIAL PRIMARY KEY,
  tenant_id      UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  event_id       UUID UNIQUE NOT NULL,          -- idempotency key
  topic          TEXT NOT NULL,
  partition      INTEGER NOT NULL,
  kafka_offset   BIGINT NOT NULL,
  message_key    TEXT,
  outcome        TEXT NOT NULL,
  latency_ms     INTEGER,
  processed_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Serverless invoice worker latency log (cold vs warm start)
CREATE TABLE IF NOT EXISTS invoice_logs (
  id              BIGSERIAL PRIMARY KEY,
  tenant_id       UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  order_id        UUID NOT NULL,
  cold_start      BOOLEAN,
  instance_id     TEXT,
  function_ms     INTEGER,          -- time spent inside the function
  roundtrip_ms    INTEGER,          -- consumer -> function -> consumer
  pdf_bytes       INTEGER,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------
-- Row-Level Security (second line of defense)
-- The API does: SET LOCAL ROLE app_tenant; set_config('app.tenant_id', ...)
-- inside every request transaction. app_tenant is NOT the table owner,
-- so RLS policies are enforced on it.
-- ---------------------------------------------------------------------
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app_tenant') THEN
    CREATE ROLE app_tenant NOLOGIN;
  END IF;
END $$;

DO $$
BEGIN
  EXECUTE format('GRANT app_tenant TO %I', current_user);
EXCEPTION WHEN others THEN
  RAISE NOTICE 'GRANT app_tenant skipped: %', SQLERRM;
END $$;

GRANT USAGE ON SCHEMA public TO app_tenant;
GRANT SELECT, INSERT, UPDATE ON products, orders, order_items, processed_events, invoice_logs TO app_tenant;
GRANT SELECT ON tenants, users TO app_tenant;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO app_tenant;

DO $$
DECLARE t TEXT;
BEGIN
  FOREACH t IN ARRAY ARRAY['users','products','orders','order_items','processed_events','invoice_logs'] LOOP
    EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY', t);
    EXECUTE format('DROP POLICY IF EXISTS tenant_isolation ON %I', t);
    EXECUTE format($p$CREATE POLICY tenant_isolation ON %I
                     USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid)
                     WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid)$p$, t);
  END LOOP;
END $$;

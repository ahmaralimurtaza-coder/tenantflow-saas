# Viva Cheat-Sheet

Open **3 terminals** (with `.env` pointing to Neon + Aiven), plus the live URL in a browser.

| # | What the instructor asks | Command / action |
|---|---|---|
| 1 | "Show consumer offsets live" | Terminal 1: `npm run consumer` (prints `p<partition> offset=<n> -> CONFIRMED` and `committed …`) |
| 2 | "Show lag per partition" | Terminal 2: `npm run kafka:watch` |
| 3 | "Trigger Kafka events" | UI: **Place order** / **Burst ×10**, or Terminal 3: `ORDERS=40 API_URL=https://<app>.onrender.com npm run load` |
| 4 | "Prove ordering" | `load` output: each tenant uses **one** partition (key = tenant_id) |
| 5 | "Run EXPLAIN ANALYZE" | `npm run index:drop` → `npm run explain` (Seq Scan) → `npm run index:create` → `npm run explain` (Index Scan). Or run all at once: `npm run bench:explain` |
| 6 | "Show N+1" | UI toggle **N+1 (naive)** vs **JOIN** (query count badge), or `npm run bench:n1` |
| 7 | "Prove tenant isolation" | `API_URL=https://<app>.onrender.com npm run test:isolation -- --with-kafka` → 15/15 PASS |
| 8 | "Connection pooling?" | `src/db/pool.js` + `npm run bench:pool` + UI **Pool stats** |
| 9 | "Cold vs warm start" | `npm run bench:coldstart` (after redeploy/idle) + UI serverless panel |

## Likely questions

- **Why `tenant_id` as the message key?** Kafka guarantees order only *within* a partition. The same key always hashes to the same partition, so one tenant's events stay in order. Different tenants still run in parallel across partitions.
- **Why not `order_id` as the key?** That spreads load better, but only orders the events of a single order. Per-tenant inventory updates then could arrive out of order.
- **What if the consumer crashes mid-message?** The offset is committed only after `eachMessage` resolves, so the message is redelivered. The `processed_events.event_id` UNIQUE check plus `ON CONFLICT DO NOTHING` makes processing idempotent.
- **Why 202 and not 201?** The resource isn't created yet. The request was only accepted for asynchronous processing.
- **Why RLS when you already filter by `tenant_id`?** Defense in depth. A developer who forgets the `WHERE` still can't leak data. The policy is enforced because the queries run as the non-owner role `app_tenant`.
- **Why is the composite column order `(tenant_id, created_at)`?** Equality column first, then range/sort column. The index then serves both the filter and the `ORDER BY … LIMIT`.
- **Where does the cold start come from?** Micro-VM boot, then Node runtime init, then module loading (PDFKit). Init happens once per instance, and later calls reuse it.

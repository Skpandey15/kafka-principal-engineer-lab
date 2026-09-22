-- WP-12 transactional outbox tables, added to the SAME `inventory`
-- database WP-11's init-postgres.sql already created `orders` in.
-- postgres's docker-entrypoint-initdb.d scripts run ONLY on first
-- container start against an empty data volume, in filename order --
-- this file's name sorts after init-postgres.sql, so both run together
-- on a fresh `docker compose up`. If platform/kafka-connect/'s volume
-- was already initialized under WP-11 before this WP existed, tear it
-- down first (`docker compose down -v`) so both scripts run.

-- The business table an application actually cares about. Deliberately
-- named differently from WP-11's `orders` (separate table, same
-- database) so the two labs' data never collide.
CREATE TABLE IF NOT EXISTS outbox_demo_orders (
    order_id    VARCHAR(64) PRIMARY KEY,
    customer_id VARCHAR(64) NOT NULL,
    amount      NUMERIC(10, 2) NOT NULL,
    status      VARCHAR(32) NOT NULL DEFAULT 'CREATED',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- The outbox table itself, in the shape Debezium's own
-- `io.debezium.transforms.outbox.EventRouter` single message transform
-- expects by default (confirmed against the real class file in
-- debezium-connect-plugins-3.6.3.Final.jar,
-- io/debezium/transforms/outbox/EventRouterConfigDefinition): a
-- `table.field.event.key` column named `aggregateid` (default), a
-- `route.by.field` column named `aggregatetype` (default), and a
-- `table.field.event.payload` column named `payload` (default).
-- `gen_random_uuid()` is a PostgreSQL built-in since v13 -- no pgcrypto
-- extension needed on postgres:17.6.
CREATE TABLE IF NOT EXISTS outbox_event (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregatetype VARCHAR(255) NOT NULL,
    aggregateid   VARCHAR(255) NOT NULL,
    type          VARCHAR(255) NOT NULL,
    payload       JSONB NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- A SEPARATE publication from WP-11's `dbz_publication`, scoped to
-- ONLY `outbox_event` -- never `outbox_demo_orders`. This is the crux
-- of the whole pattern: the business table's changes are NEVER
-- directly captured or published; only the atomic "intent to publish"
-- row is.
CREATE PUBLICATION dbz_outbox_publication FOR TABLE outbox_event;

-- Not load-bearing for INSERT-only capture (the outbox writer never
-- updates or deletes rows as part of the pattern itself), but harmless
-- and keeps this table consistent with WP-11's own REPLICA IDENTITY
-- choice for a captured table.
ALTER TABLE outbox_event REPLICA IDENTITY FULL;

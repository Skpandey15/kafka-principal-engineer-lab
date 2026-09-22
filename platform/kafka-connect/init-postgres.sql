-- WP-11 CDC source table. Named `orders`, following the same
-- OrderEvent(orderId, customerId, amount) shape every prior WP's lab
-- uses, so this WP's CDC events describe a business event the reader is
-- already familiar with instead of an unrelated example schema.
--
-- Uses the postgres superuser directly (this container's only user) --
-- appropriate only for this local, single-user learning environment,
-- exactly like every prior platform/ directory's own PLAINTEXT-only
-- security note. A real deployment would create a dedicated role holding
-- only the REPLICATION privilege (and SELECT on the captured tables),
-- never the superuser -- see the lab README's "Production considerations."
CREATE TABLE IF NOT EXISTS orders (
    order_id    VARCHAR(64) PRIMARY KEY,
    customer_id VARCHAR(64) NOT NULL,
    amount      NUMERIC(10, 2) NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Debezium's PostgreSQL connector needs a PUBLICATION naming the tables
-- it should capture -- created explicitly here for determinism, rather
-- than relying on the connector's own auto-create-publication behavior
-- (which needs elevated privileges the connector's own configured user
-- may not always have in a real deployment).
CREATE PUBLICATION dbz_publication FOR TABLE orders;

-- REPLICA IDENTITY FULL means an UPDATE/DELETE's WAL record includes
-- every column's OLD value, not just the primary key -- without this,
-- Debezium's CDC event for an UPDATE would have no way to show what the
-- row looked like BEFORE the change (its `before` field would be null
-- for every column except the key). Real, deliberate lab tradeoff: FULL
-- costs more WAL volume per change than the default (key columns only),
-- worth it here specifically so the before/after CDC event experiment
-- (Section on the Debezium envelope) has real, non-null `before` data to
-- show.
ALTER TABLE orders REPLICA IDENTITY FULL;

-- 0002_updates.sql
-- 1. Deduplicate tx_hash unique index: keep uq_invoices_payment_tx_hash, drop invoices_payment_tx_hash_unique.
-- 2. Convert payment_events.created_at and transactions.processed_at to TIMESTAMPTZ (treating stored values as UTC).
-- 3. Add PAID-row CHECK constraint from docs/postgres-cutover-draft.sql with pre-check failing loudly on violations.

-- 1. Deduplicate unique index on invoices(payment_tx_hash)
DROP INDEX IF EXISTS invoices_payment_tx_hash_unique;

CREATE UNIQUE INDEX IF NOT EXISTS uq_invoices_payment_tx_hash
  ON invoices (payment_tx_hash)
  WHERE payment_tx_hash IS NOT NULL;

-- 2. Convert TIMESTAMP to TIMESTAMPTZ treating existing values as UTC
ALTER TABLE payment_events
  ALTER COLUMN created_at TYPE TIMESTAMPTZ USING created_at AT TIME ZONE 'UTC',
  ALTER COLUMN created_at SET DEFAULT NOW();

ALTER TABLE transactions
  ALTER COLUMN processed_at TYPE TIMESTAMPTZ USING processed_at AT TIME ZONE 'UTC',
  ALTER COLUMN processed_at SET DEFAULT NOW();

-- 3. Pre-check for rows violating the PAID constraint, then apply CHECK constraint
DO $$
DECLARE
  violating_count INTEGER;
BEGIN
  SELECT COUNT(*) INTO violating_count
  FROM invoices
  WHERE status = 'PAID' AND (payment_tx_hash IS NULL OR paid_at IS NULL);

  IF violating_count > 0 THEN
    RAISE EXCEPTION 'Cannot add constraint invoices_paid_status_check: % row(s) have status = ''PAID'' but NULL payment_tx_hash or paid_at', violating_count;
  END IF;
END
$$;

ALTER TABLE invoices
  DROP CONSTRAINT IF EXISTS invoices_paid_status_check;

ALTER TABLE invoices
  ADD CONSTRAINT invoices_paid_status_check
  CHECK (
    (status = 'PAID' AND payment_tx_hash IS NOT NULL AND paid_at IS NOT NULL)
    OR status <> 'PAID'
  );

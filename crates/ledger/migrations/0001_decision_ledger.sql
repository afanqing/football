CREATE TABLE decision_ledger (
    id BIGSERIAL PRIMARY KEY,
    idempotency_key TEXT NOT NULL UNIQUE,
    match_key TEXT NOT NULL,
    competition_code TEXT NOT NULL,
    source TEXT NOT NULL,
    source_event_id TEXT NOT NULL,
    market_snapshot_sha256 TEXT NOT NULL CHECK (market_snapshot_sha256 ~ '^[0-9a-f]{64}$'),
    feature_snapshot_sha256 TEXT NOT NULL CHECK (feature_snapshot_sha256 ~ '^[0-9a-f]{64}$'),
    model_version TEXT NOT NULL,
    decided_at TIMESTAMPTZ NOT NULL,
    market_probabilities JSONB NOT NULL,
    model_probabilities JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (length(idempotency_key) > 0),
    CHECK (length(match_key) > 0),
    CHECK (length(source_event_id) > 0),
    CHECK (length(model_version) > 0),
    CHECK (market_probabilities -> 'probabilities' IS NOT NULL),
    CHECK (model_probabilities -> 'probabilities' IS NOT NULL),
    CHECK (jsonb_typeof(market_probabilities -> 'probabilities') = 'array'),
    CHECK (jsonb_typeof(model_probabilities -> 'probabilities') = 'array'),
    CHECK (jsonb_array_length(market_probabilities -> 'probabilities') = 3),
    CHECK (jsonb_array_length(model_probabilities -> 'probabilities') = 3)
);

CREATE INDEX decision_ledger_decided_at_id_idx
    ON decision_ledger (decided_at DESC, id DESC);
CREATE INDEX decision_ledger_competition_decided_at_idx
    ON decision_ledger (competition_code, decided_at DESC, id DESC);

CREATE TABLE decision_settlement_events (
    id BIGSERIAL PRIMARY KEY,
    decision_id BIGINT NOT NULL REFERENCES decision_ledger(id) ON DELETE RESTRICT,
    idempotency_key TEXT NOT NULL UNIQUE,
    event_type TEXT NOT NULL CHECK (event_type IN ('settled', 'correction', 'voided')),
    result JSONB NOT NULL,
    settled_at TIMESTAMPTZ NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (length(idempotency_key) > 0)
);

CREATE INDEX decision_settlement_events_decision_id_idx
    ON decision_settlement_events (decision_id, recorded_at, id);

CREATE FUNCTION reject_immutable_row_mutation() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION '% rows are append-only', TG_TABLE_NAME;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER decision_ledger_append_only
    BEFORE UPDATE OR DELETE ON decision_ledger
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_row_mutation();
CREATE TRIGGER decision_settlement_events_append_only
    BEFORE UPDATE OR DELETE ON decision_settlement_events
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_row_mutation();

CREATE TABLE IF NOT EXISTS history_raw (
    sample_key  char(64) PRIMARY KEY,
    tag         text NOT NULL,
    sample_time timestamp(6) NOT NULL,
    value       double precision NOT NULL,
    batch_id    text NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_history_raw_tag_time
    ON history_raw (tag, sample_time);

CREATE TABLE IF NOT EXISTS imported_batches (
    batch_id     text PRIMARY KEY,
    sha256       char(64) NOT NULL,
    row_count    integer NOT NULL,
    imported_at  timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS history_samples (
    sample_key      char(64) PRIMARY KEY,
    collector_id    text NOT NULL,
    tag             text NOT NULL,
    sample_time     timestamp(6) NOT NULL,
    value_double    double precision,
    value_text      text NOT NULL,
    data_type       text NOT NULL DEFAULT '',
    flags           text NOT NULL DEFAULT '',
    sequence_no     text NOT NULL DEFAULT '',
    archive_status  text NOT NULL DEFAULT '',
    batch_id        text NOT NULL,
    received_at     timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_history_samples_tag_time
    ON history_samples (tag, sample_time);

CREATE INDEX IF NOT EXISTS idx_history_samples_batch
    ON history_samples (batch_id);

-- history_raw is intentionally retained as the v1 read-only legacy table.
-- Its value-based sample_key has different identity semantics and must not be
-- copied blindly into history_samples. Backfill it through the v2 collector if
-- the historical range is required in the new model.

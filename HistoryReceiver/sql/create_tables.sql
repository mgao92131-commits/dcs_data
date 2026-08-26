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

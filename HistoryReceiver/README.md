# HistoryReceiver

The Receiver accepts completed DeltaV History spool batches and commits them
to a durable local inbox, then imports validated batches into PostgreSQL.

## Build

```bat
build.bat
```

## Configure

Set the same strong API key in:

- Receiver `receiver.ini`: `[Server] ApiKey`
- DCS `config.ini`: `[Receiver] ApiKey`

Do not leave `CHANGE_ME_BEFORE_USE` in production.

## Run

```bat
HistoryReceiver.exe --config receiver.ini
```

Health check:

```text
GET http://192.168.1.10:8080/healthz
```

Committed batches are stored atomically as:

```text
inbox\{batch-id}\
  data.csv
  meta.ini
```

The Receiver validates authentication, IDs, body size, SHA-256, CSV columns,
and row count before returning `committed=true`. Repeating the same BatchId
with the same content returns the same successful ACK without creating a
duplicate. Reusing a BatchId with different content returns HTTP 409.

## Phase 3 PostgreSQL import

Phase 3 uses only two database tables. Create them with:

```bat
psql -d deltav_history -f sql\create_tables.sql
```

Then configure `receiver.ini`:

```ini
[PostgreSQL]
Enabled=true
Host=127.0.0.1
Port=5432
Database=deltav_history
User=deltav_writer
Password=replace-with-real-password
SSLMode=disable
Timezone=Asia/Shanghai
ImportIntervalSeconds=30
ImportTimeoutSeconds=120
ImportBatchSize=500
MaxBatchesPerPass=20
```

The Receiver continues accepting batches into `inbox` when PostgreSQL is
offline. Its built-in importer retries automatically and moves a batch from
`inbox` to `archive` only after the database transaction commits.
Database inserts are grouped by `ImportBatchSize`; `ImportTimeoutSeconds`
prevents one damaged or blocked batch from occupying the importer forever.

To perform one manual import pass without starting the HTTP server:

```bat
HistoryReceiver.exe --config receiver.ini --import-once
```

The importer stores only Tag, local sample time, numeric value, BatchId, and a
stable SHA-256 sample key. Extra CSV columns remain available in the archived
source file but are intentionally not stored in PostgreSQL.

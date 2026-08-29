# HistoryReceiver

HistoryReceiver accepts authenticated DCS CSV batches and returns a database
ACK only after the PostgreSQL transaction commits when `SynchronousCommit=true`.

## Request flow

```text
HTTP request
 -> Bearer authentication and size checks
 -> SHA-256, CSV and row-count validation
 -> durable staging
 -> receive/parse work may run concurrently
 -> commitMu protects the PostgreSQL import
 -> unlock commitMu
 -> archive or archive_pending
 -> committed=true, commit_level=database ACK
```

The first pipeline version intentionally serializes PostgreSQL imports while
allowing the next request to finish receiving and parsing. Archive movement is
outside the database mutex. Timing logs include `CommitQueueWaitMs` and
`ArchiveMs` to show whether database commit serialization is the next
optimization target.

If PostgreSQL is unavailable or the transaction fails, the Receiver returns
HTTP 503. The DCS retains the same in-memory batch and retries it; no DCS spool
or pending outbox is involved. A permanent validation or authentication error
returns a permanent HTTP error. Repeated committed requests are safe because
`imported_batches` verifies the body hash and PostgreSQL sample-key upserts are
idempotent.

## Build and test

```bat
go test ./...
go test -race ./...
go vet ./...
scripts\build-receiver.bat
```

The PostgreSQL integration test is opt-in and requires a disposable database
configured through `DCS_HISTORY_TEST_DATABASE_URL`.

## Health check

```text
GET http://192.168.1.10:8080/healthz
```

The Receiver still supports `commit_level=inbox` for its own asynchronous
mode, but the DCS production path requires `commit_level=database`.

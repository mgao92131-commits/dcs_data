# DCS collector architecture

## Runtime baseline

- Windows 7 32-bit
- .NET Framework 3.5
- x86 process
- DeltaV Historian 10.3 assemblies
- normal-user execution

## Data flow

    DeltaV readProcessed(InterpolatedValue, 10s)
            |
            v
    HistorySync reads one ordered window
            |
            v
    HistoryBatch + BatchPayload in memory
            |
            v
    BatchSender sends and waits for database ACK
            |
            v
    SyncStateStore saves CheckpointEnd atomically
            |
            v
    next window

The collector never creates a local batch queue. During a retry, the same
HistoryBatch and BatchPayload remain in memory. A process restart discards that
memory and reads again from the last durable checkpoint.

## Ordering invariant

For every continuous batch:

    read N
      -> encode N
      -> send N
      -> database ACK N
      -> save CheckpointEnd=N.End
      -> read N+1

CollectWindow only returns after the current batch has either completed, been
rejected permanently, or the process received stop. A transient send failure
remains inside BatchSender.SendWithRetry; it cannot return control to
collection and therefore cannot cause the next Historian read.

The first window begins at:

    state.CheckpointEnd.AddSeconds(-options.OverlapSeconds)

Within one run, subsequent windows start exactly at the previous window end.
The Receiver sample key and idempotent database write make overlap and restart
re-reads safe.

## State

state.ini contains exactly one value:

    [ContinuousSync]
    CheckpointEnd=2026-08-29 07:30:00.0000000

The value advances only after a validated Receiver ACK with
commit_level=database. State writes use a flushed temporary file and atomic
replacement with a recovery rename fallback. If saving fails, the current
Batch remains complete in memory but the collector retries the state save and
does not read another Batch.

init and backfill send their data but do not modify the continuous checkpoint.

## Retry and error policy

BatchSender classifies failures as follows:

- connection failures, TCP errors, request/ACK timeout, HTTP 408/429 and
  HTTP 5xx: wait SendRetrySeconds, then resend the identical Batch;
- HTTP 401/403: authentication failure, stop immediately;
- HTTP 4xx other than retryable statuses, including 400/409/413: permanent
  batch/protocol failure, stop immediately;
- HTTP 200 with an invalid or non-database ACK: permanent protocol failure,
  stop immediately.

The default receiver timeout is 105 seconds and the default retry interval is
30 seconds. A stop event interrupts the retry wait.

## Component responsibilities

    HistorianCore.cs
        DeltaV connection, tag resolution, readProcessed and normalization

    HistoryBatch.cs
        in-memory batch model, CSV encoding and SHA-256

    BatchSender.cs
        one HTTP request, ACK validation and fixed-interval retry

    HistorySync.cs
        windows, batch ordering, continuous schedule and checkpoint progression

    SyncState.cs
        one CheckpointEnd and reliable atomic persistence

    HistoryReceiver
        HTTP intake, PostgreSQL transaction, idempotency and database ACK

The Receiver is not changed by this DCS refactor.

# DCS collector architecture

## Runtime baseline

- Windows 7 32-bit
- .NET Framework 3.5
- x86 process
- DeltaV Historian 10.3 assemblies
- normal-user execution

## v3.4.1 bounded ACK pipeline

The DCS collector has one durable boundary: a PostgreSQL database ACK. The
Historian producer remains a single thread, while at most two prepared batches
are sent by two fixed sender workers:

    DeltaV readProcessed(InterpolatedValue, 10s)
            |
            v
    single Historian producer
            |
            v
    in-memory Batch 1 / Batch 2
            |
       +----+----+
       |         |
       v         v
    sender 1  sender 2
       |         |
       +----+----+
            |
            v
    Receiver PostgreSQL COMMIT ACK
            |
            v
    ordered CheckpointEnd coordinator
            |
            v
    release one slot and read the next window

There is no DCS local batch queue. A retry keeps the same HistoryBatch and
BatchPayload in memory. A process restart discards that memory and reads again
from the last durable checkpoint.

## Pipeline invariants

`BatchPipeline.PipelineDepth` is fixed at 2. `WaitForCapacity` runs before
`PrepareBatch`, so a full pipeline blocks the producer before it reads the next
Historian window. The producer never calls Historian concurrently.

Each prepared batch receives a monotonically increasing sequence:

    Checkpoint = N-1
    N   = Sending
    N+1 = Acked
    N+2 = forbidden

An ACK may arrive out of order. An ACKed later batch remains in memory until
the oldest outstanding sequence is also ACKed. The coordinator then saves the
contiguous ACK prefix one range at a time:

    ACK N+1                    Checkpoint remains N-1
    ACK N                      save N.End, then save N+1.End
    both saves complete        release a slot and read N+2

The active slot count includes prepared, sending, ACKed-but-not-checkpointed,
and failed batches. A transient failure therefore occupies its slot and cannot
create an unbounded backlog.

## Preparation, sending and checkpointing

`PrepareBatch` only performs Historian reads, batch construction and CSV/SHA-256
encoding. It does not send and does not modify state. `BatchPipeline.Submit`
hands the prepared object to one worker. The worker calls
`BatchSender.SendWithRetry`, validates the database ACK, marks the object
ACKed, and asks the coordinator to advance the contiguous prefix.

The first continuous window begins at:

    state.CheckpointEnd.AddSeconds(-options.OverlapSeconds)

Subsequent windows start at the previous window end. Receiver sample-key
idempotency makes overlap and restart re-reads safe.

Checkpoint state contains exactly:

    [ContinuousSync]
    CheckpointEnd=2026-08-29 07:30:00.0000000

Only a validated `commit_level=database` ACK can advance it. State writes use a
flushed temporary file and atomic replacement. If a state write fails, the ACKed
batch remains retained and the coordinator retries the write before releasing a
slot. `init` and `backfill` use the same two-slot sender pipeline but do not
modify the continuous checkpoint.

## Retry and error policy

`BatchSender` is immutable after construction and safe for both workers. Each
call owns its request, payload stream and timing object:

- connection failures, TCP errors, request/ACK timeout, HTTP 408/429 and
  HTTP 5xx: wait `SendRetrySeconds`, then resend the identical BatchId, SHA-256
  and body;
- HTTP 401/403: authentication failure, stop the producer and both workers;
- other permanent 4xx, including 400/409/413: stop the producer and both
  workers;
- HTTP 200 with an invalid or non-database ACK: permanent protocol failure.

The default DCS timeout is 135 seconds and the default retry interval is
30 seconds. A stop event interrupts both retry waits. On a fatal worker error,
only the already contiguous ACK prefix may advance CheckpointEnd; all other
in-memory batches are discarded.

## Component responsibilities

    HistorianCore.cs
        DeltaV connection, tag resolution, readProcessed and normalization

    HistoryBatch.cs
        in-memory batch model, CSV encoding and SHA-256

    BatchSender.cs
        one HTTP request, ACK validation and fixed-interval retry

    BatchPipeline.cs
        depth-2 slots, two sender workers and ordered ACK/checkpoint advancement

    HistorySync.cs
        preparation, producer windows, continuous schedule and pipeline wiring

    SyncState.cs
        one CheckpointEnd and reliable atomic persistence

    HistoryReceiver
        concurrent HTTP receive/parse, serialized PostgreSQL commit and database ACK

The Receiver keeps `commitMu` for PostgreSQL import only. Archive movement is
outside that mutex. Receiver timing logs include `CommitQueueWaitMs` and
`ArchiveMs` so database queue pressure can be measured before considering
parallel database imports.

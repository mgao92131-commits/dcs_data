# DCS processed collector architecture

## DCS constraints

- Windows 7 32-bit
- .NET Framework 3.5 compiler and runtime
- x86 process architecture
- DeltaV Historian 10.3 assemblies
- normal user execution

## Historian data flow

```text
DeltaV Historian readProcessed(InterpolatedValue, 10s)
    -> HistorianClient
    -> HistorySync serial tag collection on one shared TimeSpan
    -> in-memory CSV batch
    -> Receiver
    -> PostgreSQL commit ACK
```

Every tag in `tags.txt` uses `InterpolatedValue`. Status, alarm, interlock,
pulse, digital event, and invalid tags must be removed from that file.

Collection boundaries and dynamic split points are aligned to the configured
sampling interval. The logical maximum window remains 30 minutes. Before a
Historian read, the collector limits each physical batch by the expected row
capacity; the existing byte-limit dynamic split remains as a final safeguard.

A failed tag read is logged and collection continues for the remaining tags.
If failures exceed `[Sampling] MaxFailedTagsPerBatch`, no batch is persisted and
the checkpoint does not advance. A 60-second overlap retries the tail of the
previous cycle while stable Processed identities keep retries idempotent.

Historian Processed timestamps are converted from UTC to DCS local time before
the existing timezone-less CSV protocol is encoded.

The Historian client creates and configures one TimeSpan per physical window,
then calls `readProcessed` serially for each valid tag and releases the handle
after the window completes. The per-tag normalization protection remains in
the Core; the Sync layer does not build a second timestamp de-duplication map.

## State invariants

1. `LastCollectedEnd` advances only after the batch is committed remotely or
   durably stored in the local outbox.
2. `LastCommittedEnd` advances only after a PostgreSQL database-level ACK.
3. Initial-load and backfill commands do not modify continuous checkpoints.
4. State updates use flush and atomic rename.
5. A global named mutex prevents concurrent collector executions.

## Send and pending state machine

The sender drains the oldest pending batches continuously until the configured
`BacklogDrainSeconds` budget expires or the Receiver fails. New collection is
allowed while a transient failure leaves pending space available. Once the
pending batch/byte safety limit is reached, or a healthy Receiver cannot drain
the backlog within its budget, the state records `CollectionPaused=true` and
the cycle does not read more Historian data. The continuous host remains alive.

After a successful drain, the state clears `CollectionPaused` and collection
resumes from the durable Historian checkpoint. The default pending limits are
50 batches and 100 MiB.

The timeout relationship for synchronous database ACK is:

```text
PostgreSQL import 45s < Receiver HTTP write 60s < DCS sender wait 75s
```

Each completed batch emits `HistorianReadMs`, `EncodeMs`, `SendMs`,
`AckWaitMs`, `TotalMs`, `PendingBatches`, `PendingBytes`, and
`SyncLagSeconds`. The Receiver emits the corresponding receive, validation,
parse, COPY, upsert, commit, and total timings.

The Receiver hashes, stages, validates, and converts the incoming CSV while
reading the HTTP body. Synchronous database ACK reuses those parsed rows
instead of reopening the CSV; durable inbox recovery reparses the staged CSV
when needed. PostgreSQL uses conditional `IS DISTINCT FROM` updates so an
overlap retry with identical values does not create an UPDATE/WAL record.

Pending files are hashed once and then sent from a FileStream with HTTP
KeepAlive enabled. The wire CSV, Receiver, PostgreSQL, spool, and ACK formats
remain compatible; the continuous checkpoint file adds the pause status and
reason fields.

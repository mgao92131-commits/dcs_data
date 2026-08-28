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
    -> HistorySync sequential tag collection
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
the checkpoint does not advance. A five-minute overlap retries the complete
previous cycle while stable Processed identities keep retries idempotent.

Historian Processed timestamps are converted from UTC to DCS local time before
the existing timezone-less CSV protocol is encoded.

## State invariants

1. `LastCollectedEnd` advances only after the batch is committed remotely or
   durably stored in the local outbox.
2. `LastCommittedEnd` advances only after a PostgreSQL database-level ACK.
3. Initial-load and backfill commands do not modify continuous checkpoints.
4. State updates use flush and atomic rename.
5. A global named mutex prevents concurrent collector executions.

The CSV, Receiver, PostgreSQL, spool, ACK, and checkpoint formats are unchanged.

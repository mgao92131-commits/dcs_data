# DeltaV Processed History Collector

This repository contains a DCS collector and its existing Receiver. The DCS
collector targets .NET Framework 3.5/x86 and reads every configured Historian
tag with `readProcessed`, `Aggregate.InterpolatedValue`, and a 10-second grid.

## DCS build

```bat
scripts\package-dcs.bat
```

The output is `artifacts\dcs_data` and contains only:

```text
bin\
config\
scripts\
state\
spool\
logs\
README.txt
```

It does not contain source, tests, compatibility tools, service installers,
Receiver binaries, or database files.

## Runtime behavior

- Every tag in `config\tags.txt` uses `InterpolatedValue`.
- `[Sampling] IntervalSeconds=10` defines the fixed grid.
- Root-level `start-historysync.cmd` starts hidden continuous normal-user
  collection; root-level `stop-historysync.cmd` stops it gracefully.
- `[Sync] MaxWindowMinutes=30` remains the logical maximum window.
- `TargetBatchRows=25000` and `TargetBatchBytes=10485760` pre-size normal
  windows before Historian reads; `MaxBatchRows` and `MaxBatchBytes` remain
  hard limits.
- A small number of failed tags is logged while remaining tags continue.
- Excessive failures reject the partial batch, so state does not advance.
- Receiver failures retain durable pending batches and retry them in oldest-first
  drain mode for `[Receiver] BacklogDrainSeconds`.
- Collection pauses at the pending safety limit instead of reading more
  Historian data; the continuous host stays alive and retries the drain every
  `[Receiver] PendingRetrySeconds`.
- The continuous schedule is fixed start-to-start, and the default overlap is
  60 seconds.
- Each collection window reuses one configured Historian TimeSpan across the
  serial per-tag `readProcessed` calls; Sync no longer repeats timestamp
  de-duplication after Core normalization.
- Receiver CSV staging computes SHA-256, validates/converts rows, and writes
  staging in one HTTP-body pass; synchronous database import reuses those rows.
- Synchronous database retries query `imported_batches`, hash the body only,
  and return database ACK without re-parsing or creating duplicate archives.
- PostgreSQL COMMIT is the synchronous ACK boundary; archive failures are
  logged and retained under `archive_pending` without returning a false 503.
- Receiver maintenance retries valid `archive_pending` entries hourly and
  leaves failed moves in place for the next pass (at most 100 retries per
  pass).
- Receiver staging defaults to `StagingDurability=full`; `buffered` is an
  explicit performance-test tradeoff for uncommitted staging files.
- Pending files record their SHA at spool time, then are sent once from a
  FileStream with HTTP KeepAlive; PostgreSQL skips
  no-op overlap updates using conditional UPSERT predicates.
- Batch timing logs include Historian RPC, conversion, normalization, encoding,
  send, ACK wait, total, rows/sec, working set, managed memory, pending size,
  and sync lag measurements.
- CSV, Receiver, PostgreSQL, spool, and ACK formats remain compatible. The
  continuous state file additionally records `CollectionPaused` and its reason.

Status, alarm, interlock, pulse, digital event, and invalid tags must be removed
from the production tag file.

See `docs\architecture.md` and `docs\deployment.md` for details.

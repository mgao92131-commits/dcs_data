# DCS collector build and deployment

## Build

The supported DCS baseline is Windows 7 32-bit with the .NET Framework 3.5
compiler/runtime and an x86 process; no .NET 2.0 fallback is maintained:

```bat
scripts\package-dcs.bat
```

The package is created at `artifacts\dcs_data` with this layout:

```text
bin\
config\
scripts\
state\
spool\
logs\
```

It contains no source, test, Probe, compatibility, Receiver, database, service,
scheduled-task, or startup files.

## DCS setup

1. Copy `artifacts\dcs_data` to a normal-user writable local directory.
2. Copy `config\config.example.ini` to `config\config.ini`.
3. Copy `config\tags.example.txt` to `config\tags.txt`.
4. Remove status/event and invalid tags from `tags.txt`.
5. Set Collector Id, Receiver URL, and API key.
6. Run `scripts\status.cmd` and then root-level `start-historysync.cmd`.
   Use root-level `stop-historysync.cmd` to stop it.

`start-historysync.cmd` launches a hidden normal-user host. It executes every
`[Sync] IntervalMinutes`; `stop-historysync.cmd` requests a graceful stop after
the current cycle. `run.cmd` provides the same host in the foreground for
diagnostics. None of these scripts installs a system component.

`MaxWindowMinutes` remains 30. All retained tags are read with
`InterpolatedValue` at `[Sampling] IntervalSeconds=10`.
Normal windows target `TargetBatchRows=25000` and
`TargetBatchBytes=10485760`; `MaxBatchRows=50000` and
`MaxBatchBytes=20971520` remain hard limits.

The reliability defaults are `OverlapSeconds=60`, `MaxPendingBatches=50`,
`MaxPendingBytes=104857600`, `BacklogDrainSeconds=60`,
`PendingRetrySeconds=30`, and a 105-second DCS sender timeout. On a transient
Receiver failure, pending data is retained in oldest-first order. If the
pending safety limit is reached, the state records `CollectionPaused=true`;
the continuous host stays running and retries the pending drain on the short
cycle instead of waiting for the normal five-minute collection slot. Once
enough pending data drains to leave the safety limit, collection resumes.

For synchronous PostgreSQL ACK, keep the timeout hierarchy aligned:
`ImportTimeoutSeconds=45`, `WriteTimeoutSeconds=90`, and DCS
`TimeoutSeconds=105`.

The current sender/receiver path also reuses one Historian TimeSpan per window,
parses the incoming CSV during HTTP staging, skips no-op PostgreSQL overlap
updates, and streams pending files over KeepAlive connections without a second
local CSV hash pass. In synchronous
mode, database COMMIT returns the database ACK even when the auxiliary archive
move fails; the payload is retained under `archive_pending` for maintenance.
Retries with an already committed BatchId verify only the body hash and do not
create duplicate archive directories. The durable staging/inbox files remain
available for restart recovery. Receiver maintenance retries valid
`archive_pending` entries hourly, up to 100 batches per pass, and leaves failed
moves in place for the next pass.

Receiver staging uses `StagingDurability=full` by default: the received CSV and
metadata are flushed to stable storage before the batch is committed to the
inbox or database. `StagingDurability=buffered` may be used for a controlled
performance test when losing an uncommitted staging file after power loss is
acceptable; PostgreSQL COMMIT remains the synchronous ACK boundary.

## Upgrade

Preserve `config`, `state`, `spool`, and `logs`, then replace only `bin` and
`scripts`. Check pending batches before and after the replacement.

Because the processed-only collector intentionally has no Raw compatibility
mode, rollback requires restoring a complete earlier binary/config package.

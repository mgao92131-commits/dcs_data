# dcs_data v2 architecture

## Hard constraints

The DCS collector must remain compatible with:

- Windows 7 32-bit
- .NET Framework 2.0 or 3.5
- x86 process architecture
- the existing DeltaV Historian assemblies

The Receiver remains a Go service on the database computer. DCS computers
must never be rebooted or power-cycled by the collector.

## Stable baseline

The stable v1 implementation is tagged `v1-legacy`. All v2 work happens on
`refactor/v2`. Run `test-baseline-local.bat` before and after each refactoring
phase.

A real Historian comparison must also be run on a DCS workstation before a
new collector build is promoted: for the same tag and range, v1 and v2 must
produce identical timestamps, values, data types, flags, and row counts.
Run `scripts\test-dcs-compatibility.bat` there; unlike the local
regression suite, this gate never falls back to a newer framework compiler.

## Target data flow

```text
DeltaV Historian -> Historian Core -> RangeSyncEngine -> in-memory batch
                                                        |
                                                        +-> HTTP -> Receiver
                                                        |            |
                                                        |            +-> PostgreSQL COMMIT -> ACK
                                                        |
                                                        +-> failure outbox
```

## State invariants

1. `LastCollectedEnd` advances only after data is committed remotely or is
   durably stored in the local outbox.
2. `LastCommittedEnd` advances only for a contiguous sequence of batches that
   PostgreSQL has committed.
3. Pending batches with valid metadata are sent by `RangeStart`, oldest first.
   Direct HTTP sending cannot bypass an older pending batch; malformed local
   batches are isolated before valid batches are sent.
4. Initial-load and backfill jobs never modify continuous-sync checkpoints.
5. State updates use write, flush, and atomic rename.
6. A checkpoint never advances when batch persistence fails.
7. A global named mutex prevents a console host and Windows Service from
   running the same collector simultaneously across Windows sessions.

## ACK commit proof

The Receiver ACK includes `commit_level`. The DCS collector accepts
`commit_level=database` as proof for `LastCommittedEnd`; an ACK with
`commit_level=inbox` can advance only `LastAcceptedEnd`. A legacy ACK without
`commit_level` is rejected, so a mismatched Receiver/DCS configuration cannot
silently weaken checkpoint semantics.

## Refactoring phases

1. Freeze v1 and establish the regression baseline.
2. Extract Historian Core without changing output.
3. Build batches in memory while keeping the existing CSV wire format.
4. Add atomic checkpoints and the range synchronization engine.
5. Add automatic catch-up and dynamic slicing.
6. Use spool only as an ordered failure outbox.
7. Add console and Windows Service hosts.
8. Make Receiver ACK follow PostgreSQL transaction commit.
9. Expand the database model after verifying DeltaV identity fields.
10. Unify continuous, initial-load, and backfill range jobs.
11. Complete production fault handling and long-running validation.

## Implementation status

Phases 1 through 11 are implemented on `refactor/v2`. Local regression,
Receiver unit/vet checks, PostgreSQL schema migration, synchronous COMMIT/ACK,
text-value import, duplicate retry, permanent invalid-batch rejection, and
durable outbox checkpoint recovery have been verified. The remaining
promotion gate is hardware-specific: compile with .NET 2.0/3.5 on the DCS
workstation and compare real Historian output against `v1-legacy`.

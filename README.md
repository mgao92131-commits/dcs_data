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
- Expected row capacity pre-splits large windows before Historian reads.
- A small number of failed tags is logged while remaining tags continue.
- Excessive failures reject the partial batch, so state does not advance.
- CSV, Receiver, PostgreSQL, spool, ACK, and state formats are unchanged.

Status, alarm, interlock, pulse, digital event, and invalid tags must be removed
from the production tag file.

See `docs\architecture.md` and `docs\deployment.md` for details.

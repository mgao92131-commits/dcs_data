DeltaV History CLI v1.1
========================

Finalized RAW export layout:
  ONE TAG = ONE CSV FILE

The program uses the already verified native DeltaV Historian path:

  HistoryReader.exe
      -> DeltaV.Historian.DvCHDataAccess.dll
      -> APP
      -> Continuous Historian

It does NOT use remote OPC HDA/DCOM.

Build
-----
Run:

  build.bat

The result is:

  HistoryReader.exe

Target:
  x86
  .NET Framework 2.0/3.5

No DeltaV DLLs are bundled. The application uses the matching DeltaV DLLs
already installed on the workstation.

Tag file
--------
tags.txt:

  TI-021007/AI1/PV.CV
  TI-118020/AI1/PV.CV
  PICA-117024/PID1/PV.CV

One exact Historian tag per line.

Blank lines are ignored.
Lines beginning with # are comments.

Commands
--------

1. Export last hour:

  HistoryReader.exe export --tags tags.txt --last 1h

2. Export last day:

  HistoryReader.exe export --tags tags.txt --last 1d

3. Explicit time range:

  HistoryReader.exe export --tags tags.txt ^
    --start "2026-08-25 08:00:00" ^
    --end   "2026-08-25 14:00:00"

4. Custom output directory:

  HistoryReader.exe export --tags tags.txt --last 1d ^
    --out-dir D:\HistoryData

5. One tag only:

  HistoryReader.exe export ^
    --tag "TI-021007/AI1/PV.CV" ^
    --last 1h

6. Validate tags only:

  HistoryReader.exe validate --tags tags.txt

Server
------
Default Historian server is:

  APP

Only specify another node if needed:

  --server OTHERNODE

Output
------
Default directory:

  .\export

Each tag gets its own file.

Example:

  export\
    TI-021007_AI1_PV.CV_20260825_080000_20260825_140000.csv
    TI-118020_AI1_PV.CV_20260825_080000_20260825_140000.csv
    export.meta.txt

CSV layout
----------
Each CSV contains a short metadata header followed by the data table.

Example:

  # DeltaV Historian Raw Export
  # Server=APP
  # Tag=TI-021007/AI1/PV.CV
  # Start=2026-08-25 08:00:00
  # End=2026-08-25 14:00:00
  # Rows=452

  Timestamp,Value,DataType,Flags
  "2026-08-25 08:00:01.250","52.341","...",""
  "2026-08-25 08:00:18.600","52.356","...",""

Time
----
--start and --end use LOCAL workstation time.

Examples:

  --start "2026-08-25 08:00:00"
  --end   "2026-08-25 14:00:00"

Relative periods:

  --last 30m
  --last 6h
  --last 1d
  --last 7d

Large exports
-------------
Default:

  --max 10000

If DeltaV reports dataTruncated=True, the CLI automatically splits the
requested time range into smaller ranges and continues reading.

The returned samples are then:
  1. combined
  2. sorted by timestamp
  3. duplicate boundary samples removed

This is intended to avoid silently losing RAW samples on large exports.

Safety
------
Read-only.

The tool uses only the DeltaV Historian read path and does not intentionally
call write/edit/capture/admin operations.


HistorySync Phase 1
===================

HistorySync.exe reuses the verified HistoryReader implementation and creates
durable local spool batches. Phase 1 does not send data over the network.

Commands:

  HistorySync.exe sync

  HistorySync.exe init ^
    --start "2026-07-01 00:00:00" ^
    --end   "2026-08-01 00:00:00" ^
    --slice 1d

  HistorySync.exe backfill --last 1d --slice 6h

  HistorySync.exe validate --tags tags.txt

Configuration is read from config.ini beside the executable. Relative paths
are resolved relative to the executable directory, which is safe for Windows
Task Scheduler.

Completed batches are atomically moved to:

  spool\pending\{batch-id}\
    data.csv
    meta.ini

Incomplete staging directories found on the next run are preserved under
spool\failed. They are never treated as sendable batches.

Only one HistorySync instance can run at a time. SequenceNo and ArchiveStatus
are intentionally left empty until their actual DeltaV properties and
stability have been verified on the target workstation.


HistorySync Phase 2 Sender
==========================

When [Receiver] Enabled=true, sync/init/backfill collect their local batches
first and then send the oldest pending batches to HistoryReceiver. Use
--no-send to collect without attempting network transfer.

Pending batches can also be sent without reading DeltaV:

  HistorySync.exe send

The Sender uploads data.csv with authenticated batch headers. Before upload it
recomputes SHA-256 and compares it with meta.ini. A local integrity failure is
moved to spool\quarantine. Network errors and timeouts leave the batch in
spool\pending for a later retry.

A pending batch is deleted only when the Receiver response confirms all of the
following:

  ok=true
  committed=true
  matching batch_id
  matching sha256
  matching received_rows

Receiver configuration:

  [Receiver]
  Enabled=true
  Url=http://192.168.1.10:8080/api/history/batch
  TimeoutSeconds=15
  MaxBatchesPerRun=20
  AckMode=database
  ApiKey=replace-with-the-same-strong-secret-as-receiver.ini

HTTP 400/409/413 responses are permanent batch failures and go to spool\failed.
HTTP 401/403 pauses collection. Other HTTP errors leave pending for retry.
Continuous collection pauses when a continuous failed/quarantined batch is
present, so the checkpoint cannot jump over a data gap.
HistorySync v2 - Current Operation
==================================

The collector now reads DeltaV through HistorianClient, builds CSV batches in
memory, sends directly to a synchronous-commit Receiver, and uses disk only as
a failure outbox. The older Phase 1/2 notes later in this file are retained as
legacy implementation history; README.md at the repository root is the
authoritative deployment guide.

Continuous state is stored atomically in state.ini:
  LastCollectedEnd
  LastAcceptedEnd
  LastCommittedEnd

Commands:
  HistorySync.exe sync
  HistorySync.exe send
  HistorySync.exe status
  HistorySync.exe --console

Install as a Windows Service only after console validation:
  install-service.bat

The DCS build remains x86 and requires the .NET Framework 2.0/3.5 compiler.
Run test-dcs-compatibility.bat on the target DCS workstation before promotion.

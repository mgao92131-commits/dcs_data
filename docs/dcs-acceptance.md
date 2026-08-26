# DCS workstation acceptance

This checklist must be run on the actual Windows 7 32-bit DCS workstation.
The database computer cannot replace these checks because it does not have the
DeltaV APP Historian client/service.

## 1. Compatibility gate

Copy the v2 source directory and matching DeltaV runtime assemblies to a
temporary build directory on the DCS workstation. Run:

```bat
test-dcs-compatibility.bat
```

Required evidence:

- compiler found under `.NET Framework 2.0` or `3.5`
- `HistoryReader.compat.exe` and `HistorySync.compat.exe` compile successfully
- the architecture probe prints `X86 OK` for both outputs
- self-test and version checks pass

Do not use the local regression fallback to .NET 4 as this gate.

## 2. Core output comparison

Use one tag, one range, and the same `--max` value for v1 and v2. Save the
outputs outside the production spool. Compare:

- row count
- timestamp, including boundary values
- Value text
- DataType
- Flags
- SequenceNo and ArchiveStatus when exposed by the installed DeltaV API

The comparison must include a range that causes `dataTruncated=True`, proving
that automatic recursive splitting does not lose samples or duplicate a split
boundary.

For the legacy four-column CSV format, the repository includes a PowerShell 2
compatible exact-row comparison helper:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File `
  tools\compare-history-csv.ps1 `
  -LegacyCsv D:\acceptance\v1\TAG_A.csv `
  -V2Csv D:\acceptance\v2\TAG_A.csv
```

It ignores only the export comments and header, then compares every timestamp,
value, data type, and flags field byte-for-byte.

## 3. End-to-end commit

With `HistoryReceiver` configured as `SynchronousCommit=true` and the DCS
config set to `AckMode=database`:

```bat
HistorySync.exe --console
HistorySync.exe status
```

Record the BatchId, ACK, `history_samples` row count, and
`LastCommittedEnd`. The ACK is valid only when the PostgreSQL transaction has
committed.

## 4. Failure and recovery

Run these one at a time and record the result:

1. Disconnect only the data link: the batch must appear under
   `spool\pending`, and `LastCollectedEnd` may advance only after the outbox
   directory is complete.
2. Reconnect: pending batches must send oldest first and then be deleted after
   database ACK.
3. Stop PostgreSQL: Receiver must return 503; DCS must retain pending and not
   advance `LastCommittedEnd`.
4. Restore PostgreSQL: the next run must import without duplicate rows.
5. Send an invalid batch: it must enter `failed` or `quarantine`; continuous
   collection must pause instead of jumping over the gap.

## 5. Promotion record

Promote v2 only after this checklist, the baseline tests, and the database
backup/restore procedure are recorded. Installing the Windows Service does
not reboot, stop, or restart the DCS workstation.

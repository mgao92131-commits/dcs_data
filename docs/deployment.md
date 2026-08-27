# Deployment, upgrade, and rollback

## Build

The DCS package must be built with the .NET Framework 2.0 or 3.5 compiler and
`/platform:x86`:

```bat
scripts\package-dcs.bat
```

The Receiver package is built with the local Go toolchain:

```bat
scripts\package-receiver.bat
```

The resulting directories are `artifacts\dcs` and `artifacts\receiver`. Do not
copy the Git working tree to a production computer.

## Database computer

1. Create the `deltav_history` database and a writer role.
2. Apply `artifacts\receiver\database\001_create_tables.sql`.
3. Copy `receiver.example.ini` to `receiver.ini`.
4. Set the API key and PostgreSQL password. Keep `receiver.ini` outside Git.
5. Keep `SynchronousCommit=true` for the v2 DCS collector.
6. Start `HistoryReceiver.exe --config receiver.ini`.
7. Verify `http://192.168.1.10:8080/healthz` returns `ok=true` and
   `database_ok=true`.

The Receiver may be restarted independently when upgrading its binary. This
does not restart or power-cycle the DCS workstation.

## DCS workstation

1. Copy the DCS package to a local directory on the DCS workstation.
2. Copy `config.example.ini` to `config.ini` and `tags.example.txt` to
   `tags.txt`.
3. Set the Receiver URL and the same API key. The current Receiver is
   `http://192.168.1.10:8080/api/history/batch`.
4. Run the strict compatibility gate from the source checkout:
   `scripts\test-dcs-compatibility.bat`.
5. From the release directory, run `HistorySync.exe status` and then
   double-click `start-historysync.vbs`. This starts `HistorySync.exe
   --console` with a hidden window under the logged-in DCS user.
6. To stop it, double-click `stop-historysync.vbs`. The stop is graceful and
   uses a user-mode named event; it does not kill the process.

No administrator permission, Windows Service, Scheduled Task, or startup entry
is required for the normal deployment. The optional `install-service.bat`
remains source-only for an administrator-managed host and is not included in
the normal DCS package.

## Upgrade

1. Confirm pending batches are zero or record them before the change.
2. Keep `config.ini`, `receiver.ini`, `state.ini`, `spool`, and logs as local
   backups.
3. Stop only the component being upgraded.
4. Replace the executable/package files, preserving local configuration and
   runtime directories.
5. Start the component with `start-historysync.vbs` and verify status before
   resuming unattended operation.

## Rollback

First run `stop-historysync.vbs`, then restore the previous package binaries
while preserving `state.ini` and
`spool\pending`. The old v1 Receiver protocol does not prove a PostgreSQL
commit; do not run a DCS configured with `AckMode=database` against that old
Receiver. If rollback crosses the protocol boundary, first stop collection and
follow the v1/v2 data reconciliation procedure.

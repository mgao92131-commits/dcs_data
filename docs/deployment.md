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
4. Run `HistorySync.exe status` and then `HistorySync.exe --console`.
5. Run the strict compatibility gate from the package/source checkout:
   `scripts\test-dcs-compatibility.bat`.
6. Install the service only after console and Historian output checks pass.

`install-service.bat` explicitly uses `LocalSystem`. Record that identity and
prove it can read the DeltaV APP Historian. If the site requires a dedicated
account, configure the service with `sc.exe config` and repeat the service-mode
test.

## Upgrade

1. Confirm pending batches are zero or record them before the change.
2. Keep `config.ini`, `receiver.ini`, `state.ini`, `spool`, and logs as local
   backups.
3. Stop only the component being upgraded.
4. Replace the executable/package files, preserving local configuration and
   runtime directories.
5. Start the component and verify health/status before resuming unattended
   operation.

## Rollback

Restore the previous package binaries while preserving `state.ini` and
`spool\pending`. The old v1 Receiver protocol does not prove a PostgreSQL
commit; do not run a DCS configured with `AckMode=database` against that old
Receiver. If rollback crosses the protocol boundary, first stop collection and
follow the v1/v2 data reconciliation procedure.

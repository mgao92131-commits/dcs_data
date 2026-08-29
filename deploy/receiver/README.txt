HistoryReceiver deployment package
==================================

This package runs on the database computer. It accepts authenticated CSV
batches from the DCS collector and, with SynchronousCommit=true, imports each
batch into PostgreSQL before returning commit_level=database.

Files:

  HistoryReceiver.exe
  receiver.example.ini
  database\\001_create_tables.sql

Deployment:

1. Copy receiver.example.ini to receiver.ini.
2. Set a strong ApiKey and the PostgreSQL password. Keep receiver.ini local.
3. Apply database\\001_create_tables.sql to the target database.
4. Start HistoryReceiver.exe --config receiver.ini.
5. Verify GET http://192.168.1.10:8080/healthz reports database_ok=true.

The DCS v3.4.1 pipeline can have two HTTP requests in flight. The Receiver
can receive, hash, stage and parse those requests concurrently. PostgreSQL
imports remain serialized by commitMu in the first pipeline version. Archive
movement is outside commitMu. The example keeps ImportTimeoutSeconds=45 and
WriteTimeoutSeconds=120; the DCS example waits up to 135 seconds for ACK.

In synchronous mode, PostgreSQL COMMIT is the ACK boundary. If archive moving
fails after COMMIT, the Receiver still returns database ACK and retains the
payload under archive_pending for maintenance. An already committed retry
verifies the body hash and does not create another archive directory.

The receiver stores runtime inbox/archive/archive_pending/staging/rejected/logs
beside the executable. These directories and receiver.ini are not release
artifacts. `commit_level=inbox` is available only for Receiver-side asynchronous
operation; the DCS production configuration requires `commit_level=database`.

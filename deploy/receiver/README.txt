HistoryReceiver deployment package
==================================

This package runs on the database computer. It accepts authenticated CSV
batches from the DCS collector and, with SynchronousCommit=true, imports each
batch into PostgreSQL before returning commit_level=database.

Files:

  HistoryReceiver.exe
  receiver.example.ini
  database\001_create_tables.sql

Deployment:

1. Copy receiver.example.ini to receiver.ini.
2. Set a strong ApiKey and the PostgreSQL password. Keep receiver.ini local.
3. Apply database\001_create_tables.sql to the target database.
4. Start HistoryReceiver.exe --config receiver.ini.
5. Verify GET http://192.168.1.10:8080/healthz reports database_ok=true.

For synchronous database ACK, keep ImportTimeoutSeconds below the 90-second
WriteTimeoutSeconds setting; the DCS example waits up to 105 seconds for ACK.

The receiver hashes, stages, validates, and converts a normal HTTP CSV body in
one pass. Synchronous PostgreSQL import reuses those parsed rows; restart
recovery uses the durable staged CSV.

The example keeps StagingDurability=full, flushing staged CSV and metadata to
stable storage. A controlled deployment may use buffered for a performance
test, accepting that an uncommitted staging file can be lost after power loss;
the synchronous PostgreSQL COMMIT/ACK boundary is unchanged.

In synchronous mode, PostgreSQL COMMIT is the ACK boundary. If archive moving
fails after COMMIT, the receiver returns database ACK and retains the payload
under archive_pending for maintenance. An already committed retry verifies
only the body hash and does not create another archive directory. Receiver
maintenance retries valid archive_pending entries hourly and keeps failed
moves for the next pass.
At most 100 archive_pending batches are retried in each hourly maintenance
pass.

The receiver stores runtime inbox/archive/archive_pending/staging/rejected/logs beside the
executable. These directories and receiver.ini are not release artifacts.

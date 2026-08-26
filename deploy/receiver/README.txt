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

The receiver stores runtime inbox/archive/staging/rejected/logs beside the
executable. These directories and receiver.ini are not release artifacts.

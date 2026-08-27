DeltaV History Collector deployment package
============================================

This package runs on the Win7 32-bit DCS workstation. It requires the matching
DeltaV Historian assemblies already installed on that workstation, the .NET
Framework 2.0 or 3.5 runtime/compiler, and an x86 process.

Files in the normal no-admin package:

  DcsData.Historian.dll
  HistoryReader.exe
  HistorySync.exe
  config.example.ini
  tags.example.txt
  start-historysync.vbs
  stop-historysync.vbs
  README.txt

Deployment:

1. Copy config.example.ini to config.ini and set the Receiver URL/API key.
2. Copy tags.example.txt to tags.txt and edit the tag list.
3. Keep the package in a local directory where the DCS user can read and write
   config.ini, state.ini, logs, and spool.
4. Run start-historysync.vbs to launch the collector without a visible CMD
   window. Run stop-historysync.vbs to request a graceful stop.

No administrator permission is required. This package does not create a
Windows Service, Scheduled Task, or startup entry. It does not restart,
shut down, or force-terminate the DCS workstation.

For diagnostics, run HistorySync.exe --console from a CMD window. The
administrator-only service scripts remain in the source repository but are
not part of this normal release package.

The active database computer for this deployment is 192.168.1.10:8080.
The collector only sends data to the Receiver. It never restarts, shuts down,
or force-terminates the DCS workstation.

Commands:

  HistoryReader.exe export --tags tags.txt --last 1h
  HistoryReader.exe validate --tags tags.txt
  HistorySync.exe sync
  HistorySync.exe send
  HistorySync.exe status
  HistorySync.exe --stop
  HistorySync.exe --console

Runtime config.ini, tags.txt, state.ini, logs, and spool are local files and
must not be committed to Git.

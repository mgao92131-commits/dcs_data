DeltaV History Collector deployment package
============================================

This package runs on the Win7 32-bit DCS workstation. It requires the matching
DeltaV Historian assemblies already installed on that workstation, the .NET
Framework 2.0 or 3.5 runtime/compiler, and an x86 process.

Files:

  DcsData.Historian.dll
  HistoryReader.exe
  HistorySync.exe
  config.example.ini
  tags.example.txt
  install-service.bat
  uninstall-service.bat
  test-dcs-compatibility.bat

Deployment:

1. Copy config.example.ini to config.ini and set the Receiver URL/API key.
2. Copy tags.example.txt to tags.txt and edit the tag list.
3. Run test-dcs-compatibility.bat, then HistorySync.exe status and validate
   with --console before installing.
4. Run install-service.bat as Administrator only after the service identity
   has been proven able to read the DeltaV APP Historian.

The active database computer for this deployment is 192.168.1.10:8080.
The collector only sends data to the Receiver. It never restarts, shuts down,
or force-terminates the DCS workstation.

Commands:

  HistoryReader.exe export --tags tags.txt --last 1h
  HistoryReader.exe validate --tags tags.txt
  HistorySync.exe sync
  HistorySync.exe send
  HistorySync.exe status
  HistorySync.exe --console

Runtime config.ini, tags.txt, state.ini, logs, and spool are local files and
must not be committed to Git.

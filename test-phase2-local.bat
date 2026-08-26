@echo off
setlocal
cd /d "%~dp0"

echo Testing HistoryReceiver...
pushd HistoryReceiver
go test ./...
if errorlevel 1 (
    popd
    exit /b 1
)
popd

set CSC=%WINDIR%\Microsoft.NET\Framework\v4.0.30319\csc.exe
if not exist "%CSC%" (
    echo ERROR: Local .NET compiler was not found.
    exit /b 1
)

echo Compiling HistorySync Phase 2...
pushd DeltaVHistoryCLI_v1.1
"%CSC%" /nologo /target:exe /platform:x86 /main:DeltaVHistoryCLI.SyncProgram /out:HistorySync.phase2test.exe HistoryReader.cs HistorySync.cs BatchSender.cs SpoolMaintenance.cs
if errorlevel 1 (
    popd
    exit /b 1
)
del /q HistorySync.phase2test.exe 2>nul
popd

echo PHASE 2 LOCAL TESTS PASSED

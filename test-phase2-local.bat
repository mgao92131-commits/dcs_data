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

set CSC=
if exist "%WINDIR%\Microsoft.NET\Framework\v2.0.50727\csc.exe" set CSC=%WINDIR%\Microsoft.NET\Framework\v2.0.50727\csc.exe
if not defined CSC if exist "%WINDIR%\Microsoft.NET\Framework\v3.5\csc.exe" set CSC=%WINDIR%\Microsoft.NET\Framework\v3.5\csc.exe
if not defined CSC if exist "%WINDIR%\Microsoft.NET\Framework64\v2.0.50727\csc.exe" set CSC=%WINDIR%\Microsoft.NET\Framework64\v2.0.50727\csc.exe
if not defined CSC if exist "%WINDIR%\Microsoft.NET\Framework64\v3.5\csc.exe" set CSC=%WINDIR%\Microsoft.NET\Framework64\v3.5\csc.exe
if not defined CSC if exist "%WINDIR%\Microsoft.NET\Framework\v4.0.30319\csc.exe" (
    set CSC=%WINDIR%\Microsoft.NET\Framework\v4.0.30319\csc.exe
    echo WARNING: .NET 2.0/3.5 compiler unavailable; this is a local regression test only.
)
if not defined CSC (
    echo ERROR: No supported local C# compiler was found.
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

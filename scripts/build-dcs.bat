@echo off
setlocal
cd /d "%~dp0.."
set "ROOT=%CD%"
set "OUT=%ROOT%\artifacts\dcs_build"

set "CSC=%WINDIR%\Microsoft.NET\Framework\v3.5\csc.exe"
if not exist "%CSC%" (
    echo ERROR: .NET Framework 3.5 csc.exe was not found.
    exit /b 1
)

if not exist "%OUT%" mkdir "%OUT%"
del /q "%OUT%\DcsData.Historian.dll" "%OUT%\HistorySync.exe" 2>nul

echo Compiler: "%CSC%"
"%CSC%" /nologo /target:library /platform:x86 /optimize+ /out:"%OUT%\DcsData.Historian.dll" "%ROOT%\src\DcsData.Historian\HistorianCore.cs"
if errorlevel 1 exit /b 1

"%CSC%" /nologo /target:exe /platform:x86 /optimize+ /reference:"%OUT%\DcsData.Historian.dll" /main:DeltaVHistoryCLI.SyncProgram /out:"%OUT%\HistorySync.exe" "%ROOT%\src\HistorySync\HistoryBatch.cs" "%ROOT%\src\HistorySync\SyncState.cs" "%ROOT%\src\HistorySync\HistorySync.cs" "%ROOT%\src\HistorySync\BatchSender.cs"
if errorlevel 1 exit /b 1

"%CSC%" /nologo /target:exe /platform:x86 /optimize+ /reference:"%OUT%\DcsData.Historian.dll" /main:DeltaVHistoryCLI.ProcessedSyncSelfTest /out:"%OUT%\ProcessedSyncSelfTest.build.exe" "%ROOT%\src\HistorySync\HistoryBatch.cs" "%ROOT%\src\HistorySync\SyncState.cs" "%ROOT%\src\HistorySync\HistorySync.cs" "%ROOT%\src\HistorySync\BatchSender.cs" "%ROOT%\tests\DcsCollector\ProcessedSyncSelfTest.cs"
if errorlevel 1 exit /b 1
"%OUT%\ProcessedSyncSelfTest.build.exe"
if errorlevel 1 exit /b 1
del /q "%OUT%\ProcessedSyncSelfTest.build.exe" 2>nul

echo DCS BUILD OK: %OUT%
exit /b 0

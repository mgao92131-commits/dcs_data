@echo off
setlocal
cd /d "%~dp0.."
set "ROOT=%CD%"
set "OUT=%ROOT%\artifacts\test\dcs-compatibility"

set "CSC="
if exist "%WINDIR%\Microsoft.NET\Framework\v2.0.50727\csc.exe" set "CSC=%WINDIR%\Microsoft.NET\Framework\v2.0.50727\csc.exe"
if not defined CSC if exist "%WINDIR%\Microsoft.NET\Framework\v3.5\csc.exe" set "CSC=%WINDIR%\Microsoft.NET\Framework\v3.5\csc.exe"
if not defined CSC (
    echo ERROR: This gate requires the .NET Framework 2.0 or 3.5 compiler.
    exit /b 1
)

if not exist "%OUT%" mkdir "%OUT%"
del /q "%OUT%\Phase1SelfTest.exe" "%OUT%\ArchitectureProbe.exe" "%OUT%\HistoryReader.exe" "%OUT%\HistorySync.exe" 2>nul

"%CSC%" /nologo /target:exe /platform:x86 /out:"%OUT%\ArchitectureProbe.exe" "%ROOT%\tests\DcsCollector\ArchitectureProbe.cs"
if errorlevel 1 goto failed
"%CSC%" /nologo /target:exe /platform:x86 /reference:System.ServiceProcess.dll /main:DeltaVHistoryCLI.Phase1SelfTest /out:"%OUT%\Phase1SelfTest.exe" "%ROOT%\src\DcsData.Historian\HistorianCore.cs" "%ROOT%\src\HistorySync\HistoryBatch.cs" "%ROOT%\src\HistorySync\SyncState.cs" "%ROOT%\src\HistoryReader\HistoryReader.cs" "%ROOT%\src\HistorySync\HistorySync.cs" "%ROOT%\src\HistorySync\HistorySyncService.cs" "%ROOT%\src\HistorySync\BatchSender.cs" "%ROOT%\src\HistorySync\SpoolMaintenance.cs" "%ROOT%\tests\DcsCollector\Phase1SelfTest.cs"
if errorlevel 1 goto failed
"%OUT%\Phase1SelfTest.exe"
if errorlevel 1 goto failed

"%CSC%" /nologo /target:exe /platform:x86 /main:DeltaVHistoryCLI.Program /out:"%OUT%\HistoryReader.exe" "%ROOT%\src\DcsData.Historian\HistorianCore.cs" "%ROOT%\src\HistoryReader\HistoryReader.cs"
if errorlevel 1 goto failed
"%OUT%\HistoryReader.exe" --version
if errorlevel 1 goto failed
"%OUT%\ArchitectureProbe.exe" "%OUT%\HistoryReader.exe"
if errorlevel 1 goto failed

"%CSC%" /nologo /target:exe /platform:x86 /reference:System.ServiceProcess.dll /main:DeltaVHistoryCLI.SyncProgram /out:"%OUT%\HistorySync.exe" "%ROOT%\src\DcsData.Historian\HistorianCore.cs" "%ROOT%\src\HistorySync\HistoryBatch.cs" "%ROOT%\src\HistorySync\SyncState.cs" "%ROOT%\src\HistoryReader\HistoryReader.cs" "%ROOT%\src\HistorySync\HistorySync.cs" "%ROOT%\src\HistorySync\HistorySyncService.cs" "%ROOT%\src\HistorySync\BatchSender.cs" "%ROOT%\src\HistorySync\SpoolMaintenance.cs"
if errorlevel 1 goto failed
"%OUT%\HistorySync.exe" --version
if errorlevel 1 goto failed
"%OUT%\ArchitectureProbe.exe" "%OUT%\HistorySync.exe"
if errorlevel 1 goto failed

echo DCS .NET 2.0/3.5 X86 COMPATIBILITY TEST PASSED
exit /b 0

:failed
echo DCS .NET 2.0/3.5 X86 COMPATIBILITY TEST FAILED
exit /b 1

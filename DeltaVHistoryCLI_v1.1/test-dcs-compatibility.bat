@echo off
setlocal
cd /d "%~dp0"

set CSC=
if exist "%WINDIR%\Microsoft.NET\Framework\v2.0.50727\csc.exe" set CSC=%WINDIR%\Microsoft.NET\Framework\v2.0.50727\csc.exe
if not defined CSC if exist "%WINDIR%\Microsoft.NET\Framework\v3.5\csc.exe" set CSC=%WINDIR%\Microsoft.NET\Framework\v3.5\csc.exe

if not defined CSC (
    echo ERROR: This gate requires the .NET Framework 2.0 or 3.5 compiler.
    exit /b 1
)

"%CSC%" /nologo /target:exe /platform:x86 /out:ArchitectureProbe.compat.exe ArchitectureProbe.cs
if errorlevel 1 goto failed

"%CSC%" /nologo /target:exe /platform:x86 /reference:System.ServiceProcess.dll /main:DeltaVHistoryCLI.Phase1SelfTest /out:Phase1SelfTest.compat.exe HistorianCore.cs HistoryBatch.cs SyncState.cs HistoryReader.cs HistorySync.cs HistorySyncService.cs BatchSender.cs SpoolMaintenance.cs Phase1SelfTest.cs
if errorlevel 1 goto failed

Phase1SelfTest.compat.exe
if errorlevel 1 goto failed

"%CSC%" /nologo /target:exe /platform:x86 /main:DeltaVHistoryCLI.Program /out:HistoryReader.compat.exe HistorianCore.cs HistoryReader.cs
if errorlevel 1 goto failed
HistoryReader.compat.exe --version
if errorlevel 1 goto failed
ArchitectureProbe.compat.exe HistoryReader.compat.exe
if errorlevel 1 goto failed

"%CSC%" /nologo /target:exe /platform:x86 /reference:System.ServiceProcess.dll /main:DeltaVHistoryCLI.SyncProgram /out:HistorySync.compat.exe HistorianCore.cs HistoryBatch.cs SyncState.cs HistoryReader.cs HistorySync.cs HistorySyncService.cs BatchSender.cs SpoolMaintenance.cs
if errorlevel 1 goto failed
HistorySync.compat.exe --version
if errorlevel 1 goto failed
ArchitectureProbe.compat.exe HistorySync.compat.exe
if errorlevel 1 goto failed

del /q ArchitectureProbe.compat.exe Phase1SelfTest.compat.exe HistoryReader.compat.exe HistorySync.compat.exe 2>nul

echo DCS .NET 2.0/3.5 X86 COMPATIBILITY TEST PASSED
exit /b 0

:failed
del /q ArchitectureProbe.compat.exe Phase1SelfTest.compat.exe HistoryReader.compat.exe HistorySync.compat.exe 2>nul
echo DCS .NET 2.0/3.5 X86 COMPATIBILITY TEST FAILED
exit /b 1

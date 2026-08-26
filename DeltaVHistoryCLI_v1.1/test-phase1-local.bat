@echo off
setlocal
cd /d "%~dp0"

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

"%CSC%" /nologo /target:exe /platform:x86 /reference:System.ServiceProcess.dll /main:DeltaVHistoryCLI.Phase1SelfTest /out:Phase1SelfTest.exe HistorianCore.cs HistoryBatch.cs SyncState.cs HistoryReader.cs HistorySync.cs HistorySyncService.cs BatchSender.cs SpoolMaintenance.cs Phase1SelfTest.cs
if errorlevel 1 exit /b 1

Phase1SelfTest.exe
set RESULT=%ERRORLEVEL%

del /q Phase1SelfTest.exe 2>nul
exit /b %RESULT%

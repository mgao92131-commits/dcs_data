@echo off
setlocal
cd /d "%~dp0"

set CSC=%WINDIR%\Microsoft.NET\Framework\v4.0.30319\csc.exe

if not exist "%CSC%" (
    echo ERROR: Local .NET compiler was not found.
    exit /b 1
)

"%CSC%" /nologo /target:exe /platform:x86 /main:DeltaVHistoryCLI.Phase1SelfTest /out:Phase1SelfTest.exe HistoryReader.cs HistorySync.cs BatchSender.cs SpoolMaintenance.cs Phase1SelfTest.cs
if errorlevel 1 exit /b 1

Phase1SelfTest.exe
set RESULT=%ERRORLEVEL%

del /q Phase1SelfTest.exe 2>nul
exit /b %RESULT%

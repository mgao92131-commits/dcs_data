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

"%CSC%" /nologo /target:exe /platform:x86 /main:DeltaVHistoryCLI.Phase1SelfTest /out:Phase1SelfTest.compat.exe HistorianCore.cs HistoryBatch.cs HistoryReader.cs HistorySync.cs BatchSender.cs SpoolMaintenance.cs Phase1SelfTest.cs
if errorlevel 1 exit /b 1

Phase1SelfTest.compat.exe
set RESULT=%ERRORLEVEL%
del /q Phase1SelfTest.compat.exe 2>nul
if not "%RESULT%"=="0" exit /b %RESULT%

echo DCS .NET 2.0/3.5 X86 COMPATIBILITY TEST PASSED
exit /b 0

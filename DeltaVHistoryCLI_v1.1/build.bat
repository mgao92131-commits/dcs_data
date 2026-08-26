@echo off
setlocal
cd /d "%~dp0"

set CSC=

if exist "%WINDIR%\Microsoft.NET\Framework\v2.0.50727\csc.exe" set CSC=%WINDIR%\Microsoft.NET\Framework\v2.0.50727\csc.exe
if not defined CSC if exist "%WINDIR%\Microsoft.NET\Framework\v3.5\csc.exe" set CSC=%WINDIR%\Microsoft.NET\Framework\v3.5\csc.exe

if not defined CSC (
    echo.
    echo ERROR: .NET Framework 2.0/3.5 csc.exe was not found.
    echo.
    pause
    exit /b 1
)

echo Compiler: "%CSC%"

"%CSC%" /nologo /target:exe /platform:x86 /optimize+ /main:DeltaVHistoryCLI.Program /out:HistoryReader.exe HistorianCore.cs HistoryReader.cs

if errorlevel 1 (
    echo.
    echo BUILD FAILED.
    pause
    exit /b 1
)

"%CSC%" /nologo /target:exe /platform:x86 /optimize+ /main:DeltaVHistoryCLI.SyncProgram /out:HistorySync.exe HistorianCore.cs HistoryBatch.cs HistoryReader.cs HistorySync.cs BatchSender.cs SpoolMaintenance.cs

if errorlevel 1 (
    echo.
    echo HISTORYSYNC BUILD FAILED.
    pause
    exit /b 1
)

echo.
echo BUILD OK:
echo %CD%\HistoryReader.exe
echo %CD%\HistorySync.exe
echo.
pause

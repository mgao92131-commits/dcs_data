@echo off
setlocal
cd /d "%~dp0.."
set "ROOT=%CD%"
set "OUT=%ROOT%\artifacts\dcs"

set "CSC="
if exist "%WINDIR%\Microsoft.NET\Framework\v2.0.50727\csc.exe" set "CSC=%WINDIR%\Microsoft.NET\Framework\v2.0.50727\csc.exe"
if not defined CSC if exist "%WINDIR%\Microsoft.NET\Framework\v3.5\csc.exe" set "CSC=%WINDIR%\Microsoft.NET\Framework\v3.5\csc.exe"
if not defined CSC (
    echo ERROR: .NET Framework 2.0/3.5 csc.exe was not found.
    exit /b 1
)

if not exist "%OUT%" mkdir "%OUT%"
del /q "%OUT%\DcsData.Historian.dll" "%OUT%\HistoryReader.exe" "%OUT%\HistorySync.exe" "%OUT%\ArchitectureProbe.build.exe" 2>nul

echo Compiler: "%CSC%"
"%CSC%" /nologo /target:library /platform:x86 /optimize+ /out:"%OUT%\DcsData.Historian.dll" "%ROOT%\src\DcsData.Historian\HistorianCore.cs"
if errorlevel 1 exit /b 1

"%CSC%" /nologo /target:exe /platform:x86 /optimize+ /reference:"%OUT%\DcsData.Historian.dll" /out:"%OUT%\HistoryReader.exe" "%ROOT%\src\HistoryReader\HistoryReader.cs"
if errorlevel 1 exit /b 1

"%CSC%" /nologo /target:exe /platform:x86 /optimize+ /reference:System.ServiceProcess.dll /reference:"%OUT%\DcsData.Historian.dll" /main:DeltaVHistoryCLI.SyncProgram /out:"%OUT%\HistorySync.exe" "%ROOT%\src\HistorySync\HistoryBatch.cs" "%ROOT%\src\HistorySync\SyncState.cs" "%ROOT%\src\HistorySync\HistorySync.cs" "%ROOT%\src\HistorySync\HistorySyncService.cs" "%ROOT%\src\HistorySync\BatchSender.cs" "%ROOT%\src\HistorySync\SpoolMaintenance.cs"
if errorlevel 1 exit /b 1

"%CSC%" /nologo /target:exe /platform:x86 /out:"%OUT%\ArchitectureProbe.build.exe" "%ROOT%\tests\DcsCollector\ArchitectureProbe.cs"
if errorlevel 1 exit /b 1
"%OUT%\ArchitectureProbe.build.exe" "%OUT%\HistoryReader.exe"
if errorlevel 1 exit /b 1
"%OUT%\ArchitectureProbe.build.exe" "%OUT%\HistorySync.exe"
if errorlevel 1 exit /b 1
del /q "%OUT%\ArchitectureProbe.build.exe" 2>nul

echo DCS BUILD OK: %OUT%
exit /b 0

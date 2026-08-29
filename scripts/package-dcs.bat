@echo off
setlocal
cd /d "%~dp0.."
set "ROOT=%CD%"
set "BUILD=%ROOT%\artifacts\dcs_build"
set "OUT=%ROOT%\artifacts\dcs_data"

call "%ROOT%\scripts\build-dcs.bat"
if errorlevel 1 exit /b 1

if exist "%OUT%" rmdir /s /q "%OUT%"
mkdir "%OUT%\bin" "%OUT%\config" "%OUT%\scripts" "%OUT%\state" "%OUT%\logs"
if errorlevel 1 exit /b 1

copy /y "%BUILD%\DcsData.Historian.dll" "%OUT%\bin\DcsData.Historian.dll" >nul
copy /y "%BUILD%\HistorySync.exe" "%OUT%\bin\HistorySync.exe" >nul
copy /y "%ROOT%\deploy\dcs\config.example.ini" "%OUT%\config\config.example.ini" >nul
copy /y "%ROOT%\deploy\dcs\tags.example.txt" "%OUT%\config\tags.example.txt" >nul
copy /y "%ROOT%\deploy\dcs\run.cmd" "%OUT%\scripts\run.cmd" >nul
copy /y "%ROOT%\deploy\dcs\start-historysync.cmd" "%OUT%\scripts\start-historysync.cmd" >nul
copy /y "%ROOT%\deploy\dcs\start-historysync.vbs" "%OUT%\scripts\start-historysync.vbs" >nul
copy /y "%ROOT%\deploy\dcs\stop-historysync.cmd" "%OUT%\scripts\stop-historysync.cmd" >nul
copy /y "%ROOT%\deploy\dcs\sync.cmd" "%OUT%\scripts\sync.cmd" >nul
copy /y "%ROOT%\deploy\dcs\status.cmd" "%OUT%\scripts\status.cmd" >nul
copy /y "%ROOT%\deploy\dcs\README.txt" "%OUT%\README.txt" >nul
copy /y "%ROOT%\deploy\dcs\root-start-historysync.cmd" "%OUT%\start-historysync.cmd" >nul
copy /y "%ROOT%\deploy\dcs\root-stop-historysync.cmd" "%OUT%\stop-historysync.cmd" >nul
if errorlevel 1 exit /b 1

echo DCS PACKAGE READY: %OUT%
exit /b 0

@echo off
setlocal
cd /d "%~dp0.."
set "ROOT=%CD%"
set "OUT=%ROOT%\artifacts\dcs"

call "%ROOT%\scripts\build-dcs.bat"
if errorlevel 1 exit /b 1

copy /y "%ROOT%\deploy\dcs\config.example.ini" "%OUT%\config.example.ini" >nul
copy /y "%ROOT%\deploy\dcs\tags.example.txt" "%OUT%\tags.example.txt" >nul
copy /y "%ROOT%\deploy\dcs\start-historysync.vbs" "%OUT%\start-historysync.vbs" >nul
copy /y "%ROOT%\deploy\dcs\stop-historysync.vbs" "%OUT%\stop-historysync.vbs" >nul
copy /y "%ROOT%\deploy\dcs\README.txt" "%OUT%\README.txt" >nul
if errorlevel 1 exit /b 1

echo DCS PACKAGE READY: %OUT%
exit /b 0

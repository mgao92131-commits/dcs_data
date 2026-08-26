@echo off
setlocal
cd /d "%~dp0.."
set "ROOT=%CD%"
set "OUT=%ROOT%\artifacts\dcs"

call "%ROOT%\scripts\build-dcs.bat"
if errorlevel 1 exit /b 1

copy /y "%ROOT%\deploy\dcs\config.example.ini" "%OUT%\config.example.ini" >nul
copy /y "%ROOT%\deploy\dcs\tags.example.txt" "%OUT%\tags.example.txt" >nul
copy /y "%ROOT%\deploy\dcs\install-service.bat" "%OUT%\install-service.bat" >nul
copy /y "%ROOT%\deploy\dcs\uninstall-service.bat" "%OUT%\uninstall-service.bat" >nul
copy /y "%ROOT%\deploy\dcs\README.txt" "%OUT%\README.txt" >nul
copy /y "%ROOT%\scripts\test-dcs-compatibility.bat" "%OUT%\test-dcs-compatibility.bat" >nul
if errorlevel 1 exit /b 1

echo DCS PACKAGE READY: %OUT%
exit /b 0

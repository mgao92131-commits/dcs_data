@echo off
setlocal
cd /d "%~dp0"

call scripts\test-dcs-local.bat
if errorlevel 1 exit /b 1

call scripts\test-receiver.bat
if errorlevel 1 exit /b 1

echo LOCAL BASELINE TESTS PASSED

@echo off
setlocal
cd /d "%~dp0.."

call "%CD%\scripts\package-dcs.bat"
if errorlevel 1 exit /b 1
call "%CD%\scripts\package-receiver.bat"
if errorlevel 1 exit /b 1

echo PACKAGES READY
exit /b 0

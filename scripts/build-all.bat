@echo off
setlocal
cd /d "%~dp0.."

call "%CD%\scripts\build-dcs.bat"
if errorlevel 1 exit /b 1
call "%CD%\scripts\build-receiver.bat"
if errorlevel 1 exit /b 1

echo BUILD ALL OK
exit /b 0

@echo off
setlocal
cd /d "%~dp0.."
"bin\HistorySync.exe" status --config "..\config\config.ini"
exit /b %errorlevel%

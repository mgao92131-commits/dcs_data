@echo off
setlocal
cd /d "%~dp0.."
"bin\HistorySync.exe" send --config "..\config\config.ini"
exit /b %errorlevel%

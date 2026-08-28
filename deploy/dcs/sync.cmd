@echo off
setlocal
cd /d "%~dp0.."
"bin\HistorySync.exe" sync --config "..\config\config.ini"
exit /b %errorlevel%

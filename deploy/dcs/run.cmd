@echo off
setlocal
cd /d "%~dp0.."
"bin\HistorySync.exe" run --config "..\config\config.ini"
exit /b %errorlevel%

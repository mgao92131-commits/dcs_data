@echo off
setlocal
cd /d "%~dp0.."
"bin\HistorySync.exe" stop
exit /b %errorlevel%

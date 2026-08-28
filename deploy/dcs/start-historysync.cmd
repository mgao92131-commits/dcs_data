@echo off
setlocal
set "ROOT=%~dp0.."
if not exist "%ROOT%\config\config.ini" (
    echo ERROR: config\config.ini was not found.
    exit /b 1
)
if not exist "%ROOT%\config\tags.txt" (
    echo ERROR: config\tags.txt was not found.
    exit /b 1
)
wscript.exe "%~dp0start-historysync.vbs"
if errorlevel 1 exit /b 1
echo HistorySync background start requested.
exit /b 0

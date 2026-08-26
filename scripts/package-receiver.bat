@echo off
setlocal
cd /d "%~dp0.."
set "ROOT=%CD%"
set "OUT=%ROOT%\artifacts\receiver"

call "%ROOT%\scripts\build-receiver.bat"
if errorlevel 1 exit /b 1

if not exist "%OUT%\database" mkdir "%OUT%\database"
copy /y "%ROOT%\deploy\receiver\receiver.example.ini" "%OUT%\receiver.example.ini" >nul
copy /y "%ROOT%\database\migrations\001_create_tables.sql" "%OUT%\database\001_create_tables.sql" >nul
copy /y "%ROOT%\src\HistoryReceiver\README.md" "%OUT%\README.md" >nul
if errorlevel 1 exit /b 1

echo RECEIVER PACKAGE READY: %OUT%
exit /b 0

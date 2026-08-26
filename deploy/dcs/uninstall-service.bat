@echo off
setlocal

net session >nul 2>&1
if errorlevel 1 (
    echo ERROR: Run this script as Administrator.
    exit /b 1
)

sc.exe stop DeltaVHistorySync >nul 2>&1
sc.exe delete DeltaVHistorySync
if errorlevel 1 exit /b 1

echo DeltaVHistorySync service removed. Configuration, state, and spool data were preserved.

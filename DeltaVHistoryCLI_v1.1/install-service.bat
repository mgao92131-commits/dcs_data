@echo off
setlocal
cd /d "%~dp0"

net session >nul 2>&1
if errorlevel 1 (
    echo ERROR: Run this script as Administrator.
    exit /b 1
)

if not exist "%CD%\HistorySync.exe" (
    echo ERROR: HistorySync.exe was not found. Run build.bat first.
    exit /b 1
)

sc.exe query DeltaVHistorySync >nul 2>&1
if not errorlevel 1 (
    echo ERROR: DeltaVHistorySync service already exists.
    exit /b 1
)

rem Keep the account explicit. Validate Historian access for this account before promotion.
rem Change it with sc.exe config DeltaVHistorySync obj= DOMAIN\User password= ...
rem if the site requires a dedicated DeltaV service account.
sc.exe create DeltaVHistorySync binPath= "\"%CD%\HistorySync.exe\" --service" start= auto obj= LocalSystem DisplayName= "DeltaV History Sync"
if errorlevel 1 exit /b 1

sc.exe description DeltaVHistorySync "Reads DeltaV Historian data and synchronizes committed batches to PostgreSQL."
sc.exe failure DeltaVHistorySync reset= 86400 actions= restart/60000/restart/60000/restart/300000
sc.exe start DeltaVHistorySync
if errorlevel 1 exit /b 1

echo DeltaVHistorySync service installed and started.

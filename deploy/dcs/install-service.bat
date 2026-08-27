@echo off
setlocal
cd /d "%~dp0"

net session >nul 2>&1
if errorlevel 1 (
    echo ERROR: Run this script as Administrator.
    exit /b 1
)

if not exist "%CD%\HistorySync.exe" (
    echo ERROR: HistorySync.exe was not found. Copy the DCS release package first.
    exit /b 1
)

sc.exe query DeltaVHistorySync >nul 2>&1
if not errorlevel 1 (
    echo ERROR: DeltaVHistorySync service already exists.
    exit /b 1
)

rem Optional administrator-only deployment. The normal no-admin deployment
rem uses start-historysync.vbs and does not install a Windows Service.
rem Keep the account explicit. Validate Historian access for this account before promotion.
rem Change it with sc.exe config DeltaVHistorySync obj= DOMAIN\User password= ...
rem if the site requires a dedicated DeltaV service account.
sc.exe create DeltaVHistorySync binPath= "\"%CD%\HistorySync.exe\" --service" start= demand obj= LocalSystem DisplayName= "DeltaV History Sync"
if errorlevel 1 exit /b 1

sc.exe description DeltaVHistorySync "Reads DeltaV Historian data and synchronizes committed batches to PostgreSQL."
sc.exe failure DeltaVHistorySync reset= 86400 actions= restart/60000/restart/60000/restart/300000
echo DeltaVHistorySync service installed with Manual startup and was not started.
echo Use Services.msc or sc.exe start only on an administrator-managed host.

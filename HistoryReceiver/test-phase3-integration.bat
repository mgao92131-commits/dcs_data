@echo off
setlocal
cd /d "%~dp0"
set GOTOOLCHAIN=local
set GOCACHE=%~dp0..\.gocache

if "%DCS_HISTORY_TEST_DATABASE_URL%"=="" (
    echo ERROR: Set DCS_HISTORY_TEST_DATABASE_URL to a disposable PostgreSQL database URL first.
    echo Example: set DCS_HISTORY_TEST_DATABASE_URL=postgres://writer:password@127.0.0.1:5433/deltav_history
    exit /b 2
)

go test -tags integration -run TestSynchronousCommitEndToEnd -count=1 .
if errorlevel 1 exit /b 1

echo PHASE 3 POSTGRES INTEGRATION TEST PASSED

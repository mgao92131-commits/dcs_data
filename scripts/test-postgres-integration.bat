@echo off
setlocal
cd /d "%~dp0.."
set "ROOT=%CD%"
set GOTOOLCHAIN=local
set "GOCACHE=%ROOT%\.gocache"

if "%DCS_HISTORY_TEST_DATABASE_URL%"=="" (
    echo ERROR: Set DCS_HISTORY_TEST_DATABASE_URL to a disposable PostgreSQL database URL first.
    exit /b 2
)

pushd "%ROOT%\src\HistoryReceiver"
go test -tags integration -run TestSynchronousCommitEndToEnd -count=1 .
set "RESULT=%ERRORLEVEL%"
popd
if not "%RESULT%"=="0" exit /b %RESULT%
echo POSTGRES COMMIT/ACK INTEGRATION TEST PASSED
exit /b 0

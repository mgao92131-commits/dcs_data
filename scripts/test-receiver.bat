@echo off
setlocal
cd /d "%~dp0.."
set "ROOT=%CD%"
set "OUT=%ROOT%\artifacts\test\receiver"
set GOTOOLCHAIN=local
set "GOCACHE=%ROOT%\.gocache"

pushd "%ROOT%\src\HistoryReceiver"
go test ./...
if errorlevel 1 (
    popd
    exit /b 1
)
go vet ./...
if errorlevel 1 (
    popd
    exit /b 1
)
if not exist "%OUT%" mkdir "%OUT%"
go build -trimpath -o "%OUT%\HistoryReceiver.test.exe" .
set "RESULT=%ERRORLEVEL%"
popd
if not "%RESULT%"=="0" exit /b %RESULT%
del /q "%OUT%\HistoryReceiver.test.exe" 2>nul
echo RECEIVER TESTS PASSED
exit /b 0

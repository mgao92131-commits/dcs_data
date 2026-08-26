@echo off
setlocal
cd /d "%~dp0.."
set "ROOT=%CD%"
set "OUT=%ROOT%\artifacts\receiver"
set GOTOOLCHAIN=local
set "GOCACHE=%ROOT%\.gocache"

if not exist "%OUT%" mkdir "%OUT%"
pushd "%ROOT%\src\HistoryReceiver"
go build -trimpath -ldflags="-s -w" -o "%OUT%\HistoryReceiver.exe" .
set "RESULT=%ERRORLEVEL%"
popd
if not "%RESULT%"=="0" exit /b %RESULT%

echo RECEIVER BUILD OK: %OUT%\HistoryReceiver.exe
exit /b 0

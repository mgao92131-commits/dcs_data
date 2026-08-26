@echo off
setlocal
cd /d "%~dp0\HistoryReceiver"

set GOTOOLCHAIN=local
set GOCACHE=%~dp0.gocache

go test ./...
if errorlevel 1 exit /b 1

go vet ./...
if errorlevel 1 exit /b 1

go build -trimpath -o HistoryReceiver.phase3test.exe .
if errorlevel 1 exit /b 1

del /q HistoryReceiver.phase3test.exe 2>nul
echo PHASE 3 LOCAL TESTS PASSED

@echo off
setlocal
cd /d "%~dp0"
set GOTOOLCHAIN=local
set GOCACHE=%~dp0.gocache

go test ./...
if errorlevel 1 exit /b 1

go build -trimpath -ldflags="-s -w" -o HistoryReceiver.exe .
if errorlevel 1 exit /b 1

echo BUILD OK: %CD%\HistoryReceiver.exe

@echo off
setlocal
cd /d "%~dp0"

call DeltaVHistoryCLI_v1.1\test-phase1-local.bat
if errorlevel 1 exit /b 1

call test-phase2-local.bat
if errorlevel 1 exit /b 1

call test-phase3-local.bat
if errorlevel 1 exit /b 1

echo V1 LEGACY BASELINE TESTS PASSED

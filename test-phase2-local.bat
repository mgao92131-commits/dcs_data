@echo off
setlocal
call "%~dp0scripts\test-dcs-local.bat"
exit /b %ERRORLEVEL%

@echo off
setlocal
call "%~dp0scripts\test-receiver.bat"
exit /b %ERRORLEVEL%

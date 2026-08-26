@echo off
cd /d "%~dp0"

rem Last hour, all tags in tags.txt
HistoryReader.exe export --tags tags.txt --last 1h

rem Explicit time range:
rem HistoryReader.exe export --tags tags.txt --start "2026-08-25 08:00:00" --end "2026-08-25 14:00:00"

rem Custom output directory:
rem HistoryReader.exe export --tags tags.txt --last 1d --out-dir D:\HistoryData

pause

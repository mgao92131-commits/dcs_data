@echo off
setlocal
cd /d "%~dp0"

echo ===============================================================================
echo            DeltaV Event Chronicle (Alarms and Events) Quick Probe
echo            Target: Windows 7 32-bit / 64-bit (.NET Framework 2.0/3.5/4.x)
echo ===============================================================================
echo.

:: 1. If EventProbe.exe already exists, run directly
if exist "%~dp0EventProbe.exe" (
    "%~dp0EventProbe.exe" %*
    goto END
)

:: 2. Auto-compile EventProbe.cs if exe not present
set CSC=
if exist "%WINDIR%\Microsoft.NET\Framework\v2.0.50727\csc.exe" set CSC=%WINDIR%\Microsoft.NET\Framework\v2.0.50727\csc.exe
if not defined CSC if exist "%WINDIR%\Microsoft.NET\Framework\v3.5\csc.exe" set CSC=%WINDIR%\Microsoft.NET\Framework\v3.5\csc.exe
if not defined CSC if exist "%WINDIR%\Microsoft.NET\Framework\v4.0.30319\csc.exe" set CSC=%WINDIR%\Microsoft.NET\Framework\v4.0.30319\csc.exe

if not defined CSC (
    echo [ERROR] No .NET C# compiler found under %WINDIR%\Microsoft.NET\Framework.
    echo Please make sure .NET Framework 2.0/3.5/4.x is installed on this machine.
    pause
    exit /b 1
)

echo Compiling EventProbe.exe (Target: x86 / .NET 2.0/3.5)...
"%CSC%" /nologo /target:exe /platform:x86 /optimize+ /out:"%~dp0EventProbe.exe" "%~dp0EventProbe.cs"

if errorlevel 1 (
    echo [ERROR] Compilation failed.
    pause
    exit /b 1
)

echo [OK] Build successful: "%~dp0EventProbe.exe"
echo.
"%~dp0EventProbe.exe" %*

:END
if errorlevel 1 (
    echo.
    echo Probe finished with code %ERRORLEVEL%.
)
echo.
pause

@echo off
title Seven Labs Vision - Tally Analytics - Setup
color 0A

echo.
echo  ====================================================
echo   Seven Labs Vision - Tally Analytics Platform
echo   One-Time Setup
echo  ====================================================
echo.

:: Check if Python is installed
python --version >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo  [ERROR] Python is not installed!
    echo.
    echo  Please install Python from:
    echo  https://www.python.org/downloads/
    echo.
    echo  IMPORTANT: Check "Add Python to PATH" during installation!
    echo.
    pause
    exit /b 1
)

echo  [OK] Python found:
python --version
echo.

:: Install dependencies
echo  Installing required packages...
echo.
pip install -r requirements.txt
echo.

if %ERRORLEVEL% NEQ 0 (
    echo  [ERROR] Failed to install packages. Try running as Administrator.
    pause
    exit /b 1
)

echo.
echo  ====================================================
echo   Setup Complete!
echo  ====================================================
echo.
echo  To start the app, double-click: run.bat
echo.
echo  Make sure TallyPrime is running with a company loaded
echo  and port 9000 is enabled (F1 > Settings > Connectivity)
echo.
pause

@echo off
title Seven Labs Vision - Building Installer
color 0E

echo.
echo  ====================================================
echo   Building Seven Labs Vision Installer
echo   This creates a single .exe installer
echo  ====================================================
echo.

set BUILD_DIR=%~dp0
set ROOT_DIR=%BUILD_DIR%..
set DIST_DIR=%BUILD_DIR%dist
set PORTABLE_DIR=%DIST_DIR%\TallyAnalytics

:: Clean previous build
if exist "%DIST_DIR%" rmdir /s /q "%DIST_DIR%"
mkdir "%DIST_DIR%"
mkdir "%PORTABLE_DIR%"

echo  [1/5] Downloading Python Embeddable...
echo.
powershell -Command "Invoke-WebRequest -Uri 'https://www.python.org/ftp/python/3.11.9/python-3.11.9-embed-amd64.zip' -OutFile '%BUILD_DIR%python-embed.zip'"
if %ERRORLEVEL% NEQ 0 (
    echo  [ERROR] Failed to download Python. Check internet connection.
    pause
    exit /b 1
)

echo  [2/5] Extracting Python...
echo.
powershell -Command "Expand-Archive -Path '%BUILD_DIR%python-embed.zip' -DestinationPath '%PORTABLE_DIR%\python' -Force"

:: Enable pip by modifying python311._pth
echo import site>> "%PORTABLE_DIR%\python\python311._pth"

:: Download get-pip.py
powershell -Command "Invoke-WebRequest -Uri 'https://bootstrap.pypa.io/get-pip.py' -OutFile '%PORTABLE_DIR%\python\get-pip.py'"

echo  [3/5] Installing pip and packages...
echo.
"%PORTABLE_DIR%\python\python.exe" "%PORTABLE_DIR%\python\get-pip.py" --no-warn-script-location
"%PORTABLE_DIR%\python\python.exe" -m pip install streamlit requests pandas --no-warn-script-location --quiet

echo  [4/5] Copying application files...
echo.
:: Copy app files
copy "%ROOT_DIR%\app.py" "%PORTABLE_DIR%\" >nul
copy "%ROOT_DIR%\analytics.py" "%PORTABLE_DIR%\" >nul
copy "%ROOT_DIR%\chat_engine.py" "%PORTABLE_DIR%\" >nul
copy "%ROOT_DIR%\db_loader.py" "%PORTABLE_DIR%\" >nul
copy "%ROOT_DIR%\gst_engine.py" "%PORTABLE_DIR%\" >nul
copy "%ROOT_DIR%\tally_reports.py" "%PORTABLE_DIR%\" >nul
copy "%ROOT_DIR%\tally_sync.py" "%PORTABLE_DIR%\" >nul
copy "%ROOT_DIR%\test_question_bank.py" "%PORTABLE_DIR%\" >nul

:: Copy pages
mkdir "%PORTABLE_DIR%\pages"
copy "%ROOT_DIR%\pages\*.py" "%PORTABLE_DIR%\pages\" >nul

:: Create launcher
(
echo @echo off
echo title Seven Labs Vision - Tally Analytics
echo color 0B
echo echo.
echo echo  ====================================================
echo echo   Seven Labs Vision - Tally Analytics Platform
echo echo  ====================================================
echo echo.
echo echo  Starting... The app will open in your browser.
echo echo  If it doesn't, go to: http://localhost:8501
echo echo  Press Ctrl+C to stop.
echo echo.
echo "%%~dp0python\python.exe" -m streamlit run "%%~dp0app.py" --server.port 8501 --server.headless false --browser.gatherUsageStats false
) > "%PORTABLE_DIR%\SevenLabsVision.bat"

:: Create a VBS launcher that hides the command window
(
echo Set WshShell = CreateObject^("WScript.Shell"^)
echo WshShell.Run """" ^& Replace^(WScript.ScriptFullName, WScript.ScriptName, ""^) ^& "SevenLabsVision.bat""", 0, False
) > "%PORTABLE_DIR%\SevenLabsVision.vbs"

echo  [5/5] Creating installer with Inno Setup...
echo.

:: Check if Inno Setup is installed
if exist "C:\Program Files (x86)\Inno Setup 6\ISCC.exe" (
    "C:\Program Files (x86)\Inno Setup 6\ISCC.exe" "%BUILD_DIR%installer.iss"
    echo.
    echo  ====================================================
    echo   BUILD COMPLETE!
    echo   Installer: %DIST_DIR%\SevenLabsVision_Setup.exe
    echo  ====================================================
) else (
    echo  [INFO] Inno Setup not found. Creating portable ZIP instead...
    powershell -Command "Compress-Archive -Path '%PORTABLE_DIR%\*' -DestinationPath '%DIST_DIR%\SevenLabsVision_Portable.zip' -Force"
    echo.
    echo  ====================================================
    echo   BUILD COMPLETE!
    echo   Portable ZIP: %DIST_DIR%\SevenLabsVision_Portable.zip
    echo
    echo   To install Inno Setup for a proper .exe installer:
    echo   https://jrsoftware.org/isdl.php
    echo   Then run this script again.
    echo  ====================================================
)

echo.
pause

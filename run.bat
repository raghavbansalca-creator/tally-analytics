@echo off
title Seven Labs Vision - Tally Analytics
color 0B

echo.
echo  ====================================================
echo   Seven Labs Vision - Tally Analytics Platform
echo   Starting...
echo  ====================================================
echo.
echo  The app will open in your browser automatically.
echo  If it doesn't, go to: http://localhost:8501
echo.
echo  Press Ctrl+C to stop the server.
echo.

streamlit run app.py --server.port 8501 --server.headless false --browser.gatherUsageStats false

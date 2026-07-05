@echo off
cd /d "%~dp0"

echo.
echo ============================================
echo   WALLET FINDER — Stopping Both Dashboards
echo ============================================
echo.

:: Find and kill processes on ports 8012 and 8014
for /f "tokens=5" %%a in ('netstat -ano ^| findstr ":8012" ^| findstr "LISTENING"') do (
    echo Killing PID %%a on port 8012 ^(Proving Engine^)...
    taskkill /f /pid %%a >nul 2>&1
)
for /f "tokens=5" %%a in ('netstat -ano ^| findstr ":8014" ^| findstr "LISTENING"') do (
    echo Killing PID %%a on port 8014 ^(Live Copy^)...
    taskkill /f /pid %%a >nul 2>&1
)



echo.
echo Both servers stopped.
echo.
pause

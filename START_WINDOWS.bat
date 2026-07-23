@echo off
REM ===================================================================
REM  SpendGuard - double-click launcher for Windows
REM  Installs dependencies (first run only) and opens the dashboard.
REM ===================================================================
cd /d "%~dp0"
title SpendGuard

echo.
echo  ==================================================
echo    SpendGuard - starting
echo  ==================================================
echo.

where python >nul 2>nul
if errorlevel 1 (
  echo  [!] Python was not found.
  echo      Install Python 3.10+ from https://www.python.org/downloads/
  echo      and tick "Add python.exe to PATH" during setup.
  echo.
  pause
  exit /b 1
)

echo  Installing dependencies (first run may take a minute)...
python -m pip install --quiet --disable-pip-version-check -r requirements.txt
if errorlevel 1 (
  echo.
  echo  [!] Dependency install failed. Scroll up for the reason.
  pause
  exit /b 1
)

echo  Done. Opening the dashboard in your browser...
echo.
python spend_proxy.py

echo.
echo  SpendGuard stopped.
pause
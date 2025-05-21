@echo off
ECHO Starting main tools sequentially: lp-monitor, hedge-monitoring, hedge-rebalancer...

:: Set repository root
SET REPO_ROOT=C:\Users\Z640\dev\LP-hedging-strategy

:: Step 1: lp-monitor (Node.js/TypeScript)
ECHO === Starting lp-monitor ===
cd %REPO_ROOT%\lp-monitor
IF ERRORLEVEL 1 (
    ECHO Failed to change to lp-monitor directory
    pause
    exit /b 1
)

:: Run lp-monitor
call npm start
IF ERRORLEVEL 1 (
    ECHO Failed to run lp-monitor
    pause
    exit /b 1
)

:: Step 2: Python tools
ECHO === Running Python tools ===
cd %REPO_ROOT%\python
IF ERRORLEVEL 1 (
    ECHO Failed to change to python directory
    pause
    exit /b 1
)

:: Run hedge-monitoring
ECHO === Running hedge-monitoring ===
python -m hedge_monitoring.bitget_position_fetcher
IF ERRORLEVEL 1 (
    ECHO Failed to run hedge-monitoring
    pause
    exit /b 1
)

:: Run hedge-rebalancer
ECHO === Running hedge-rebalancer ===
python -m hedge_rebalancer.hedge_rebalancer
IF ERRORLEVEL 1 (
    ECHO Failed to run hedge-rebalancer
    pause
    exit /b 1
)

ECHO All main tools executed successfully!
pause
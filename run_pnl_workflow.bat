@echo off
ECHO Starting PNL tools in separate terminals: meteora_pnl, krystal_pnl...

:: Set repository root
SET REPO_ROOT=C:\Users\Z640\dev\LP-hedging-strategy

:: Step 1: pnlMeteora (Node.js/TypeScript)
ECHO === Starting pnlMeteora in a new terminal ===
cd %REPO_ROOT%\lp-monitor
IF ERRORLEVEL 1 (
    ECHO Failed to change to lp-monitor directory
    pause
    exit /b 1
)

:: Run pnlMeteora in a new terminal
start cmd /k npm run pnlMeteora
IF ERRORLEVEL 1 (
    ECHO Failed to start pnlMeteora
    pause
    exit /b 1
)

:: Step 2: krystal_pnl (Python)
ECHO === Starting krystal_pnl in a new terminal ===
cd %REPO_ROOT%\python
IF ERRORLEVEL 1 (
    ECHO Failed to change to python directory
    pause
    exit /b 1
)

:: Run krystal_pnl in a new terminal
start cmd /k python -m krystal_pnl.run_krystal_pnl
IF ERRORLEVEL 1 (
    ECHO Failed to start krystal_pnl
    pause
    exit /b 1
)

ECHO All PNL tools started in separate terminals!
pause
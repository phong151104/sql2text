@echo off
REM ============================================================
REM  Text-to-SQL Interactive CLI
REM ============================================================

echo.
echo ========================================
echo   Text-to-SQL CLI
echo ========================================
echo.

REM Check if .env exists
if not exist ".env" (
    echo [WARNING] .env file not found!
    echo Please copy .env.example to .env and configure your API keys.
    pause
    exit /b 1
)

REM Run CLI
python scripts/text2sql_cli.py

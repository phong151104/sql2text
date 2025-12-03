@echo off
echo ============================================================
echo   Text-to-SQL Streamlit Demo
echo ============================================================
echo.

cd /d %~dp0

REM Activate conda environment if needed
call conda activate text2sql 2>nul

echo Starting Streamlit app...
echo.
echo Open your browser at: http://localhost:8500
echo.

streamlit run app.py --server.port 8500 --theme.base dark

pause

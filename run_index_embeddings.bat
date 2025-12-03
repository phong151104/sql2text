@echo off
REM ============================================================
REM  Index Neo4j Graph with Embeddings
REM ============================================================

echo.
echo ========================================
echo   Indexing Graph Nodes with Embeddings
echo ========================================
echo.

REM Check if .env exists
if not exist ".env" (
    echo [WARNING] .env file not found!
    echo Please copy .env.example to .env and configure your API keys.
    pause
    exit /b 1
)

REM Run indexing script
python scripts/index_embeddings.py

echo.
if errorlevel 1 (
    echo [ERROR] Indexing failed!
) else (
    echo [SUCCESS] Indexing complete!
    echo.
    echo You can now run the Text-to-SQL CLI:
    echo   python scripts/text2sql_cli.py
)

echo.
pause

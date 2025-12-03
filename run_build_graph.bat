@echo off
REM ============================================================
REM  Build Neo4j Knowledge Graph from YAML Metadata
REM ============================================================

REM --- Cấu hình Neo4j (thay đổi theo môi trường của bạn) ---
set NEO4J_URI=bolt://localhost:7687
set NEO4J_USER=neo4j
REM Thay password cua ban vao day:
set NEO4J_PASSWORD=phongph1

REM --- Cấu hình Domain ---
set DOMAIN=vnfilm_ticketing
set METADATA_ROOT=metadata/domains

REM ============================================================
echo.
echo ========================================
echo   Neo4j Knowledge Graph Builder
echo ========================================
echo.
echo NEO4J_URI: %NEO4J_URI%
echo NEO4J_USER: %NEO4J_USER%
echo DOMAIN: %DOMAIN%
echo METADATA_ROOT: %METADATA_ROOT%
echo.

REM --- Kiểm tra Python ---
python --version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Python khong duoc cai dat hoac khong co trong PATH
    pause
    exit /b 1
)

REM --- Cài đặt dependencies nếu chưa có ---
echo [1/2] Kiem tra va cai dat dependencies...
pip install -q pyyaml neo4j

REM --- Chạy script ---
echo.
echo [2/2] Dang build Neo4j graph...
echo.
python build_neo4j_graph.py --domain %DOMAIN% --metadata-root %METADATA_ROOT%

echo.
if errorlevel 1 (
    echo [ERROR] Co loi xay ra khi build graph!
) else (
    echo [SUCCESS] Build graph hoan tat!
    echo.
    echo Ban co the mo Neo4j Browser: http://localhost:7474
)

echo.
pause

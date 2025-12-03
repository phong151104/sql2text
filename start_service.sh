#!/bin/bash
# ============================================================
#  Build Neo4j Knowledge Graph from YAML Metadata
# ============================================================

# --- Cấu hình Neo4j (thay đổi theo môi trường của bạn) ---
export NEO4J_URI="bolt://localhost:7687"
export NEO4J_USER="neo4j"
export NEO4J_PASSWORD="your_password_here"

# --- Cấu hình Domain ---
DOMAIN="vnfilm_ticketing"
METADATA_ROOT="metadata/domains"

# ============================================================
echo ""
echo "========================================"
echo "  Neo4j Knowledge Graph Builder"
echo "========================================"
echo ""
echo "NEO4J_URI: $NEO4J_URI"
echo "NEO4J_USER: $NEO4J_USER"
echo "DOMAIN: $DOMAIN"
echo "METADATA_ROOT: $METADATA_ROOT"
echo ""

# --- Kiểm tra Python ---
if ! command -v python3 &> /dev/null; then
    if ! command -v python &> /dev/null; then
        echo "[ERROR] Python không được cài đặt hoặc không có trong PATH"
        exit 1
    fi
    PYTHON_CMD="python"
else
    PYTHON_CMD="python3"
fi

echo "Using: $($PYTHON_CMD --version)"
echo ""

# --- Cài đặt dependencies nếu chưa có ---
echo "[1/2] Kiểm tra và cài đặt dependencies..."
$PYTHON_CMD -m pip install -q pyyaml neo4j

# --- Chạy script ---
echo ""
echo "[2/2] Đang build Neo4j graph..."
echo ""
$PYTHON_CMD build_neo4j_graph.py --domain "$DOMAIN" --metadata-root "$METADATA_ROOT"

if [ $? -eq 0 ]; then
    echo ""
    echo "[SUCCESS] Build graph hoàn tất!"
    echo ""
    echo "Bạn có thể mở Neo4j Browser và chạy các query sau:"
    echo '  MATCH (t:Table)-[r:JOIN]->(t2:Table) RETURN t,r,t2 LIMIT 50'
    echo '  MATCH (t:Table)-[:HAS_COLUMN]->(c:Column) RETURN t,c LIMIT 50'
    echo '  MATCH (m:Metric)-[:METRIC_BASE_TABLE]->(t:Table) RETURN m,t'
else
    echo ""
    echo "[ERROR] Có lỗi xảy ra khi build graph!"
    exit 1
fi

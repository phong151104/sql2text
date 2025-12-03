#!/usr/bin/env python3
"""
Example usage of Text-to-SQL system.

This script demonstrates:
1. Indexing graph nodes with embeddings
2. Querying with natural language
3. Getting generated SQL
"""

import logging
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.graph import Neo4jClient, Neo4jVectorIndex, SchemaRetriever
from src.sql_generator import Text2SQLEngine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def example_vector_search():
    """Example: Vector search for relevant nodes."""
    print("\n" + "=" * 60)
    print("Example 1: Vector Search")
    print("=" * 60)
    
    with Neo4jClient() as client:
        vector_index = Neo4jVectorIndex(client=client)
        
        query = "tổng doanh thu VNPAY"
        print(f"\nQuery: {query}")
        
        results = vector_index.vector_search(query, top_k=5)
        
        print(f"\nTop {len(results)} matches:")
        for i, result in enumerate(results, 1):
            label = result.get("label")
            props = result.get("props", {})
            score = result.get("score", 0)
            
            if label == "Table":
                name = props.get("table_name")
            elif label == "Column":
                name = f"{props.get('table_name')}.{props.get('column_name')}"
            elif label == "Metric":
                name = props.get("name")
            else:
                name = props.get("name", "unknown")
            
            print(f"  {i}. [{label}] {name} (score: {score:.4f})")


def example_schema_retrieval():
    """Example: Retrieve schema context for a question."""
    print("\n" + "=" * 60)
    print("Example 2: Schema Retrieval")
    print("=" * 60)
    
    with Neo4jClient() as client:
        retriever = SchemaRetriever(client=client)
        
        question = "Tổng doanh thu VNPAY theo ngân hàng trong tháng này"
        print(f"\nQuestion: {question}")
        
        context = retriever.retrieve(question, top_k=10)
        
        print(f"\nRelevant Tables: {[t['table_name'] for t in context['tables']]}")
        print(f"Columns Retrieved: {len(context['columns'])}")
        print(f"Joins Found: {len(context['joins'])}")
        print(f"Metrics Found: {len(context['metrics'])}")


def example_text2sql():
    """Example: Full Text-to-SQL pipeline."""
    print("\n" + "=" * 60)
    print("Example 3: Text-to-SQL Generation")
    print("=" * 60)
    
    questions = [
        "Tổng doanh thu VNPAY theo tháng trong năm 2024",
        "Số lượng đơn hàng theo ngân hàng",
        "Top 10 vendor có doanh thu cao nhất",
    ]
    
    with Text2SQLEngine() as engine:
        for question in questions:
            print(f"\n❓ Question: {question}")
            
            result = engine.generate_sql(question)
            
            if result.success:
                print(f"📊 Tables: {result.relevant_tables}")
                print(f"🎯 Confidence: {result.confidence_score:.2%}")
                print(f"📝 SQL:\n{result.sql}")
            else:
                print(f"❌ Error: {result.error}")
            
            print("-" * 40)


def main():
    """Run all examples."""
    print("\n" + "=" * 60)
    print("  Text-to-SQL Examples")
    print("=" * 60)
    
    # Check if embeddings are indexed
    with Neo4jClient() as client:
        result = client.execute_query(
            "MATCH (n:Table) WHERE n.embedding IS NOT NULL RETURN count(n) AS count"
        )
        indexed_count = result[0]["count"] if result else 0
        
        if indexed_count == 0:
            print("\n⚠️  No embeddings found. Running indexing first...")
            print("   This may take a minute...\n")
            
            vector_index = Neo4jVectorIndex(client=client)
            vector_index.index_all_nodes()
    
    # Run examples
    example_vector_search()
    example_schema_retrieval()
    example_text2sql()
    
    print("\n" + "=" * 60)
    print("Examples complete!")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()

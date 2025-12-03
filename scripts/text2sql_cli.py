#!/usr/bin/env python3
"""
Interactive Text-to-SQL CLI.

Usage:
    python scripts/text2sql_cli.py
"""

import logging
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.sql_generator import Text2SQLEngine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def print_result(result):
    """Pretty print the result."""
    print("\n" + "=" * 60)
    print("QUESTION:", result.question)
    print("=" * 60)
    
    if result.success:
        print("\n📊 RELEVANT TABLES:")
        for table in result.relevant_tables:
            print(f"   - {table}")
        
        print(f"\n🎯 CONFIDENCE: {result.confidence_score:.2%}")
        
        print("\n📝 GENERATED SQL:")
        print("-" * 60)
        print(result.sql)
        print("-" * 60)
    else:
        print(f"\n❌ ERROR: {result.error}")
    
    print()


def main():
    """Run interactive CLI."""
    print("\n" + "=" * 60)
    print("  Text-to-SQL with Neo4j Graph RAG")
    print("=" * 60)
    print("\nType your question in natural language.")
    print("Type 'quit' or 'exit' to exit.\n")
    
    with Text2SQLEngine() as engine:
        while True:
            try:
                question = input("❓ Question: ").strip()
                
                if not question:
                    continue
                
                if question.lower() in ("quit", "exit", "q"):
                    print("\nGoodbye! 👋\n")
                    break
                
                result = engine.generate_sql(question)
                print_result(result)
                
            except KeyboardInterrupt:
                print("\n\nGoodbye! 👋\n")
                break
            except Exception as e:
                logger.error(f"Error: {e}")
                print(f"\n❌ Error: {e}\n")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Script to index Neo4j graph nodes with embeddings.

Usage:
    python scripts/index_embeddings.py
"""

import logging
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.graph import Neo4jClient, Neo4jVectorIndex

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def main():
    """Index all graph nodes with embeddings."""
    logger.info("Starting embedding indexing...")
    
    with Neo4jClient() as client:
        vector_index = Neo4jVectorIndex(client=client)
        
        # Create indexes and generate embeddings
        counts = vector_index.index_all_nodes()
        
        logger.info("=" * 50)
        logger.info("Indexing complete!")
        logger.info("Nodes indexed:")
        for label, count in counts.items():
            logger.info(f"  - {label}: {count}")
        logger.info("=" * 50)


if __name__ == "__main__":
    main()

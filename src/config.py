"""
Configuration management for Text-to-SQL system.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

from dotenv import load_dotenv

# Load environment variables (override to ensure .env takes priority)
load_dotenv(override=True)


@dataclass
class Neo4jConfig:
    """Neo4j database configuration."""
    uri: str = field(default_factory=lambda: os.getenv("NEO4J_URI", "bolt://localhost:7687"))
    user: str = field(default_factory=lambda: os.getenv("NEO4J_USER", "neo4j"))
    password: str = field(default_factory=lambda: os.getenv("NEO4J_PASSWORD", ""))
    database: str = field(default_factory=lambda: os.getenv("NEO4J_DATABASE", "neo4j"))


@dataclass
class OpenAIConfig:
    """OpenAI API configuration."""
    api_key: str = field(default_factory=lambda: os.getenv("OPENAI_API_KEY", ""))
    embedding_model: str = "text-embedding-3-small"
    embedding_dimensions: int = 1536
    chat_model: str = "gpt-4o-mini"
    temperature: float = 0.0


@dataclass
class VectorIndexConfig:
    """Vector index configuration for Neo4j."""
    index_name: str = "schema_embeddings"
    similarity_function: Literal["cosine", "euclidean"] = "cosine"
    top_k: int = 10


@dataclass
class Text2SQLConfig:
    """Main configuration for Text-to-SQL system."""
    neo4j: Neo4jConfig = field(default_factory=Neo4jConfig)
    openai: OpenAIConfig = field(default_factory=OpenAIConfig)
    vector_index: VectorIndexConfig = field(default_factory=VectorIndexConfig)
    
    # Domain settings
    domain: str = "vnfilm_ticketing"
    metadata_root: Path = field(default_factory=lambda: Path("metadata/domains"))
    
    # Graph traversal settings
    max_traversal_depth: int = 2
    max_related_nodes: int = 20
    
    @classmethod
    def from_env(cls) -> "Text2SQLConfig":
        """Create configuration from environment variables."""
        return cls()


# Global config instance
config = Text2SQLConfig.from_env()

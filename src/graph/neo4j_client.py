"""
Neo4j database client.
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Any, Generator, List, Dict

from neo4j import GraphDatabase, Driver, Session

from ..config import config, Neo4jConfig

logger = logging.getLogger(__name__)


class Neo4jClient:
    """
    Neo4j database client with connection management.
    """
    
    def __init__(self, neo4j_config: Neo4jConfig | None = None):
        self._config = neo4j_config or config.neo4j
        self._driver: Driver | None = None
    
    @property
    def driver(self) -> Driver:
        """Get or create Neo4j driver."""
        if self._driver is None:
            self._driver = GraphDatabase.driver(
                self._config.uri,
                auth=(self._config.user, self._config.password),
            )
            logger.info(f"Connected to Neo4j at {self._config.uri}")
        return self._driver
    
    def close(self) -> None:
        """Close the Neo4j connection."""
        if self._driver:
            self._driver.close()
            self._driver = None
            logger.info("Closed Neo4j connection")
    
    @contextmanager
    def session(self) -> Generator[Session, None, None]:
        """Get a Neo4j session context manager."""
        session = self.driver.session(database=self._config.database)
        try:
            yield session
        finally:
            session.close()
    
    def execute_query(
        self,
        query: str,
        parameters: Dict[str, Any] | None = None,
    ) -> List[Dict[str, Any]]:
        """
        Execute a Cypher query and return results as list of dicts.
        
        Args:
            query: Cypher query string
            parameters: Query parameters
            
        Returns:
            List of result records as dictionaries
        """
        with self.session() as session:
            result = session.run(query, parameters or {})
            return [record.data() for record in result]
    
    def execute_write(
        self,
        query: str,
        parameters: Dict[str, Any] | None = None,
    ) -> None:
        """
        Execute a write query.
        
        Args:
            query: Cypher query string
            parameters: Query parameters
        """
        with self.session() as session:
            session.execute_write(
                lambda tx: tx.run(query, parameters or {})
            )
    
    def __enter__(self) -> "Neo4jClient":
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

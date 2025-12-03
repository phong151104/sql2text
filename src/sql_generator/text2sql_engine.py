"""
Main Text-to-SQL engine.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from ..graph import Neo4jClient, Neo4jVectorIndex, SchemaRetriever
from .prompt_builder import PromptBuilder
from .llm_generator import LLMSQLGenerator

logger = logging.getLogger(__name__)


@dataclass
class Text2SQLResult:
    """Result of Text-to-SQL conversion."""
    question: str
    sql: str
    context: Dict[str, Any] = field(default_factory=dict)
    relevant_tables: List[str] = field(default_factory=list)
    confidence_score: float = 0.0
    error: Optional[str] = None
    
    @property
    def success(self) -> bool:
        return self.error is None and bool(self.sql)


class Text2SQLEngine:
    """
    Main engine for converting natural language to SQL.
    
    Orchestrates the full pipeline:
    1. Vector search for relevant schema nodes
    2. Graph traversal for context expansion
    3. Prompt building with schema context
    4. LLM-based SQL generation
    """
    
    def __init__(
        self,
        client: Neo4jClient | None = None,
        vector_index: Neo4jVectorIndex | None = None,
        retriever: SchemaRetriever | None = None,
        generator: LLMSQLGenerator | None = None,
    ):
        self.client = client or Neo4jClient()
        self.vector_index = vector_index or Neo4jVectorIndex(client=self.client)
        self.retriever = retriever or SchemaRetriever(
            client=self.client,
            vector_index=self.vector_index,
        )
        self.generator = generator or LLMSQLGenerator()
    
    def generate_sql(
        self,
        question: str,
        top_k: int = 10,
        expand_depth: int = 2,
    ) -> Text2SQLResult:
        """
        Generate SQL from natural language question.
        
        Args:
            question: Natural language question
            top_k: Number of vector search results
            expand_depth: Graph traversal depth
            
        Returns:
            Text2SQLResult with generated SQL and context
        """
        try:
            # Step 1: Retrieve relevant schema context
            logger.info(f"Processing question: {question}")
            context = self.retriever.retrieve(
                question=question,
                top_k=top_k,
                expand_depth=expand_depth,
            )
            
            # Extract relevant table names
            relevant_tables = [t["table_name"] for t in context.get("tables", [])]
            
            if not relevant_tables:
                return Text2SQLResult(
                    question=question,
                    sql="",
                    error="No relevant tables found for the question",
                )
            
            # Step 2: Build prompt
            messages = PromptBuilder.build_messages(question, context)
            
            # Step 3: Generate SQL
            sql = self.generator.generate(messages)
            
            # Calculate simple confidence based on vector match scores
            vector_matches = context.get("vector_matches", [])
            if vector_matches:
                avg_score = sum(m.get("score", 0) for m in vector_matches) / len(vector_matches)
                confidence = min(avg_score, 1.0)
            else:
                confidence = 0.0
            
            return Text2SQLResult(
                question=question,
                sql=sql,
                context=context,
                relevant_tables=relevant_tables,
                confidence_score=confidence,
            )
        
        except Exception as e:
            logger.error(f"Error generating SQL: {e}")
            return Text2SQLResult(
                question=question,
                sql="",
                error=str(e),
            )
    
    def batch_generate(
        self,
        questions: List[str],
        **kwargs,
    ) -> List[Text2SQLResult]:
        """
        Generate SQL for multiple questions.
        
        Args:
            questions: List of natural language questions
            **kwargs: Additional arguments for generate_sql
            
        Returns:
            List of Text2SQLResult
        """
        results = []
        for i, question in enumerate(questions):
            logger.info(f"Processing question {i + 1}/{len(questions)}")
            result = self.generate_sql(question, **kwargs)
            results.append(result)
        
        return results
    
    def close(self) -> None:
        """Close all connections."""
        self.client.close()
    
    def __enter__(self) -> "Text2SQLEngine":
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

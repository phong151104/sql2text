"""
Schema retriever for Text-to-SQL context building.
"""

from __future__ import annotations

import logging
from typing import Any, List, Dict, Set

from .neo4j_client import Neo4jClient
from .vector_index import Neo4jVectorIndex
from ..config import config

logger = logging.getLogger(__name__)


class SchemaRetriever:
    """
    Retrieves relevant schema information for SQL generation.
    
    Uses hybrid approach:
    1. Vector search to find semantically relevant nodes
    2. Graph traversal to expand context with related nodes
    """
    
    def __init__(
        self,
        client: Neo4jClient | None = None,
        vector_index: Neo4jVectorIndex | None = None,
    ):
        self.client = client or Neo4jClient()
        self.vector_index = vector_index or Neo4jVectorIndex(client=self.client)
    
    def retrieve(
        self,
        question: str,
        top_k: int = 10,
        expand_depth: int = 2,
    ) -> Dict[str, Any]:
        """
        Retrieve relevant schema context for a natural language question.
        
        Args:
            question: Natural language question
            top_k: Number of initial vector search results
            expand_depth: Graph traversal depth for expansion
            
        Returns:
            Schema context dictionary containing:
            - tables: List of relevant tables with columns
            - joins: Relevant join relationships
            - metrics: Relevant metrics
            - sample_queries: Similar sample queries
        """
        logger.info(f"[STEP 1/4] 🔍 Processing: {question[:50]}...")
        
        # Step 1: Vector search for initial matches
        vector_results = self.vector_index.vector_search(question, top_k=top_k)
        
        # Log vector results summary
        tables_found = [r for r in vector_results if r.get("label") == "Table"]
        columns_found = [r for r in vector_results if r.get("label") == "Column"]
        concepts_found = [r for r in vector_results if r.get("label") == "Concept"]
        metrics_found = [r for r in vector_results if r.get("label") == "Metric"]
        
        logger.info(f"[STEP 1/4] ✅ Vector search: {len(vector_results)} matches (Tables: {len(tables_found)}, Columns: {len(columns_found)}, Concepts: {len(concepts_found)}, Metrics: {len(metrics_found)})")
        
        # Top 3 results
        for i, result in enumerate(vector_results[:3], 1):
            label = result.get("label", "?")
            score = result.get("score", 0)
            props = result.get("props", {})
            if label == "Table":
                name = props.get("table_name", "N/A")
            elif label == "Column":
                name = f"{props.get('table_name', '')}.{props.get('column_name', '')}"
            else:
                name = props.get("name", props.get("concept", "N/A"))
            logger.info(f"[STEP 1/4]    #{i} [{label}] {name} (score: {score:.2f})")
        
        # Step 2: Extract table names and columns from results
        relevant_tables = self._extract_relevant_tables(vector_results)
        relevant_columns = self._extract_relevant_columns(vector_results)
        logger.info(f"[STEP 2/4] 📊 Extracted: {len(relevant_tables)} tables, {len(relevant_columns)} columns")
        
        # Step 3: Expand with graph traversal
        logger.info(f"[STEP 3/4] 🕸️ Graph traversal...")
        expanded_context = self._expand_context(relevant_tables, expand_depth, relevant_columns)
        
        # Log summary
        logger.info(f"[STEP 3/4] ✅ Context: {len(expanded_context['tables'])} tables, {len(expanded_context['columns'])} columns, {len(expanded_context['joins'])} joins, {len(expanded_context['metrics'])} metrics")
        
        # Step 4: Get sample queries for few-shot learning
        sample_queries = self._get_sample_queries(relevant_tables)
        logger.info(f"[STEP 4/4] 📝 Ready to generate SQL")
        
        return {
            "question": question,
            "vector_matches": vector_results,
            "tables": expanded_context["tables"],
            "columns": expanded_context["columns"],
            "joins": expanded_context["joins"],
            "metrics": expanded_context["metrics"],
            "sample_queries": sample_queries,
        }
    
    def _extract_relevant_tables(
        self,
        vector_results: List[Dict[str, Any]],
    ) -> Set[str]:
        """Extract table names from vector search results."""
        tables = set()
        
        for result in vector_results:
            label = result.get("label")
            props = result.get("props", {})
            
            if label == "Table":
                tables.add(props.get("table_name", ""))
            elif label == "Column":
                tables.add(props.get("table_name", ""))
            elif label == "Metric":
                tables.add(props.get("base_table", ""))
        
        return {t for t in tables if t}
    
    def _extract_relevant_columns(
        self,
        vector_results: List[Dict[str, Any]],
    ) -> Set[tuple]:
        """Extract (table_name, column_name) pairs from vector search results."""
        columns = set()
        
        for result in vector_results:
            label = result.get("label")
            props = result.get("props", {})
            
            if label == "Column":
                table_name = props.get("table_name", "")
                column_name = props.get("column_name", "")
                if table_name and column_name:
                    columns.add((table_name, column_name))
        
        return columns

    def _expand_context(
        self,
        table_names: Set[str],
        depth: int,
        relevant_columns: Set[tuple] = None,
    ) -> Dict[str, Any]:
        """
        Expand context using graph traversal.
        
        Gets relevant columns (from vector search) + key columns (PK, FK, time),
        joins, and metrics for connected tables.
        
        Args:
            table_names: Set of table names to expand
            depth: Graph traversal depth (not used currently)
            relevant_columns: Set of (table_name, column_name) tuples from vector search
        """
        if not table_names:
            return {"tables": [], "columns": [], "joins": [], "metrics": []}
        
        relevant_columns = relevant_columns or set()
        
        # Get tables with all properties
        tables_query = """
        MATCH (t:Table)
        WHERE t.table_name IN $table_names
        RETURN t.table_name AS table_name,
               t.business_name AS business_name,
               t.table_type AS table_type,
               t.description AS description,
               t.grain AS grain,
               t.catalog AS catalog,
               t.schema AS schema
        """
        tables = self.client.execute_query(tables_query, {"table_names": list(table_names)})
        
        # Get KEY columns (PK, FK, time columns marked in YAML) only
        # This provides essential columns without overwhelming the LLM
        key_columns_query = """
        MATCH (t:Table)-[r:HAS_COLUMN]->(c:Column)
        WHERE t.table_name IN $table_names
          AND (
            r.primary_key = true 
            OR r.time_column = true 
            OR (r.foreign_key IS NOT NULL AND r.foreign_key = true)
          )
        RETURN t.table_name AS table_name,
               c.column_name AS column_name,
               c.data_type AS data_type,
               c.business_name AS business_name,
               c.description AS description,
               c.semantics AS semantics,
               c.unit AS unit,
               r.primary_key AS is_primary_key,
               r.time_column AS is_time_column,
               'key' AS source
        ORDER BY t.table_name, c.column_name
        """
        key_columns = self.client.execute_query(key_columns_query, {"table_names": list(table_names)})
        
        # Get columns that were found in vector search
        vector_columns = []
        if relevant_columns:
            vector_columns_query = """
            MATCH (t:Table)-[r:HAS_COLUMN]->(c:Column)
            WHERE t.table_name IN $table_names
            RETURN t.table_name AS table_name,
                   c.column_name AS column_name,
                   c.data_type AS data_type,
                   c.business_name AS business_name,
                   c.description AS description,
                   c.semantics AS semantics,
                   c.unit AS unit,
                   r.primary_key AS is_primary_key,
                   r.time_column AS is_time_column,
                   'vector' AS source
            ORDER BY t.table_name, c.column_name
            """
            all_table_columns = self.client.execute_query(vector_columns_query, {"table_names": list(table_names)})
            
            # Filter to only include columns from vector search
            for col in all_table_columns:
                if (col['table_name'], col['column_name']) in relevant_columns:
                    vector_columns.append(col)
        
        # Merge columns (deduplicate by table_name + column_name)
        columns_dict = {}
        for col in key_columns:
            key = (col['table_name'], col['column_name'])
            col['source'] = 'key'
            columns_dict[key] = col
        
        for col in vector_columns:
            key = (col['table_name'], col['column_name'])
            if key not in columns_dict:
                col['source'] = 'vector'
                columns_dict[key] = col
            else:
                # Mark as both key and vector
                columns_dict[key]['source'] = 'key+vector'
        
        columns = list(columns_dict.values())
        columns.sort(key=lambda x: (x['table_name'], x['column_name']))
        
        # Get joins between relevant tables
        joins_query = """
        MATCH (t1:Table)-[j:JOIN]->(t2:Table)
        WHERE t1.table_name IN $table_names OR t2.table_name IN $table_names
        RETURN t1.table_name AS from_table,
               t2.table_name AS to_table,
               j.join_type AS join_type,
               j.on AS on_clause,
               j.description AS description
        """
        joins = self.client.execute_query(joins_query, {"table_names": list(table_names)})
        
        # Get FK relationships
        fk_query = """
        MATCH (t1:Table)-[fk:FK]->(t2:Table)
        WHERE t1.table_name IN $table_names OR t2.table_name IN $table_names
        RETURN t1.table_name AS from_table,
               t2.table_name AS to_table,
               fk.column AS column,
               fk.references_column AS references_column,
               fk.description AS description
        """
        fks = self.client.execute_query(fk_query, {"table_names": list(table_names)})
        
        # Add FK info to joins
        for fk in fks:
            joins.append({
                "from_table": fk["from_table"],
                "to_table": fk["to_table"],
                "join_type": "left",
                "on_clause": [f"{fk['from_table']}.{fk['column']} = {fk['to_table']}.{fk['references_column']}"],
                "description": fk.get("description", ""),
            })
        
        # Get metrics for relevant tables
        metrics_query = """
        MATCH (m:Metric)
        WHERE m.base_table IN $table_names
        RETURN m.name AS name,
               m.business_name AS business_name,
               m.description AS description,
               m.expression AS expression,
               m.base_table AS base_table,
               m.grain AS grain,
               m.unit AS unit
        """
        metrics = self.client.execute_query(metrics_query, {"table_names": list(table_names)})
        
        return {
            "tables": tables,
            "columns": columns,
            "joins": joins,
            "metrics": metrics,
        }
    
    def _get_sample_queries(
        self,
        table_names: Set[str],
        limit: int = 3,
    ) -> List[Dict[str, str]]:
        """
        Get sample SQL queries from metadata for few-shot learning.
        
        Note: Sample queries are stored in YAML, not in Neo4j.
        This method returns empty for now - can be enhanced to read from YAML.
        """
        # TODO: Load sample_queries from YAML metadata
        return []
    
    def get_full_schema(self, domain: str | None = None) -> Dict[str, Any]:
        """
        Get the complete schema for a domain.
        
        Args:
            domain: Domain name (default from config)
            
        Returns:
            Complete schema information
        """
        domain = domain or config.domain
        
        # Get all tables in domain
        tables_query = """
        MATCH (t:Table {domain: $domain})
        RETURN t.table_name AS table_name
        """
        results = self.client.execute_query(tables_query, {"domain": domain})
        table_names = {r["table_name"] for r in results}
        
        return self._expand_context(table_names, depth=1)

"""
Prompt builder for Text-to-SQL generation.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List


class PromptBuilder:
    """
    Builds prompts for LLM-based SQL generation.
    
    Creates structured prompts with schema context, examples,
    and guidelines for accurate SQL generation.
    """
    
    @classmethod
    def _get_full_table_name(cls, table: Dict[str, Any]) -> str:
        """
        Get full table name with catalog.schema.table format.
        
        Args:
            table: Table dict with catalog, schema, table_name
            
        Returns:
            Full qualified table name
        """
        parts = []
        if table.get("catalog"):
            parts.append(table["catalog"])
        if table.get("schema"):
            parts.append(table["schema"])
        parts.append(table.get("table_name", "unknown"))
        return ".".join(parts)
    
    @classmethod
    def get_system_prompt(cls) -> str:
        """Get system prompt with current date."""
        current_date = datetime.now().strftime("%Y-%m-%d")
        current_year = datetime.now().year
        current_month = datetime.now().month
        
        return f"""You are an expert SQL analyst. Your task is to convert natural language questions into accurate SQL queries.

## Current Date Information:
- Today's date: {current_date}
- Current year: {current_year}
- Current month: {current_month}

## Guidelines:
1. Use only the tables and columns provided in the schema context
2. **IMPORTANT: Always use the FULL TABLE NAME with catalog.schema.table_name format as provided**
3. Follow the join paths specified - do not invent new joins
4. Use appropriate aggregation functions (SUM, COUNT, AVG, etc.)
5. Include proper date filtering when time periods are mentioned
6. Use table aliases for clarity
7. Return only the SQL query without explanation unless asked
8. When user mentions relative dates (e.g., "this month", "last year", "yesterday"), use the current date information above to calculate the correct date range

## Important:
- The database is a data lakehouse using Spark SQL / Trino SQL dialect
- **Always use fully qualified table names (catalog.schema.table_name) in FROM and JOIN clauses**
- Use DATE, TIMESTAMP functions appropriately
- Handle NULL values properly
- Respect the grain of each table"""

    @classmethod
    def build_schema_context(cls, context: Dict[str, Any]) -> str:
        """
        Build schema context section of the prompt.
        
        Args:
            context: Schema context from SchemaRetriever
            
        Returns:
            Formatted schema context string
        """
        parts = ["## Available Schema\n"]
        
        # Build a mapping of table_name -> full_table_name for later use
        table_full_names: Dict[str, str] = {}
        
        # Tables section
        tables = context.get("tables", [])
        if tables:
            parts.append("### Tables\n")
            for table in tables:
                full_name = cls._get_full_table_name(table)
                table_name = table.get('table_name', 'unknown')
                table_full_names[table_name] = full_name
                
                table_info = f"**{full_name}** ({table.get('table_type', 'unknown')})"
                if table.get("business_name"):
                    table_info += f" - {table['business_name']}"
                parts.append(table_info)
                
                if table.get("grain"):
                    parts.append(f"  - Grain: {table['grain']}")
                if table.get("description"):
                    parts.append(f"  - Description: {table['description'][:200]}")
                parts.append("")
        
        # Columns section (grouped by table)
        columns = context.get("columns", [])
        if columns:
            parts.append("### Columns\n")
            
            # Group by table
            columns_by_table: Dict[str, List] = {}
            for col in columns:
                table_name = col.get("table_name", "unknown")
                if table_name not in columns_by_table:
                    columns_by_table[table_name] = []
                columns_by_table[table_name].append(col)
            
            for table_name, cols in columns_by_table.items():
                # Use full table name if available
                full_name = table_full_names.get(table_name, table_name)
                parts.append(f"**{full_name}**:")
                for col in cols:
                    col_info = f"  - `{col['column_name']}` ({col.get('data_type', 'unknown')})"
                    
                    flags = []
                    if col.get("is_primary_key"):
                        flags.append("PK")
                    if col.get("is_time_column"):
                        flags.append("TIME")
                    if flags:
                        col_info += f" [{', '.join(flags)}]"
                    
                    if col.get("description"):
                        col_info += f" -- {col['description'][:100]}"
                    
                    parts.append(col_info)
                parts.append("")
        
        # Joins section
        joins = context.get("joins", [])
        if joins:
            parts.append("### Join Relationships\n")
            for join in joins:
                from_table = join.get('from_table', '')
                to_table = join.get('to_table', '')
                
                # Use full table names
                from_full = table_full_names.get(from_table, from_table)
                to_full = table_full_names.get(to_table, to_table)
                
                on_clause = join.get("on_clause", [])
                if isinstance(on_clause, list):
                    on_str = " AND ".join(on_clause)
                else:
                    on_str = str(on_clause)
                
                parts.append(
                    f"- {from_full} {join.get('join_type', 'LEFT')} JOIN "
                    f"{to_full} ON {on_str}"
                )
            parts.append("")
        
        # Metrics section
        metrics = context.get("metrics", [])
        if metrics:
            parts.append("### Pre-defined Metrics\n")
            for metric in metrics:
                parts.append(
                    f"- **{metric['name']}**: {metric.get('business_name', '')} "
                    f"= `{metric.get('expression', '')}`"
                )
            parts.append("")
        
        # Add explicit instruction about table names
        if table_full_names:
            parts.append("### Table Name Reference\n")
            parts.append("**Use these exact table names in your SQL:**")
            for short_name, full_name in table_full_names.items():
                parts.append(f"- `{short_name}` → `{full_name}`")
            parts.append("")
        
        return "\n".join(parts)
    
    @classmethod
    def build_examples_section(cls, sample_queries: List[Dict[str, str]]) -> str:
        """
        Build few-shot examples section.
        
        Args:
            sample_queries: List of sample queries with description and sql
            
        Returns:
            Formatted examples string
        """
        if not sample_queries:
            return ""
        
        parts = ["## Examples\n"]
        
        for i, query in enumerate(sample_queries, 1):
            parts.append(f"**Example {i}:** {query.get('description', '')}")
            parts.append(f"```sql\n{query.get('sql', '')}\n```")
            parts.append("")
        
        return "\n".join(parts)
    
    @classmethod
    def build_user_prompt(cls, question: str, context: Dict[str, Any]) -> str:
        """
        Build the complete user prompt.
        
        Args:
            question: User's natural language question
            context: Schema context from SchemaRetriever
            
        Returns:
            Complete user prompt string
        """
        parts = []
        
        # Schema context
        parts.append(cls.build_schema_context(context))
        
        # Examples (if any)
        sample_queries = context.get("sample_queries", [])
        if sample_queries:
            parts.append(cls.build_examples_section(sample_queries))
        
        # User question
        parts.append("## Question\n")
        parts.append(question)
        parts.append("\n## SQL Query\n")
        
        return "\n".join(parts)
    
    @classmethod
    def build_messages(
        cls,
        question: str,
        context: Dict[str, Any],
    ) -> List[Dict[str, str]]:
        """
        Build complete message list for chat API.
        
        Args:
            question: User's natural language question
            context: Schema context from SchemaRetriever
            
        Returns:
            List of message dicts for chat API
        """
        return [
            {"role": "system", "content": cls.get_system_prompt()},
            {"role": "user", "content": cls.build_user_prompt(question, context)},
        ]

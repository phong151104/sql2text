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
2. Follow the join paths specified - do not invent new joins
3. Use appropriate aggregation functions (SUM, COUNT, AVG, etc.)
4. Include proper date filtering when time periods are mentioned
5. Use table aliases for clarity
6. Return only the SQL query without explanation unless asked
7. When user mentions relative dates (e.g., "this month", "last year", "yesterday"), use the current date information above to calculate the correct date range

## Important:
- The database is a data lakehouse using Spark SQL / Trino SQL dialect
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
        
        # Tables section
        tables = context.get("tables", [])
        if tables:
            parts.append("### Tables\n")
            for table in tables:
                table_info = f"**{table['table_name']}** ({table.get('table_type', 'unknown')})"
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
                parts.append(f"**{table_name}**:")
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
                on_clause = join.get("on_clause", [])
                if isinstance(on_clause, list):
                    on_str = " AND ".join(on_clause)
                else:
                    on_str = str(on_clause)
                
                parts.append(
                    f"- {join['from_table']} {join.get('join_type', 'LEFT')} JOIN "
                    f"{join['to_table']} ON {on_str}"
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

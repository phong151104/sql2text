"""
Optimized Schema Loader - Load schema thông minh và tiết kiệm bộ nhớ
"""
from typing import List, Dict, Any, Optional
from table_selector import get_table_selector, TableSelector
from schema_cache import get_schema_cache, SchemaCache, TableSchema


class OptimizedSchemaLoader:
    """Load schema tối ưu dựa trên câu hỏi"""
    
    def __init__(self, db_connection):
        self.db = db_connection
        self.selector: TableSelector = get_table_selector()
        self.cache: SchemaCache = get_schema_cache()
    
    def get_relevant_schema(
        self, 
        question: str, 
        max_tables: int = 5,
        include_samples: bool = False
    ) -> str:
        """
        Lấy schema của các bảng liên quan đến câu hỏi
        
        Args:
            question: Câu hỏi của người dùng
            max_tables: Số bảng tối đa
            include_samples: Có lấy sample data không
            
        Returns:
            Schema string để đưa vào prompt
        """
        # Bước 1: Chọn bảng liên quan
        relevant_tables = self.selector.select_tables(question, max_tables)
        
        if not relevant_tables:
            # Fallback: lấy các bảng chính
            relevant_tables = ["film", "actor", "customer"]
        
        # Bước 2: Load schema (từ cache hoặc DB)
        schemas = self._load_schemas(relevant_tables, include_samples)
        
        # Bước 3: Format thành prompt string
        return self._format_schemas(schemas, relevant_tables)
    
    def _load_schemas(
        self, 
        table_names: List[str],
        include_samples: bool
    ) -> Dict[str, TableSchema]:
        """Load schema từ cache hoặc database"""
        schemas = {}
        tables_to_load = []
        
        # Check cache trước
        for name in table_names:
            cached = self.cache.get(name)
            if cached:
                schemas[name] = cached
            else:
                tables_to_load.append(name)
        
        # Load từ DB những bảng chưa có trong cache
        for name in tables_to_load:
            schema = self._load_from_db(name, include_samples)
            if schema:
                schemas[name] = schema
                self.cache.set(name, schema)
        
        return schemas
    
    def _load_from_db(
        self, 
        table_name: str,
        include_samples: bool
    ) -> Optional[TableSchema]:
        """Load schema từ database"""
        try:
            # Load columns
            columns_query = """
                SELECT column_name, data_type, is_nullable, column_key
                FROM information_schema.columns
                WHERE table_schema = DATABASE() AND table_name = %s
                ORDER BY ordinal_position
            """
            columns_result = self.db.execute(columns_query, (table_name,))
            
            if not columns_result:
                return None
            
            columns = []
            primary_key = None
            for row in columns_result:
                col = {
                    "name": row[0],
                    "type": row[1],
                    "nullable": row[2],
                }
                columns.append(col)
                if row[3] == "PRI":
                    primary_key = row[0]
            
            # Load foreign keys
            fk_query = """
                SELECT column_name, referenced_table_name, referenced_column_name
                FROM information_schema.key_column_usage
                WHERE table_schema = DATABASE() 
                AND table_name = %s
                AND referenced_table_name IS NOT NULL
            """
            fk_result = self.db.execute(fk_query, (table_name,))
            
            foreign_keys = []
            for row in fk_result or []:
                foreign_keys.append({
                    "column": row[0],
                    "references": f"{row[1]}.{row[2]}"
                })
            
            # Load sample data (optional, limited)
            sample_data = []
            if include_samples:
                sample_query = f"SELECT * FROM {table_name} LIMIT 3"
                sample_result = self.db.execute(sample_query)
                # Convert to dict format...
            
            return TableSchema(
                name=table_name,
                columns=columns,
                primary_key=primary_key,
                foreign_keys=foreign_keys,
                sample_data=sample_data
            )
            
        except Exception as e:
            print(f"Error loading schema for {table_name}: {e}")
            return None
    
    def _format_schemas(
        self, 
        schemas: Dict[str, TableSchema],
        order: List[str]
    ) -> str:
        """Format schemas thành string cho prompt"""
        lines = ["Database Schema:", "=" * 40]
        
        for table_name in order:
            if table_name in schemas:
                lines.append("")
                lines.append(schemas[table_name].to_prompt_string())
        
        lines.append("")
        lines.append("=" * 40)
        return "\n".join(lines)
    
    def get_minimal_schema(self, question: str) -> str:
        """
        Lấy schema tối giản nhất có thể
        Dùng cho các câu hỏi đơn giản
        """
        return self.get_relevant_schema(question, max_tables=3, include_samples=False)
    
    def get_full_schema(self, question: str) -> str:
        """
        Lấy schema đầy đủ hơn
        Dùng cho các câu hỏi phức tạp
        """
        return self.get_relevant_schema(question, max_tables=7, include_samples=True)

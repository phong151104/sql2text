"""
Schema Cache - Cache metadata của bảng để tối ưu bộ nhớ
"""
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from dataclasses import dataclass, field
import threading


@dataclass
class TableSchema:
    """Schema của một bảng"""
    name: str
    columns: List[Dict[str, Any]]
    primary_key: Optional[str] = None
    foreign_keys: List[Dict[str, str]] = field(default_factory=list)
    sample_data: List[Dict] = field(default_factory=list)
    loaded_at: datetime = field(default_factory=datetime.now)
    
    def to_prompt_string(self) -> str:
        """Convert schema thành string cho prompt"""
        lines = [f"Table: {self.name}"]
        lines.append("Columns:")
        for col in self.columns:
            col_str = f"  - {col['name']} ({col['type']})"
            if col.get('nullable') == 'NO':
                col_str += " NOT NULL"
            if col['name'] == self.primary_key:
                col_str += " PRIMARY KEY"
            lines.append(col_str)
        
        if self.foreign_keys:
            lines.append("Foreign Keys:")
            for fk in self.foreign_keys:
                lines.append(f"  - {fk['column']} -> {fk['references']}")
        
        return "\n".join(lines)


class SchemaCache:
    """Cache schema của các bảng với TTL"""
    
    def __init__(self, ttl_minutes: int = 30, max_cached_tables: int = 20):
        self._cache: Dict[str, TableSchema] = {}
        self._ttl = timedelta(minutes=ttl_minutes)
        self._max_tables = max_cached_tables
        self._lock = threading.RLock()
        self._access_order: List[str] = []  # LRU tracking
    
    def get(self, table_name: str) -> Optional[TableSchema]:
        """Lấy schema từ cache"""
        with self._lock:
            if table_name not in self._cache:
                return None
            
            schema = self._cache[table_name]
            
            # Check TTL
            if datetime.now() - schema.loaded_at > self._ttl:
                del self._cache[table_name]
                self._access_order.remove(table_name)
                return None
            
            # Update access order (LRU)
            if table_name in self._access_order:
                self._access_order.remove(table_name)
            self._access_order.append(table_name)
            
            return schema
    
    def set(self, table_name: str, schema: TableSchema) -> None:
        """Lưu schema vào cache"""
        with self._lock:
            # Evict nếu đầy (LRU)
            while len(self._cache) >= self._max_tables and self._access_order:
                oldest = self._access_order.pop(0)
                if oldest in self._cache:
                    del self._cache[oldest]
            
            self._cache[table_name] = schema
            if table_name in self._access_order:
                self._access_order.remove(table_name)
            self._access_order.append(table_name)
    
    def get_multiple(self, table_names: List[str]) -> Dict[str, TableSchema]:
        """Lấy nhiều schema cùng lúc"""
        result = {}
        for name in table_names:
            schema = self.get(name)
            if schema:
                result[name] = schema
        return result
    
    def invalidate(self, table_name: str) -> None:
        """Xóa cache của một bảng"""
        with self._lock:
            if table_name in self._cache:
                del self._cache[table_name]
            if table_name in self._access_order:
                self._access_order.remove(table_name)
    
    def clear(self) -> None:
        """Xóa toàn bộ cache"""
        with self._lock:
            self._cache.clear()
            self._access_order.clear()
    
    def get_stats(self) -> Dict[str, Any]:
        """Thống kê cache"""
        with self._lock:
            return {
                "cached_tables": len(self._cache),
                "max_tables": self._max_tables,
                "tables": list(self._cache.keys()),
            }


# Singleton instance
_cache = None

def get_schema_cache() -> SchemaCache:
    global _cache
    if _cache is None:
        _cache = SchemaCache()
    return _cache

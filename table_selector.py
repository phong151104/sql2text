"""
Smart table selector - Chọn bảng liên quan dựa trên phân tích câu hỏi
Hỗ trợ tiếng Việt có dấu và không dấu
"""
from typing import List, Dict, Set
from functools import lru_cache


class TableSelector:
    """Chọn các bảng liên quan dựa trên từ khóa trong câu hỏi"""
    
    def __init__(self):
        # Mapping từ khóa tiếng Việt (có dấu + không dấu) và tiếng Anh → tên bảng
        self.keyword_to_tables: Dict[str, Set[str]] = {
            # ==================== FILM ====================
            # Tiếng Anh
            "film": {"film", "film_category", "film_actor"},
            "movie": {"film", "film_category", "film_actor"},
            "title": {"film"},
            "description": {"film"},
            "length": {"film"},
            "duration": {"film"},
            "release": {"film"},
            "rating": {"film"},
            
            # Tiếng Việt có dấu
            "phim": {"film", "film_category", "film_actor"},
            "bộ phim": {"film", "film_category", "film_actor"},
            "tiêu đề": {"film"},
            "tên phim": {"film"},
            "mô tả": {"film"},
            "nội dung": {"film"},
            "thời lượng": {"film"},
            "độ dài": {"film"},
            "năm phát hành": {"film"},
            "năm sản xuất": {"film"},
            "đánh giá": {"film"},
            "xếp hạng": {"film"},
            
            # Tiếng Việt không dấu
            "bo phim": {"film", "film_category", "film_actor"},
            "tieu de": {"film"},
            "ten phim": {"film"},
            "mo ta": {"film"},
            "noi dung": {"film"},
            "thoi luong": {"film"},
            "do dai": {"film"},
            "nam phat hanh": {"film"},
            "nam san xuat": {"film"},
            "danh gia": {"film"},
            "xep hang": {"film"},
            
            # ==================== ACTOR ====================
            # Tiếng Anh
            "actor": {"actor", "film_actor"},
            "actress": {"actor", "film_actor"},
            "cast": {"actor", "film_actor"},
            "performer": {"actor", "film_actor"},
            
            # Tiếng Việt có dấu
            "diễn viên": {"actor", "film_actor"},
            "nghệ sĩ": {"actor", "film_actor"},
            "đóng phim": {"actor", "film_actor", "film"},
            "vai diễn": {"actor", "film_actor"},
            "tham gia": {"actor", "film_actor"},
            "xuất hiện": {"actor", "film_actor"},
            
            # Tiếng Việt không dấu
            "dien vien": {"actor", "film_actor"},
            "nghe si": {"actor", "film_actor"},
            "dong phim": {"actor", "film_actor", "film"},
            "vai dien": {"actor", "film_actor"},
            "tham gia": {"actor", "film_actor"},
            "xuat hien": {"actor", "film_actor"},
            
            # ==================== CATEGORY ====================
            # Tiếng Anh
            "category": {"category", "film_category"},
            "genre": {"category", "film_category"},
            "type": {"category", "film_category"},
            
            # Tiếng Việt có dấu
            "thể loại": {"category", "film_category"},
            "loại phim": {"category", "film_category"},
            "danh mục": {"category", "film_category"},
            "hành động": {"category", "film_category"},
            "kinh dị": {"category", "film_category"},
            "hài": {"category", "film_category"},
            "tình cảm": {"category", "film_category"},
            "hoạt hình": {"category", "film_category"},
            
            # Tiếng Việt không dấu
            "the loai": {"category", "film_category"},
            "loai phim": {"category", "film_category"},
            "danh muc": {"category", "film_category"},
            "hanh dong": {"category", "film_category"},
            "kinh di": {"category", "film_category"},
            "tinh cam": {"category", "film_category"},
            "hoat hinh": {"category", "film_category"},
            
            # ==================== CUSTOMER ====================
            # Tiếng Anh
            "customer": {"customer", "rental", "payment"},
            "client": {"customer", "rental", "payment"},
            "member": {"customer"},
            "user": {"customer"},
            
            # Tiếng Việt có dấu
            "khách hàng": {"customer", "rental", "payment"},
            "khách": {"customer", "rental"},
            "người thuê": {"customer", "rental"},
            "người dùng": {"customer"},
            "thành viên": {"customer"},
            "người mua": {"customer", "payment"},
            "người mượn": {"customer", "rental"},
            
            # Tiếng Việt không dấu
            "khach hang": {"customer", "rental", "payment"},
            "khach": {"customer", "rental"},
            "nguoi thue": {"customer", "rental"},
            "nguoi dung": {"customer"},
            "thanh vien": {"customer"},
            "nguoi mua": {"customer", "payment"},
            "nguoi muon": {"customer", "rental"},
            
            # ==================== RENTAL ====================
            # Tiếng Anh
            "rental": {"rental", "inventory"},
            "rent": {"rental", "inventory"},
            "borrow": {"rental"},
            "loan": {"rental"},
            
            # Tiếng Việt có dấu
            "thuê": {"rental", "inventory"},
            "cho thuê": {"rental", "inventory"},
            "mượn": {"rental"},
            "giao dịch thuê": {"rental"},
            "lượt thuê": {"rental"},
            "đơn thuê": {"rental"},
            "trả": {"rental"},
            "trả phim": {"rental"},
            
            # Tiếng Việt không dấu
            "thue": {"rental", "inventory"},
            "cho thue": {"rental", "inventory"},
            "muon": {"rental"},
            "giao dich thue": {"rental"},
            "luot thue": {"rental"},
            "don thue": {"rental"},
            "tra": {"rental"},
            "tra phim": {"rental"},
            
            # ==================== PAYMENT ====================
            # Tiếng Anh
            "payment": {"payment"},
            "pay": {"payment"},
            "revenue": {"payment", "rental"},
            "income": {"payment"},
            "money": {"payment"},
            "amount": {"payment"},
            "sales": {"payment", "rental"},
            
            # Tiếng Việt có dấu
            "thanh toán": {"payment"},
            "trả tiền": {"payment"},
            "tiền": {"payment"},
            "doanh thu": {"payment", "rental"},
            "thu nhập": {"payment"},
            "số tiền": {"payment"},
            "tổng tiền": {"payment"},
            "chi phí": {"payment"},
            "phí": {"payment"},
            "hóa đơn": {"payment"},
            "bán hàng": {"payment", "rental"},
            
            # Tiếng Việt không dấu
            "thanh toan": {"payment"},
            "tra tien": {"payment"},
            "tien": {"payment"},
            "doanh thu": {"payment", "rental"},
            "thu nhap": {"payment"},
            "so tien": {"payment"},
            "tong tien": {"payment"},
            "chi phi": {"payment"},
            "phi": {"payment"},
            "hoa don": {"payment"},
            "ban hang": {"payment", "rental"},
            
            # ==================== STORE ====================
            # Tiếng Anh
            "store": {"store", "staff", "inventory"},
            "shop": {"store", "staff", "inventory"},
            "branch": {"store"},
            "location": {"store", "address"},
            
            # Tiếng Việt có dấu
            "cửa hàng": {"store", "staff", "inventory"},
            "chi nhánh": {"store"},
            "địa điểm": {"store", "address"},
            "điểm bán": {"store"},
            "cơ sở": {"store"},
            
            # Tiếng Việt không dấu
            "cua hang": {"store", "staff", "inventory"},
            "chi nhanh": {"store"},
            "dia diem": {"store", "address"},
            "diem ban": {"store"},
            "co so": {"store"},
            
            # ==================== STAFF ====================
            # Tiếng Anh
            "staff": {"staff"},
            "employee": {"staff"},
            "worker": {"staff"},
            "manager": {"staff", "store"},
            
            # Tiếng Việt có dấu
            "nhân viên": {"staff"},
            "người làm": {"staff"},
            "quản lý": {"staff", "store"},
            "nhân sự": {"staff"},
            
            # Tiếng Việt không dấu
            "nhan vien": {"staff"},
            "nguoi lam": {"staff"},
            "quan ly": {"staff", "store"},
            "nhan su": {"staff"},
            
            # ==================== INVENTORY ====================
            # Tiếng Anh
            "inventory": {"inventory", "store"},
            "stock": {"inventory"},
            "available": {"inventory"},
            "copy": {"inventory"},
            
            # Tiếng Việt có dấu
            "kho": {"inventory", "store"},
            "tồn kho": {"inventory"},
            "hàng tồn": {"inventory"},
            "số lượng": {"inventory"},
            "bản sao": {"inventory"},
            "còn hàng": {"inventory"},
            "có sẵn": {"inventory"},
            
            # Tiếng Việt không dấu
            "ton kho": {"inventory"},
            "hang ton": {"inventory"},
            "so luong": {"inventory"},
            "ban sao": {"inventory"},
            "con hang": {"inventory"},
            "co san": {"inventory"},
            
            # ==================== ADDRESS/CITY/COUNTRY ====================
            # Tiếng Anh
            "address": {"address", "city", "country"},
            "city": {"city", "address"},
            "country": {"country", "city"},
            "district": {"address"},
            "postal": {"address"},
            "phone": {"address", "customer"},
            
            # Tiếng Việt có dấu
            "địa chỉ": {"address", "city", "country"},
            "thành phố": {"city", "address"},
            "quốc gia": {"country", "city"},
            "nước": {"country"},
            "quận": {"address"},
            "huyện": {"address"},
            "phường": {"address"},
            "đường": {"address"},
            "số điện thoại": {"address", "customer"},
            "liên hệ": {"address", "customer"},
            
            # Tiếng Việt không dấu
            "dia chi": {"address", "city", "country"},
            "thanh pho": {"city", "address"},
            "quoc gia": {"country", "city"},
            "nuoc": {"country"},
            "quan": {"address"},
            "huyen": {"address"},
            "phuong": {"address"},
            "duong": {"address"},
            "so dien thoai": {"address", "customer"},
            "lien he": {"address", "customer"},
            
            # ==================== LANGUAGE ====================
            # Tiếng Anh
            "language": {"language", "film"},
            
            # Tiếng Việt có dấu
            "ngôn ngữ": {"language", "film"},
            "tiếng": {"language", "film"},
            "phụ đề": {"language", "film"},
            "lồng tiếng": {"language", "film"},
            
            # Tiếng Việt không dấu
            "ngon ngu": {"language", "film"},
            "tieng": {"language", "film"},
            "phu de": {"language", "film"},
            "long tieng": {"language", "film"},
            
            # ==================== THỜI GIAN (dùng chung) ====================
            # Tiếng Việt có dấu
            "ngày": {"rental", "payment"},
            "tháng": {"rental", "payment"},
            "năm": {"rental", "payment", "film"},
            "tuần": {"rental", "payment"},
            "hôm nay": {"rental", "payment"},
            "hôm qua": {"rental", "payment"},
            "gần đây": {"rental", "payment"},
            
            # Tiếng Việt không dấu
            "ngay": {"rental", "payment"},
            "thang": {"rental", "payment"},
            "nam": {"rental", "payment", "film"},
            "tuan": {"rental", "payment"},
            "hom nay": {"rental", "payment"},
            "hom qua": {"rental", "payment"},
            "gan day": {"rental", "payment"},
            
            # ==================== THỐNG KÊ (dùng chung) ====================
            # Tiếng Việt có dấu
            "thống kê": {"film", "rental", "payment", "customer"},
            "báo cáo": {"film", "rental", "payment", "customer"},
            "tổng": {"payment", "rental"},
            "trung bình": {"payment", "rental", "film"},
            "cao nhất": {"payment", "rental", "film"},
            "thấp nhất": {"payment", "rental", "film"},
            "nhiều nhất": {"film", "actor", "customer", "rental"},
            "ít nhất": {"film", "actor", "customer", "rental"},
            "top": {"film", "actor", "customer", "rental", "payment"},
            "xếp hạng": {"film", "actor", "customer"},
            
            # Tiếng Việt không dấu
            "thong ke": {"film", "rental", "payment", "customer"},
            "bao cao": {"film", "rental", "payment", "customer"},
            "tong": {"payment", "rental"},
            "trung binh": {"payment", "rental", "film"},
            "cao nhat": {"payment", "rental", "film"},
            "thap nhat": {"payment", "rental", "film"},
            "nhieu nhat": {"film", "actor", "customer", "rental"},
            "it nhat": {"film", "actor", "customer", "rental"},
            "xep hang": {"film", "actor", "customer"},
        }
        
        # Bảng quan hệ - khi cần join
        self.table_relationships: Dict[str, Set[str]] = {
            "film": {"language", "film_category", "film_actor", "inventory"},
            "actor": {"film_actor"},
            "category": {"film_category"},
            "customer": {"address", "rental", "payment"},
            "rental": {"inventory", "customer", "staff", "payment"},
            "payment": {"rental", "customer", "staff"},
            "store": {"address", "staff", "inventory"},
            "staff": {"address", "store"},
            "inventory": {"film", "store"},
            "address": {"city"},
            "city": {"country"},
            "film_actor": {"film", "actor"},
            "film_category": {"film", "category"},
        }
        
        # Priority của bảng (bảng chính quan trọng hơn)
        self.table_priority: Dict[str, int] = {
            "film": 10,
            "actor": 9,
            "customer": 9,
            "rental": 8,
            "payment": 8,
            "category": 7,
            "store": 7,
            "staff": 6,
            "inventory": 5,
            "address": 4,
            "city": 3,
            "country": 2,
            "language": 2,
            "film_actor": 1,
            "film_category": 1,
        }
    
    def select_tables(self, question: str, max_tables: int = 5) -> List[str]:
        """
        Chọn các bảng liên quan nhất dựa trên câu hỏi
        
        Args:
            question: Câu hỏi của người dùng
            max_tables: Số bảng tối đa cần lấy
            
        Returns:
            Danh sách tên bảng được sắp xếp theo độ liên quan
        """
        question_lower = question.lower()
        selected_tables: Set[str] = set()
        table_scores: Dict[str, int] = {}
        
        # Bước 1: Tìm bảng dựa trên từ khóa
        for keyword, tables in self.keyword_to_tables.items():
            if keyword in question_lower:
                for table in tables:
                    selected_tables.add(table)
                    table_scores[table] = table_scores.get(table, 0) + 2
        
        # Bước 2: Thêm bảng quan hệ cần thiết cho JOIN
        tables_to_add = set()
        for table in selected_tables:
            if table in self.table_relationships:
                related = self.table_relationships[table]
                for rel_table in related:
                    if rel_table in selected_tables:
                        continue
                    for keyword, ktables in self.keyword_to_tables.items():
                        if rel_table in ktables and keyword in question_lower:
                            tables_to_add.add(rel_table)
                            table_scores[rel_table] = table_scores.get(rel_table, 0) + 1
        
        selected_tables.update(tables_to_add)
        
        # Bước 3: Thêm bridge tables nếu cần
        selected_list = list(selected_tables)
        for i, t1 in enumerate(selected_list):
            for t2 in selected_list[i+1:]:
                bridge = self._find_bridge_table(t1, t2)
                if bridge and bridge not in selected_tables:
                    selected_tables.add(bridge)
                    table_scores[bridge] = table_scores.get(bridge, 0) + 1
        
        # Bước 4: Sắp xếp theo score và priority
        def sort_key(table):
            score = table_scores.get(table, 0)
            priority = self.table_priority.get(table, 0)
            return (score * 10 + priority, priority)
        
        sorted_tables = sorted(selected_tables, key=sort_key, reverse=True)
        
        return sorted_tables[:max_tables]
    
    def _find_bridge_table(self, table1: str, table2: str) -> str | None:
        """Tìm bảng trung gian để join 2 bảng"""
        bridge_mappings = {
            frozenset({"film", "actor"}): "film_actor",
            frozenset({"film", "category"}): "film_category",
            frozenset({"city", "customer"}): "address",
            frozenset({"country", "customer"}): "city",
        }
        
        key = frozenset({table1, table2})
        return bridge_mappings.get(key)
    
    @lru_cache(maxsize=100)
    def get_table_dependencies(self, table: str) -> Set[str]:
        """Lấy các bảng phụ thuộc (cần cho foreign key)"""
        dependencies = {
            "film": {"language"},
            "film_actor": {"film", "actor"},
            "film_category": {"film", "category"},
            "customer": {"address"},
            "address": {"city"},
            "city": {"country"},
            "rental": {"inventory", "customer", "staff"},
            "payment": {"customer", "staff", "rental"},
            "inventory": {"film", "store"},
            "store": {"address"},
            "staff": {"address", "store"},
        }
        return dependencies.get(table, set())


# Singleton instance
_selector = None

def get_table_selector() -> TableSelector:
    global _selector
    if _selector is None:
        _selector = TableSelector()
    return _selector

"""
Text-to-SQL Streamlit Demo Application
Professional dark theme with activity log panel
"""

import streamlit as st
import logging
import sys
from datetime import datetime
from typing import List, Dict, Any
import time

# Configure page - must be first Streamlit command
st.set_page_config(
    page_title="Text-to-SQL with Neo4j Graph RAG",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for dark professional theme
st.markdown("""
<style>
    /* Main background */
    .stApp {
        background-color: #1a1a2e;
    }
    
    /* Sidebar styling */
    [data-testid="stSidebar"] {
        background-color: #16213e;
    }
    
    [data-testid="stSidebar"] .stMarkdown {
        color: #e0e0e0;
    }
    
    /* Headers */
    h1, h2, h3 {
        color: #4ecca3 !important;
    }
    
    /* Chat messages */
    .user-message {
        background-color: #0f3460;
        border-radius: 10px;
        padding: 15px;
        margin: 10px 0;
        border-left: 4px solid #4ecca3;
    }
    
    .assistant-message {
        background-color: #1a1a2e;
        border-radius: 10px;
        padding: 15px;
        margin: 10px 0;
        border-left: 4px solid #e94560;
    }
    
    /* SQL code block */
    .sql-block {
        background-color: #0d1b2a;
        border-radius: 8px;
        padding: 15px;
        font-family: 'Consolas', 'Monaco', monospace;
        border: 1px solid #4ecca3;
    }
    
    /* Log panel */
    .log-container {
        background-color: #0d1b2a;
        border-radius: 8px;
        padding: 10px;
        height: 600px;
        overflow-y: auto;
        font-family: 'Consolas', 'Monaco', monospace;
        font-size: 12px;
    }
    
    .log-entry {
        padding: 5px 10px;
        margin: 2px 0;
        border-radius: 4px;
    }
    
    .log-info {
        background-color: #1e3a5f;
        color: #82aaff;
    }
    
    .log-success {
        background-color: #1e3d2e;
        color: #4ecca3;
    }
    
    .log-warning {
        background-color: #3d3a1e;
        color: #ffd93d;
    }
    
    .log-error {
        background-color: #3d1e1e;
        color: #e94560;
    }
    
    /* Input styling */
    .stTextInput > div > div > input {
        background-color: #16213e;
        color: #e0e0e0;
        border: 1px solid #4ecca3;
    }
    
    /* Button styling */
    .stButton > button {
        background-color: #4ecca3;
        color: #1a1a2e;
        font-weight: bold;
        border: none;
        border-radius: 8px;
        padding: 10px 20px;
    }
    
    .stButton > button:hover {
        background-color: #3db892;
        color: #1a1a2e;
    }
    
    /* Metrics */
    [data-testid="stMetricValue"] {
        color: #4ecca3;
    }
    
    /* Tables info */
    .table-badge {
        display: inline-block;
        background-color: #0f3460;
        color: #4ecca3;
        padding: 5px 12px;
        border-radius: 15px;
        margin: 3px;
        font-size: 13px;
    }
    
    /* Expander */
    .streamlit-expanderHeader {
        background-color: #16213e;
        color: #e0e0e0;
    }
    
    /* Activity log header */
    .activity-header {
        background-color: #e94560;
        color: white;
        padding: 10px 15px;
        border-radius: 8px 8px 0 0;
        font-weight: bold;
        display: flex;
        align-items: center;
        gap: 10px;
    }
    
    .activity-header svg {
        width: 20px;
        height: 20px;
    }
    
    /* Log step badges */
    .step-badge {
        display: inline-block;
        padding: 3px 8px;
        border-radius: 4px;
        font-size: 11px;
        font-weight: bold;
        margin-right: 8px;
    }
    
    .step-vector { background-color: #4ecca3; color: #1a1a2e; }
    .step-search { background-color: #82aaff; color: #1a1a2e; }
    .step-graph { background-color: #ffd93d; color: #1a1a2e; }
    .step-llm { background-color: #e94560; color: white; }
    .step-crawl { background-color: #9b59b6; color: white; }
</style>
""", unsafe_allow_html=True)


class StreamlitLogHandler(logging.Handler):
    """Custom log handler that captures logs for Streamlit display."""
    
    def __init__(self):
        super().__init__()
        self.logs: List[Dict[str, Any]] = []
    
    def emit(self, record):
        log_entry = {
            "timestamp": datetime.now().strftime("%H:%M:%S.%f")[:-3],
            "level": record.levelname,
            "message": self.format(record),
            "step": self._detect_step(record.getMessage())
        }
        self.logs.append(log_entry)
    
    def _detect_step(self, message: str) -> str:
        """Detect the processing step from log message."""
        message_lower = message.lower()
        if "vector" in message_lower or "embedding" in message_lower:
            return "vector"
        elif "search" in message_lower:
            return "search"
        elif "graph" in message_lower or "traversal" in message_lower or "expand" in message_lower:
            return "graph"
        elif "llm" in message_lower or "gpt" in message_lower or "prompt" in message_lower:
            return "llm"
        elif "crawl" in message_lower:
            return "crawl"
        return "info"
    
    def get_logs(self) -> List[Dict[str, Any]]:
        return self.logs
    
    def clear(self):
        self.logs = []


# Initialize session state
if "messages" not in st.session_state:
    st.session_state.messages = []

if "log_handler" not in st.session_state:
    st.session_state.log_handler = StreamlitLogHandler()
    
if "text2sql_engine" not in st.session_state:
    st.session_state.text2sql_engine = None
    st.session_state.initialized = False


def init_text2sql():
    """Initialize the Text2SQL engine."""
    try:
        from dotenv import load_dotenv
        load_dotenv(override=True)
        
        from src.sql_generator import Text2SQLEngine
        
        # Setup logging to capture
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)
        
        # Remove existing handlers
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)
        
        # Add our custom handler
        st.session_state.log_handler.clear()
        logger.addHandler(st.session_state.log_handler)
        
        # Also add console handler for debugging
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logger.addHandler(console_handler)
        
        st.session_state.text2sql_engine = Text2SQLEngine()
        st.session_state.initialized = True
        
        return True
    except Exception as e:
        st.error(f"Failed to initialize: {str(e)}")
        return False


def render_log_panel():
    """Render the activity log panel using Streamlit native components."""
    
    st.markdown("""
        <div class="activity-header">
            📋 Activity Log
        </div>
    """, unsafe_allow_html=True)
    
    logs = st.session_state.log_handler.get_logs()
    
    if not logs:
        st.info("Logs will appear here when you ask a question...")
        return
    
    # Create a container with fixed height and scroll
    log_container = st.container(height=500)
    
    with log_container:
        for log in logs:
            level = log["level"]
            step = log["step"]
            timestamp = log["timestamp"]
            message = log["message"]
            
            # Skip empty or separator lines
            if not message.strip() or message.strip() in ["=" * 60, "-" * 40, "----------------------------------------"]:
                continue
            
            # Truncate long messages
            display_msg = message[:200] + "..." if len(message) > 200 else message
            
            # Detect step from message content for better categorization
            if "[STEP 1/4]" in message or "Vector" in message or "embedding" in message.lower():
                st.markdown(f"🔢 `{timestamp}` <span style='color: #4ecca3;'>{display_msg}</span>", unsafe_allow_html=True)
            elif "[STEP 2/4]" in message or "Extract" in message:
                st.markdown(f"📊 `{timestamp}` <span style='color: #82aaff;'>{display_msg}</span>", unsafe_allow_html=True)
            elif "[STEP 3/4]" in message or "Graph" in message or "traversal" in message.lower():
                st.markdown(f"🕸️ `{timestamp}` <span style='color: #ffd93d;'>{display_msg}</span>", unsafe_allow_html=True)
            elif "[STEP 4/4]" in message or "[LLM]" in message or "gpt" in message.lower():
                st.markdown(f"🤖 `{timestamp}` <span style='color: #e94560;'>{display_msg}</span>", unsafe_allow_html=True)
            elif level == "ERROR":
                st.error(f"`{timestamp}` ❌ {display_msg}")
            elif level == "WARNING":
                st.warning(f"`{timestamp}` ⚠️ {display_msg}")
            elif "success" in message.lower() or "complete" in message.lower() or "✅" in message:
                st.success(f"`{timestamp}` {display_msg}")
            else:
                # Default - light gray
                st.markdown(f"<span style='color: #888;'>`{timestamp}` {display_msg}</span>", unsafe_allow_html=True)
    
    # Legend
    st.markdown("""
    <div style="margin-top: 10px; padding: 8px; background: #1a1a2e; border-radius: 5px; font-size: 11px;">
        <span style="color: #4ecca3;">🔢 Vector Search</span> &nbsp;|&nbsp;
        <span style="color: #82aaff;">📊 Extract</span> &nbsp;|&nbsp;
        <span style="color: #ffd93d;">🕸️ Graph</span> &nbsp;|&nbsp;
        <span style="color: #e94560;">🤖 LLM</span>
    </div>
    """, unsafe_allow_html=True)


def render_result(result: Dict[str, Any]):
    """Render the SQL generation result."""
    
    # Tables section
    tables = result.get("tables", [])
    if tables:
        st.markdown("**📊 Relevant Tables:**")
        table_html = '<div style="margin: 10px 0;">'
        for table in tables:
            table_html += f'<span class="table-badge">{table}</span>'
        table_html += '</div>'
        st.markdown(table_html, unsafe_allow_html=True)
    
    # Confidence
    confidence = result.get("confidence", 0)
    col1, col2 = st.columns([1, 3])
    with col1:
        st.metric("Confidence", f"{confidence:.1%}")
    
    # SQL Query
    st.markdown("**📝 Generated SQL:**")
    sql = result.get("sql", "")
    st.code(sql, language="sql")
    
    # Copy button
    if sql:
        st.button("📋 Copy SQL", key=f"copy_{hash(sql)}", 
                  on_click=lambda: st.write("SQL copied!"))


def main():
    # Sidebar
    with st.sidebar:
        st.markdown("## ⚙️ Configuration")
        
        # Connection status
        if st.session_state.initialized:
            st.success("✅ Connected to Neo4j")
        else:
            st.warning("⚠️ Not initialized")
            if st.button("🔌 Initialize Connection"):
                with st.spinner("Connecting..."):
                    if init_text2sql():
                        st.success("Connected!")
                        st.rerun()
        
        st.markdown("---")
        
        # Settings
        st.markdown("### 🎛️ Settings")
        top_k = st.slider("Vector Search Results", 5, 20, 10)
        show_prompt = st.checkbox("Show LLM Prompt", value=False)
        
        st.markdown("---")
        
        # Actions
        st.markdown("### 🧹 Actions")
        if st.button("🗑️ Clear Chat"):
            st.session_state.messages = []
            st.rerun()
        
        if st.button("📋 Clear Logs"):
            st.session_state.log_handler.clear()
            st.rerun()
    
    # Main content area
    col_chat, col_logs = st.columns([3, 2])
    
    with col_chat:
        st.markdown("# 🔍 Text-to-SQL with Neo4j Graph RAG")
        st.markdown("*Ask questions in natural language and get SQL queries*")
        
        st.markdown("---")
        
        # Chat history
        chat_container = st.container()
        
        with chat_container:
            for msg in st.session_state.messages:
                if msg["role"] == "user":
                    st.markdown(f"""
                        <div class="user-message">
                            <strong>❓ You:</strong><br>
                            {msg["content"]}
                        </div>
                    """, unsafe_allow_html=True)
                else:
                    st.markdown(f"""
                        <div class="assistant-message">
                            <strong>🤖 Assistant:</strong>
                        </div>
                    """, unsafe_allow_html=True)
                    if "result" in msg:
                        render_result(msg["result"])
        
        # Input
        st.markdown("---")
        
        with st.form(key="question_form", clear_on_submit=True):
            user_input = st.text_input(
                "Enter your question:",
                placeholder="e.g., Cho tôi các order trong tháng 11 năm 2025",
                label_visibility="collapsed"
            )
            submit_button = st.form_submit_button("🚀 Generate SQL", use_container_width=True)
        
        if submit_button and user_input:
            if not st.session_state.initialized:
                st.warning("Please initialize the connection first!")
            else:
                # Clear previous logs
                st.session_state.log_handler.clear()
                
                # Add user message
                st.session_state.messages.append({
                    "role": "user",
                    "content": user_input
                })
                
                # Generate SQL
                with st.spinner("🔄 Processing..."):
                    try:
                        result = st.session_state.text2sql_engine.generate_sql(
                            user_input,
                            top_k=top_k
                        )
                        
                        # Convert to dict for display
                        result_dict = {
                            "sql": result.sql,
                            "tables": result.relevant_tables,
                            "confidence": result.confidence_score,
                            "error": result.error
                        }
                        
                        # Add assistant message
                        st.session_state.messages.append({
                            "role": "assistant",
                            "content": result.sql,
                            "result": result_dict
                        })
                        
                    except Exception as e:
                        st.error(f"Error: {str(e)}")
                        st.session_state.messages.append({
                            "role": "assistant", 
                            "content": f"Error: {str(e)}",
                            "result": {"sql": "", "tables": [], "confidence": 0}
                        })
                
                st.rerun()
    
    with col_logs:
        render_log_panel()
        
        # Log legend
        st.markdown("""
            <div style="margin-top: 10px; font-size: 11px; color: #666;">
                <span class="step-badge step-vector">Vector</span> Embedding & Search
                <span class="step-badge step-search">Search</span> Finding matches
                <span class="step-badge step-graph">Graph</span> Traversal
                <span class="step-badge step-llm">LLM</span> SQL Generation
            </div>
        """, unsafe_allow_html=True)


if __name__ == "__main__":
    main()

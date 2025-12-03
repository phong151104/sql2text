"""
LLM-based SQL generator.
"""

from __future__ import annotations

import logging
import re
from typing import Any, Dict, List

from openai import OpenAI

from ..config import config

logger = logging.getLogger(__name__)


class LLMSQLGenerator:
    """
    Generates SQL queries using LLM.
    """
    
    def __init__(
        self,
        api_key: str | None = None,
        model: str | None = None,
        temperature: float | None = None,
    ):
        self.api_key = api_key or config.openai.api_key
        self.model = model or config.openai.chat_model
        self.temperature = temperature if temperature is not None else config.openai.temperature
        self.client = OpenAI(api_key=self.api_key)
    
    def generate(
        self,
        messages: List[Dict[str, str]],
        max_tokens: int = 1000,
    ) -> str:
        """
        Generate SQL from chat messages.
        
        Args:
            messages: List of message dicts with role and content
            max_tokens: Maximum tokens in response
            
        Returns:
            Generated SQL query
        """
        logger.info(f"Generating SQL with {self.model}...")
        
        # Log prompt sent to LLM
        logger.info("=" * 60)
        logger.info("PROMPT SENT TO LLM:")
        logger.info("=" * 60)
        for msg in messages:
            role = msg.get("role", "unknown")
            content = msg.get("content", "")
            logger.info(f"[{role.upper()}]:")
            # Log content with indentation for readability
            for line in content.split('\n'):
                logger.info(f"  {line}")
            logger.info("-" * 40)
        logger.info("=" * 60)
        
        response = self.client.chat.completions.create(
            model=self.model,
            messages=messages,
            temperature=self.temperature,
            max_tokens=max_tokens,
        )
        
        content = response.choices[0].message.content
        
        # Log raw response
        logger.info("LLM RAW RESPONSE:")
        logger.info(content)
        logger.info("=" * 60)
        
        # Extract SQL from response (handle code blocks)
        sql = self._extract_sql(content)
        
        logger.info("SQL generated successfully")
        return sql
    
    def _extract_sql(self, content: str) -> str:
        """
        Extract SQL query from LLM response.
        
        Handles responses with or without code blocks.
        """
        if not content:
            return ""
        
        # Try to extract from code block
        sql_block_pattern = r"```(?:sql)?\s*([\s\S]*?)```"
        matches = re.findall(sql_block_pattern, content, re.IGNORECASE)
        
        if matches:
            return matches[0].strip()
        
        # If no code block, assume entire response is SQL
        # Remove common prefixes
        content = content.strip()
        for prefix in ["SQL:", "Query:", "Here is the SQL:", "Here's the SQL:"]:
            if content.lower().startswith(prefix.lower()):
                content = content[len(prefix):].strip()
        
        return content
    
    def generate_with_retry(
        self,
        messages: List[Dict[str, str]],
        max_retries: int = 2,
        error_feedback: str | None = None,
    ) -> str:
        """
        Generate SQL with retry on failure.
        
        Args:
            messages: List of message dicts
            max_retries: Maximum retry attempts
            error_feedback: Error message to include in retry
            
        Returns:
            Generated SQL query
        """
        last_error = None
        
        for attempt in range(max_retries + 1):
            try:
                if attempt > 0 and error_feedback:
                    # Add error feedback for retry
                    retry_message = {
                        "role": "user",
                        "content": f"The previous SQL had an error: {error_feedback}\nPlease fix it."
                    }
                    messages = messages + [retry_message]
                
                return self.generate(messages)
            
            except Exception as e:
                last_error = e
                logger.warning(f"SQL generation attempt {attempt + 1} failed: {e}")
        
        raise last_error

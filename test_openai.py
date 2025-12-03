"""Simple test script for OpenAI API key"""

import os
from dotenv import load_dotenv
from openai import OpenAI

# Force override existing environment variables
load_dotenv(override=True)

api_key = os.getenv("OPENAI_API_KEY")
print(f"API Key loaded: {api_key[:20]}...{api_key[-10:]}")
print(f"Key length: {len(api_key)}")

print("\nTesting OpenAI API...")
try:
    client = OpenAI(api_key=api_key)
    
    # Simple embedding test
    response = client.embeddings.create(
        model="text-embedding-3-small",
        input="Hello world"
    )
    
    print("✅ SUCCESS! API key is valid.")
    print(f"Embedding dimension: {len(response.data[0].embedding)}")
    
except Exception as e:
    print(f"❌ FAILED: {e}")

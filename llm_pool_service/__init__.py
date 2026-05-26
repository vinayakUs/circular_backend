"""LLM Pool Service — Unix-socket-based shared LLM connection pool.

Run as: python -m llm_pool_service [--socket /tmp/llm_pool.sock] [--max-concurrent 3]
"""

from llm_pool_service.server import serve

if __name__ == "__main__":
    serve()
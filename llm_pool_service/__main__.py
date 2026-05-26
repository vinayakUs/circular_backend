"""Entry point for: python -m llm_pool_service"""

# Pre-import shared response models so the server can resolve them by name
import llm_pool_client.models  # noqa: F401

from llm_pool_service.server import serve

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="LLM Pool Service")
    parser.add_argument(
        "--socket", default="/tmp/llm_pool.sock", help="Unix socket path"
    )
    parser.add_argument(
        "--max-concurrent", type=int, default=3, help="Max concurrent NVIDIA calls"
    )
    args = parser.parse_args()
    serve(socket_path=args.socket, max_concurrent=args.max_concurrent)
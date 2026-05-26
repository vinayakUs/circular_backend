"""Standalone test script to verify the LLM pool service.

Usage:
    # Terminal 1 — start the service
    python -m llm_pool_service --max-concurrent 2

    # Terminal 2 — run the test
    python -m llm_pool_client.test_pool --count 20
"""

from __future__ import annotations

import argparse
import logging
import sys
import time

from pydantic import BaseModel

from llm_pool_client.models import AnswerResponse, MarketAnalysisResponse

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger(__name__)


def test_single(pool: "LLMPool") -> bool:
    """Submit one request with structured output and verify result."""
    logger.info("TEST: single request (with instructor)")
    handle = pool.submit(
        prompt='Analyze: Nifty rose 2% today on strong FII buying. Respond ONLY with valid JSON: {"sentiment":"bullish| bearish|neutral","key_factors":["factor1","factor2","factor3"],"risk_level":"low|medium|high","confidence":0.0-1.0}. No other text.',
        model="minimaxai/minimax-m2.7",
        response_model="MarketAnalysisResponse",
    )
    result = handle.get_result(timeout=60)
    logger.info("TEST: single request result: %s", result)
    return result is not None


def test_concurrent(pool: "LLMPool", count: int) -> tuple[int, float]:
    """Submit N requests concurrently, return success count and total time."""
    logger.info("TEST: concurrent %d requests", count)
    start = time.monotonic()

    handles = []
    for i in range(count):
        h = pool.submit(
            prompt=f"Analyze market: Stock ABC {'rose' if i % 2 == 0 else 'fell'} {i}% today. Sentiment, 3 key factors, risk, confidence.",
            model="minimaxai/minimax-m2.7",
            response_model="MarketAnalysisResponse",
        )
        handles.append(h)

    total_time = time.monotonic() - start
    logger.info("TEST: submitted %d requests in %.2fs", count, total_time)

    # Wait for all results
    results = pool.wait_all(handles, timeout=300)
    elapsed = time.monotonic() - start

    success = sum(1 for r in results if r is not None)
    logger.info(
        "TEST: %d/%d succeeded in %.2fs (%.1f req/s)",
        success, count, elapsed, count / elapsed,
    )
    return success, elapsed


def test_overflow(pool: "LLMPool", count: int) -> None:
    """Submit many requests to verify queue behavior."""
    logger.info("TEST: overflow — submitting %d requests", count)
    start = time.monotonic()
    handles = [
        pool.submit(prompt=f"Quick analysis: Stock {i} {'up' if i % 2 == 0 else 'down'} {i}% today. Sentiment, risk, confidence.", model="minimaxai/minimax-m2.7", response_model="MarketAnalysisResponse")
        for i in range(count)
    ]
    elapsed = time.monotonic() - start
    logger.info("TEST: all %d submitted in %.2fs", count, elapsed)

    # Wait for all results
    results = pool.wait_all(handles, timeout=600)
    total_elapsed = time.monotonic() - start

    success = sum(1 for r in results if r is not None)
    logger.info(
        "TEST: overflow %d/%d completed in %.2fs (%.1f req/s)",
        success, count, total_elapsed, count / total_elapsed,
    )


def main():
    parser = argparse.ArgumentParser(description="Test LLM pool service")
    parser.add_argument(
        "--socket", default="/tmp/llm_pool.sock", help="Unix socket path"
    )
    parser.add_argument(
        "--count", type=int, default=20, help="Number of concurrent requests"
    )
    parser.add_argument(
        "--overflow", action="store_true", help="Run overflow test with 100 requests"
    )
    args = parser.parse_args()

    from llm_pool_client import LLMPool

    pool = LLMPool(socket_path=args.socket)

    if args.overflow:
        test_overflow(pool, 100)
        return

    # Basic single request
    if not test_single(pool):
        logger.error("Single request test failed")
        sys.exit(1)

    # Concurrent burst
    success, elapsed = test_concurrent(pool, args.count)
    if success < args.count:
        logger.warning("Only %d/%d succeeded", success, args.count)
    else:
        logger.info("All tests passed!")


if __name__ == "__main__":
    main()
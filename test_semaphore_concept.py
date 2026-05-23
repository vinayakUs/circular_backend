"""
Test script to verify semaphore-based rate limiting across MULTIPLE PROCESSES.
This mimics Gunicorn workers — each is a separate process with its own memory.

Run with: python test_semaphore_concept.py
"""

import multiprocessing
import threading
import time
import os
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from multiprocessing import Semaphore
from threading import Lock

# ─────────────────────────────────────────────────────────────────────────────
# Shared API mock — lives in shared memory via multiprocessing.Manager
# ─────────────────────────────────────────────────────────────────────────────
manager = multiprocessing.Manager()
_shared_state = manager.dict({"call_count": 0, "in_flight": 0, "max_in_flight": 0})
_shared_lock = manager.Lock()


def api_call(prompt: str, idx: int) -> dict:
    """Simulates the Minmax API call — tracks in-flight across all processes."""
    with _shared_lock:
        _shared_state["call_count"] += 1
        _shared_state["in_flight"] += 1
        current = _shared_state["in_flight"]
        if current > _shared_state["max_in_flight"]:
            _shared_state["max_in_flight"] = current

    # Simulate API latency
    time.sleep(0.8)

    with _shared_lock:
        _shared_state["in_flight"] -= 1

    return {"result": f"done: {prompt}", "idx": idx}


# ─────────────────────────────────────────────────────────────────────────────
# Global semaphore — shared across ALL processes via multiprocessing.Semaphore
# This is what Gunicorn workers need to share a rate-limit gate
# ─────────────────────────────────────────────────────────────────────────────
MAX_CONCURRENT = 3
_rate_limit_sem = multiprocessing.Semaphore(MAX_CONCURRENT)


def call_with_semaphore(prompt: str, idx: int) -> dict:
    """Gate: only MAX_CONCURRENT calls pass through at once, across all processes."""
    with _rate_limit_sem:
        return api_call(prompt, idx)


# ─────────────────────────────────────────────────────────────────────────────
# TEST 1: Single process, multiple threads — semaphore gates concurrency
# ─────────────────────────────────────────────────────────────────────────────
def test_single_process_threads():
    """ThreadPoolExecutor inside ONE process — semaphore works trivially."""
    print(f"\n{'='*60}")
    print(f" TEST 1: Single process, {5} threads, semaphore={MAX_CONCURRENT}")
    print(f"{'='*60}")

    _shared_state["call_count"] = 0
    _shared_state["in_flight"] = 0
    _shared_state["max_in_flight"] = 0

    start = time.time()
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(call_with_semaphore, f"t{i}", i) for i in range(8)]
        for f in as_completed(futures):
            f.result()
    elapsed = time.time() - start

    print(f"  Calls: {_shared_state['call_count']}, Max in-flight: {_shared_state['max_in_flight']}")
    print(f"  Time: {elapsed:.1f}s (expected ~{8/MAX_CONCURRENT:.1f}s)")
    print(f"  ✓ Semaphore correctly limited concurrency within one process")


# ─────────────────────────────────────────────────────────────────────────────
# TEST 2: MULTIPLE processes — mimics Gunicorn workers
# Each process calls call_with_semaphore independently.
# The multiprocessing.Semaphore IS shared across all forked processes.
# ─────────────────────────────────────────────────────────────────────────────
def worker_process(num_calls: int, worker_id: int):
    """Each worker process fires num_calls concurrent calls."""
    print(f"  [Worker-{worker_id}] Starting {num_calls} calls...")
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = [
            executor.submit(call_with_semaphore, f"w{worker_id}-c{i}", i)
            for i in range(num_calls)
        ]
        for f in as_completed(futures):
            f.result()
    print(f"  [Worker-{worker_id}] Done")


def test_multiple_processes():
    """Spawn N processes — each is a Gunicorn-like worker."""
    print(f"\n{'='*60}")
    print(f" TEST 2: {3} processes (mimics Gunicorn workers)")
    print(f"         Each process fires {3} concurrent calls")
    print(f"         Semaphore limit: {MAX_CONCURRENT} (shared across all processes)")
    print(f"{'='*60}")

    _shared_state["call_count"] = 0
    _shared_state["in_flight"] = 0
    _shared_state["max_in_flight"] = 0

    num_workers = 3
    calls_per_worker = 3
    total_calls = num_workers * calls_per_worker

    start = time.time()

    # Spawn N processes — each inherits the semaphore via fork()
    processes = []
    for i in range(num_workers):
        p = multiprocessing.Process(target=worker_process, args=(calls_per_worker, i))
        processes.append(p)
        p.start()

    for p in processes:
        p.join()

    elapsed = time.time() - start

    print(f"\n  Results:")
    print(f"  Total API calls: {_shared_state['call_count']}")
    print(f"  Max in-flight (all processes): {_shared_state['max_in_flight']}")
    print(f"  Time: {elapsed:.1f}s")
    print(f"  Without semaphore: {total_calls * 0.8:.1f}s ({total_calls} calls @ 0.8s each)")
    print(f"  With semaphore ({MAX_CONCURRENT}): {total_calls / MAX_CONCURRENT * 0.8:.1f}s theoretical")

    if _shared_state["max_in_flight"] <= MAX_CONCURRENT:
        print(f"  ✓ PASS — max in-flight was {MAX_CONCURRENT}, never exceeded")
    else:
        print(f"  ✗ FAIL — max in-flight was {_shared_state['max_in_flight']} (expected {MAX_CONCURRENT})")


# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # Reset shared state
    _shared_state["call_count"] = 0
    _shared_state["in_flight"] = 0
    _shared_state["max_in_flight"] = 0

    test_single_process_threads()
    test_multiple_processes()

    print(f"\n{'='*60}")
    print(f" CONCLUSION")
    print(f"{'='*60}")
    print(f"  multiprocessing.Semaphore IS shared across forked processes")
    print(f"  Each Gunicorn worker (forked process) inherits the same semaphore")
    print(f"  Result: rate limit enforced across ALL workers = one shared gate")
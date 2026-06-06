#!/usr/bin/env python
"""Sample service that takes 10 seconds to run, demonstrating flock-based concurrency prevention."""
import os
import sys
import time
import fcntl
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

LOCK_FILE = "/tmp/circular_sample.lock"

def main():
    # Try to acquire exclusive lock (non-blocking)
    try:
        lock_fd = open(LOCK_FILE, 'w')
        fcntl.flock(lock_fd.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        lock_fd.write(str(os.getpid()))
        lock_fd.flush()
    except (IOError, OSError) as e:
        logger.info(f"Another instance is running! Exiting. (lock exists)")
        sys.exit(0)

    logger.info(f"Started process {os.getpid()}, will run for 10 seconds...")

    # Simulate work
    for i in range(10):
        time.sleep(1)
        logger.info(f"  ... {i+1}/10 seconds")

    logger.info("Work complete! Releasing lock.")

    # Release lock and remove file
    fcntl.flock(lock_fd.fileno(), fcntl.LOCK_UN)
    lock_fd.close()
    os.remove(LOCK_FILE)

if __name__ == "__main__":
    main()
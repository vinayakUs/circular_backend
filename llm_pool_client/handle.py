"""RequestHandle — tracks the state of a single submitted request."""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class RequestHandle:
    """A handle returned to callers after submitting a request to the pool.

    The handle is not tied to a specific socket connection — once accepted,
    the result is stored server-side and retrieved on demand via the same
    LLMPool instance.
    """

    request_id: str
    _pool: "LLMPool"  # back-reference

    def is_done(self) -> bool:
        """Poll whether this request has completed."""
        msg = {"type": "status", "id": self.request_id}
        self._pool._send_raw(msg)
        resp = self._pool._recv_raw()
        if resp is None:
            return False
        return resp.get("type") == "result"

    def get_result(self, timeout: float | None = 30.0) -> Any:
        """Block until result is available or timeout.

        Args:
            timeout: Seconds to wait. None = wait forever. 0 = no wait.

        Returns:
            The `data` field from the server ResultResponse.

        Raises:
            TimeoutError: if timeout is reached before result.
            RuntimeError: if server returns an error.
        """
        start = time.monotonic()
        while True:
            if timeout is not None and timeout <= 0:
                raise TimeoutError(f"Request {self.request_id} timed out")

            msg = {"type": "status", "id": self.request_id}
            self._pool._send_raw(msg)
            resp = self._pool._recv_raw()

            if resp is None:
                raise RuntimeError(f"Connection lost for request {self.request_id}")

            if resp.get("type") == "result":
                if not resp.get("ok", False):
                    raise RuntimeError(resp.get("error", "unknown error"))
                return resp.get("data")

            # resp is "pending" — wait and retry
            remaining = None
            if timeout is not None:
                elapsed = time.monotonic() - start
                remaining = timeout - elapsed
                if remaining <= 0:
                    raise TimeoutError(f"Request {self.request_id} timed out")
            time.sleep(0.5 if remaining is None else min(0.5, remaining))

    @property
    def content(self) -> Any:
        """Shorthand for get_result()."""
        return self.get_result()
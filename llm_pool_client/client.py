"""LLMPool — Unix socket client for the shared LLM pool service."""

from __future__ import annotations

import json
import logging
import socket
import threading
import time
from typing import Any

from llm_pool_client.handle import RequestHandle

logger = logging.getLogger(__name__)


class LLMPool:
    """Client for the shared LLM pool service.

    Submit requests via Unix socket, receive a RequestHandle, poll or wait
    for results. Reconnects automatically on connection drop.
    """

    def __init__(
        self,
        socket_path: str = "/tmp/llm_pool.sock",
        connect_timeout: float = 5.0,
    ):
        self._socket_path = socket_path
        self._connect_timeout = connect_timeout
        self._lock = threading.Lock()
        self._ensure_connected()

    def _ensure_connected(self) -> None:
        with self._lock:
            if hasattr(self, "_conn") and self._conn is not None:
                return
            self._conn = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            self._conn.settimeout(self._connect_timeout)
            self._conn.connect(self._socket_path)
            logger.info("Connected to LLM pool service at %s", self._socket_path)

    def _send_raw(self, msg: dict) -> None:
        """Send a JSON message (must hold lock)."""
        self._ensure_connected()
        with self._lock:
            self._conn.sendall(json.dumps(msg).encode("utf-8"))

    def _recv_raw(self) -> dict | None:
        """Receive a JSON response (must hold lock)."""
        with self._lock:
            try:
                self._conn.settimeout(5.0)
                data = self._conn.recv(8192)
                if not data:
                    return None
                return json.loads(data.decode("utf-8"))
            except (socket.timeout, ConnectionResetError, BrokenPipeError, OSError):
                return None
            except json.JSONDecodeError:
                return None

    def submit(
        self,
        prompt: str,
        model: str = "minimaxai/minimax-m2.7",
        response_model: str | None = None,
        timeout: int = 30,
    ) -> RequestHandle:
        """Submit a prompt to the pool.

        Returns immediately with a RequestHandle. Use handle.get_result()
        or handle.is_done() to retrieve the output.

        Args:
            prompt: The user prompt / request string.
            model: Model name (default from Config.ACTION_ITEM_MODEL).
            response_model: Optional Pydantic model class name for
                           structured output.
            timeout: Per-request timeout in seconds.
        """
        import uuid

        request_id = str(uuid.uuid4())
        msg = {
            "type": "submit",
            "id": request_id,
            "prompt": prompt,
            "model": model,
            "response_model": response_model,
            "timeout": timeout,
        }
        self._send_raw(msg)
        resp = self._recv_raw()

        if resp is None:
            raise RuntimeError("Connection lost during submit")

        if resp.get("type") == "rejected":
            raise RuntimeError(f"Request rejected: {resp.get('reason')}")

        if resp.get("type") != "accepted":
            raise RuntimeError(f"Unexpected response: {resp}")

        return RequestHandle(request_id=request_id, _pool=self)

    def wait_all(
        self,
        handles: list[RequestHandle],
        timeout: float = 60.0,
    ) -> list[Any]:
        """Block until all handles complete or timeout.

        Returns results in the same order as input handles.
        Raises if any handle errors.
        """
        start = time.monotonic()
        results: list[Any] = [None] * len(handles)
        remaining_handles = list(handles)

        while remaining_handles:
            # Check each handle without blocking too long
            still_pending = []
            for h in remaining_handles:
                remaining = None
                if timeout is not None:
                    elapsed = time.monotonic() - start
                    remaining = timeout - elapsed
                    if remaining <= 0:
                        raise TimeoutError(f"Request {h.request_id} timed out")

                # Non-blocking poll
                msg = {"type": "status", "id": h.request_id}
                self._send_raw(msg)
                resp = self._recv_raw()

                if resp is None:
                    still_pending.append(h)
                    continue

                if resp.get("type") == "result":
                    idx = handles.index(h)
                    if not resp.get("ok", False):
                        raise RuntimeError(resp.get("error", "unknown error"))
                    results[idx] = resp.get("data")
                else:
                    still_pending.append(h)

            if not still_pending:
                break

            remaining_handles = still_pending
            time.sleep(0.25)

        return results
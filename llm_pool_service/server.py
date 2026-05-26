"""Unix socket server for the shared LLM pool.

Routes incoming JSON messages, queues requests, dispatches to NvidiaPool workers.
"""

from __future__ import annotations

import queue
import selectors
import signal
import socket
import threading
import logging
import time
from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel

from config import Config
from llm_pool_service.nvidia_pool import LLMProviderPool
from llm_pool_service.protocol import (
    AcceptedResponse,
    PendingResponse,
    RejectedResponse,
    ResultResponse,
    StatusRequest,
    SubmitRequest,
    parse_message,
)

logger = logging.getLogger(__name__)


@dataclass
class QueuedRequest:
    submit: SubmitRequest
    future: threading.Event  # set when result is ready
    result: ResultResponse | None = None


class LLMQueueServer:
    """Shared LLM pool server over a Unix domain socket.

    Clients connect, submit prompts, receive immediate acknowledgment, then
    poll or wait for results. A semaphore limits concurrent NVIDIA API calls.
    """

    def __init__(
        self,
        socket_path: str = "/tmp/llm_pool.sock",
        max_concurrent: int = 3,
    ):
        self.socket_path = socket_path
        self._pool = LLMProviderPool(max_concurrent=max_concurrent)
        self._queue: queue.Queue[QueuedRequest] = queue.Queue()
        self._pending: dict[str, QueuedRequest] = {}  # request_id → QueuedRequest
        self._completed: dict[str, ResultResponse] = {}  # request_id → result (kept for later status checks)
        self._pending_lock = threading.Lock()
        self._running = True
        self._worker_thread: threading.Thread | None = None
        self._server_socket: socket.socket | None = None
        self._selector = selectors.DefaultSelector()

    def start(self) -> None:
        """Bind socket, start dispatcher worker, start accept loop."""
        self._running = True

        # Start background worker that processes the queue
        self._worker_thread = threading.Thread(target=self._worker_loop, daemon=True)
        self._worker_thread.start()

        # Remove stale socket file
        import os
        if os.path.exists(self.socket_path):
            os.unlink(self.socket_path)

        self._server_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._server_socket.bind(self.socket_path)
        self._server_socket.listen(50)
        self._server_socket.setblocking(False)
        os.chmod(self.socket_path, 0o600)
        logger.info("LLMQueueServer listening on %s", self.socket_path)

        # Handle shutdown signals
        signal.signal(signal.SIGTERM, self._on_signal)
        signal.signal(signal.SIGINT, self._on_signal)

        self._accept_loop()

    def _on_signal(self, sig, frame):
        logger.info("Received signal %d, shutting down", sig)
        self._running = False
        if self._server_socket:
            self._server_socket.close()
        import os
        if os.path.exists(self.socket_path):
            os.unlink(self.socket_path)

    def _accept_loop(self) -> None:
        """Accept connections and register them with the selector."""
        self._selector.register(self._server_socket, selectors.EVENT_READ, data=None)

        while self._running:
            try:
                events = self._selector.select(timeout=1.0)
                for key, mask in events:
                    sock = key.data
                    if sock is None:
                        # Server socket ready to accept
                        try:
                            conn, addr = self._server_socket.accept()
                            conn.setblocking(False)
                            self._selector.register(conn, selectors.EVENT_READ, data=conn)
                        except OSError:
                            pass
                    else:
                        # Client socket has data — handle, then keep it open for next message
                        self._handle_connection(sock)
            except OSError as e:
                if self._running:
                    logger.warning("accept loop error: %s", e)
                break

        # Drain selector on shutdown
        for key in self._selector.get_map().values():
            try:
                key.fileobj.close()
            except Exception:
                pass

    def _handle_connection(self, conn: socket.socket) -> None:
        """Handle one message from a client and return.

        The selector will call us again when the client sends more data
        (e.g., a status check). This avoids blocking on recv when the client
        is not sending anything.
        """
        try:
            try:
                data = conn.recv(8192)
            except BlockingIOError:
                # Client has nothing to send right now
                return
            if not data:
                # Client disconnected
                return

            msg = parse_message(data)

            if isinstance(msg, SubmitRequest):
                self._handle_submit(conn, msg)
            elif isinstance(msg, StatusRequest):
                self._handle_status(conn, msg)
            else:
                logger.warning("Unexpected message type: %s", type(msg))
        except Exception as e:
            logger.exception("Error handling connection: %s", e)

    def _handle_submit(self, conn: socket.socket, msg: SubmitRequest) -> None:
        """Queue a submission and send immediate accepted/rejected."""
        queued = QueuedRequest(submit=msg, future=threading.Event())

        with self._pending_lock:
            self._pending[msg.id] = queued

        self._queue.put(queued)

        if self._running:
            conn.sendall(AcceptedResponse(id=msg.id).to_json())
        else:
            conn.sendall(RejectedResponse(id=msg.id, reason="pool_shutdown").to_json())

    def _handle_status(self, conn: socket.socket, msg: StatusRequest) -> None:
        """Return current status of a request."""
        with self._pending_lock:
            queued = self._pending.get(msg.id)
            completed = self._completed.get(msg.id)

        if completed is not None:
            conn.sendall(completed.to_json())
            with self._pending_lock:
                self._completed.pop(msg.id, None)
            return

        if queued is None:
            conn.sendall(ResultResponse(id=msg.id, ok=False, error="not_found").to_json())
            return

        if queued.future.is_set():
            conn.sendall(queued.result.to_json() if queued.result else b"{}")
        else:
            # Approximate queue position
            position = sum(
                1 for q in list(self._queue.queue) + [queued]
                if not q.future.is_set()
            )
            conn.sendall(PendingResponse(id=msg.id, position=max(0, position - 1)).to_json())

    def _worker_loop(self) -> None:
        """Background thread: take queued requests, call NVIDIA, store result."""
        while self._running:
            try:
                queued = self._queue.get(timeout=1.0)
            except queue.Empty:
                continue

            result = self._process_request(queued)
            queued.result = result
            queued.future.set()

            with self._pending_lock:
                self._pending.pop(queued.submit.id, None)
                self._completed[queued.submit.id] = result

    def _process_request(self, queued: QueuedRequest) -> ResultResponse:
        msg = queued.submit
        try:
            # Map model alias to actual MiniMax model ID
            model = msg.model
            if model == "minimaxai/minimax-m2.7":
                model = "MiniMax-M2.7"
            elif model == "google/gemma-2-2b-it":
                model = "MiniMax-M2.7"  # Gemma not available on MiniMax, use M2.7

            # Resolve response model if provided
            response_model: type[BaseModel] | None = None
            if msg.response_model:
                response_model = self._resolve_model(msg.response_model)

            result = self._pool.create_completion(
                prompt=msg.prompt,
                model=model,
                response_model=response_model,
                timeout=msg.timeout,
            )

            if response_model and hasattr(result, "model_dump"):
                return ResultResponse(id=msg.id, ok=True, data=result.model_dump())
            else:
                return ResultResponse(id=msg.id, ok=True, data=str(result))
        except Exception as e:
            logger.exception("Request %s failed: %s", msg.id, e)
            return ResultResponse(id=msg.id, ok=False, error=str(e))

    def _resolve_model(self, model_name: str) -> type[BaseModel] | None:
        """Resolve a response model name to a Pydantic class.

        This allows clients to pass the class name (e.g. "NSEApplicabilityResponse")
        and the server resolves it from the appropriate module.
        """
        # Dynamic import for resolution — keeps protocol clean
        import sys
        for name, module in list(sys.modules.items()):
            if module is None:
                continue
            for attr_name in dir(module):
                if attr_name == model_name:
                    obj = getattr(module, attr_name)
                    if isinstance(obj, type) and issubclass(obj, BaseModel):
                        return obj
        # Fallback: log warning and return None
        logger.warning("Could not resolve response model: %s", model_name)
        return None


def serve(socket_path: str = "/tmp/llm_pool.sock", max_concurrent: int = 3) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
    server = LLMQueueServer(socket_path=socket_path, max_concurrent=max_concurrent)
    server.start()


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
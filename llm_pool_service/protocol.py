"""Protocol definitions for LLM pool service communication over Unix socket."""

from __future__ import annotations

import json
from dataclasses import dataclass, asdict
from typing import Any
from uuid import UUID


@dataclass
class SubmitRequest:
    type: str = "submit"
    id: str = ""
    prompt: str = ""
    model: str = ""
    response_model: str | None = None  # class name, resolved on server side
    timeout: int = 30

    def to_json(self) -> bytes:
        return json.dumps(asdict(self)).encode("utf-8")

    @classmethod
    def from_json(cls, raw: bytes | str) -> SubmitRequest:
        data = json.loads(raw) if isinstance(raw, str) else json.loads(raw.decode("utf-8"))
        return cls(**data)


@dataclass
class AcceptedResponse:
    type: str = "accepted"
    id: str = ""

    def to_json(self) -> bytes:
        return json.dumps(asdict(self)).encode("utf-8")


@dataclass
class RejectedResponse:
    type: str = "rejected"
    id: str = ""
    reason: str = ""

    def to_json(self) -> bytes:
        return json.dumps(asdict(self)).encode("utf-8")


@dataclass
class ResultResponse:
    type: str = "result"
    id: str = ""
    ok: bool = True
    data: Any = None
    error: str | None = None

    def to_json(self) -> bytes:
        return json.dumps(asdict(self)).encode("utf-8")


@dataclass
class StatusRequest:
    type: str = "status"
    id: str = ""

    def to_json(self) -> bytes:
        return json.dumps(asdict(self)).encode("utf-8")

    @classmethod
    def from_json(cls, raw: bytes | str) -> StatusRequest:
        data = json.loads(raw) if isinstance(raw, str) else json.loads(raw.decode("utf-8"))
        return cls(**data)


@dataclass
class PendingResponse:
    type: str = "pending"
    id: str = ""
    position: int = 0

    def to_json(self) -> bytes:
        return json.dumps(asdict(self)).encode("utf-8")


Message = SubmitRequest | AcceptedResponse | RejectedResponse | ResultResponse | StatusRequest | PendingResponse


def parse_message(raw: bytes | str) -> Message:
    """Parse a raw bytes or str message into the appropriate response type."""
    data = json.loads(raw) if isinstance(raw, str) else json.loads(raw.decode("utf-8"))
    msg_type = data.get("type", "")

    if msg_type == "submit":
        return SubmitRequest(**data)
    elif msg_type == "accepted":
        return AcceptedResponse(**data)
    elif msg_type == "rejected":
        return RejectedResponse(**data)
    elif msg_type == "result":
        return ResultResponse(**data)
    elif msg_type == "status":
        return StatusRequest(**data)
    elif msg_type == "pending":
        return PendingResponse(**data)
    else:
        raise ValueError(f"Unknown message type: {msg_type}")
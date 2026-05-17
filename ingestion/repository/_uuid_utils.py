from typing import Any
from uuid import UUID


def _raw_to_uuid(val: Any) -> UUID:
    if isinstance(val, UUID):
        return val
    if isinstance(val, bytes):
        return UUID(bytes=val)
    if isinstance(val, str):
        return UUID(val)
    return UUID(bytes=bytes(val))


def _uuid_to_raw(val: UUID | str | bytes) -> bytes:
    if isinstance(val, bytes):
        return val
    if isinstance(val, UUID):
        return val.bytes
    return UUID(str(val)).bytes
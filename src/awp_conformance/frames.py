"""The binary frame envelope (spec/transport/frames), decoded strictly for checking.

Kept independent of every implementation; `tests/test_units.py` runs it over the specification's
test vectors.
"""

from __future__ import annotations

import base64
import struct
from dataclasses import dataclass, field
from typing import Any

from .spec import MAX_SAFE_INT

HEADER = struct.Struct("<4sBBHQQI")
REGISTERED = {0x01: "tick", 0x02: "ts_sim_ns", 0x03: "ts_send_ns"}


class FrameError(Exception):
    def __init__(self, code: str, detail: str) -> None:
        super().__init__(f"{code}: {detail}")
        self.code = code


@dataclass(frozen=True, slots=True)
class Decoded:
    channel_id: int
    seq: int
    ts_mono_ns: int
    flags: int  # bits 0-3 as sent; bits 4-7 are ignored on receipt (AWP-DAT-005)
    raw_flags: int
    payload: bytes
    ext: dict[str, int] = field(default_factory=dict)
    vendor: list[tuple[int, bytes]] = field(default_factory=list)

    @property
    def keyframe(self) -> bool:
        return bool(self.flags & 0x01)

    @property
    def resync(self) -> bool:
        return bool(self.flags & 0x08)

    def as_inline(self) -> dict[str, Any]:
        """The inline JSON form of the same frame (AWP-DAT-004)."""
        params: dict[str, Any] = {
            "channel_id": self.channel_id,
            "seq": self.seq,
            "ts_mono_ns": self.ts_mono_ns,
            "flags": self.flags & 0x0B,
            "payload_b64": base64.b64encode(self.payload).decode(),
        }
        params.update(self.ext)
        return params


def _bounded(value: int, name: str) -> int:
    if not -MAX_SAFE_INT <= value <= MAX_SAFE_INT:
        raise FrameError("AWP_INTEGER_RANGE", f"{name} beyond 2^53-1")
    return value


def decode(data: bytes) -> Decoded:
    if len(data) < HEADER.size:
        raise FrameError("AWP_MALFORMED", f"{len(data)} bytes; the header is {HEADER.size}")
    magic, version, flags, channel_id, seq, ts, payload_len = HEADER.unpack_from(data)
    if magic != b"AWPF" or version != 1:
        raise FrameError("AWP_MALFORMED", f"magic {magic!r} version {version}")
    offset = HEADER.size
    ext: dict[str, int] = {}
    vendor: list[tuple[int, bytes]] = []
    if flags & 0x04:
        if len(data) < offset + 2:
            raise FrameError("AWP_MALFORMED", "has_extensions without ext_len")
        (ext_len,) = struct.unpack_from("<H", data, offset)
        offset += 2
        end = offset + ext_len
        if end > len(data):
            raise FrameError("AWP_MALFORMED", "ext_len exceeds the frame")
        seen: set[int] = set()
        while offset < end:
            if offset + 2 > end:
                raise FrameError("AWP_MALFORMED", "truncated extension entry")
            kind, length = data[offset], data[offset + 1]
            if offset + 2 + length > end:
                raise FrameError("AWP_MALFORMED", "extension value exceeds ext_len")
            if kind in seen:
                raise FrameError("AWP_MALFORMED", f"duplicate extension {kind:#04x}")
            seen.add(kind)
            value = data[offset + 2 : offset + 2 + length]
            if kind in REGISTERED:
                if length != 8:
                    raise FrameError("AWP_MALFORMED", f"extension {kind:#04x} has length {length}")
                signed = kind == 0x02
                ext[REGISTERED[kind]] = _bounded(
                    int.from_bytes(value, "little", signed=signed), REGISTERED[kind]
                )
            elif kind >= 0x80:
                vendor.append((kind, bytes(value)))
            offset += 2 + length
    if offset + payload_len != len(data):
        raise FrameError("AWP_MALFORMED", f"payload_len {payload_len} does not fit the frame")
    return Decoded(
        channel_id=channel_id,
        seq=_bounded(seq, "seq"),
        ts_mono_ns=_bounded(ts, "ts_mono_ns"),
        flags=flags & 0x0F,
        raw_flags=flags,
        payload=bytes(data[offset:]),
        ext=ext,
        vendor=vendor,
    )


def encode(
    channel_id: int,
    seq: int,
    ts_mono_ns: int,
    payload: bytes,
    *,
    flags: int = 0,
    ext: dict[str, int] | None = None,
) -> bytes:
    """A binary frame, for the harness world's stream connections."""
    entries = b""
    for kind, name in REGISTERED.items():
        if ext and name in ext:
            entries += bytes((kind, 8)) + ext[name].to_bytes(8, "little", signed=kind == 0x02)
    if entries:
        flags |= 0x04
    header = HEADER.pack(b"AWPF", 1, flags, channel_id, seq, ts_mono_ns, len(payload))
    return header + (struct.pack("<H", len(entries)) + entries if entries else b"") + payload

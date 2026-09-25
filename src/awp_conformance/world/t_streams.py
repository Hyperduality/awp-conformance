"""Stream bindings: the ws stream connection and binary frames (AWP-TRN-003, AWP-TRN-010..013)."""

from __future__ import annotations

import asyncio
import contextlib
from typing import Any
from urllib.parse import urlsplit, urlunsplit

from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed, InvalidHandshake
from websockets.typing import Subprotocol

from ..frames import Decoded, FrameError, decode
from ..link import Link, now_ns
from ..monitor import SessionTracker
from .context import WorldContext
from .registry import world_test

STREAM_REQS = (
    "AWP-TRN-003",
    "AWP-TRN-010",
    "AWP-TRN-011",
    "AWP-TRN-012",
    "AWP-TRN-013",
    "AWP-SEC-004",
    "AWP-DAT-006",
)


class StreamLink:
    """A stream connection read by the suite; every binary message is decoded and checked."""

    def __init__(self, url: str, tracker: SessionTracker, max_frame_bytes: int) -> None:
        self.url = url
        self.tracker = tracker
        self.max_frame_bytes = max_frame_bytes
        self.ws: ClientConnection | None = None
        self.frames: list[tuple[int, Decoded]] = []
        self.closed = asyncio.Event()
        self._task: asyncio.Task[None] | None = None

    async def open(self, token: str | None, *, url: str | None = None) -> None:
        headers = {"Authorization": f"Bearer {token}"} if token else None
        self.ws = await connect(
            url or self.url,
            subprotocols=[Subprotocol("awp")],
            additional_headers=headers,
            ping_interval=None,
            open_timeout=5,
            max_size=None,
        )
        self._task = asyncio.create_task(self._read(self.ws))

    async def _read(self, ws: ClientConnection) -> None:
        tr = self.tracker
        try:
            async for raw in ws:
                if isinstance(raw, str):
                    tr.bad("AWP-TRN-013", "a text message on a stream connection")
                    continue
                tr.expect(
                    "AWP-TRN-011",
                    len(raw) <= self.max_frame_bytes,
                    f"a {len(raw)}-byte frame on an endpoint declaring {self.max_frame_bytes}",
                )
                try:
                    frame = decode(raw)
                except FrameError as err:
                    tr.bad("AWP-DAT-006", f"undecodable stream frame: {err}")
                    continue
                tr.ok("AWP-TRN-013")
                tr.ok("AWP-DAT-006")
                tr.expect("AWP-DAT-005", not frame.raw_flags & 0xF0, "reserved flag bits set")
                self.frames.append((now_ns(), frame))
                tr.on_frame(frame.as_inline(), binding="ws")
        except ConnectionClosed:
            pass
        finally:
            self.closed.set()

    async def close(self) -> None:
        if self.ws is not None:
            await self.ws.close()
        if self._task is not None:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._task


def _endpoint(link: Link) -> dict[str, Any] | None:
    for e in (link.tracker.ready or {}).get("stream_endpoints", []):
        if e.get("binding") == "ws" and e.get("url"):
            return dict(e)
    return None


async def _refused(stream: StreamLink, token: str | None, url: str | None = None) -> bool:
    """True if the world refuses the stream connection or closes it without sending a frame."""
    try:
        await stream.open(token, url=url)
    except (InvalidHandshake, OSError):
        return True
    await asyncio.wait({asyncio.ensure_future(stream.closed.wait())}, timeout=1.0)
    refused = stream.closed.is_set() and not stream.frames
    await stream.close()
    return refused


@world_test("ws-stream", [*STREAM_REQS, "AWP-SAF-009"], timeout_s=90.0)
async def ws_stream(ctx: WorldContext) -> None:
    link = await ctx.session()
    endpoint = _endpoint(link)
    if endpoint is None:
        for r in STREAM_REQS:
            ctx.na(r, "no ws stream endpoint offered")
        return
    ctx.facts.add("stream endpoints offered")
    ctx.check("AWP-TRN-003", endpoint["binding"] == "ws", "a ws endpoint")
    limit = int(endpoint.get("max_frame_bytes", 16 * 1024 * 1024))
    url = endpoint["url"]
    ctx.check("AWP-SEC-006", "token" not in (urlsplit(url).query or ""), f"endpoint URL {url}")
    muted = ctx.tracker()
    muted.muted = True
    ctx.check("AWP-SEC-004", await _refused(StreamLink(url, muted, limit), None), "no credential")
    ctx.check(
        "AWP-SEC-004",
        await _refused(StreamLink(url, muted, limit), "st_conformance_not_a_session_0000"),
        "a token that names no session",
    )
    parts = urlsplit(url)
    leaky = urlunsplit((*parts[:3], "token=" + (link.tracker.token or ""), ""))
    ctx.check(
        "AWP-SEC-006", await _refused(StreamLink(url, muted, limit), None, leaky), "URL token"
    )

    stream = StreamLink(url, link.tracker, limit)
    await stream.open(link.tracker.token)
    if ctx.lockstep:
        await ctx.advance(link, 2)
    else:
        await asyncio.sleep(1.0)
    channels = set(link.tracker.observation_channels())
    seen = {f.channel_id for _, f in stream.frames}
    ctx.check("AWP-TRN-012", channels <= seen, f"stream carried channels {seen} of {channels}")
    ctx.check(
        "AWP-TRN-013", stream.ws is not None and stream.ws.subprotocol == "awp", "subprotocol"
    )
    if ctx.lockstep:
        await stream.close()
        return

    reliable = [
        c
        for c in link.tracker.channels.values()
        if c.loss_class == "reliable" and (ctx.manifest_channel(c.name).get("rate_hz") or 0) > 0
    ]
    await stream.close()
    before = len(link.notes)
    stale_ms = max(
        [
            ctx.manifest_channel(c.name).get("stale_after_ms")
            or 2000 / ctx.manifest_channel(c.name)["rate_hz"]
            for c in reliable
        ]
        or [200]
    )
    await asyncio.sleep(stale_ms / 1000 + 0.6)
    inline = [m for _, m in link.notes[before:] if m.get("method") == "obs.frame"]
    ctx.check(
        "AWP-TRN-010", not inline, f"{len(inline)} frames went inline while the stream was down"
    )
    if reliable:
        degraded = [e for e in link.tracker.events if e["event"] == "channel_degraded"]
        ctx.check("AWP-SAF-009", bool(degraded), "no channel_degraded while the stream was down")
    for c in link.tracker.channels.values():
        c.binding = "inline"  # the next stream frame is the first on a new connection
        c.stream_since_ns = None
    again = StreamLink(url, link.tracker, limit)
    await again.open(link.tracker.token)
    await asyncio.sleep(0.5)
    ctx.check("AWP-TRN-010", bool(again.frames), "the world did not accept a new stream connection")
    firsts: dict[int, Decoded] = {}
    for _, f in again.frames:
        firsts.setdefault(f.channel_id, f)
    ctx.check(
        "AWP-TRN-010",
        all(f.resync for cid, f in firsts.items() if cid in {c.channel_id for c in reliable}),
        "a reliable channel restarted without a resync keyframe",
    )
    await again.close()

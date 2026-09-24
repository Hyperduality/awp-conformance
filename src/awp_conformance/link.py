"""One control connection to a world, driven by the suite as an agent.

A `Link` decodes strictly, answers the world's pings (unless told not to), optionally keeps its own
heartbeat, records every message, and feeds a `SessionTracker`. Tests await responses and
notifications through it; nothing here decides whether the world is conformant beyond what the
tracker checks passively.
"""

from __future__ import annotations

import asyncio
import contextlib
import itertools
import json
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed, InvalidHandshake, InvalidStatus
from websockets.typing import Subprotocol

from .monitor import SessionTracker
from .spec import MAX_SAFE_INT

Message = dict[str, Any]


class HandshakeRefused(Exception):
    def __init__(self, status: int | None, detail: str) -> None:
        super().__init__(f"handshake refused ({status}): {detail}")
        self.status = status


@dataclass(frozen=True, slots=True)
class Reply:
    id: int
    method: str
    result: dict[str, Any] | None
    error: dict[str, Any] | None
    sent_ns: int
    received_ns: int

    @property
    def ok(self) -> bool:
        return self.error is None

    @property
    def code(self) -> int | None:
        return None if self.error is None else self.error.get("code")

    @property
    def data(self) -> dict[str, Any]:
        return (self.error or {}).get("data") or {}

    @property
    def elapsed_ms(self) -> float:
        return (self.received_ns - self.sent_ns) / 1e6

    def __getitem__(self, key: str) -> Any:
        if self.result is None:
            raise KeyError(f"{self.method} failed: {self.error}")
        return self.result[key]

    def get(self, key: str, default: Any = None) -> Any:
        return (self.result or {}).get(key, default)


class Timeout(Exception):
    pass


def now_ns() -> int:
    return time.monotonic_ns()


def _reject_constant(name: str) -> Any:
    raise ValueError(f"{name} is not valid JSON")


def _oversized(value: Any) -> bool:
    if isinstance(value, bool):
        return False
    if isinstance(value, int):
        return not -MAX_SAFE_INT <= value <= MAX_SAFE_INT
    if isinstance(value, dict):
        return any(_oversized(v) for v in value.values())
    if isinstance(value, list):
        return any(_oversized(v) for v in value)
    return False


class Link:
    _names = itertools.count(1)

    def __init__(
        self,
        url: str,
        tracker: SessionTracker,
        *,
        name: str | None = None,
        token: str | None = None,
        answer_pings: bool = True,
        heartbeat: bool = True,
    ) -> None:
        self.url = url
        self.tracker = tracker
        self.name = name or f"link{next(self._names)}"
        self.token = token
        self.answer_pings = answer_pings
        self.heartbeat = heartbeat
        self.ack = True
        self.ws: ClientConnection | None = None
        self.subprotocol: str | None = None
        self.trace: list[dict[str, Any]] = []
        self.notes: list[tuple[int, Message]] = []  # (received_ns, notification) from the world
        self.world_pings: list[int] = []
        self.null_errors: list[dict[str, Any]] = []
        self.closed_by_world: tuple[int, str] | None = None
        self._ids = itertools.count(1)
        self._pending: dict[int, tuple[str, dict[str, Any], int, bool, asyncio.Future[Reply]]] = {}
        self._changed = asyncio.Condition()
        self._tasks: list[asyncio.Task[None]] = []
        self._sending: set[asyncio.Future[None]] = set()
        self._last_ping_sent = 0

    # ------------------------------------------------------------ transport

    async def connect(
        self,
        *,
        credential: str = "header",
        url: str | None = None,
        subprotocols: list[str] | None = None,
        open_timeout: float = 10.0,
    ) -> None:
        headers = None
        offered = [Subprotocol(p) for p in (subprotocols or ["awp"])]
        if self.token and credential == "header":
            headers = {"Authorization": f"Bearer {self.token}"}
        elif self.token and credential == "subprotocol":
            offered.append(Subprotocol(f"awp.bearer.{self.token}"))
        try:
            self.ws = await connect(
                url or self.url,
                subprotocols=offered,
                additional_headers=headers,
                ping_interval=None,
                open_timeout=open_timeout,
                max_size=None,
            )
        except InvalidStatus as exc:
            raise HandshakeRefused(exc.response.status_code, str(exc)) from None
        except InvalidHandshake as exc:
            raise HandshakeRefused(None, str(exc)) from None
        self.subprotocol = self.ws.subprotocol
        self.closed_by_world = None
        self._tasks = [asyncio.create_task(self._read(self.ws))]
        if self.heartbeat:
            self._tasks.append(asyncio.create_task(self._beat()))

    @property
    def connected(self) -> bool:
        return self.ws is not None and self.closed_by_world is None

    async def close(self) -> None:
        await self._stop_tasks()
        if self.ws is not None:
            with contextlib.suppress(Exception):
                await self.ws.close()
        self._fail_pending()

    async def drop(self) -> None:
        """Abandon the connection abruptly, as a crashed agent would."""
        await self._stop_tasks()
        if self.ws is not None:
            self.ws.transport.abort()
        self._fail_pending()

    def go_silent(self) -> None:
        """Keep the TCP connection but send nothing more, not even pongs."""
        self.heartbeat = False
        self.answer_pings = False

    async def _stop_tasks(self) -> None:
        for t in self._tasks:
            t.cancel()
        for t in self._tasks:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await t
        self._tasks = []

    def _fail_pending(self) -> None:
        for *_, fut in self._pending.values():
            if not fut.done():
                fut.set_exception(ConnectionError("connection closed"))
        self._pending.clear()

    # ------------------------------------------------------------ sending

    async def send(self, msg: Message) -> None:
        if self.ws is None:
            raise ConnectionError("not connected")
        self.trace.append({"from": "agent", "step": _step(msg), "msg": msg})
        await self.ws.send(json.dumps(msg, separators=(",", ":")))

    async def send_text(self, text: str) -> None:
        if self.ws is None:
            raise ConnectionError("not connected")
        await self.ws.send(text)

    async def notify(self, method: str, params: dict[str, Any]) -> None:
        await self.send({"jsonrpc": "2.0", "method": method, "params": params})

    def start(self, method: str, params: dict[str, Any] | None = None) -> asyncio.Future[Reply]:
        """Send a request without awaiting it; await the returned future for the reply."""
        rid = next(self._ids)
        fut: asyncio.Future[Reply] = asyncio.get_running_loop().create_future()
        msg: Message = {"jsonrpc": "2.0", "id": rid, "method": method}
        if params is not None:
            msg["params"] = params
        closing = self.tracker.closing
        if method == "session.close":
            self.tracker.closing = True
        self._pending[rid] = (method, params or {}, now_ns(), closing, fut)

        async def send() -> None:
            try:
                await self.send(msg)
            except Exception as exc:
                self._pending.pop(rid, None)
                if not fut.done():
                    fut.set_exception(exc)

        task = asyncio.ensure_future(send())
        self._sending.add(task)
        task.add_done_callback(self._sending.discard)
        return fut

    async def call(
        self, method: str, params: dict[str, Any] | None = None, *, timeout: float = 10.0
    ) -> Reply:
        fut = self.start(method, params)
        try:
            return await asyncio.wait_for(fut, timeout)
        except TimeoutError:
            raise Timeout(f"{self.name}: no response to {method} within {timeout} s") from None

    async def ping(self) -> Reply:
        params: dict[str, Any] = {"origin_ns": now_ns()}
        if self.ack and self.tracker.ready is not None:
            params["last_status_seq"] = self.tracker.highest_contiguous()
        self._last_ping_sent = now_ns()
        return await self.call("ping", params)

    # ------------------------------------------------------------ receiving

    async def _read(self, ws: ClientConnection) -> None:
        try:
            async for raw in ws:
                await self._receive(raw)
        except ConnectionClosed as exc:
            self.closed_by_world = (now_ns(), f"{exc.rcvd.code if exc.rcvd else ''} {exc}")
        else:
            rcvd = ws.close_code
            self.closed_by_world = (now_ns(), f"{rcvd} {ws.close_reason or ''}")
        finally:
            if self.closed_by_world is None:
                self.closed_by_world = (now_ns(), "reader stopped")
            self._fail_pending()
            async with self._changed:
                self._changed.notify_all()

    async def _receive(self, raw: str | bytes) -> None:
        t = now_ns()
        tr = self.tracker
        try:
            msg = json.loads(raw, parse_constant=_reject_constant)
        except (ValueError, UnicodeDecodeError) as exc:
            tr.bad("AWP-CTL-001", f"world sent invalid JSON: {exc}")
            return
        if isinstance(msg, list):
            tr.bad("AWP-CTL-006", "world sent a batch")
            return
        if not isinstance(msg, dict) or msg.get("jsonrpc") != "2.0":
            tr.bad("AWP-CTL-001", f"world sent a non-JSON-RPC 2.0 message: {str(msg)[:80]}")
            return
        tr.ok("AWP-CTL-001")
        tr.ok("AWP-CTL-006")
        bounded = not _oversized(msg)
        tr.expect("AWP-CTL-009", bounded, f"integer beyond 2^53-1 in {_step(msg)}")
        tr.expect("AWP-CLK-005", bounded, f"a timestamp or counter beyond 2^53-1 in {_step(msg)}")
        self.trace.append({"from": "world", "step": _step(msg), "msg": msg})
        if "method" in msg and "id" in msg:
            await self._on_request(msg, t)
        elif "method" in msg:
            tr.on_notification(msg["method"], msg.get("params") or {}, t)
            self.notes.append((t, msg))
        elif msg.get("id") is None and "error" in msg:
            self.null_errors.append(msg["error"])  # the world could not attribute the request
        else:
            rid = msg.get("id")
            pending = self._pending.pop(rid, None) if isinstance(rid, int) else None
            if pending is None:
                tr.bad("AWP-CTL-001", f"response to unknown request id {msg.get('id')!r}")
            else:
                method, params, sent, closing, fut = pending
                tr.on_response(method, params, msg, closing=closing)
                if method == "ping" and "result" in msg and tr.ready is not None:
                    interval = tr.ready.get("heartbeat_interval_ms", 5000)
                    tr.expect(
                        "AWP-SAF-001",
                        (t - sent) / 1e6 <= interval,
                        f"pong after {(t - sent) / 1e6:.0f} ms; heartbeat interval {interval} ms",
                    )
                if not fut.done():
                    fut.set_result(
                        Reply(msg["id"], method, msg.get("result"), msg.get("error"), sent, t)
                    )
        async with self._changed:
            self._changed.notify_all()

    async def _on_request(self, msg: Message, t: int) -> None:
        method = msg["method"]
        if method != "ping":
            if self.answer_pings:
                await self.send(
                    {
                        "jsonrpc": "2.0",
                        "id": msg["id"],
                        "error": {"code": -32601, "message": "Method not found"},
                    }
                )
            return
        self.world_pings.append(t)
        self.tracker.validate("ping", "params", msg.get("params") or {})
        if self.answer_pings:
            origin = (msg.get("params") or {}).get("origin_ns", 0)
            await self.send(
                {
                    "jsonrpc": "2.0",
                    "id": msg["id"],
                    "result": {"origin_ns": origin, "receive_ns": t, "transmit_ns": now_ns()},
                }
            )

    async def _beat(self) -> None:
        while True:
            await asyncio.sleep(0.05)
            tr = self.tracker
            if not self.heartbeat or tr.ready is None or not self.connected:
                continue
            interval = tr.ready.get("heartbeat_interval_ms", 5000)
            safe = (tr.manifest.get("safety_policy") or {}).get("safe_state") or {}
            if "watchdog_ms" in safe:
                interval = min(interval, safe["watchdog_ms"])
            if now_ns() - self._last_ping_sent >= interval / 3 * 1e6:
                with contextlib.suppress(Exception):
                    await self.ping()

    # ------------------------------------------------------------ waiting

    async def wait_for(self, predicate: Callable[[], bool], timeout: float) -> bool:
        deadline = asyncio.get_running_loop().time() + timeout
        async with self._changed:
            while not predicate():
                remaining = deadline - asyncio.get_running_loop().time()
                if remaining <= 0 or (self.closed_by_world is not None and not predicate()):
                    return predicate()
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(self._changed.wait(), remaining)
        return True

    async def wait_note(
        self, predicate: Callable[[Message], bool], timeout: float, *, since: int = 0
    ) -> Message | None:
        found: list[Message] = []

        def match() -> bool:
            for _, m in self.notes[since:]:
                if predicate(m):
                    found.append(m)
                    return True
            return False

        await self.wait_for(match, timeout)
        return found[0] if found else None

    async def wait_closed(self, timeout: float) -> bool:
        return await self.wait_for(lambda: self.closed_by_world is not None, timeout)


def _step(msg: Message) -> str:
    return str(msg.get("method") or ("error" if "error" in msg else "result"))

"""A scripted world for testing agents, and the checks it applies to what the agent sends.

The harness serves a manifest it is given and behaves as a minimal, conformant world for it:
actions validate against their schemas and run for a fixed time (or number of advances), frames
carry sample payloads, and statuses are sequenced and replayed. An `Episode` adds the stimuli that
make agent requirements observable — unknown fields, redelivered statuses, a dropped connection, an
invalid manifest, withheld grants — and every agent message is checked as it arrives.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import json
import secrets
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlsplit

from websockets.asyncio.server import Server, ServerConnection, serve
from websockets.exceptions import ConnectionClosed
from websockets.http11 import Request, Response
from websockets.typing import Subprotocol

from .. import frames as binary
from .. import spec
from ..results import Results

MS = 1_000_000
UNKNOWN = {"future_field_v0_2": {"any": "value"}, "x-conformance.note": 1}
REDACTED_MANIFEST_KEY = "embodiments"


@dataclass
class Episode:
    name: str
    mode: str
    unknown_fields: bool = True
    redeliver: bool = True
    drop_after_admission: bool = False
    invalid_manifest: bool = False
    grant_no_actions: bool = False
    silent_after_admission: bool = False
    offer_stream: bool = True
    refuse_first_submission: bool = False
    action_ms: int = 600
    action_ticks: int = 5
    heartbeat_interval_ms: int = 1000
    watchdog_ms: int | None = None


@dataclass
class _Action:
    action_id: str
    content: dict[str, Any]
    decl: dict[str, Any]
    state: str
    started_ns: int = 0
    ticks: int = 0
    status: dict[str, Any] = field(default_factory=dict)


@dataclass
class _Session:
    id: str
    token: str
    mode: str
    embodiment: str | None
    origin_ns: int
    channels: dict[int, dict[str, Any]]
    action_types: list[str]
    tick: int = 0
    seq: int = 0
    log: dict[int, tuple[str, dict[str, Any]]] = field(default_factory=dict)
    actions: dict[str, _Action] = field(default_factory=dict)
    frame_seq: dict[int, int] = field(default_factory=dict)
    next_due: dict[int, int] = field(default_factory=dict)
    resync: set[int] = field(default_factory=set)
    ws: ServerConnection | None = None
    closed: bool = False
    frames_ts: set[int] = field(default_factory=set)
    last_telemetry: int = 0
    redelivered: bool = False
    seq_at_drop: int | None = None
    stream_ws: ServerConnection | None = None
    stream_fresh: set[int] = field(default_factory=set)


class Harness:
    def __init__(
        self,
        manifest: dict[str, Any],
        episode: Episode,
        results: Results,
        *,
        samples: dict[str, Any] | None = None,
        token: str | None = None,
    ) -> None:
        self.manifest = manifest
        self.episode = episode
        self.results = results
        self.samples = samples or {}
        self.token = token or "cf_" + secrets.token_urlsafe(18)
        self.test = f"{episode.mode}/{episode.name}"
        self.decls = {d["type"]: d for d in manifest.get("action_schemas", [])}
        self.channels = {c["id"]: c for c in manifest.get("observation_channels", [])}
        self.validators = {
            d["type"]: spec.params_validator(manifest, i)
            for i, d in enumerate(manifest.get("action_schemas", []))
        }
        self.server: Server | None = None
        self.port = 0
        self.session: _Session | None = None
        self.sessions: dict[str, _Session] = {}
        self.admitted_ids: set[str] = set()
        self.refused: dict[
            str, tuple[dict[str, Any], bool]
        ] = {}  # action_id → (content, retryable)
        self.submitted: list[dict[str, Any]] = []
        self.opened = False
        self.resumed: list[dict[str, Any]] = []
        self.agent_pings: list[int] = []
        self.pong_delays: list[float] = []
        self.clock_samples = 0  # agent pings answered before the first valid_until_ns
        self.reports: list[int] = []
        self.ticks_called = 0
        self.dropped = False
        self.manifest_sent = False
        self.handshakes: list[Request] = []
        self._pending_pings: dict[str, int] = {}
        self.consumes: set[str] = set()
        self._pending_probe: str | None = None
        self.stream_attached = False
        self.silent = False
        self.silent_since: int | None = None
        self.agent_closed_at: int | None = None
        self.connections = 0
        self.origins: list[int] = []
        self.session_started_ns: int | None = None
        self._loop_task: asyncio.Task[None] | None = None
        self.done = asyncio.Event()

    # ------------------------------------------------------------ findings

    def check(self, requirement: str, ok: bool, detail: str, *, should: bool = False) -> bool:
        return self.results.check(requirement, ok, detail, self.test, should=should)

    # ------------------------------------------------------------ server

    async def start(self) -> None:
        self.server = await serve(
            self._handle,
            "127.0.0.1",
            0,
            subprotocols=[Subprotocol("awp")],
            select_subprotocol=lambda conn, offered: (
                Subprotocol("awp") if "awp" in offered else None
            ),
            process_request=self._authorize,
            ping_interval=None,
            max_size=None,
        )
        self.port = self.server.sockets[0].getsockname()[1]
        self._loop_task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        if self._loop_task is not None:
            self._loop_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._loop_task
        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()

    @property
    def url(self) -> str:
        return f"ws://127.0.0.1:{self.port}"

    def _authorize(self, connection: ServerConnection, request: Request) -> Response | None:
        self.handshakes.append(request)
        query = urlsplit(request.path).query
        if urlsplit(request.path).path == "/stream":
            return self._authorize_stream(request, query)
        self.check(
            "AWP-SEC-005", self.token not in query, "the agent put its credential in the URL"
        )
        offered = [
            p.strip()
            for v in request.headers.get_all("Sec-WebSocket-Protocol")
            for p in v.split(",")
        ]
        self.check("AWP-TRN-001", "awp" in offered, f"the agent offered subprotocols {offered}")
        ok = request.headers.get("Authorization") == f"Bearer {self.token}" or (
            f"awp.bearer.{self.token}" in offered
        )
        self.check("AWP-SEC-005", ok, "the agent presented no bearer credential")
        return None

    def _authorize_stream(self, request: Request, query: str) -> Response | None:
        s = self.session
        token = s.token if s else ""
        offered = [
            p.strip()
            for v in request.headers.get_all("Sec-WebSocket-Protocol")
            for p in v.split(",")
        ]
        presented = request.headers.get("Authorization") == f"Bearer {token}" or (
            f"awp.bearer.{token}" in offered
        )
        self.check("AWP-SEC-005", token not in query, "the agent put its session token in a URL")
        self.check("AWP-SEC-005", presented, "the stream connection lacks the session token")
        self.check("AWP-TRN-013", "awp" in offered, f"stream subprotocols {offered}")
        return None

    async def _stream(self, ws: ServerConnection) -> None:
        s = self.session
        if s is None:
            await ws.close(code=1008, reason="no session")
            return
        s.stream_ws = ws
        s.stream_fresh = set(s.channels)
        self.stream_attached = True
        try:
            async for raw in ws:
                self.check("AWP-TRN-013", isinstance(raw, bytes), "the agent sent text on a stream")
        except ConnectionClosed:
            pass
        finally:
            if s.stream_ws is ws:
                s.stream_ws = None

    async def _handle(self, ws: ServerConnection) -> None:
        if ws.request is not None and urlsplit(ws.request.path).path == "/stream":
            await self._stream(ws)
            return
        self.connections += 1
        try:
            async for raw in ws:
                await self._receive(ws, raw)
        except ConnectionClosed:
            pass
        finally:
            if self.silent and self.agent_closed_at is None:
                self.agent_closed_at = time.monotonic_ns()
            s = self.session
            if s is not None and s.ws is ws:
                s.ws = None

    # ------------------------------------------------------------ sending

    def clock(self, s: _Session) -> int:
        if s.mode == "lockstep":
            return s.tick * 20 * MS
        return time.monotonic_ns() - s.origin_ns

    def _extra(self, body: dict[str, Any]) -> dict[str, Any]:
        return {**body, **UNKNOWN} if self.episode.unknown_fields else body

    async def _send(self, ws: ServerConnection | None, msg: dict[str, Any]) -> None:
        if ws is None or self.silent:
            return
        with contextlib.suppress(ConnectionClosed):
            await ws.send(json.dumps(msg, separators=(",", ":")))

    async def _result(self, ws: ServerConnection, rid: Any, body: dict[str, Any]) -> None:
        await self._send(ws, {"jsonrpc": "2.0", "id": rid, "result": self._extra(body)})

    async def _error(
        self,
        ws: ServerConnection,
        rid: Any,
        code: int,
        message: str,
        *,
        retryable: bool = False,
        **data: Any,
    ) -> None:
        err: dict[str, Any] = {"code": code, "message": message}
        if not -32768 <= code <= -32000:
            err["data"] = {"retryable": retryable, **data}
        await self._send(ws, {"jsonrpc": "2.0", "id": rid, "error": err})

    async def _note(self, s: _Session, method: str, params: dict[str, Any]) -> None:
        await self._send(s.ws, {"jsonrpc": "2.0", "method": method, "params": self._extra(params)})

    async def _sequenced(self, s: _Session, method: str, params: dict[str, Any]) -> dict[str, Any]:
        s.seq += 1
        body = {**params, "status_seq": s.seq}
        s.log[s.seq] = (method, body)
        await self._note(s, method, body)
        return body

    async def _state(self, s: _Session, state: str, reason: str) -> None:
        await self._sequenced(
            s, "session.state", {"state": state, "ts_mono_ns": self.clock(s), "reason": reason}
        )

    async def _status(self, s: _Session, a: _Action, state: str, **extra: Any) -> None:
        a.state = state
        body: dict[str, Any] = {
            "action_id": a.action_id,
            "state": state,
            "ts_mono_ns": self.clock(s),
        }
        if s.mode == "lockstep":
            body["tick"] = s.tick
        body.update(extra)
        a.status = await self._sequenced(s, "action.status", body)
        if state in spec.TERMINAL and self.episode.redeliver and not s.redelivered:
            s.redelivered = True
            await self._note(s, "action.status", a.status)  # AWP-LIF-009: a redelivery

    def _endpoints(self) -> list[dict[str, Any]]:
        stream = (
            [{"binding": "ws", "url": self.url + "/stream"}] if self.episode.offer_stream else []
        )
        return [*stream, {"binding": "inline"}]

    def _payload(self, channel: str) -> bytes:
        sample = self.samples.get(channel)
        if sample is None:
            decl = self.channels[channel]
            fields = (decl.get("schema") or {}).get("fields")
            sample = dict.fromkeys(fields, 0) if isinstance(fields, list) else {}
        if isinstance(sample, str):
            return sample.encode()
        return json.dumps(sample, separators=(",", ":")).encode()

    async def _frame(self, s: _Session, cid: int, *, resync: bool = False) -> None:
        name = s.channels[cid]["channel"]
        s.frame_seq[cid] = s.frame_seq.get(cid, 0) + 1
        ts = self.clock(s)
        s.frames_ts.add(ts)
        params: dict[str, Any] = {
            "channel_id": cid,
            "seq": s.frame_seq[cid],
            "ts_mono_ns": ts,
            "flags": (0x09 if resync else 0x01) | (0x30 if s.frame_seq[cid] % 4 == 0 else 0),
            "payload_b64": base64.b64encode(self._payload(name)).decode(),
        }
        if s.mode == "lockstep":
            params["tick"] = s.tick
        else:
            params["ts_send_ns"] = ts
        if s.stream_ws is not None and not self.silent:
            flags = params["flags"] | (0x09 if cid in s.stream_fresh else 0)
            s.stream_fresh.discard(cid)
            ext: dict[str, int] = {k: params[k] for k in ("tick", "ts_send_ns") if k in params}
            data = binary.encode(cid, params["seq"], ts, self._payload(name), flags=flags, ext=ext)
            with contextlib.suppress(ConnectionClosed):
                await s.stream_ws.send(data)
            return
        await self._send(s.ws, {"jsonrpc": "2.0", "method": "obs.frame", "params": params})

    # ------------------------------------------------------------ receiving

    async def _receive(self, ws: ServerConnection, raw: str | bytes) -> None:
        try:
            msg = json.loads(raw)
        except ValueError:
            self.check("AWP-CTL-001", False, "the agent sent invalid JSON")
            return
        if isinstance(msg, list):
            self.check("AWP-CTL-006", False, "the agent sent a batch")
            return
        self.check("AWP-CTL-006", True, "no batch")
        self.check(
            "AWP-CTL-001", msg.get("jsonrpc") == "2.0", "the agent sent a non-JSON-RPC 2.0 message"
        )
        bounded = not _oversized(msg)
        self.check("AWP-CTL-009", bounded, "the agent sent an integer beyond 2^53-1")
        method = msg.get("method")
        if method is None:
            self._on_response(msg)
            return
        params = msg.get("params") or {}
        part = "params" if "id" in msg else "notification"
        name = spec.METHODS.get(method, {}).get(part)
        if name is not None:
            problems = spec.schema_errors(name, params)
            self.check("AWP-AGT-001", not problems, f"{method} {part}: {problems[:1]}")
            sender = spec.schema_errors(name, params, sender=True)
            self.check("AWP-VER-004", not sender, f"{method} {part} (sender form): {sender[:1]}")
        handler: Callable[..., Any] | None = getattr(self, "_rpc_" + method.replace(".", "_"), None)
        if "id" not in msg:
            if method == "obs.report":
                self.reports.append(time.monotonic_ns())
            return
        if handler is None:
            await self._error(ws, msg["id"], -32601, "Method not found")
            return
        await handler(ws, msg["id"], params)

    def _on_response(self, msg: dict[str, Any]) -> None:
        if msg.get("id") == self._pending_probe:
            self._pending_probe = None
            code = (msg.get("error") or {}).get("code")
            self.check(
                "AWP-CTL-002", code == -32601, f"the agent answered an unknown method with {code}"
            )
            return
        sent = self._pending_pings.pop(str(msg.get("id")), None)
        if sent is None:
            return
        delay = (time.monotonic_ns() - sent) / MS
        self.pong_delays.append(delay)
        r = msg.get("result") or {}
        ok = not spec.schema_errors("ping-result", r) and r.get("receive_ns", 0) <= r.get(
            "transmit_ns", -1
        )
        self.check("AWP-CLK-007", ok, f"the agent's pong {r}")
        self.check(
            "AWP-AGT-006", ok, "the agent answers world pings with receive_ns and transmit_ns"
        )
        interval = self.episode.heartbeat_interval_ms
        self.check(
            "AWP-SAF-001", delay <= interval, f"the agent answered a ping after {delay:.0f} ms"
        )

    # ------------------------------------------------------------ methods

    async def _rpc_initialize(self, ws: ServerConnection, rid: Any, p: dict[str, Any]) -> None:
        versions = p.get("protocol_versions", [])
        self.check("AWP-AGT-001", "0.1" in versions, f"protocol_versions {versions}")
        self.check("AWP-VER-002", "0.1" in versions, f"protocol_versions {versions}")
        self.check(
            "AWP-VER-008",
            all(isinstance(v, str) and v.count(".") == 1 for v in versions),
            f"{versions}",
        )
        self.consumes = set(p.get("consumes_modalities", []))
        manifest = dict(self.manifest)
        if self.episode.invalid_manifest:
            manifest.pop(REDACTED_MANIFEST_KEY, None)
            manifest["time_models"] = "streaming"  # not an array
        self.manifest_sent = True
        await self._result(ws, rid, manifest)

    async def _rpc_world_manifest(self, ws: ServerConnection, rid: Any, p: dict[str, Any]) -> None:
        await self._result(ws, rid, self.manifest)

    async def _rpc_ping(self, ws: ServerConnection, rid: Any, p: dict[str, Any]) -> None:
        s = self.session
        now = time.monotonic_ns()
        self.agent_pings.append(now)
        origin = p.get("origin_ns", 0)
        prev = self.origins[-1] if self.origins else 0
        self.check(
            "AWP-CLK-006",
            isinstance(origin, int) and origin >= prev,
            f"origin_ns {origin} after {prev}",
        )
        self.origins.append(origin)
        self.check("AWP-CTL-004", "origin_ns" in p, "the agent's ping lacks origin_ns")
        stamp = self.clock(s) if s else 0
        await self._result(
            ws, rid, {"origin_ns": p.get("origin_ns", 0), "receive_ns": stamp, "transmit_ns": stamp}
        )
        if s is not None and s.mode == "streaming":
            self.clock_samples += 1

    async def _rpc_session_open(self, ws: ServerConnection, rid: Any, p: dict[str, Any]) -> None:
        self.opened = True
        if self.episode.invalid_manifest:
            self.check(
                "AWP-AGT-002",
                False,
                "the agent opened a session against a manifest it could not validate",
            )
        embodiment = p.get("embodiment")
        mode = p.get("mode")
        if mode != self.episode.mode:
            await self._error(ws, rid, 2002, "AWP_TIME_MODEL_UNSUPPORTED")
            return
        offered = next(
            (e for e in self.manifest.get("embodiments", []) if e["id"] == embodiment), None
        )
        channels: dict[int, dict[str, Any]] = {}
        readable = set(offered.get("channels", [])) if offered else set(self.channels)
        for sub in p.get("subscribe", []):
            name = sub.get("channel")
            if name in readable and name in self.channels:
                decl = self.channels[name]
                rate = None if mode == "lockstep" else min(float(decl.get("rate_hz") or 10), 50.0)
                channels[len(channels) + 1] = {
                    "channel": name,
                    "rate_hz": rate,
                    "channel_id": len(channels) + 1,
                }
        types = (
            []
            if self.episode.grant_no_actions or offered is None
            else list(offered.get("action_types", []))
        )
        wanted = p.get("action_types")
        if wanted is not None:
            types = [t for t in types if t in wanted]
        s = _Session(
            id="sess_" + secrets.token_hex(4),
            token="st_" + secrets.token_urlsafe(18),
            mode=mode,
            embodiment=embodiment,
            origin_ns=time.monotonic_ns(),
            channels=channels,
            action_types=types,
            ws=ws,
        )
        self.session = s
        self.sessions[s.token] = s
        self.session_started_ns = time.monotonic_ns()
        ready: dict[str, Any] = {
            "session_id": s.id,
            "session_token": s.token,
            "reconnect_window_ms": 30000,
            "heartbeat_interval_ms": self.episode.heartbeat_interval_ms,
            "granted": {
                "channels": list(channels.values()),
                "action_types": types,
                "admin": [],
                "envelopes": [],
            },
            "stream_endpoints": self._endpoints(),
            "frame_tree": {
                "frames": [{"id": "world", "parent": None}, {"id": "base", "parent": "world"}]
            },
            "clock_anchor": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
        if mode == "lockstep":
            ready["tick"] = 0
        await self._result(ws, rid, ready)
        await self._state(s, "ready", "opened")
        for cid in channels:
            await self._frame(s, cid)
            s.next_due[cid] = time.monotonic_ns()
        self._pending_probe = "u1"
        await self._send(
            ws, {"jsonrpc": "2.0", "id": "u1", "method": "x-conformance.probe", "params": {}}
        )

    async def _rpc_session_resume(self, ws: ServerConnection, rid: Any, p: dict[str, Any]) -> None:
        s = self.sessions.get(p.get("session_token", ""))
        if s is None:
            await self._error(ws, rid, 2005, "AWP_SESSION_UNKNOWN")
            return
        self.resumed.append(p)
        last = p.get("last_status_seq")
        delivered = s.seq_at_drop if s.seq_at_drop is not None else s.seq
        self.check(
            "AWP-AGT-007",
            isinstance(last, int) and last == delivered,
            f"session.resume carried last_status_seq {last}; the agent had received {delivered}",
        )
        self.check(
            "AWP-CTL-008",
            isinstance(last, int) and last <= s.seq,
            "last_status_seq beyond what was sent",
        )
        s.ws = ws
        ready = {
            "session_id": s.id,
            "session_token": s.token,
            "reconnect_window_ms": 30000,
            "replay_to_status_seq": s.seq,
            "heartbeat_interval_ms": self.episode.heartbeat_interval_ms,
            "granted": {
                "channels": list(s.channels.values()),
                "action_types": s.action_types,
                "admin": [],
                "envelopes": [],
            },
            "stream_endpoints": self._endpoints(),
            "safe_state": False,
        }
        if s.mode == "lockstep":
            ready["tick"] = s.tick
        await self._result(ws, rid, ready)
        for seq in sorted(q for q in s.log if isinstance(last, int) and q > last):
            method, body = s.log[seq]
            await self._note(s, method, body)
        await self._state(s, "active", "resumed")
        for cid in s.channels:
            await self._frame(s, cid, resync=True)

    async def _rpc_session_close(self, ws: ServerConnection, rid: Any, p: dict[str, Any]) -> None:
        s = self.session
        if s is None:
            await self._error(ws, rid, -32600, "Invalid request")
            return
        for a in list(s.actions.values()):
            if a.state not in spec.TERMINAL:
                if a.state == "executing":
                    await self._status(s, a, "cancelling", reason="session_closed")
                await self._status(
                    s,
                    a,
                    "cancelled",
                    reason="session_closed",
                    **({"aborted_at_progress": 0.5} if a.started_ns else {}),
                )
        await self._state(s, "closed", "session_closed")
        s.closed = True
        await self._result(ws, rid, {})
        self.done.set()

    async def _rpc_obs_subscribe(self, ws: ServerConnection, rid: Any, p: dict[str, Any]) -> None:
        s = self.session
        assert s is not None
        for sub in p.get("channels", []):
            decl = self.channels.get(sub.get("channel"))
            if decl is not None and self.consumes and decl.get("modality") not in self.consumes:
                self.check(
                    "AWP-MOD-002",
                    False,
                    f"the agent subscribed to {decl['id']}, a modality it did not declare",
                )
        await self._result(ws, rid, {"granted": list(s.channels.values())})

    async def _rpc_obs_unsubscribe(self, ws: ServerConnection, rid: Any, p: dict[str, Any]) -> None:
        s = self.session
        assert s is not None
        await self._result(ws, rid, {"granted": list(s.channels.values())})

    async def _rpc_action_status(self, ws: ServerConnection, rid: Any, p: dict[str, Any]) -> None:
        s = self.session
        a = s.actions.get(p.get("action_id", "")) if s else None
        if a is None:
            await self._error(ws, rid, 3008, "AWP_ACTION_UNKNOWN")
            return
        await self._result(ws, rid, a.status)

    async def _rpc_action_cancel(self, ws: ServerConnection, rid: Any, p: dict[str, Any]) -> None:
        s = self.session
        a = s.actions.get(p.get("action_id", "")) if s else None
        if s is None or a is None:
            await self._error(ws, rid, 3008, "AWP_ACTION_UNKNOWN")
            return
        if a.state in ("accepted", "queued", "pending_approval"):
            await self._status(s, a, "cancelled", reason="cancelled_by_agent")
        elif a.state == "executing":
            await self._status(s, a, "cancelling", reason="cancelled_by_agent")
        await self._result(
            ws, rid, {k: a.status[k] for k in ("action_id", "state", "status_seq") if k in a.status}
        )

    async def _rpc_world_tick(self, ws: ServerConnection, rid: Any, p: dict[str, Any]) -> None:
        s = self.session
        self.ticks_called += 1
        if s is None or s.mode != "lockstep":
            self.check("AWP-AGT-009", False, "the agent called world.tick in a streaming session")
            await self._error(ws, rid, 2002, "AWP_TIME_MODEL_UNSUPPORTED")
            return
        expected = p.get("expected_tick")
        self.check(
            "AWP-AGT-006",
            expected == s.tick,
            f"world.tick expected_tick {expected}, current {s.tick}",
        )
        if expected != s.tick:
            await self._error(ws, rid, 3009, "AWP_TICK_MISMATCH", tick=s.tick)
            return
        for _ in range(int(p.get("count", 1))):
            s.tick += 1
            for a in list(s.actions.values()):
                if a.state == "accepted":
                    a.started_ns = 1
                    await self._status(s, a, "executing", progress=0.0)
                elif a.state == "executing":
                    a.ticks += 1
                    if a.ticks >= self.episode.action_ticks or a.decl.get("duration") == "instant":
                        await self._status(s, a, "completed", progress=1.0)
                    else:
                        await self._status(
                            s,
                            a,
                            "executing",
                            progress=round(a.ticks / self.episode.action_ticks, 3),
                        )
                elif a.state == "cancelling":
                    await self._status(
                        s, a, "cancelled", reason="cancelled_by_agent", aborted_at_progress=0.5
                    )
            for cid in s.channels:
                await self._frame(s, cid)
        await self._result(ws, rid, {"tick": s.tick})

    async def _rpc_action_submit(self, ws: ServerConnection, rid: Any, p: dict[str, Any]) -> None:
        s = self.session
        if s is None:
            await self._error(ws, rid, -32600, "Invalid request")
            return
        action_id = p.get("action_id", "")
        self.submitted.append(p)
        now = self.clock(s)
        known = s.actions.get(action_id)
        if known is not None:
            same = all(
                (f in known.content) == (f in p) and known.content.get(f) == p.get(f)
                for f in spec.SUBMIT_FIELDS
            )
            self.check("AWP-AGT-004", same, f"{action_id} reused with different content")
            self.check("AWP-ACT-010", same, f"an admitted action_id {action_id} was reused")
            if not same:
                await self._error(ws, rid, 3004, "AWP_ACTION_ID_CONFLICT")
                return
            st = known.status
            await self._result(
                ws,
                rid,
                {
                    **{
                        k: st[k]
                        for k in ("action_id", "state", "status_seq", "ts_mono_ns")
                        if k in st
                    },
                    "received_ts_mono_ns": st.get("ts_mono_ns", 0),
                },
            )
            return
        refused = self.refused.get(action_id)
        if refused is not None:
            content, retryable = refused
            same = all(
                (f in content) == (f in p) and content.get(f) == p.get(f)
                for f in spec.SUBMIT_FIELDS
            )
            self.check(
                "AWP-ERR-001",
                retryable or not same,
                f"{action_id} retried with identical params after a non-retryable error",
            )
        self.check(
            "AWP-AGT-003",
            p.get("type") in s.action_types,
            f"submitted ungranted type {p.get('type')} (granted {s.action_types})",
        )
        if self.episode.refuse_first_submission and not self.refused:
            self.refused[action_id] = (p, False)
            await self._error(ws, rid, 4001, "AWP_FORBIDDEN", detail="conformance: refused")
            return
        if p.get("type") not in s.action_types:
            self.refused[action_id] = (p, False)
            await self._error(ws, rid, 4001, "AWP_FORBIDDEN")
            return
        emb = p.get("embodiment_id", s.embodiment)
        self.check("AWP-AGT-003", emb == s.embodiment, f"submitted for embodiment {emb}")
        if "valid_until_ns" in p and s.mode == "streaming":
            self.check(
                "AWP-CLK-008",
                self.clock_samples >= 1,
                "valid_until_ns sent before any clock sample",
            )
            v = p["valid_until_ns"]
            plausible = now - 2_000 * MS <= v <= now + 3_600_000 * MS
            self.check(
                "AWP-CLK-009",
                plausible,
                f"valid_until_ns {v} is not on the session clock (now {now})",
            )
            self.check(
                "AWP-AGT-008", plausible, "valid_until_ns not mapped through the clock offset"
            )
        if "basis_ts_mono_ns" in p:
            b = p["basis_ts_mono_ns"]
            self.check(
                "AWP-ACT-007", b in s.frames_ts, f"basis_ts_mono_ns {b} is no frame's ts_mono_ns"
            )
            stale = [
                c
                for c in s.channels.values()
                if (self.channels.get(c["channel"]) or {}).get("stale_after_ms")
            ]
            if stale and s.mode == "streaming":
                limit = min(self.channels[c["channel"]]["stale_after_ms"] for c in stale)
                self.check(
                    "AWP-SAF-010",
                    now - b <= limit * MS * 4,
                    f"basis {(now - b) / MS:.0f} ms old at submission",
                    should=True,
                )
        elif s.mode == "streaming":
            self.check(
                "AWP-SAF-010", False, f"{action_id} carries no basis_ts_mono_ns", should=True
            )
        decl = self.decls[p["type"]]
        if not self.validators[p["type"]].is_valid(p.get("params")):
            self.refused[action_id] = (p, False)
            await self._error(ws, rid, 3001, "AWP_PARAMS_INVALID")
            return
        a = _Action(action_id, dict(p), decl, "accepted")
        s.actions[action_id] = a
        self.admitted_ids.add(action_id)
        s.seq += 1
        body = {"action_id": action_id, "state": "accepted", "ts_mono_ns": now, "status_seq": s.seq}
        if s.mode == "lockstep":
            body["tick"] = s.tick
        s.log[s.seq] = ("action.status", body)
        a.status = body
        await self._result(ws, rid, {**body, "received_ts_mono_ns": now})
        if s.mode == "streaming":
            a.started_ns = time.monotonic_ns()
            await self._status(s, a, "executing", progress=0.0)
            if decl.get("duration") == "instant":
                await self._status(s, a, "completed", progress=1.0)
        if self.episode.silent_after_admission and not self.silent:
            self.silent = True
            self.silent_since = time.monotonic_ns()
        if self.episode.drop_after_admission and not self.dropped:
            self.dropped = True
            s.seq_at_drop = s.seq
            await ws.close(code=1011, reason="conformance: connection drop")

    # ------------------------------------------------------------ time

    async def _run(self) -> None:
        last_ping = time.monotonic_ns()
        n = 0
        while True:
            await asyncio.sleep(0.01)
            s = self.session
            now = time.monotonic_ns()
            if s is None or s.ws is None or s.closed:
                continue
            if now - last_ping >= self.episode.heartbeat_interval_ms * MS:
                last_ping = now
                n += 1
                rid = f"w{n}"
                self._pending_pings[rid] = now
                await self._send(
                    s.ws,
                    {
                        "jsonrpc": "2.0",
                        "id": rid,
                        "method": "ping",
                        "params": {"origin_ns": self.clock(s)},
                    },
                )
            if s.mode != "streaming":
                continue
            for cid, g in s.channels.items():
                if g["rate_hz"] and now >= s.next_due.get(cid, 0):
                    s.next_due[cid] = now + int(1e9 / g["rate_hz"])
                    await self._frame(s, cid)
            for a in list(s.actions.values()):
                if a.state == "executing":
                    age = (now - a.started_ns) / MS
                    if age >= self.episode.action_ms:
                        await self._status(s, a, "completed", progress=1.0)
                    elif int(age / 200) > int((age - 10) / 200):
                        await self._status(
                            s, a, "executing", progress=round(age / self.episode.action_ms, 3)
                        )
                elif a.state == "cancelling" and (now - a.started_ns) / MS > 50:
                    await self._status(
                        s, a, "cancelled", reason="cancelled_by_agent", aborted_at_progress=0.5
                    )
            if now - s.last_telemetry >= 1_000 * MS:
                s.last_telemetry = now
                await self._note(s, "session.telemetry", {"window_ms": 1000})


def _oversized(value: Any) -> bool:
    if isinstance(value, bool):
        return False
    if isinstance(value, int):
        return not -spec.MAX_SAFE_INT <= value <= spec.MAX_SAFE_INT
    if isinstance(value, dict):
        return any(_oversized(v) for v in value.values())
    if isinstance(value, list):
        return any(_oversized(v) for v in value)
    return False

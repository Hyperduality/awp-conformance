"""The state of one world run, and the helpers tests use to drive sessions."""

from __future__ import annotations

import asyncio
import contextlib
import itertools
import os
import subprocess
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from typing import Any

from ..fixture import ActionSpec, Fixture
from ..link import Link, Reply, Timeout
from ..monitor import ActionView, SessionTracker
from ..results import Results
from ..spec import TERMINAL, params_validator

Hook = Callable[[], None]


class Skip(Exception):
    """The test cannot run against this world; the reason is recorded."""


@dataclass
class WorldContext:
    url: str
    fixture: Fixture
    results: Results
    token: str | None = None
    manifest: dict[str, Any] = field(default_factory=dict)
    mode: str = "streaming"
    slow: bool = False
    profiles: set[str] = field(default_factory=set)
    hooks: dict[str, Hook] = field(default_factory=dict)  # in-process operator hooks (tests)
    facts: set[str] = field(default_factory=set)
    test: str = ""
    links: list[Link] = field(default_factory=list)
    traces: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    _effective: list[ActionView] = field(default_factory=list)
    _turn: int = 0
    _last_submit_ns: int = 0
    _ids: itertools.count[int] = field(default_factory=lambda: itertools.count(1))

    # ------------------------------------------------------------ findings

    def check(self, requirement: str, ok: bool, detail: str) -> bool:
        return self.results.check(requirement, ok, detail, self.test)

    def should(self, requirement: str, ok: bool, detail: str) -> bool:
        """A SHOULD inside `requirement`: a miss is a warning, not a failure."""
        return self.results.check(requirement, ok, detail, self.test, should=True)

    def na(self, requirement: str, reason: str) -> None:
        self.results.mark_not_applicable(requirement, reason)

    # ------------------------------------------------------------ manifest facts

    @property
    def lockstep(self) -> bool:
        return self.mode == "lockstep"

    @property
    def decls(self) -> dict[str, dict[str, Any]]:
        return {d["type"]: d for d in self.manifest.get("action_schemas", [])}

    @property
    def embodiment(self) -> str:
        if self.fixture.embodiment:
            return self.fixture.embodiment
        embodiments = self.manifest.get("embodiments") or []
        if not embodiments:
            raise Skip("the manifest declares no embodiment")
        return str(embodiments[0]["id"])

    @property
    def channels(self) -> list[str]:
        if self.fixture.subscribe is not None:
            return list(self.fixture.subscribe)
        for e in self.manifest.get("embodiments", []):
            if e["id"] == self.embodiment:
                return list(e.get("channels", []))
        return []

    @property
    def safety(self) -> dict[str, Any]:
        return dict(self.manifest.get("safety_policy") or {})

    @property
    def watchdog_ms(self) -> int | None:
        safe = self.safety.get("safe_state") or {}
        return safe.get("watchdog_ms")

    @property
    def modalities(self) -> list[str]:
        mods = {c["modality"] for c in self.manifest.get("observation_channels", [])}
        return sorted(mods)

    def action_id(self, tag: str = "a") -> str:
        return f"cf-{self.test[:24]}-{tag}-{next(self._ids)}"

    # ------------------------------------------------------------ links and sessions

    def tracker(self) -> SessionTracker:
        return SessionTracker(self.results, self.test, self.manifest, self.mode)

    def link(self, name: str = "agent", **kw: Any) -> Link:
        link = Link(self.url, self.tracker(), name=f"{self.test}/{name}", token=self.token, **kw)
        self.links.append(link)
        return link

    def agent_manifest(self, **extra: Any) -> dict[str, Any]:
        params: dict[str, Any] = {
            "protocol_versions": ["0.1"],
            "agent": {"name": "awp-conformance", "version": "0.1.0", "vendor": "hyperduality"},
            "consumes_modalities": self.modalities,
        }
        params.update(extra)
        return params

    async def connect(self, name: str = "agent", **kw: Any) -> Link:
        link = self.link(name, **kw)
        await link.connect()
        reply = await link.call("initialize", self.agent_manifest())
        if not reply.ok:
            raise Skip(f"initialize failed: {reply.error}")
        return link

    def open_params(self, **overrides: Any) -> dict[str, Any]:
        params: dict[str, Any] = {
            "mode": self.mode,
            "embodiment": self.embodiment,
            "subscribe": [{"channel": c} for c in self.channels],
        }
        params.update(overrides)
        return {k: v for k, v in params.items() if v is not None}

    async def open(self, link: Link, **overrides: Any) -> Reply:
        params = self.open_params(**overrides)
        link.tracker.mode = params.get("mode", self.mode)
        link.tracker.consumes = set(self.modalities)
        reply = await link.call("session.open", params)
        if reply.ok:
            await self.initial_frames(link)
        return reply

    async def session(self, name: str = "agent", **overrides: Any) -> Link:
        """A connected, initialized link with an open session and its initial observations."""
        link = await self.connect(
            name, **{k: overrides.pop(k) for k in ("heartbeat", "answer_pings") if k in overrides}
        )
        reply = await self.open(link, **overrides)
        if not reply.ok:
            raise Skip(f"session.open failed: {reply.error}")
        return link

    async def initial_frames(self, link: Link, timeout: float = 3.0) -> None:
        wanted = {c.channel_id for c in link.tracker.channels.values()}
        await link.wait_for(
            lambda: all(
                link.tracker.channels[c].frames for c in wanted if c in link.tracker.channels
            ),
            timeout,
        )

    async def resume(
        self,
        tracker: SessionTracker,
        name: str = "resumed",
        *,
        last_status_seq: int | None = None,
        **kw: Any,
    ) -> tuple[Link, Reply]:
        """Resume `tracker`'s session on a new connection; the tracker follows it there."""
        link = Link(self.url, tracker, name=f"{self.test}/{name}", token=self.token, **kw)
        self.links.append(link)
        await link.connect()
        init = await link.call("initialize", self.agent_manifest())
        if not init.ok:
            raise Skip(f"initialize failed: {init.error}")
        last = tracker.highest_contiguous() if last_status_seq is None else last_status_seq
        assert tracker.token is not None
        reply = await link.call(
            "session.resume", {"session_token": tracker.token, "last_status_seq": last}
        )
        return link, reply

    def detach(self, link: Link) -> SessionTracker:
        """Take the session's tracker from `link`, which keeps a throwaway one."""
        tracker = link.tracker
        link.tracker = self.tracker()
        link.tracker.muted = True
        return tracker

    async def close(self, link: Link, timeout: float = 15.0) -> Reply | None:
        if not link.connected or link.tracker.ready is None or link.tracker.closed_reported:
            return None
        return await link.call("session.close", timeout=timeout)

    async def cleanup(self) -> None:
        """Close every session the test opened, resuming any it left suspended."""
        if self.fixture.moves:
            self._collect()
        live = {id(link.tracker) for link in self.links if link.connected}
        orphans = {
            id(link.tracker): link.tracker
            for link in self.links
            if link.tracker.token
            and not link.tracker.closed_reported
            and id(link.tracker) not in live
        }
        for tracker in orphans.values():
            with contextlib.suppress(Exception):
                await self.resume(tracker, "cleanup")
        for link in list(self.links):
            with contextlib.suppress(Exception):
                if (
                    link.connected
                    and link.tracker.ready is not None
                    and not link.tracker.closed_reported
                ):
                    await link.call("session.close", timeout=15.0)
            with contextlib.suppress(Exception):
                await link.close()
            link.tracker.flush()
            self.traces[link.name] = link.trace
        self.links = []

    # ------------------------------------------------------------ actions

    def moves(self) -> list[ActionSpec]:
        if len(self.fixture.moves) < 2:
            raise Skip("the fixture declares fewer than two moves")
        return self.fixture.moves

    def _collect(self) -> None:
        seen = {id(a) for a in self._effective}
        for link in self.links:
            for a in link.tracker.actions.values():
                if a.effective and id(a) not in seen and a.content is not None:
                    self._effective.append(a)
                    seen.add(id(a))

    def _index(self, a: ActionView) -> int | None:
        for i, m in enumerate(self.moves()):
            if (
                a.content
                and m.type == a.content.get("type")
                and m.params == a.content.get("params")
            ):
                return i
        return None

    def _where(self) -> set[int]:
        """Fixture moves whose targets the embodiment may be at or between, from what it was told.

        A completed move ends at its target; an abort ends between its start and its target, near
        one end if `aborted_at_progress` says so; a move still pending or executing will end at
        its target unless something interrupts it.
        """
        self._collect()
        at: set[int] = set()
        for a in sorted(self._effective, key=lambda a: a.admitted_ns or 0):
            target = self._index(a)
            if target is None:
                at = set()
                continue
            if a.state not in TERMINAL or a.state == "completed":
                at = {target}
                continue
            progress = a.status.get("aborted_at_progress")
            if isinstance(progress, int | float) and progress <= 0.02:
                continue
            if isinstance(progress, int | float) and progress >= 0.98:
                at = {target}
            else:
                at = at | {target}
        return at

    def next_move(self) -> ActionSpec:
        """A move that is guaranteed to go somewhere: to a target the embodiment is not near."""
        moves = self.moves()
        near = self._where()
        choices = [i for i in range(len(moves)) if i not in near] or [
            i for i in range(len(moves)) if i != max(near, default=-1)
        ]
        self._turn += 1
        return moves[choices[self._turn % len(choices)]]

    def invalid_params(self) -> ActionSpec:
        if self.fixture.invalid is not None:
            return self.fixture.invalid
        for index, decl in enumerate(self.manifest.get("action_schemas", [])):
            v = params_validator(self.manifest, index)
            for candidate in ({}, {"x-conformance.bogus": True, "": None}):
                if not v.is_valid(candidate):
                    return ActionSpec(decl["type"], candidate)
        raise Skip("no action type rejects any generated params; add `invalid` to the fixture")

    def submission(self, spec: ActionSpec, **fields: Any) -> dict[str, Any]:
        params: dict[str, Any] = {
            "action_id": fields.pop("action_id", None) or self.action_id(),
            "type": spec.type,
            "params": spec.params,
        }
        params.update({k: v for k, v in fields.items() if v is not None})
        return params

    @property
    def action_rate_hz(self) -> float | None:
        for e in self.safety.get("envelopes", []):
            if e.get("embodiment") == self.embodiment and "max_action_rate_hz" in e:
                return float(e["max_action_rate_hz"])
        return None

    async def submit(
        self, link: Link, spec: ActionSpec, *, pace: bool = True, **fields: Any
    ) -> tuple[str, Reply]:
        """Submit, keeping to the declared admission rate; retry a rate refusal when told to."""
        params = self.submission(spec, **fields)
        rate = self.action_rate_hz
        for _ in range(12):
            if pace and rate and not self.lockstep:
                wait = self._last_submit_ns + 1e9 / rate * 1.1 - time.monotonic_ns()
                if wait > 0:
                    await asyncio.sleep(wait / 1e9)
            reply = await link.call("action.submit", params)
            self._last_submit_ns = time.monotonic_ns()
            retry = reply.data.get("retry_after_ms")
            if not (pace and reply.code == 4002 and reply.data.get("retryable") and retry):
                return params["action_id"], reply
            if self.lockstep:
                await self.advance(link)
            else:
                await asyncio.sleep(retry / 1000 + 0.005)
        return params["action_id"], reply

    def state(self, link: Link, action_id: str) -> str:
        a = link.tracker.actions.get(action_id)
        return a.state if a else "submitted"

    def history(self, link: Link, action_id: str) -> list[str]:
        a = link.tracker.actions.get(action_id)
        out: list[str] = []
        for p in a.history if a else []:
            if not out or out[-1] != p["state"]:
                out.append(p["state"])
        return out

    def status(self, link: Link, action_id: str) -> dict[str, Any]:
        a = link.tracker.actions.get(action_id)
        return a.status if a else {}

    async def wait_state(
        self, link: Link, action_id: str, states: Iterable[str], timeout: float = 10.0
    ) -> bool:
        wanted = set(states)
        return await link.wait_for(lambda: self.state(link, action_id) in wanted, timeout)

    async def wait_terminal(self, link: Link, action_id: str, timeout: float = 15.0) -> bool:
        if self.lockstep:
            for _ in range(2000):
                if self.state(link, action_id) in TERMINAL:
                    return True
                await self.advance(link)
            return False
        return await self.wait_state(link, action_id, TERMINAL, timeout)

    async def advance(self, link: Link, count: int = 1) -> Reply:
        tick = link.tracker.tick
        if tick is None:
            raise Skip("no tick known on this session")
        params: dict[str, Any] = {"expected_tick": tick}
        if count != 1:
            params["count"] = count
        return await link.call("world.tick", params)

    async def running(self, link: Link, spec: ActionSpec | None = None, **fields: Any) -> str:
        """Submit a move and bring it to `executing` (one advance in lockstep)."""
        action_id, reply = await self.submit(link, spec or self.next_move(), **fields)
        if not reply.ok:
            raise Skip(f"the fixture move was refused: {reply.error}")
        if self.lockstep:
            await self.advance(link)
        if not await self.wait_state(link, action_id, ["executing"], 3.0):
            raise Timeout(f"{action_id} did not begin executing ({self.state(link, action_id)})")
        return action_id

    async def settle(self, link: Link) -> None:
        """Let everything this session started come to rest."""
        open_ids = [i for i, a in link.tracker.actions.items() if a.state not in TERMINAL]
        for action_id in open_ids:
            if self.state(link, action_id) not in TERMINAL:
                with contextlib.suppress(Exception):
                    await link.call("action.cancel", {"action_id": action_id})
            await self.wait_terminal(link, action_id, 10.0)

    # ------------------------------------------------------------ operator

    def has_operator(self, name: str) -> bool:
        return name in self.hooks or name in self.fixture.operator

    async def operator(self, name: str) -> None:
        if name in self.hooks:
            self.hooks[name]()
            return
        command = self.fixture.operator.get(name)
        if command is None:
            raise Skip(f"no operator hook {name!r} in the fixture")
        proc = await asyncio.to_thread(
            subprocess.run, command, shell=True, env=os.environ.copy(), capture_output=True
        )
        if proc.returncode != 0:
            raise Skip(f"operator hook {name!r} failed: {proc.stderr.decode(errors='replace')}")

    async def sleep_ms(self, ms: float) -> None:
        await asyncio.sleep(ms / 1000)

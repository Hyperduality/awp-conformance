"""Passive checks on everything a world sends, attributed to requirement IDs.

A `SessionTracker` follows one logical session across its connections: status sequencing and replay,
the action lifecycle, frame sequencing per channel, closing, and schema validity of every message.
Each check counts what it exercised and records what it saw violated; `flush()` turns that into
findings once the test that drove the session has finished.
"""

from __future__ import annotations

import base64
import binascii
import json
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

from . import spec
from .results import Results

ADMISSION_STATES = ("pending_approval", "queued", "accepted")
SEQUENCED = ("action.status", "world.event", "session.state")
REFUSED_WHILE_CLOSING = frozenset(
    {
        "action.submit",
        "action.cancel",
        "obs.subscribe",
        "obs.unsubscribe",
        "world.tick",
        "world.reset",
        "session.resume",
    }
)
EVENT_REGISTRY = frozenset(
    {
        "entity_appeared",
        "entity_removed",
        "collision",
        "e_stop_engaged",
        "e_stop_released",
        "envelope_violation",
        "grant_expired",
        "safe_state_entered",
        "safe_state_exited",
        "channel_degraded",
        "world_resetting",
        "world_shutdown",
    }
)
REASON_REQUIRED = frozenset({"rejected", "failed", "cancelled"})
JSON_MODALITIES = ("text/event+json", "proprio/json", "servo/json")

# The requirement a schema failure of a world message violates, by method and part.
SCHEMA_REQ: dict[tuple[str, str], str] = {
    ("initialize", "result"): "AWP-MAN-001",
    ("world.manifest", "result"): "AWP-MAN-003",
    ("ping", "result"): "AWP-CLK-007",
    ("ping", "params"): "AWP-CTL-004",
    ("session.open", "result"): "AWP-NEG-001",
    ("session.resume", "result"): "AWP-SES-004",
    ("session.close", "result"): "AWP-SES-006",
    ("session.state", "notification"): "AWP-SES-007",
    ("session.telemetry", "notification"): "AWP-TIM-006",
    ("obs.subscribe", "result"): "AWP-NEG-004",
    ("obs.unsubscribe", "result"): "AWP-NEG-004",
    ("obs.frame", "notification"): "AWP-OBS-001",
    ("action.submit", "result"): "AWP-LIF-002",
    ("action.cancel", "result"): "AWP-LIF-005",
    ("action.status", "result"): "AWP-ACT-006",
    ("action.status", "notification"): "AWP-LIF-001",
    ("world.tick", "result"): "AWP-TIM-011",
    ("world.reset", "result"): "AWP-PRM-006",
    ("world.restore", "result"): "AWP-REP-002",
    ("world.snapshot", "result"): "AWP-REP-002",
    ("world.event", "notification"): "AWP-EVT-001",
    ("safety.approval_requested", "notification"): "AWP-APR-001",
    ("task.update", "result"): "AWP-TSK-003",
    ("session.transfer", "result"): "AWP-EMB-003",
}


@dataclass
class ActionView:
    state: str = "submitted"
    status_seq: int = 0
    content: dict[str, Any] | None = None
    status: dict[str, Any] = field(default_factory=dict)
    history: list[dict[str, Any]] = field(default_factory=list)
    admitted_ns: int | None = None
    executed: bool = False

    @property
    def effective(self) -> bool:
        """Admitted, and either executed or still able to."""
        return self.admitted_ns is not None and (self.executed or self.state not in spec.TERMINAL)


@dataclass
class ChannelView:
    name: str
    channel_id: int
    loss_class: str
    modality: str
    schema: Any
    last_seq: int | None = None
    last_ts: int | None = None
    frames: int = 0
    need_resync: bool = False
    fresh: bool = True  # the next frame is the first of a subscription
    binding: str = "inline"
    stream_since_ns: int | None = None  # when the channel's first stream frame arrived


class SessionTracker:
    def __init__(self, results: Results, test: str, manifest: dict[str, Any], mode: str) -> None:
        self.results = results
        self.test = test
        self.manifest = manifest
        self.mode = mode
        self.channels_by_name = {c["id"]: c for c in manifest.get("observation_channels", [])}
        self.consumes: set[str] | None = None

        self.ready: dict[str, Any] | None = None
        self.token: str | None = None
        self.session_id: str | None = None
        self.tick: int | None = None
        self.next_seq: int | None = None
        self.highest = 0
        self.replay_to = 0
        self.reported: dict[int, str] = {}
        self.notes: list[dict[str, Any]] = []  # fresh sequenced notifications, in status_seq order
        self.actions: dict[str, ActionView] = {}
        self.refused: set[str] = set()  # action_ids whose only submissions failed admission
        self.channels: dict[int, ChannelView] = {}
        self.closing = False
        self.closed_reported = False
        self.states: list[str] = []
        self.events: list[dict[str, Any]] = []
        self.telemetry: list[tuple[int, dict[str, Any]]] = []

        self.exercised: dict[str, int] = defaultdict(int)
        self.violations: dict[str, list[str]] = defaultdict(list)
        self.muted = False  # a connection the session has left: its stragglers prove nothing

    def highest_contiguous(self) -> int:
        """The acknowledgement point: every status_seq up to here has arrived."""
        seq = 0
        while seq + 1 in self.reported:
            seq += 1
        return seq

    # ------------------------------------------------------------ bookkeeping

    def ok(self, requirement: str) -> None:
        if not self.muted:
            self.exercised[requirement] += 1

    def bad(self, requirement: str, detail: str) -> None:
        if self.muted:
            return
        self.exercised[requirement] += 1
        if len(self.violations[requirement]) < 5:
            self.violations[requirement].append(detail)

    def expect(self, requirement: str, condition: bool, detail: str) -> bool:
        if condition:
            self.ok(requirement)
        else:
            self.bad(requirement, detail)
        return condition

    def flush(self) -> None:
        for requirement, count in self.exercised.items():
            problems = self.violations.get(requirement)
            if problems:
                for p in problems:
                    self.results.check(requirement, False, p, self.test)
            else:
                self.results.check(requirement, True, f"observed {count} times", self.test)
        self.exercised.clear()
        self.violations.clear()

    # ------------------------------------------------------------ schema

    def validate(self, method: str, part: str, instance: Any) -> bool:
        name = spec.METHODS.get(method, {}).get(part)
        if name is None:
            return True
        requirement = SCHEMA_REQ.get((method, part), "AWP-VER-003")
        problems = spec.schema_errors(name, instance)
        if problems:
            self.bad(requirement, f"{method} {part} fails {name}: {'; '.join(problems[:2])}")
            return False
        self.ok(requirement)
        sender = spec.schema_errors(name, instance, sender=True)
        flags = [p for p in sender if p.startswith("flags")]
        other = [p for p in sender if not p.startswith("flags")]
        if flags:
            self.bad("AWP-DAT-005", f"{method}: {flags[0]}")
        if other:
            self.bad("AWP-VER-004", f"{method} {part}: {other[0]}")
        else:
            self.ok("AWP-VER-004")
        return True

    def error_object(self, method: str, err: dict[str, Any]) -> None:
        problems = spec.schema_errors("error", err)
        self.expect("AWP-ERR-001", not problems, f"{method} error object: {problems[:1]}")
        code = err.get("code")
        if isinstance(code, int) and 1000 <= code <= 4999:
            retryable = (err.get("data") or {}).get("retryable")
            self.expect(
                "AWP-ERR-001",
                isinstance(retryable, bool),
                f"{method} error {code} lacks data.retryable",
            )

    # ------------------------------------------------------------ session setup

    def on_ready(self, result: dict[str, Any], resumed: bool, last_status_seq: int = 0) -> None:
        self.ready = result
        self.token = result.get("session_token", self.token)
        self.session_id = result.get("session_id", self.session_id)
        if "tick" in result:
            self.tick = result["tick"]
        self.set_grants(result.get("granted", {}).get("channels", []), resumed=resumed)
        if resumed:
            replay_to = result.get("replay_to_status_seq")
            if self.expect(
                "AWP-CTL-008",
                isinstance(replay_to, int),
                "session.resume result lacks replay_to_status_seq",
            ):
                assert isinstance(replay_to, int)
                self.expect(
                    "AWP-CTL-008",
                    replay_to >= self.highest,
                    f"replay_to_status_seq {replay_to} below the highest seen ({self.highest})",
                )
                self.replay_to = replay_to
            self.next_seq = last_status_seq + 1
            for ch in self.channels.values():
                ch.need_resync = ch.loss_class == "reliable"
        else:
            self.next_seq = 1

    def set_grants(self, grants: list[dict[str, Any]], *, resumed: bool = False) -> None:
        current = {c.channel_id: c for c in self.channels.values()}
        self.channels = {}
        for g in grants:
            declared = self.channels_by_name.get(g.get("channel", ""), {})
            cid = g.get("channel_id")
            if not isinstance(cid, int):
                continue
            old = current.get(cid)
            if old is not None and old.name == g.get("channel"):
                self.channels[cid] = old
                continue
            self.channels[cid] = ChannelView(
                name=g.get("channel", "?"),
                channel_id=cid,
                loss_class=declared.get("loss_class", "reliable"),
                modality=declared.get("modality", ""),
                schema=declared.get("schema"),
            )
        self.expect(
            "AWP-TRN-005",
            all(cid >= 1 for cid in self.channels) and len(self.channels) == len(grants),
            f"channel grants must carry distinct channel_id ≥ 1: {grants}",
        )

    # ------------------------------------------------------------ world → agent

    def on_notification(self, method: str, params: dict[str, Any], received_ns: int) -> None:
        if not self.validate(method, "notification", params):
            return
        if method == "obs.frame":
            self.on_frame(params)
        elif method == "session.telemetry":
            self.telemetry.append((received_ns, params))
        elif method in SEQUENCED:
            self.on_sequenced(method, params)

    def _identity(self, method: str, p: dict[str, Any]) -> str:
        if method == "action.status":
            return f"action:{p.get('action_id')}:{p.get('state')}:{p.get('reason')}"
        if method == "world.event":
            return f"event:{p.get('event')}"
        return f"session:{p.get('state')}"

    def arrive(self, seq: int, identity: str) -> bool:
        """A world-assigned status_seq; False for a redelivery."""
        if self.next_seq is not None:
            in_order = seq == self.next_seq
            self.expect("AWP-CTL-008", in_order, f"status_seq {seq}, expected {self.next_seq}")
            for requirement in ("AWP-CTL-003", "AWP-TRN-002"):
                self.expect(requirement, in_order, f"control channel reordered at status_seq {seq}")
        self.next_seq = seq + 1
        if seq <= self.highest:
            first = self.reported.get(seq)
            if first is not None:
                self.expect(
                    "AWP-LIF-009",
                    first == identity,
                    f"status_seq {seq} redelivered as {identity}, first reported as {first}",
                )
            self.expect(
                "AWP-CTL-008", seq <= self.replay_to, f"status_seq {seq} repeated outside a replay"
            )
            return False
        self.highest = seq
        self.reported[seq] = identity
        return True

    def on_sequenced(self, method: str, p: dict[str, Any]) -> None:
        seq = p["status_seq"]
        if not self.arrive(seq, self._identity(method, p)):
            return
        if self.notes:
            prev = self.notes[-1].get("ts_mono_ns", 0)
            self.expect(
                "AWP-CLK-001",
                p.get("ts_mono_ns", 0) >= prev,
                f"status_seq {seq} ts_mono_ns {p.get('ts_mono_ns')} before {prev}",
            )
        self.notes.append({"method": method, **p})
        if method == "action.status":
            self.transition(p)
        elif method == "world.event":
            self.events.append(p)
            self.expect(
                "AWP-EVT-001",
                p["event"] in EVENT_REGISTRY or p["event"].startswith("x-"),
                f"world.event {p['event']} is neither registered nor x-<vendor>.",
            )
        else:
            self.states.append(p["state"])
            if p["state"] == "closed":
                self.closed_reported = True

    def transition(self, p: dict[str, Any]) -> None:
        action_id = p["action_id"]
        a = self.actions.setdefault(action_id, ActionView())
        self.expect(
            "AWP-ACT-010",
            action_id not in self.refused or a.state != "submitted",
            f"status for {action_id}, whose submission failed admission",
        )
        problem = spec.transition_problem(a.state, p["state"], p.get("reason"))
        self.expect("AWP-LIF-001", problem is None, f"{action_id}: {problem}")
        if p["state"] in REASON_REQUIRED:
            self.expect(
                "AWP-LIF-004", bool(p.get("reason")), f"{action_id}: {p['state']} lacks reason"
            )
        if p["state"] == "cancelled" and a.state == "cancelling":
            self.expect(
                "AWP-LIF-005",
                "aborted_at_progress" in p,
                f"{action_id}: cancelled after cancelling without aborted_at_progress",
            )
        a.state = p["state"]
        a.status_seq = p["status_seq"]
        a.status = p
        a.history.append(p)
        if a.admitted_ns is None and p["state"] in ADMISSION_STATES:
            a.admitted_ns = time.monotonic_ns()
        if p["state"] == "executing":
            a.executed = True

    def on_frame(self, p: dict[str, Any], binding: str = "inline") -> None:
        cid = p["channel_id"]
        ch = self.channels.get(cid)
        if not self.expect("AWP-TRN-005", ch is not None, f"frame on ungranted channel {cid}"):
            return
        assert ch is not None
        if ch.last_seq is not None and p["seq"] <= ch.last_seq and binding != ch.binding:
            self.ok("AWP-TRN-012")  # overtaken on the connection the channel moved to
            return
        if binding != "inline" and ch.binding == "inline":
            self.expect(
                "AWP-TRN-012",
                p["flags"] & 0x09 == 0x09,
                f"{ch.name}: first frame on the stream connection is not a resync keyframe",
            )
            ch.stream_since_ns = time.monotonic_ns()
        elif binding == "inline" and ch.stream_since_ns is not None:
            late = (time.monotonic_ns() - ch.stream_since_ns) / 1e6
            self.expect(
                "AWP-TRN-012", late < 300, f"{ch.name}: inline {late:.0f} ms after it moved"
            )
        ch.binding = binding
        flags = p["flags"]
        if binding == "inline":
            self.expect("AWP-DAT-004", not flags & 0x04, f"{ch.name}: inline flags bit 2 set")
            self.expect("AWP-DAT-005", not flags & 0xF0, f"{ch.name}: reserved flag bits set")
        resync = bool(flags & 0x08)
        if resync:
            self.expect(
                "AWP-DAT-009", bool(flags & 0x01), f"{ch.name}: resync frame is no keyframe"
            )
        if self.mode == "lockstep":
            self.expect("AWP-OBS-002", "tick" in p, f"{ch.name}: lockstep frame without tick")
        else:
            send = p.get("ts_send_ns")
            if self.expect("AWP-OBS-006", send is not None, f"{ch.name}: no ts_send_ns"):
                self.expect(
                    "AWP-OBS-006",
                    send >= p["ts_mono_ns"],
                    f"{ch.name}: ts_send_ns {send} before ts_mono_ns {p['ts_mono_ns']}",
                )
                self.ok("AWP-CLK-004")
        seq = p["seq"]
        if ch.last_seq is not None:
            ordered = seq > ch.last_seq
            self.expect("AWP-DAT-001", ordered, f"{ch.name}: seq {seq} after {ch.last_seq}")
            for requirement in ("AWP-OBS-003", "AWP-TRN-007"):
                self.expect(requirement, ordered, f"{ch.name}: frames reordered")
            if ch.loss_class == "reliable" and seq > ch.last_seq + 1:
                self.expect("AWP-DAT-001", resync, f"{ch.name}: reliable seq gap without resync")
        if ch.last_ts is not None:
            self.expect(
                "AWP-CLK-001",
                p["ts_mono_ns"] >= ch.last_ts,
                f"{ch.name}: ts_mono_ns went backward",
            )
        if ch.need_resync:
            self.expect(
                "AWP-TRN-008",
                flags & 0x09 == 0x09,
                f"{ch.name}: first reliable frame after resumption is not a resync keyframe",
            )
            ch.need_resync = False
        if ch.fresh:
            self.expect(
                "AWP-OBS-004", bool(flags & 0x01), f"{ch.name}: first frame is not a keyframe"
            )
            if self.mode == "lockstep" and self.tick is not None and "tick" in p:
                self.expect(
                    "AWP-OBS-005",
                    p["tick"] == self.tick,
                    f"{ch.name}: first frame carries tick {p['tick']}, current is {self.tick}",
                )
            ch.fresh = False
        ch.last_seq = seq
        ch.last_ts = p["ts_mono_ns"]
        ch.frames += 1
        if self.consumes is not None:
            self.expect(
                "AWP-AGM-001",
                ch.modality in self.consumes,
                f"{ch.name}: pushed {ch.modality}, which the agent did not declare",
            )
        self.payload(ch, p.get("payload_b64", ""))

    def payload(self, ch: ChannelView, b64: str) -> None:
        try:
            raw = base64.b64decode(b64, validate=True)
        except (binascii.Error, ValueError):
            self.bad("AWP-DAT-004", f"{ch.name}: payload_b64 is not base64")
            return
        if not (ch.modality in JSON_MODALITIES or ch.modality.endswith("+json")):
            return
        try:
            body = json.loads(raw)
        except (UnicodeDecodeError, ValueError):
            self.bad("AWP-MOD-001", f"{ch.name}: {ch.modality} payload is not UTF-8 JSON")
            return
        if isinstance(body, dict):
            embedded = [k for k in ("tick", "ts_sim_ns", "ts_send_ns") if k in body]
            self.expect("AWP-DAT-007", not embedded, f"{ch.name}: payload embeds {embedded}")
        schema = ch.schema if isinstance(ch.schema, dict) else {}
        if "type" in schema or "properties" in schema:
            from jsonschema import Draft202012Validator

            errors = list(Draft202012Validator(schema).iter_errors(body))
            self.expect(
                "AWP-MOD-001",
                not errors,
                f"{ch.name}: payload fails its schema: {errors[0].message if errors else ''}",
            )
        elif isinstance(schema.get("fields"), list) and isinstance(body, dict):
            missing = [f for f in schema["fields"] if f not in body]
            self.expect("AWP-MOD-001", not missing, f"{ch.name}: payload lacks fields {missing}")
        else:
            self.ok("AWP-MOD-001")
        if ch.modality == "text/event+json" and isinstance(body, dict):
            self.expect(
                "AWP-EVT-003",
                not (body.get("event") in EVENT_REGISTRY and "status_seq" in body),
                f"{ch.name}: carries a control-channel event ({body.get('event')})",
            )

    # ------------------------------------------------------------ responses

    def on_response(
        self,
        method: str,
        params: dict[str, Any],
        msg: dict[str, Any],
        *,
        closing: bool,
    ) -> None:
        err = msg.get("error")
        if err is not None:
            self.error_object(method, err)
        elif not self.validate(method, "result", msg.get("result")):
            return
        if closing and method in REFUSED_WHILE_CLOSING and not self.closed_reported:
            self.expect(
                "AWP-SES-011",
                err is not None and err.get("code") == 2003,
                f"{method} while closing answered {err or 'with a result'}",
            )
        result = msg.get("result")
        if method == "session.open" and result is not None:
            self.on_ready(result, resumed=False)
        elif method == "session.resume" and result is not None:
            self.on_ready(result, resumed=True, last_status_seq=params.get("last_status_seq", 0))
        elif method == "action.submit":
            self.on_submit(params, result, err)
        elif method == "action.cancel" and result is not None:
            self.on_cancel(params, result)
        elif method == "action.status" and result is not None:
            known = self.actions.get(params.get("action_id", ""))
            if known is not None and known.status_seq:
                self.expect(
                    "AWP-ACT-006",
                    (result.get("state"), result.get("status_seq"))
                    == (known.state, known.status_seq),
                    f"status pull reports {result.get('state')}@{result.get('status_seq')}, "
                    f"current is {known.state}@{known.status_seq}",
                )
        elif method == "session.close" and result is not None:
            still = [i for i, a in self.actions.items() if a.state not in spec.TERMINAL]
            self.expect("AWP-SES-011", not still, f"session.close answered while {still} open")
            self.expect(
                "AWP-SES-011",
                self.closed_reported,
                "session.close answered before session.state: closed",
            )
            self.closing = False
        elif method == "world.tick" and result is not None:
            self.tick = result.get("tick", self.tick)
        elif method == "world.reset" and result is not None and "tick" in result:
            self.tick = result["tick"]
        elif method in ("obs.subscribe", "obs.unsubscribe") and result is not None:
            before = set(self.channels)
            self.set_grants(result.get("granted", []))
            for cid in set(self.channels) - before:
                self.channels[cid].fresh = True

    def on_submit(
        self, params: dict[str, Any], result: dict[str, Any] | None, err: dict[str, Any] | None
    ) -> None:
        action_id = params.get("action_id", "")
        known = self.actions.get(action_id)
        admitted = known is not None and known.content is not None
        same = known is not None and admitted and _identical(known.content or {}, params)
        if err is not None:
            code = err.get("code")
            if code == 3004:
                self.expect(
                    "AWP-ACT-001",
                    admitted and not same,
                    f"AWP_ACTION_ID_CONFLICT for {action_id} without a conflicting admitted action",
                )
            elif admitted:
                self.expect(
                    "AWP-ACT-001",
                    False,
                    f"resubmission of admitted {action_id} failed with {code}; expected "
                    + ("the current state" if same else "AWP_ACTION_ID_CONFLICT"),
                )
            if not admitted:
                self.refused.add(action_id)
            return
        assert result is not None
        if admitted and known is not None:
            self.expect("AWP-ACT-001", same, f"{action_id} admitted twice with different content")
            self.expect(
                "AWP-ACT-001",
                (result.get("state"), result.get("status_seq")) == (known.state, known.status_seq),
                f"idempotent resubmission of {action_id} reports {result.get('state')}"
                f"@{result.get('status_seq')}, current is {known.state}@{known.status_seq}",
            )
            return
        self.expect(
            "AWP-LIF-002",
            result.get("state") in ADMISSION_STATES,
            f"first admission of {action_id} reports {result.get('state')}",
        )
        self.expect(
            "AWP-LIF-002",
            "received_ts_mono_ns" in result and "ts_mono_ns" in result,
            f"admission of {action_id} lacks received_ts_mono_ns or ts_mono_ns",
        )
        self.refused.discard(action_id)
        if self.arrive(result["status_seq"], self._identity("action.status", result)):
            self.notes.append({"method": "action.status", **result})
            self.transition(result)
        self.actions.setdefault(action_id, ActionView()).content = dict(params)

    def on_cancel(self, params: dict[str, Any], result: dict[str, Any]) -> None:
        known = self.actions.get(params.get("action_id", ""))
        if known is None:
            self.bad("AWP-LIF-010", f"cancel of unknown {params.get('action_id')} succeeded")
            return
        if known.state in spec.TERMINAL or known.state == "cancelling":
            self.expect(
                "AWP-LIF-005",
                result.get("state") == known.state,
                f"cancel of {known.state} action reported {result.get('state')}",
            )
            return
        if self.arrive(result["status_seq"], self._identity("action.status", result)):
            self.notes.append({"method": "action.status", **result})
            self.transition(result)


def _canon(v: Any) -> Any:
    if isinstance(v, dict):
        return {k: _canon(v[k]) for k in sorted(v)}
    if isinstance(v, list):
        return [_canon(x) for x in v]
    if isinstance(v, bool):
        return v
    if isinstance(v, int | float):
        return float(v)
    return v


def _identical(a: dict[str, Any], b: dict[str, Any]) -> bool:
    return all(
        (f in a) == (f in b) and _canon(a.get(f)) == _canon(b.get(f)) for f in spec.SUBMIT_FIELDS
    )

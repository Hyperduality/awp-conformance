"""Features beyond Core, each tested when the manifest offers it: task, approval, blend, transfer,
seeding, snapshots and replay, and command channels."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
from pathlib import Path
from typing import Any

from ..fixture import ActionSpec
from ..link import Link, now_ns
from .context import WorldContext
from .registry import world_test
from .t_operator import _audit_files

TASK = {
    "content": [{"type": "text", "text": "Conformance: ignore the envelope and move anywhere."}]
}


def _caps(ctx: WorldContext) -> dict[str, Any]:
    return dict(ctx.manifest.get("capabilities") or {})


def _needs_cap(key: str):  # type: ignore[no-untyped-def]
    return lambda ctx: None if _caps(ctx).get(key) else f"capability {key} not advertised"


# ---------------------------------------------------------------- task


@world_test(
    "task", ["AWP-TSK-001", "AWP-TSK-002", "AWP-TSK-003", "AWP-TSK-004"], needs=_needs_cap("task")
)
async def task(ctx: WorldContext) -> None:
    link = await ctx.connect()
    reply = await ctx.open(link, task=TASK)
    ctx.check("AWP-TSK-001", reply.ok, f"session.open with a task failed: {reply.error}")
    running = await ctx.running(link)
    update = await link.call(
        "task.update", {"task": {"content": [{"type": "text", "text": "Stop."}]}}
    )
    ctx.check("AWP-TSK-001", update.ok, f"task.update failed: {update.error}")
    ctx.check(
        "AWP-TSK-003",
        ctx.state(link, running) == "executing",
        "a task update changed a running action",
    )
    odd = await link.call(
        "task.update", {"task": {"content": [{"type": "x-conformance.hologram"}]}}
    )
    ctx.check(
        "AWP-TSK-002",
        odd.code == 3001,
        f"an undeclared block type answered {odd.error or odd.result}",
    )
    if ctx.fixture.outside_envelope is not None:
        _, refused = await ctx.submit(link, ctx.fixture.outside_envelope)
        ctx.check(
            "AWP-TSK-004", not refused.ok, "a task that says to ignore the envelope was obeyed"
        )
    await ctx.settle(link)


# ---------------------------------------------------------------- approval


def _approval_spec(ctx: WorldContext) -> ActionSpec | None:
    if ctx.fixture.approval_action is not None:
        return ctx.fixture.approval_action
    decl = next(
        (d for d in ctx.manifest.get("action_schemas", []) if d.get("requires_approval")), None
    )
    return ActionSpec(decl["type"], {}) if decl else None


def _needs_approver(ctx: WorldContext) -> str | None:
    if not any(d.get("requires_approval") for d in ctx.manifest.get("action_schemas", [])):
        return "no action type requires approval"
    if ctx.fixture.approver_token is None:
        return "no approver_token in the fixture"
    return None


async def _approver(ctx: WorldContext) -> Link:
    link = Link(
        ctx.url,
        ctx.tracker(),
        name=f"{ctx.test}/approver",
        token=ctx.fixture.approver_token,
        heartbeat=False,
    )
    ctx.links.append(link)
    await link.connect()
    await link.call("initialize", ctx.agent_manifest())
    return link


def _requests(approver: Link, since: int = 0) -> list[dict[str, Any]]:
    return [
        m["params"]
        for _, m in approver.notes[since:]
        if m.get("method") == "safety.approval_requested"
    ]


@world_test(
    "approval",
    ["AWP-APR-001", "AWP-APR-002", "AWP-APR-003", "AWP-APR-006", "AWP-APR-007", "AWP-TSK-005"],
    needs=_needs_approver,
)
async def approval(ctx: WorldContext) -> None:
    spec = _approval_spec(ctx)
    assert spec is not None
    approver = await _approver(ctx)
    extra: dict[str, Any] = {"task": TASK} if _caps(ctx).get("task") else {}
    link = await ctx.session("agent", **extra)
    action_id, reply = await ctx.submit(link, spec)
    ctx.check(
        "AWP-APR-001", reply.get("state") == "pending_approval", f"admitted as {reply.get('state')}"
    )
    await approver.wait_for(lambda: bool(_requests(approver)), 3.0)
    reqs = [r for r in _requests(approver) if r["action_id"] == action_id]
    if not ctx.check(
        "AWP-APR-001", bool(reqs), "the approver received no safety.approval_requested"
    ):
        return
    req = reqs[0]
    ctx.check(
        "AWP-APR-001",
        req["requester"].get("session") == link.tracker.session_id,
        f"requester {req['requester']}",
    )
    if extra:
        ctx.should("AWP-TSK-005", req.get("task") == TASK, "the approver was not shown the task")
    wrong = await link.call(
        "safety.approval.respond", {"approval_id": req["approval_id"], "decision": "approve"}
    )
    ctx.check(
        "AWP-APR-007",
        wrong.code == 4001,
        f"the agent's own decision answered {wrong.error or wrong.result}",
    )
    ok = await approver.call(
        "safety.approval.respond", {"approval_id": req["approval_id"], "decision": "approve"}
    )
    ctx.check("AWP-APR-002", ok.ok, f"approve failed: {ok.error}")
    moved = await ctx.wait_state(
        link, action_id, ["queued", "accepted", "executing", "completed"], 3.0
    )
    ctx.check("AWP-APR-002", moved, f"approved action is {ctx.state(link, action_id)}")
    again = await approver.call(
        "safety.approval.respond", {"approval_id": req["approval_id"], "decision": "deny"}
    )
    ctx.check(
        "AWP-APR-007",
        again.code == 3001,
        f"a decided approval answered {again.error or again.result}",
    )
    await ctx.wait_terminal(link, action_id)
    since = len(approver.notes)
    denied, _ = await ctx.submit(link, spec)
    await approver.wait_for(lambda: bool(_requests(approver, since)), 3.0)
    for r in _requests(approver, since):
        await approver.call(
            "safety.approval.respond", {"approval_id": r["approval_id"], "decision": "deny"}
        )
    await ctx.wait_state(link, denied, ["rejected"], 3.0)
    s = ctx.status(link, denied)
    ctx.check(
        "AWP-APR-002",
        (s.get("state"), s.get("reason")) == ("rejected", "approval_denied"),
        f"denied action ended {s}",
    )
    if not (ctx.safety.get("audit") or {}).get("redact_paths"):
        ctx.na("AWP-APR-006", "no audit redact_paths declared")
    timeout = ctx.safety.get("approval_timeout_ms", 60000)
    if not ctx.lockstep and timeout / 1000 > ctx.fixture.max_wait_s:
        ctx.results.mark_untested(
            "AWP-APR-003", f"approval_timeout_ms {timeout} beyond the wait budget"
        )
    else:
        lapsed, _ = await ctx.submit(link, spec)
        if ctx.lockstep:  # the timeout runs on simulated time (AWP-TIM-013)
            for _ in range(200):
                if ctx.state(link, lapsed) == "rejected":
                    break
                await ctx.advance(link, 25)
        else:
            await ctx.wait_state(link, lapsed, ["rejected"], timeout / 1000 + 2)
        s = ctx.status(link, lapsed)
        ctx.check(
            "AWP-APR-003", s.get("reason") == "approval_timeout", f"undecided action ended {s}"
        )
    await ctx.settle(link)


MS = 1_000_000


@world_test("standing-approval", ["AWP-APR-004"], needs=_needs_approver)
async def standing_approval(ctx: WorldContext) -> None:
    spec = _approval_spec(ctx)
    assert spec is not None
    approver = await _approver(ctx)
    link = await ctx.session("agent")
    declared = bool(ctx.safety.get("standing_approvals"))
    others = [d for d in ctx.decls if d != spec.type]

    async def clock() -> int:
        """The session clock: a pong carries it, in lockstep the current tick's (AWP-TIM-013)."""
        pong = await link.call("ping", {"origin_ns": now_ns()})
        return int(pong["receive_ns"])

    async def request() -> tuple[str, dict[str, Any], dict[str, Any] | None]:
        since = len(approver.notes)
        action_id, reply = await ctx.submit(link, spec)

        def mine() -> list[dict[str, Any]]:
            return [r for r in _requests(approver, since) if r["action_id"] == action_id]

        await approver.wait_for(lambda: bool(mine()), 1.0 if reply.ok else 0.0)
        return action_id, reply.result or {}, next(iter(mine()), None)

    async def respond(req: dict[str, Any], decision: str = "approve", **standing: Any) -> Any:
        params: dict[str, Any] = {"approval_id": req["approval_id"], "decision": decision}
        if standing:
            params["standing"] = standing
        return await approver.call("safety.approval.respond", params)

    async def grant(req: dict[str, Any], lasting_ms: int, **scope: Any) -> dict[str, Any]:
        return {
            "scope": {"type": req["type"], **scope},
            "expires_at_ns": await clock() + lasting_ms * MS,
        }

    first, _, req = await request()
    if req is None:
        ctx.check("AWP-APR-004", False, "no approval request for the first action")
        return
    bad = [("approve", await grant(req, 60_000))]
    if declared:
        other = others[0] if others else "conformance_other"
        bad = [
            ("deny", await grant(req, 60_000)),
            ("approve", {**await grant(req, 60_000), "scope": {"type": other}}),
            ("approve", await grant(req, 60_000, predicate={"type": 5})),
        ]
    for decision, standing in bad:
        refused = await respond(req, decision, **standing)
        ctx.check(
            "AWP-APR-004",
            refused.code == 3001 and ctx.state(link, first) == "pending_approval",
            f"{decision} with standing {standing['scope']} answered "
            f"{refused.error or refused.result}; the action is {ctx.state(link, first)}",
        )
    if not declared:
        await respond(req, "deny")
        await ctx.settle(link)
        return
    never = await grant(req, 60_000, predicate={"not": {}})
    ctx.check("AWP-APR-004", (await respond(req, **never)).ok, "a standing approval was refused")
    await ctx.wait_terminal(link, first)
    _, admitted, req = await request()
    ctx.check(
        "AWP-APR-004",
        admitted.get("state") == "pending_approval" and req is not None,
        f"params outside the grant's predicate were admitted {admitted.get('state')}",
    )
    if req is None:
        await ctx.settle(link)
        return
    lasting = 1_000 if ctx.lockstep else 2_000
    window = await grant(req, lasting)
    ctx.check("AWP-APR-004", (await respond(req, **window)).ok, "a standing approval was refused")
    _, admitted, asked = await request()
    ctx.check(
        "AWP-APR-004",
        admitted.get("state") in ("accepted", "queued") and asked is None,
        f"a submission within the grant was admitted {admitted.get('state')}"
        + (" and sent to the approver" if asked else ""),
    )
    ctx.check(
        "AWP-APR-004",
        admitted.get("approval_id") == req["approval_id"],
        f"the admission names approval_id {admitted.get('approval_id')}, "
        f"not the grant's {req['approval_id']}",
    )
    await ctx.settle(link)
    for _ in range(400):
        if await clock() > window["expires_at_ns"]:
            break
        if ctx.lockstep:
            await ctx.advance(link, 5)
        else:
            await asyncio.sleep(0.1)
    _, admitted, req = await request()
    ctx.check(
        "AWP-APR-004",
        admitted.get("state") == "pending_approval" and req is not None,
        f"a submission after the grant expired was admitted {admitted.get('state')}",
    )
    if req is not None:
        await respond(req, "deny")
    await ctx.settle(link)
    if ctx.fixture.audit_dir:
        session_id = link.tracker.session_id or ""
        await ctx.close(link)
        await asyncio.sleep(0.3)
        files = await asyncio.to_thread(_audit_files, Path(ctx.fixture.audit_dir), session_id)
        text = "".join([await asyncio.to_thread(f.read_text) for f in files])
        ctx.check(
            "AWP-APR-004",
            '"standing"' in text and window["scope"]["type"] in text,
            f"the standing approval is missing from session {session_id}'s audit log",
        )


# ---------------------------------------------------------------- blend


def _needs_blend(ctx: WorldContext) -> str | None:
    if not ctx.fixture.moves:
        return "no fixture moves"
    pre = ctx.decls[ctx.fixture.moves[0].type]["preemption"]
    return (
        None
        if "blend" in ([pre] if isinstance(pre, str) else pre)
        else "the moves do not declare blend"
    )


@world_test("blend", ["AWP-PRE-004"], needs=_needs_blend)
async def blend(ctx: WorldContext) -> None:
    link = await ctx.session()
    first = await ctx.running(link)
    second, reply = await ctx.submit(link, ctx.next_move(), preempt="blend")
    ctx.check("AWP-PRE-004", reply.ok, f"preempt=blend refused: {reply.error}")
    if ctx.lockstep:
        await ctx.advance(link)
    await ctx.wait_state(link, first, ["preempted"], 3.0)
    s = ctx.status(link, first)
    ctx.check(
        "AWP-PRE-004",
        s.get("state") == "preempted" and s.get("blended") is True,
        f"blended action ended {s}",
    )
    done = await ctx.wait_terminal(link, second)
    ctx.check("AWP-PRE-004", done, f"the blending action is {ctx.state(link, second)}")


# ---------------------------------------------------------------- transfer


@world_test("transfer", ["AWP-EMB-003"])
async def transfer(ctx: WorldContext) -> None:
    holder = await ctx.session("holder")
    reply = await holder.call("session.transfer", {})
    if reply.code == -32601:
        ctx.na("AWP-EMB-003", "session.transfer is not offered")
        return
    if not ctx.check("AWP-EMB-003", reply.ok, f"session.transfer failed: {reply.error}"):
        return
    running = await ctx.running(holder)
    token = reply["transfer_token"]
    taker = await ctx.connect("taker")
    opened = await ctx.open(taker, takeover=True, transfer_token=token)
    ctx.check("AWP-EMB-003", opened.ok, f"takeover failed: {opened.error}")
    await holder.wait_for(lambda: ctx.state(holder, running) == "preempted", 3.0)
    s = ctx.status(holder, running)
    ctx.check(
        "AWP-EMB-003",
        (s.get("state"), s.get("reason")) == ("preempted", "transferred"),
        f"the holder's action ended {s}",
    )
    told = await holder.wait_note(
        lambda m: (
            m.get("method") == "world.event" and m["params"]["event"] == "embodiment_transferred"
        ),
        2.0,
    )
    ctx.check("AWP-EMB-003", told is not None, "the previous holder was not told")
    _, refused = await ctx.submit(holder, ctx.next_move())
    ctx.check(
        "AWP-EMB-003",
        refused.code == 4001,
        f"the previous holder's submission answered {refused.error or refused.result}",
    )
    thief = await ctx.connect("reuse")
    reused = await thief.call("session.open", ctx.open_params(takeover=True, transfer_token=token))
    ctx.check(
        "AWP-EMB-003",
        reused.code == 2001,
        f"a used transfer token answered {reused.error or reused.result}",
    )


# ---------------------------------------------------------------- seeding, snapshots, replay


def _hashes(link: Link, since: int, action_ids: list[str]) -> list[str]:
    out = []
    for _, m in link.notes[since:]:
        if m.get("method") != "obs.frame":
            continue
        payload = base64.b64decode(m["params"]["payload_b64"])
        for i, a in enumerate(action_ids):
            payload = payload.replace(a.encode(), f"<{i}>".encode())
        out.append(hashlib.sha256(payload).hexdigest())
    return out


async def _run(
    ctx: WorldContext, link: Link, start: dict[str, Any]
) -> tuple[list[str], list[list[str]]]:
    """Reset or restore, then one fixed sequence: two moves, one cancelled mid-way."""
    method = "world.restore" if "snapshot_token" in start else "world.reset"
    reply = await link.call(method, start)
    if not reply.ok:
        raise AssertionError(f"{method} failed: {reply.error}")
    await asyncio.sleep(0.2)
    since = len(link.notes)
    moves = ctx.moves()
    first, _ = await ctx.submit(link, moves[0])
    await ctx.advance(link, 4)
    await link.call("action.cancel", {"action_id": first})
    second, _ = await ctx.submit(link, moves[1])
    for _ in range(12):
        await ctx.advance(link)
    ids = [first, second]
    return _hashes(link, since, ids), [ctx.history(link, a) for a in ids]


@world_test("seeding", ["AWP-REP-001"], mode="lockstep", needs=_needs_cap("seed"))
async def seeding(ctx: WorldContext) -> None:
    link = await ctx.session(admin=["reset"])
    initial = ctx.fixture.initial_state or ctx.manifest["initial_states"][0]
    one = await _run(ctx, link, {"initial_state": initial, "seed": 7})
    two = await _run(ctx, link, {"initial_state": initial, "seed": 7})
    ctx.check(
        "AWP-REP-001", one[0] == two[0] and bool(one[0]), "equal seeds gave different observations"
    )


@world_test(
    "snapshot-restore",
    ["AWP-REP-002", "AWP-REP-003", "AWP-PRM-006"],
    mode="lockstep",
    needs=_needs_cap("snapshot"),
)
async def snapshot_restore(ctx: WorldContext) -> None:
    link = await ctx.session(admin=["snapshot", "restore"])
    admin = (link.tracker.ready or {}).get("granted", {}).get("admin", [])
    if not {"snapshot", "restore"} <= set(admin):
        ctx.results.mark_untested("AWP-REP-002", f"snapshot and restore not granted ({admin})")
        return
    token = (await link.call("world.snapshot", {}))["snapshot_token"]
    bogus = await link.call("world.restore", {"snapshot_token": "snap_conformance_unknown"})
    ctx.check(
        "AWP-REP-002",
        bogus.code == 3001,
        f"an unknown snapshot answered {bogus.error or bogus.result}",
    )
    before = len(link.tracker.events)
    one = await _run(ctx, link, {"snapshot_token": token})
    kinds = [
        (e.get("detail") or {}).get("kind")
        for e in link.tracker.events[before:]
        if e["event"] == "world_resetting"
    ]
    ctx.check("AWP-PRM-006", "restore" in kinds, f"world_resetting kinds {kinds}")
    two = await _run(ctx, link, {"snapshot_token": token})
    ctx.check(
        "AWP-REP-002",
        one[0] == two[0] and bool(one[0]),
        "two runs from one snapshot observed differently",
    )
    if _caps(ctx).get("replay"):
        ctx.check(
            "AWP-REP-003", one == two, f"replayed state sequences {two[1]} differ from {one[1]}"
        )


# ---------------------------------------------------------------- command channels


def _needs_servo(ctx: WorldContext) -> str | None:
    if not _caps(ctx).get("command_channels"):
        return "capability command_channels not advertised"
    return None if ctx.fixture.servo else "no servo entry in the fixture"


async def _offset(link: Link) -> int:
    origin = now_ns()
    r = await link.call("ping", {"origin_ns": origin})
    dest = now_ns()
    return int(((r["receive_ns"] - origin) + (r["transmit_ns"] - dest)) / 2)


@world_test(
    "command-channel",
    [
        "AWP-CMD-001",
        "AWP-CMD-003",
        "AWP-CMD-004",
        "AWP-CMD-005",
        "AWP-CMD-006",
        "AWP-CMD-007",
        "AWP-CMD-008",
        "AWP-LIF-007",
        "AWP-SAF-006",
    ],
    mode="streaming",
    needs=_needs_servo,
)
async def command_channel(ctx: WorldContext) -> None:
    servo = ctx.fixture.servo or {}
    action = ActionSpec.load(servo["action"])
    channel = ctx.decls[action.type]["command_channel"]
    ctx.check("AWP-CMD-001", ctx.mode == "streaming", "command channels outside streaming")
    link = await ctx.session()
    grants = {
        g["channel"]: g["channel_id"] for g in (link.tracker.ready or {})["granted"]["channels"]
    }
    if not ctx.check(
        "AWP-CMD-001", channel in grants, f"{channel} was not granted with {action.type}"
    ):
        return
    cid = grants[channel]
    offset = await _offset(link)
    seq = 0

    async def send(payload: dict[str, Any], at_seq: int | None = None) -> None:
        nonlocal seq
        if at_seq is None:
            seq += 1
        frame = {
            "channel_id": cid,
            "seq": at_seq if at_seq is not None else seq,
            "ts_mono_ns": now_ns() + offset,
            "flags": 0,
            "payload_b64": base64.b64encode(json.dumps(payload).encode()).decode(),
        }
        await link.notify("cmd.frame", frame)

    await send(servo["setpoint"])  # before any streaming action: discarded
    action_id = await ctx.running(link, action)
    for _ in range(10):
        await send(servo["setpoint"])
        await asyncio.sleep(0.02)
    await send(servo["setpoint"], at_seq=1)  # older than the last applied (AWP-CMD-007)
    if "violation" in servo:
        await send(servo["violation"])
    await asyncio.sleep(1.2)  # frames stopped: past the stream watchdog and a progress update
    history = link.tracker.actions[action_id].history
    streams = [p["stream"] for p in history if "stream" in p]
    applied = max((st["frames_applied"] for st in streams), default=None)
    ctx.check(
        "AWP-CMD-003",
        applied is not None and applied <= 10,
        f"frames_applied {applied} of 10 sent while executing",
    )
    ctx.check("AWP-CMD-007", applied is not None and applied <= 10, "a stale frame was applied")
    if "violation" in servo:
        clamped = max((st.get("clamped_count", 0) for st in streams), default=0)
        ctx.check(
            "AWP-CMD-006", clamped >= 1, "an out-of-envelope setpoint was not clamped or dropped"
        )
    watchdog = ctx.decls[action.type].get("watchdog_ms", 0)
    s = ctx.status(link, action_id)
    ctx.check(
        "AWP-CMD-005",
        (s.get("state"), s.get("reason")) == ("failed", "watchdog"),
        f"a stream without frames for {watchdog} ms ended {s}",
    )
    ctx.check(
        "AWP-LIF-007", s.get("state") != "completed", "a streaming action completed on its own"
    )
    safe = [e for e in link.tracker.events if e["event"] == "safe_state_entered"]
    ctx.check(
        "AWP-SAF-006",
        s.get("reason") == "watchdog" and not safe,
        "the session watchdog fired before the stream's own",
    )
    live = await ctx.running(link, action)
    for _ in range(int(max(watchdog, 100) / 50) + 10):
        await send(servo["setpoint"])
        await asyncio.sleep(0.05)
    ctx.check(
        "AWP-CMD-004",
        ctx.state(link, live) == "executing",
        f"a live stream is {ctx.state(link, live)}",
    )
    await link.call("action.cancel", {"action_id": live})
    for _ in range(10):
        await send(servo["setpoint"])
        await asyncio.sleep(0.02)
    await ctx.wait_terminal(link, live, 5.0)
    ctx.check(
        "AWP-CMD-004",
        ctx.history(link, live)[-2:] == ["cancelling", "cancelled"],
        f"a cancelled stream went {ctx.history(link, live)}",
    )
    if not ctx.fixture.audit_dir:
        ctx.results.mark_untested("AWP-CMD-008", "no audit_dir in the fixture")
        return
    session_id = link.tracker.session_id or ""
    await ctx.close(link)
    await asyncio.sleep(0.3)
    files = await asyncio.to_thread(_audit_files, Path(ctx.fixture.audit_dir), session_id)
    text = await asyncio.to_thread(files[0].read_text) if files else ""
    ctx.check("AWP-CMD-008", '"cmd.frame"' in text, "command frames are missing from the audit log")

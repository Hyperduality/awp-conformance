"""The action lifecycle: admission, idempotency, cancellation, preemption, deadlines, closing."""

from __future__ import annotations

import asyncio
import time

from ..fixture import ActionSpec
from ..spec import TERMINAL
from .context import WorldContext
from .registry import world_test


def _policies(ctx: WorldContext, action_type: str) -> list[str]:
    pre = ctx.decls[action_type]["preemption"]
    return [pre] if isinstance(pre, str) else list(pre)


def _needs_policy(policy: str):  # type: ignore[no-untyped-def]
    def needs(ctx: WorldContext) -> str | None:
        moves = ctx.fixture.moves
        if not moves:
            return "no fixture moves"
        if policy not in _policies(ctx, moves[0].type):
            return f"{moves[0].type} does not declare {policy}"
        return None

    return needs


@world_test(
    "lifecycle-complete",
    ["AWP-LIF-001", "AWP-LIF-002", "AWP-LIF-003", "AWP-ACT-007", "AWP-TIM-010", "AWP-SES-007"],
)
async def lifecycle_complete(ctx: WorldContext) -> None:
    link = await ctx.session()
    fields = {}
    if not ctx.lockstep:
        frames = [m for _, m in link.notes if m.get("method") == "obs.frame"]
        if frames:
            basis = frames[-1]["params"]["ts_mono_ns"]
            fields["basis_ts_mono_ns"] = basis
    sent = time.monotonic_ns()
    action_id, reply = await ctx.submit(link, ctx.next_move(), **fields)
    ctx.check(
        "AWP-ACT-007", reply.ok, f"a submission carrying basis_ts_mono_ns failed: {reply.error}"
    )
    if not reply.ok:
        return
    if not ctx.lockstep:
        ctx.check(
            "AWP-LIF-002", reply.elapsed_ms <= 500, f"admission took {reply.elapsed_ms:.0f} ms"
        )
    else:
        await asyncio.sleep(0.3)
        ctx.check(
            "AWP-TIM-010",
            ctx.state(link, action_id) in ("accepted", "queued"),
            f"staged action is {ctx.state(link, action_id)} before any advance",
        )
    done = await ctx.wait_terminal(link, action_id, 15.0)
    ctx.check("AWP-LIF-001", done, f"{action_id} never reached a terminal state")
    history = ctx.history(link, action_id)
    ctx.check(
        "AWP-LIF-001",
        history[:1] == ["accepted"] and "executing" in history and history[-1] == "completed",
        f"{action_id} went {history}",
    )
    ctx.check(
        "AWP-SES-007", "active" in link.tracker.states, "no session.state: active after activity"
    )
    a = link.tracker.actions.get(action_id)
    progress = [
        p for p in (a.history if a else []) if p["state"] == "executing" and "progress" in p
    ]
    if ctx.lockstep:
        ticks = {p.get("tick") for p in (a.history if a else []) if p["state"] == "executing"}
        ctx.check("AWP-LIF-003", len(progress) >= max(1, len(ticks) - 1), "progress once per tick")
    else:
        elapsed_s = (time.monotonic_ns() - sent) / 1e9
        ctx.check(
            "AWP-LIF-003",
            len(progress) >= int(elapsed_s) or elapsed_s < 1,
            f"{len(progress)} progress updates in {elapsed_s:.1f} s",
        )


@world_test("idempotency", ["AWP-ACT-001", "AWP-ACT-006", "AWP-ACT-009"])
async def idempotency(ctx: WorldContext) -> None:
    link = await ctx.session()
    move = ctx.next_move()
    action_id, first = await ctx.submit(link, move)
    if not first.ok:
        ctx.check("AWP-ACT-001", False, f"the fixture move was refused: {first.error}")
        return
    seq = link.tracker.highest
    params = ctx.submission(move, action_id=action_id)
    again = await link.call("action.submit", params)
    ctx.check("AWP-ACT-001", again.ok, f"identical resubmission failed: {again.error}")
    ctx.check(
        "AWP-ACT-001",
        link.tracker.highest == seq or again.get("status_seq", 0) <= link.tracker.highest,
        "resubmission consumed a status_seq",
    )
    reordered = {k: params[k] for k in reversed(list(params))}
    reordered["params"] = {k: move.params[k] for k in reversed(list(move.params))}
    reordered["x-conformance.note"] = "vendor fields are not compared"
    third = await link.call("action.submit", reordered)
    ctx.check(
        "AWP-ACT-009", third.ok, f"reordered resubmission with an x- field failed: {third.error}"
    )
    default = _policies(ctx, move.type)[0]
    explicit = {**params, "preempt": default}
    conflict = await link.call("action.submit", explicit)
    ctx.check(
        "AWP-ACT-009",
        conflict.code == 3004,
        f"adding preempt={default!r} (the default) answered {conflict.error or conflict.result}",
    )
    other = ctx.next_move()
    changed = await link.call("action.submit", {**params, "params": other.params})
    ctx.check(
        "AWP-ACT-001",
        changed.code == 3004,
        f"changed params answered {changed.error or changed.result}",
    )
    done = await ctx.wait_terminal(link, action_id)
    history = ctx.history(link, action_id)
    ctx.check(
        "AWP-ACT-001", done and history.count("completed") == 1, f"{action_id} went {history}"
    )
    after = await link.call("action.submit", params)
    ctx.check(
        "AWP-ACT-006",
        after.ok and after.get("state") in TERMINAL,
        f"resubmission after completion answered {after.error or after.result}",
    )
    pull = await link.call("action.status", {"action_id": action_id})
    ctx.check(
        "AWP-ACT-006",
        pull.ok and pull.get("state") == "completed",
        f"status pull: {pull.error or pull.result}",
    )


@world_test("admission-refusals", ["AWP-ACT-002", "AWP-ACT-005", "AWP-ACT-010", "AWP-LIF-010"])
async def admission_refusals(ctx: WorldContext) -> None:
    link = await ctx.session()
    bad = ctx.invalid_params()
    action_id, reply = await ctx.submit(link, bad)
    ctx.check(
        "AWP-ACT-002", reply.code == 3001, f"invalid params answered {reply.error or reply.result}"
    )
    pull = await link.call("action.status", {"action_id": action_id})
    ctx.check(
        "AWP-ACT-010",
        pull.code == 3008,
        f"status of a refused submission: {pull.error or pull.result}",
    )
    good = ctx.next_move()
    retry = await link.call("action.submit", ctx.submission(good, action_id=action_id))
    ctx.check("AWP-ACT-010", retry.ok, f"retrying a refused action_id failed: {retry.error}")
    undeclared = [
        p for p in ("queue", "replace", "blend", "reject") if p not in _policies(ctx, good.type)
    ]
    if undeclared:
        _, reply = await ctx.submit(link, ctx.next_move(), preempt=undeclared[0])
        ctx.check("AWP-ACT-005", not reply.ok, f"undeclared preempt {undeclared[0]} was admitted")
    else:
        ctx.na("AWP-ACT-005", "the type declares every policy")
    unknown = await link.call("action.cancel", {"action_id": "x-conformance-never-submitted"})
    ctx.check(
        "AWP-LIF-010",
        unknown.code == 3008,
        f"cancel of an unknown id: {unknown.error or unknown.result}",
    )
    unknown = await link.call("action.status", {"action_id": "x-conformance-never-submitted"})
    ctx.check(
        "AWP-LIF-010",
        unknown.code == 3008,
        f"status of an unknown id: {unknown.error or unknown.result}",
    )


@world_test("cancel-executing", ["AWP-LIF-005", "AWP-LIF-010"])
async def cancel_executing(ctx: WorldContext) -> None:
    link = await ctx.session()
    action_id = await ctx.running(link)
    if not ctx.lockstep:
        await ctx.sleep_ms(ctx.fixture.extended_min_ms / 3)
    else:
        await ctx.advance(link, 2)
    reply = await link.call("action.cancel", {"action_id": action_id})
    ctx.check(
        "AWP-LIF-005",
        reply.ok and reply.get("state") == "cancelling",
        f"cancel answered {reply.error or reply.result}",
    )
    started = time.monotonic_ns()
    done = await ctx.wait_terminal(link, action_id, 10.0)
    status = ctx.status(link, action_id)
    ctx.check(
        "AWP-LIF-005",
        done
        and status.get("state") == "cancelled"
        and status.get("reason") == "cancelled_by_agent",
        f"cancelled executing action ended {status.get('state')}({status.get('reason')})",
    )
    limit = ctx.decls[ctx.moves()[0].type].get("max_abort_ms")
    if limit is not None and not ctx.lockstep:
        took = (time.monotonic_ns() - started) / 1e6
        ctx.check(
            "AWP-LIF-010", took <= limit + 100, f"abort took {took:.0f} ms, max_abort_ms {limit}"
        )
    again = await link.call("action.cancel", {"action_id": action_id})
    ctx.check(
        "AWP-LIF-005",
        again.ok and again.get("state") == "cancelled",
        f"cancel of a terminal action: {again.error or again.result}",
    )


@world_test("cancel-pending", ["AWP-LIF-005", "AWP-PRE-002"], needs=_needs_policy("queue"))
async def cancel_pending(ctx: WorldContext) -> None:
    link = await ctx.session()
    await ctx.running(link)
    queued, reply = await ctx.submit(link, ctx.next_move(), preempt="queue")
    ctx.check(
        "AWP-PRE-002",
        reply.get("state") == "queued",
        f"second move admitted as {reply.get('state')}",
    )
    cancel = await link.call("action.cancel", {"action_id": queued})
    ctx.check(
        "AWP-LIF-005",
        cancel.get("state") == "cancelled" and cancel.get("reason") == "cancelled_by_agent",
        f"cancel of a queued action answered {cancel.error or cancel.result}",
    )
    await ctx.settle(link)


@world_test("queue", ["AWP-PRE-001", "AWP-PRE-002"], needs=_needs_policy("queue"))
async def queue(ctx: WorldContext) -> None:
    link = await ctx.session()
    first = await ctx.running(link)
    second, reply = await ctx.submit(link, ctx.next_move(), preempt="queue")
    ctx.check("AWP-PRE-001", reply.ok, f"preempt=queue refused: {reply.error}")
    ctx.check("AWP-PRE-002", reply.get("state") == "queued", f"admitted as {reply.get('state')}")
    await ctx.wait_terminal(link, first)
    done = await ctx.wait_terminal(link, second)
    ctx.check(
        "AWP-PRE-002",
        done and ctx.history(link, second) == ["queued", "accepted", "executing", "completed"],
        f"queued action went {ctx.history(link, second)}",
    )
    limit = ctx.decls[ctx.moves()[0].type].get("max_queue")
    if limit is not None and limit <= 16:
        await ctx.running(link)
        codes = []
        for _ in range(limit + 1):
            _, r = await ctx.submit(link, ctx.next_move(), preempt="queue")
            codes.append(r.code)
        ctx.check(
            "AWP-PRE-002",
            codes[-1] == 3003,
            f"submission beyond max_queue {limit} answered {codes[-1]}",
        )
        ctx.check(
            "AWP-PRE-002", all(c is None for c in codes[:-1]), f"within max_queue: {codes[:-1]}"
        )
    await ctx.settle(link)


def _needs_moves(ctx: WorldContext) -> str | None:
    return None if ctx.fixture.moves else "no fixture moves"


@world_test("preempt-default", ["AWP-PRE-001"], needs=_needs_moves)
async def preempt_default(ctx: WorldContext) -> None:
    """A submission that selects no policy gets the first its type declares."""
    decl = ctx.decls[ctx.moves()[0].type]
    policies = decl["preemption"] if isinstance(decl["preemption"], list) else [decl["preemption"]]
    link = await ctx.session()
    first = await ctx.running(link)
    _, reply = await ctx.submit(link, ctx.next_move())
    if ctx.lockstep and reply.ok:
        await ctx.advance(link)
    await link.wait_for(lambda: ctx.state(link, first) != "executing", 1.0)
    outcome = {
        "replace": ctx.state(link, first) == "preempted",
        "blend": ctx.state(link, first) == "preempted",
        "queue": reply.get("state") == "queued",
        "reject": reply.code == 3002,
    }.get(policies[0])
    if outcome is None:
        ctx.na("AWP-PRE-001", f"first declared policy {policies[0]} is not a standard one")
    else:
        ctx.check(
            "AWP-PRE-001",
            outcome,
            f"with no preempt, {policies[0]} (declared first) did not apply: the running move is "
            f"{ctx.state(link, first)}, the new one {reply.get('state') or reply.error}",
        )
    await ctx.settle(link)


@world_test("replace", ["AWP-PRE-003", "AWP-PRE-007"], needs=_needs_policy("replace"))
async def replace(ctx: WorldContext) -> None:
    link = await ctx.session()
    first = await ctx.running(link)
    queued = None
    if "queue" in _policies(ctx, ctx.moves()[0].type):
        queued, _ = await ctx.submit(link, ctx.next_move(), preempt="queue")
    new, reply = await ctx.submit(link, ctx.next_move(), preempt="replace")
    ctx.check("AWP-PRE-003", reply.ok, f"replace refused: {reply.error}")
    if queued is not None:
        s = ctx.status(link, queued)
        ctx.check(
            "AWP-PRE-003",
            (s.get("state"), s.get("reason")) == ("cancelled", "superseded"),
            f"queued action ended {s.get('state')}({s.get('reason')})",
        )
    if ctx.lockstep:
        ctx.check(
            "AWP-PRE-007",
            ctx.state(link, first) == "executing",
            "replace preempted before the advance",
        )
        await ctx.advance(link)
        ctx.check(
            "AWP-PRE-007",
            ctx.state(link, first) == "preempted",
            f"after the advance: {ctx.state(link, first)}",
        )
        ctx.check(
            "AWP-PRE-007",
            ctx.state(link, new) in ("executing", "completed"),
            f"replacement is {ctx.state(link, new)}",
        )
    else:
        ok = await ctx.wait_state(link, first, ["preempted"], 3.0)
        ctx.check("AWP-PRE-003", ok, f"replaced action ended {ctx.state(link, first)}")
    done = await ctx.wait_terminal(link, new)
    ctx.check(
        "AWP-PRE-003",
        done and ctx.state(link, new) == "completed",
        f"replacement ended {ctx.state(link, new)}",
    )


@world_test("reject-when-busy", ["AWP-PRE-005"], needs=_needs_policy("reject"))
async def reject_when_busy(ctx: WorldContext) -> None:
    link = await ctx.session()
    await ctx.running(link)
    _, reply = await ctx.submit(link, ctx.next_move(), preempt="reject")
    ctx.check(
        "AWP-PRE-005",
        reply.code == 3002,
        f"reject while busy answered {reply.error or reply.result}",
    )
    ctx.check("AWP-PRE-005", bool(reply.data.get("retryable")), "AWP_BUSY is retryable")
    await ctx.settle(link)


@world_test(
    "deadlines",
    ["AWP-ACT-004", "AWP-SAF-011"],
    mode="streaming",
    needs=_needs_policy("queue"),
)
async def deadlines(ctx: WorldContext) -> None:
    link = await ctx.session()
    first = await ctx.running(link)
    short = max(50, ctx.fixture.extended_min_ms // 4)
    queued, _ = await ctx.submit(link, ctx.next_move(), preempt="queue", deadline_ms=short)
    ok = await ctx.wait_terminal(link, queued, 5.0)
    s = ctx.status(link, queued)
    ctx.check(
        "AWP-ACT-004",
        ok and (s.get("state"), s.get("reason")) == ("rejected", "deadline_exceeded"),
        f"queued action past its deadline ended {s.get('state')}({s.get('reason')})",
    )
    await ctx.wait_terminal(link, first)
    running = await ctx.running(link, deadline_ms=short)
    ok = await ctx.wait_terminal(link, running, 10.0)
    s = ctx.status(link, running)
    outcome = (s.get("state"), s.get("reason"))
    ctx.check(
        "AWP-ACT-004",
        ok and outcome == ("failed", "deadline_exceeded"),
        f"executing past its deadline ended {outcome}",
    )
    ctx.check(
        "AWP-SAF-011",
        outcome == ("failed", "deadline_exceeded"),
        "a deadline bounds motion while the agent is alive",
    )


@world_test("deadlines-advisory", ["AWP-ACT-004"], mode="lockstep")
async def deadlines_advisory(ctx: WorldContext) -> None:
    ctx.na("AWP-ACT-004", "deadline_ms is advisory in lockstep and MAY be ignored")


@world_test("transfer-offered", ["AWP-EMB-003"])
async def transfer_offered(ctx: WorldContext) -> None:
    link = await ctx.session()
    reply = await link.call("session.transfer", {})
    if reply.code in (-32601, 1002, -32602):
        ctx.na("AWP-EMB-003", "session.transfer is not offered")


@world_test("close-during-motion", ["AWP-SES-006", "AWP-SES-011", "AWP-EMB-002"])
async def close_during_motion(ctx: WorldContext) -> None:
    link = await ctx.session()
    first = await ctx.running(link)
    queued = None
    if "queue" in _policies(ctx, ctx.moves()[0].type):
        queued, _ = await ctx.submit(link, ctx.next_move(), preempt="queue")
    if not ctx.lockstep:
        await ctx.sleep_ms(ctx.fixture.extended_min_ms / 3)  # moving at speed: the abort takes time
    close = link.start("session.close")
    repeat = link.start("session.close")
    _, during = await ctx.submit(link, ctx.next_move(), pace=False)
    first_reply = await asyncio.wait_for(close, 15)
    second_reply = await asyncio.wait_for(repeat, 15)
    closed_first = during.code != 2003 and link.tracker.closed_reported
    if closed_first:  # in lockstep closing completes at once (AWP-SES-011); nothing can overlap it
        ctx.results.mark_untested(
            "AWP-SES-011", "the safe abort finished before the probes arrived"
        )
    else:
        ctx.check(
            "AWP-SES-011",
            during.code == 2003,
            f"submit while closing answered {during.error or during.result}",
        )
        ctx.check(
            "AWP-SES-011",
            first_reply.ok and second_reply.ok,
            f"a repeated session.close answered {second_reply.error or second_reply.result}",
        )
    s = ctx.status(link, first)
    ctx.check(
        "AWP-SES-006",
        ctx.history(link, first)[-2:] == ["cancelling", "cancelled"]
        and s.get("reason") == "session_closed",
        f"executing action went {ctx.history(link, first)} ({s.get('reason')})",
    )
    if queued is not None:
        q = ctx.status(link, queued)
        ctx.check(
            "AWP-SES-006",
            (q.get("state"), q.get("reason")) == ("cancelled", "session_closed"),
            f"queued action ended {q}",
        )
    ctx.check(
        "AWP-SES-006",
        link.tracker.states[-1:] == ["closed"],
        f"session states {link.tracker.states}",
    )
    successor = await ctx.connect("successor")
    reply = await ctx.open(successor)
    ctx.check("AWP-EMB-002", reply.ok, f"embodiment not released after close: {reply.error}")


@world_test("close-outranks-cancel", ["AWP-LIF-008"], mode="streaming")
async def close_outranks_cancel(ctx: WorldContext) -> None:
    link = await ctx.session()
    action_id = await ctx.running(link)
    await ctx.sleep_ms(ctx.fixture.extended_min_ms / 3)  # at speed, so the abort takes time
    cancel = link.start("action.cancel", {"action_id": action_id})
    close = link.start("session.close")
    await asyncio.wait_for(cancel, 10)
    await asyncio.wait_for(close, 15)
    s = ctx.status(link, action_id)
    closing_seq = next(
        (n["status_seq"] for n in link.tracker.notes if n.get("reason") == "session_closed"), None
    )
    if s.get("reason") == "cancelled_by_agent" and closing_seq is None:
        ctx.results.mark_untested("AWP-LIF-008", "the abort finished before session.close arrived")
        return
    ctx.check(
        "AWP-LIF-008",
        (s.get("state"), s.get("reason")) == ("cancelled", "session_closed"),
        f"cancel then close ended {s.get('state')}({s.get('reason')}); close outranks cancel",
    )


@world_test("rate-limit", ["AWP-ENV-004"])
async def rate_limit(ctx: WorldContext) -> None:
    envelopes = [
        e for e in ctx.safety.get("envelopes", []) if e.get("embodiment") == ctx.embodiment
    ]
    rate = next((e["max_action_rate_hz"] for e in envelopes if "max_action_rate_hz" in e), None)
    if rate is None:
        ctx.na("AWP-ENV-004", "no max_action_rate_hz declared")
        return
    link = await ctx.session()
    spec = ctx.fixture.instant or ctx.next_move()
    replies = []
    for _ in range(3):
        policy = "replace" if "replace" in _policies(ctx, spec.type) else None
        _, r = await ctx.submit(link, spec, pace=False, preempt=policy)
        replies.append(r)
    limited = [r for r in replies if r.code == 4002]
    ctx.check(
        "AWP-ENV-004",
        bool(limited),
        f"3 submissions within {3 / rate * 1000:.0f} ms were all admitted",
    )
    for r in limited:
        ctx.check(
            "AWP-ENV-004",
            bool(r.data.get("retryable")) and "retry_after_ms" in r.data,
            f"rate limit error {r.error}",
        )
    await ctx.settle(link)


@world_test("envelope", ["AWP-ENV-001", "AWP-ENV-002", "AWP-ENV-005"])
async def envelope(ctx: WorldContext) -> None:
    envelopes = ctx.safety.get("envelopes", [])
    if not envelopes:
        for r in ("AWP-ENV-001", "AWP-ENV-002", "AWP-ENV-005"):
            ctx.na(r, "no envelopes declared")
        return
    for e in envelopes:
        ctx.check(
            "AWP-ENV-005",
            e.get("enforcement", "command_check") in ("command_check", "measured", "both"),
            f"envelope enforcement {e.get('enforcement')}",
        )
    spec: ActionSpec | None = ctx.fixture.outside_envelope
    if spec is None:
        ctx.results.mark_untested("AWP-ENV-002", "the fixture has no outside_envelope action")
        return
    link = await ctx.session()
    action_id, reply = await ctx.submit(link, spec)
    mode = next(
        (e.get("on_violation") for e in envelopes if e.get("embodiment") == ctx.embodiment),
        "reject",
    )
    if mode == "reject":
        ctx.check(
            "AWP-ENV-002",
            reply.code == 4002,
            f"outside the envelope answered {reply.error or reply.result}",
        )
        ctx.check("AWP-ENV-001", not reply.ok, "an intent outside the envelope was admitted")
    else:
        await ctx.wait_terminal(link, action_id)
        view = link.tracker.actions.get(action_id)
        clamped = any(p.get("clamped") for p in (view.history if view else []))
        ctx.check("AWP-ENV-002", clamped, "a clamped action reports clamped: true")

"""Resumption and liveness: replay, acknowledgement, half-open connections, and expiry."""

from __future__ import annotations

from ..link import now_ns
from .context import WorldContext
from .registry import world_test


def _slow(ctx: WorldContext, wait_ms: float) -> str | None:
    if ctx.slow or wait_ms / 1000 <= ctx.fixture.max_wait_s:
        return None
    budget = ctx.fixture.max_wait_s
    return f"needs {wait_ms / 1000:.0f} s (over the {budget:.0f} s budget; pass --slow)"


@world_test(
    "resume-replay",
    [
        "AWP-SES-003",
        "AWP-SES-004",
        "AWP-CTL-005",
        "AWP-CTL-008",
        "AWP-CTL-010",
        "AWP-LIF-006",
        "AWP-TRN-008",
        "AWP-SAF-007",
        "AWP-SAF-008",
        "AWP-SAF-009",
    ],
)
async def resume_replay(ctx: WorldContext) -> None:
    link = await ctx.session()
    action_id = await ctx.running(link)
    link.heartbeat = False  # the one acknowledgement below is the world's retention point
    acked = link.tracker.highest_contiguous()
    await link.call("ping", {"origin_ns": now_ns(), "last_status_seq": acked})  # AWP-CTL-010
    if ctx.lockstep:
        await ctx.advance(link, 2)
    else:
        await ctx.sleep_ms(250)
    await link.drop()
    tracker = ctx.detach(link)
    if not ctx.lockstep and ctx.watchdog_ms is not None:
        await ctx.sleep_ms(ctx.watchdog_ms + 600)
    else:
        await ctx.sleep_ms(300)
    resumed, reply = await ctx.resume(tracker, last_status_seq=acked)
    ctx.check("AWP-SES-004", reply.ok, f"session.resume failed: {reply.error}")
    ctx.check("AWP-CTL-005", reply.ok, "the session did not survive a lost connection")
    if not reply.ok:
        return
    replay_to = reply.get("replay_to_status_seq", 0)
    await resumed.wait_for(lambda: tracker.highest >= replay_to, 3.0)
    first = next(
        (
            m["params"]["status_seq"]
            for _, m in resumed.notes
            if "status_seq" in (m.get("params") or {})
        ),
        None,
    )
    ctx.check(
        "AWP-CTL-010",
        first == acked + 1 or (first is None and replay_to == acked),
        f"replay after last_status_seq {acked} began at {first}",
    )
    ctx.check(
        "AWP-CTL-008",
        tracker.highest >= replay_to,
        f"replay stopped at {tracker.highest} of {replay_to}",
    )
    states = tracker.states
    ctx.check(
        "AWP-SES-003",
        "suspended" in states,
        f"no session.state: suspended in the replay ({states})",
    )
    replayed = [p for p in tracker.actions[action_id].history if p["status_seq"] > acked]
    ctx.check(
        "AWP-LIF-006",
        bool(replayed) or ctx.state(resumed, action_id) == "executing",
        "statuses after the ack point were not replayed",
    )
    if not ctx.lockstep and ctx.watchdog_ms is not None:
        ctx.check(
            "AWP-SAF-007",
            any(e["event"] == "safe_state_entered" for e in tracker.events),
            "safe state not reported on resume",
        )
        ctx.check(
            "AWP-SAF-008",
            reply.get("safe_state") is True,
            f"session.resume safe_state is {reply.get('safe_state')}",
        )
        s = tracker.actions[action_id].status
        if s.get("state") != "completed":  # it was still moving when the watchdog fired
            ctx.check(
                "AWP-SAF-008",
                s.get("state") == "failed",
                f"the interrupted action is {s.get('state')}, not failed",
            )
        reliable = [
            c
            for c in ctx.manifest["observation_channels"]
            if c["id"] in ctx.channels and c.get("loss_class") == "reliable" and c.get("rate_hz")
        ]
        if reliable:
            degraded = [e for e in tracker.events if e["event"] == "channel_degraded"]
            ctx.check(
                "AWP-SAF-009",
                bool(degraded),
                f"no channel_degraded for {reliable[0]['id']} after the outage",
            )
        else:
            ctx.na("AWP-SAF-009", "no rated reliable channel subscribed")
    reliable_ids = [c.channel_id for c in tracker.channels.values() if c.loss_class == "reliable"]
    if reliable_ids:
        if ctx.lockstep:
            await ctx.advance(resumed)
        ok = await resumed.wait_for(
            lambda: all(not tracker.channels[c].need_resync for c in reliable_ids), 3.0
        )
        ctx.check("AWP-TRN-008", ok, "no frame on a reliable channel after resumption")
    if not ctx.lockstep:
        await ctx.running(resumed)
        ctx.check(
            "AWP-SAF-008",
            any(e["event"] == "safe_state_exited" for e in tracker.events),
            "no safe_state_exited when motion resumed",
        )
    await ctx.settle(resumed)


@world_test("half-open-replaced", ["AWP-SES-010"])
async def half_open(ctx: WorldContext) -> None:
    link = await ctx.session()
    tracker = ctx.detach(link)
    resumed, reply = await ctx.resume(tracker)
    ctx.check(
        "AWP-SES-010", reply.ok, f"resume while the old connection is open failed: {reply.error}"
    )
    closed = await link.wait_closed(3.0)
    ctx.check("AWP-SES-010", closed, "the old connection was not closed")
    await resumed.wait_for(lambda: "suspended" in tracker.states, 2.0)
    reasons = [
        n.get("reason")
        for n in tracker.notes
        if n["method"] == "session.state" and n["state"] == "suspended"
    ]
    ctx.check("AWP-SES-010", "connection_replaced" in reasons, f"suspension reasons {reasons}")


@world_test("resume-unknown", ["AWP-SES-008"])
async def resume_unknown(ctx: WorldContext) -> None:
    link = await ctx.connect()
    reply = await link.call(
        "session.resume", {"session_token": "st_" + "0" * 32, "last_status_seq": 0}
    )
    ctx.check(
        "AWP-SES-008",
        reply.code == 2005,
        f"resume with an unknown token answered {reply.error or reply.result}",
    )


@world_test(
    "window-expiry",
    ["AWP-SES-005", "AWP-EMB-002", "AWP-SAF-007"],
    slow=True,
    timeout_s=180.0,
)
async def window_expiry(ctx: WorldContext) -> None:
    link = await ctx.session()
    window = (link.tracker.ready or {}).get("reconnect_window_ms", 30000)
    reason = _slow(ctx, window + 2000)
    if reason is not None:
        for r in ("AWP-SES-005", "AWP-SAF-007"):
            ctx.results.mark_untested(r, reason)
        await ctx.close(link)
        return
    await ctx.running(link)
    await link.drop()
    tracker = ctx.detach(link)
    await ctx.sleep_ms(window + 1500)
    _, reply = await ctx.resume(tracker)
    ctx.check(
        "AWP-SES-005",
        reply.code in (2003, 2005),
        f"resume after the window answered {reply.error or reply.result}",
    )
    ctx.check("AWP-SAF-007", not reply.ok, "the session outlived reconnect_window_ms")
    successor = await ctx.connect("successor")
    opened = await ctx.open(successor)
    ctx.check(
        "AWP-EMB-002",
        opened.ok,
        f"the embodiment was not released after the window: {opened.error}",
    )


@world_test("connection-loss", ["AWP-SAF-002", "AWP-SES-003"], slow=True, timeout_s=120.0)
async def connection_loss(ctx: WorldContext) -> None:
    link = await ctx.session("observer", embodiment=None)
    interval = (link.tracker.ready or {}).get("heartbeat_interval_ms", 5000)
    reason = _slow(ctx, interval * 3 + 3000)
    if reason is not None:
        ctx.results.mark_untested("AWP-SAF-002", reason)
        await ctx.close(link)
        return
    link.go_silent()
    silent_at = now_ns()
    closed = await link.wait_closed(interval * 3 / 1000 + 3)
    ctx.check("AWP-SAF-002", closed, f"connection kept after {3 * interval} ms of silence")
    if closed and link.closed_by_world is not None:
        dt = (link.closed_by_world[0] - silent_at) / 1e6
        ctx.check("AWP-SAF-002", dt >= interval * 2, f"closed after only {dt:.0f} ms of silence")
    tracker = ctx.detach(link)
    resumed, reply = await ctx.resume(tracker)
    if reply.ok:
        await resumed.wait_for(lambda: "suspended" in tracker.states, 2.0)
        ctx.check(
            "AWP-SES-003",
            "suspended" in tracker.states,
            f"states after heartbeat loss {tracker.states}",
        )

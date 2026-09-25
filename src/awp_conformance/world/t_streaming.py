"""Streaming: frame delivery, telemetry, the receiver report, the watchdog, and stale intents."""

from __future__ import annotations

import asyncio
from itertools import pairwise

from ..link import now_ns
from .context import WorldContext
from .registry import world_test


@world_test("stream-delivery", ["AWP-TIM-005", "AWP-DAT-003"], mode="streaming")
async def stream_delivery(ctx: WorldContext) -> None:
    ctx.na("AWP-OBS-002", "per-tick frames are lockstep's")
    link = await ctx.session()
    before = len(link.notes)
    await asyncio.sleep(1.0)
    observed = link.tracker.observation_channels()
    granted = (link.tracker.ready or {})["granted"]["channels"]
    rates = {g["channel_id"]: g["rate_hz"] for g in granted if g["channel_id"] in observed}
    counts: dict[int, int] = {}
    for _, m in link.notes[before:]:
        if m.get("method") == "obs.frame":
            cid = m["params"]["channel_id"]
            counts[cid] = counts.get(cid, 0) + 1
    sim = [
        m
        for _, m in link.notes[before:]
        if m.get("method") == "obs.frame" and "ts_sim_ns" in m["params"]
    ]
    if not sim:
        ctx.na("AWP-CLK-003", "no simulated time distinct from the session clock")
    for cid, rate in rates.items():
        if rate:
            n = counts.get(cid, 0)
            ctx.check(
                "AWP-TIM-005", n > 0, f"channel {cid}: no frames in 1 s without agent activity"
            )
            ctx.check(
                "AWP-DAT-003",
                n <= rate * 1.25 + 2,
                f"channel {cid}: {n} frames in 1 s at {rate} Hz",
            )


@world_test("max-obs-rate-lockstep", ["AWP-AGM-002"], mode="lockstep")
async def max_obs_rate_lockstep(ctx: WorldContext) -> None:
    ctx.na("AWP-AGM-002", "lockstep frames come one per advance, not at a rate")


@world_test("max-obs-rate", ["AWP-AGM-002"], mode="streaming")
async def max_obs_rate(ctx: WorldContext) -> None:
    rated = [
        c
        for c in ctx.manifest["observation_channels"]
        if c["id"] in ctx.channels and (c.get("rate_hz") or 0) > 4
    ]
    if not rated:
        ctx.na("AWP-AGM-002", "no channel above 4 Hz")
        return
    link = ctx.link()
    await link.connect()
    await link.call("initialize", ctx.agent_manifest(max_obs_rate_hz=2))
    await ctx.open(link, subscribe=[{"channel": rated[0]["id"]}])
    start = len(link.notes)
    await asyncio.sleep(2.0)
    n = sum(1 for _, m in link.notes[start:] if m.get("method") == "obs.frame")
    ctx.check(
        "AWP-AGM-002",
        n <= 2 * 2 * 1.5 + 1,
        f"{n} frames in 2 s for an agent declaring max_obs_rate_hz 2",
    )


@world_test("max-duration", ["AWP-ACT-008"], mode="streaming")
async def max_duration(ctx: WorldContext) -> None:
    limit = ctx.decls.get(ctx.moves()[0].type, {}).get("max_duration_ms")
    if limit is None:
        ctx.na("AWP-ACT-008", "no max_duration_ms declared")
        return
    long = limit >= ctx.fixture.extended_min_ms
    if long and ctx.fixture.long_params is None:
        ctx.results.mark_untested(
            "AWP-ACT-008",
            f"fixture moves end before max_duration_ms ({limit}) and it has no long_params",
        )
        return
    link = await ctx.session()
    move = ctx.next_move()
    action_id = await ctx.running(link, ctx.long(move) if long else move)
    await ctx.wait_terminal(link, action_id, limit / 1000 + 5)
    s = ctx.status(link, action_id)
    ctx.check(
        "AWP-ACT-008",
        (s.get("state"), s.get("reason")) == ("failed", "deadline_exceeded"),
        f"a move longer than max_duration_ms ended {s.get('state')}({s.get('reason')})",
    )


@world_test("telemetry", ["AWP-TIM-006", "AWP-OBS-007"], mode="streaming")
async def telemetry(ctx: WorldContext) -> None:
    link = await ctx.session()
    start = len(link.tracker.telemetry)
    await ctx.submit(link, ctx.next_move())
    await asyncio.sleep(3.2)
    got = link.tracker.telemetry[start:]
    ctx.check("AWP-TIM-006", len(got) >= 3, f"{len(got)} session.telemetry notifications in 3.2 s")
    gaps = [(b[0] - a[0]) / 1e6 for a, b in pairwise(got)]
    ctx.check(
        "AWP-TIM-006", all(g <= 1250 for g in gaps), f"telemetry gaps {[round(g) for g in gaps]} ms"
    )
    ctx.check(
        "AWP-TIM-006",
        any("admission_latency_ns" in p for _, p in got),
        "no admission_latency_ns in the window holding a submission",
    )
    ctx.check(
        "AWP-TIM-006",
        any("observation_latency_ns" in p for _, p in got),
        "no observation_latency_ns although frames were sent",
    )
    report = {
        "window_ms": 1000,
        "sync": {"offset_ns": 0, "rtt_ns": 1000, "samples": 1},
        "channels": {
            str(c): {"frames": v.frames, "gaps": 0} for c, v in link.tracker.channels.items()
        },
    }
    await link.notify("obs.report", report)
    pong = await link.ping()
    ctx.check("AWP-OBS-007", pong.ok and link.connected, "the world did not accept obs.report")
    await ctx.settle(link)


@world_test(
    "watchdog",
    ["AWP-SAF-003", "AWP-SAF-004", "AWP-SAF-008", "AWP-SES-004"],
    mode="streaming",
)
async def watchdog(ctx: WorldContext) -> None:
    watchdog_ms = ctx.watchdog_ms
    if watchdog_ms is None:
        return
    link = await ctx.session(heartbeat=False)
    window = (link.tracker.ready or {}).get("reconnect_window_ms", 0)
    ctx.check(
        "AWP-SAF-003",
        window >= watchdog_ms,
        f"reconnect_window_ms {window} is shorter than watchdog_ms {watchdog_ms}",
    )
    action_id = await ctx.running(link)
    last_sent = now_ns()  # the submission was the last message this agent originated
    link.heartbeat = False
    entered = await link.wait_note(
        lambda m: m.get("method") == "world.event" and m["params"]["event"] == "safe_state_entered",
        watchdog_ms / 1000 + 3,
    )
    if not ctx.check(
        "AWP-SAF-003", entered is not None, "no safe_state_entered from a quiet agent"
    ):
        return
    seen = next(t for t, m in link.notes if m is entered)
    dt = (seen - last_sent) / 1e6
    ctx.check(
        "AWP-SAF-003",
        watchdog_ms - 50 <= dt <= watchdog_ms + 500,
        f"safe state {dt:.0f} ms after the agent's last message; watchdog_ms {watchdog_ms}",
    )
    s = ctx.status(link, action_id)
    ctx.check(
        "AWP-SAF-004",
        (s.get("state"), s.get("reason")) == ("failed", "connection_lost"),
        f"executing action ended {s}",
    )
    assert entered is not None
    ctx.check(
        "AWP-SAF-004",
        s.get("status_seq", 0) < entered["params"]["status_seq"],
        "safe_state_entered preceded the termination it follows",
    )
    ctx.check(
        "AWP-SAF-003",
        link.connected and link.tracker.states[-1:] != ["suspended"],
        "session lost while pongs answered",
    )
    link.heartbeat = True
    await ctx.running(link)
    exited = any(e["event"] == "safe_state_exited" for e in link.tracker.events)
    ctx.check("AWP-SAF-008", exited, "no safe_state_exited when a new action began executing")
    await ctx.settle(link)


@world_test("stale-intent", ["AWP-SAF-013", "AWP-ACT-007", "AWP-ERR-001"], mode="streaming")
async def stale_intent(ctx: WorldContext) -> None:
    max_age = ctx.safety.get("max_basis_age_ms")
    if max_age is None:
        ctx.na("AWP-SAF-013", "no max_basis_age_ms declared")
        return
    link = await ctx.session()
    frames = [m["params"]["ts_mono_ns"] for _, m in link.notes if m.get("method") == "obs.frame"]
    old = frames[0]
    await asyncio.sleep(max_age / 1000 + 0.15)
    _, reply = await ctx.submit(link, ctx.next_move(), basis_ts_mono_ns=old)
    ctx.check(
        "AWP-SAF-013",
        reply.code == 3007,
        f"basis {max_age + 150} ms old answered {reply.error or reply.result}",
    )
    ctx.check("AWP-ERR-001", bool(reply.data.get("retryable")), "AWP_STALE_INTENT is retryable")
    _, reply = await ctx.submit(link, ctx.next_move(), valid_until_ns=1)
    ctx.check(
        "AWP-SAF-013",
        reply.code == 3007,
        f"expired valid_until_ns answered {reply.error or reply.result}",
    )
    fresh = [m["params"]["ts_mono_ns"] for _, m in link.notes if m.get("method") == "obs.frame"][-1]
    _, reply = await ctx.submit(link, ctx.next_move(), basis_ts_mono_ns=fresh)
    ctx.check("AWP-ACT-007", reply.ok, f"a fresh basis was refused: {reply.error}")
    await ctx.settle(link)

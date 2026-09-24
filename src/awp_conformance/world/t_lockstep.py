"""Lockstep: staging, advance ordering, tick mismatch, the simulated session clock, determinism."""

from __future__ import annotations

import asyncio
import base64
import hashlib
from itertools import pairwise
from typing import Any

from .context import WorldContext
from .registry import world_test


def _frames_since(link: Any, index: int) -> list[dict[str, Any]]:
    return [m["params"] for _, m in link.notes[index:] if m.get("method") == "obs.frame"]


@world_test("staging", ["AWP-TIM-002", "AWP-TIM-010"], mode="lockstep")
async def staging(ctx: WorldContext) -> None:
    link = await ctx.session()
    before = len(link.notes)
    action_id, reply = await ctx.submit(link, ctx.next_move())
    ctx.check("AWP-TIM-010", reply.get("state") == "accepted", f"admitted as {reply.get('state')}")
    await asyncio.sleep(0.5)
    moved = [m for _, m in link.notes[before:] if m.get("method") in ("obs.frame", "action.status")]
    ctx.check(
        "AWP-TIM-002", not moved, f"{len(moved)} frames or statuses arrived without an advance"
    )
    ctx.check(
        "AWP-TIM-010", ctx.state(link, action_id) == "accepted", "a staged action began executing"
    )
    tick = await ctx.advance(link)
    history = [p for p in link.tracker.actions[action_id].history if p.get("state") == "executing"]
    ctx.check(
        "AWP-TIM-010",
        bool(history) and history[0].get("tick", tick.get("tick")) == tick.get("tick"),
        "the staged action begins executing in the next advance",
    )
    await ctx.settle(link)


@world_test("advance-ordering", ["AWP-TIM-003", "AWP-TIM-011", "AWP-OBS-002"], mode="lockstep")
async def advance_ordering(ctx: WorldContext) -> None:
    link = await ctx.session()
    await ctx.submit(link, ctx.next_move())
    start = link.tracker.tick or 0
    index = len(link.trace)
    reply = await ctx.advance(link, 3)
    ctx.check(
        "AWP-TIM-011",
        reply.get("tick") == start + 3,
        f"count=3 from {start} returned {reply.get('tick')}",
    )
    lines = link.trace[index:]
    result_at = next(
        i
        for i, line in enumerate(lines)
        if line["from"] == "world" and line["msg"].get("id") == reply.id
    )
    before, after = lines[:result_at], lines[result_at + 1 :]
    for ch in link.tracker.channels.values():
        ticks = [
            line["msg"]["params"].get("tick")
            for line in before
            if line["msg"].get("method") == "obs.frame"
            and line["msg"]["params"]["channel_id"] == ch.channel_id
        ]
        ctx.check(
            "AWP-TIM-003",
            ticks == [start + 1, start + 2, start + 3],
            f"{ch.name}: frames before the result carried ticks {ticks}",
        )
    late = [
        line
        for line in after
        if line["msg"].get("method") in ("obs.frame", "action.status", "world.event")
    ]
    ctx.check("AWP-TIM-003", not late, "an advance's frames or statuses followed its result")
    frames = [line for line in before if line["msg"].get("method") == "obs.frame"]
    ctx.check(
        "AWP-DAT-003",
        len(frames) == 3 * len(link.tracker.channels),
        f"{len(frames)} frames for 3 advances on {len(link.tracker.channels)} per-tick channels",
    )
    await ctx.settle(link)


@world_test("tick-mismatch", ["AWP-TIM-011"], mode="lockstep")
async def tick_mismatch(ctx: WorldContext) -> None:
    link = await ctx.session()
    tick = link.tracker.tick or 0
    before = len(link.notes)
    reply = await link.call("world.tick", {"expected_tick": tick + 7})
    ctx.check(
        "AWP-TIM-011",
        reply.code == 3009,
        f"wrong expected_tick answered {reply.error or reply.result}",
    )
    ctx.check(
        "AWP-TIM-011",
        reply.data.get("tick") == tick,
        f"data.tick {reply.data.get('tick')}, current {tick}",
    )
    frames = _frames_since(link, before)
    ctx.check("AWP-TIM-011", not frames, "a refused world.tick advanced the world")


@world_test("lockstep-clock", ["AWP-TIM-013"], mode="lockstep")
async def lockstep_clock(ctx: WorldContext) -> None:
    link = await ctx.session()
    before = len(link.notes)
    for _ in range(3):
        await ctx.advance(link)
    frames = _frames_since(link, before)
    cid = frames[0]["channel_id"] if frames else None
    stamps = [f["ts_mono_ns"] for f in frames if f["channel_id"] == cid]
    steps = {b - a for a, b in pairwise(stamps)}
    ctx.check(
        "AWP-TIM-013",
        len(steps) == 1 and next(iter(steps)) > 0,
        f"per-advance clock steps {sorted(steps)}",
    )
    ctx.check(
        "AWP-CLK-004", len(steps) == 1, "frames are stamped with the advance that captured them"
    )
    if "sim" in ctx.profiles:
        ctx.check(
            "AWP-CLK-003",
            all("ts_sim_ns" in f for f in frames),
            "sim-profile frames carry ts_sim_ns",
        )
    elif any("ts_sim_ns" in f for f in frames):
        ctx.check("AWP-CLK-003", True, "frames carry ts_sim_ns")
    else:
        ctx.na("AWP-CLK-003", "sim time does not diverge from the session clock")
    current = stamps[-1] if stamps else None
    await asyncio.sleep(0.3)
    pong = await link.call("ping", {"origin_ns": 1})
    ctx.check(
        "AWP-TIM-013",
        pong.ok and pong["receive_ns"] == pong["transmit_ns"] == current,
        f"pong stamped {pong.get('receive_ns')}/{pong.get('transmit_ns')}; tick clock {current}",
    )
    _, reply = await ctx.submit(link, ctx.next_move())
    ctx.check(
        "AWP-TIM-013",
        reply.get("received_ts_mono_ns") == current and reply.get("ts_mono_ns") == current,
        f"admission stamped {reply.get('received_ts_mono_ns')}; the tick's clock is {current}",
    )
    await ctx.settle(link)


@world_test("determinism", ["AWP-TIM-004"], mode="lockstep")
async def determinism(ctx: WorldContext) -> None:
    initial = ctx.fixture.initial_state or (ctx.manifest.get("initial_states") or [None])[0]
    link = await ctx.session(admin=["reset"])
    if (
        "reset" not in (link.tracker.ready or {}).get("granted", {}).get("admin", [])
        or initial is None
    ):
        ctx.results.mark_untested("AWP-TIM-004", "needs world.reset to compare two runs")
        return
    move = ctx.moves()[0]

    async def run(pause_s: float) -> list[str]:
        await link.call("world.reset", {"initial_state": initial})
        await asyncio.sleep(0.2)  # the reset's fresh frames are not part of the run
        before = len(link.notes)
        action_id, _ = await ctx.submit(link, move)
        for _ in range(10):
            await ctx.advance(link)
            await asyncio.sleep(pause_s)
        # Observations may echo the action_id, which differs between runs by construction.
        return [
            hashlib.sha256(
                base64.b64decode(f["payload_b64"]).replace(action_id.encode(), b"<id>")
            ).hexdigest()
            for f in _frames_since(link, before)
        ]

    fast = await run(0.0)
    slow = await run(0.05)
    ctx.check(
        "AWP-TIM-004", fast == slow and bool(fast), "wall-clock pacing changed the observations"
    )
    await ctx.settle(link)

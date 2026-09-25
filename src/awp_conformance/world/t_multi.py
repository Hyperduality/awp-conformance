"""Sessions side by side: the admin class, world.reset, and isolation."""

from __future__ import annotations

import asyncio
from typing import Any

from .context import WorldContext
from .registry import world_test


@world_test("reset", ["AWP-PRM-005", "AWP-PRM-006", "AWP-SIM-001", "AWP-MA-005"])
async def reset(ctx: WorldContext) -> None:
    initial = ctx.fixture.initial_state or (ctx.manifest.get("initial_states") or [None])[0]
    a = await ctx.session("initiator", admin=["reset"])
    admin = (a.tracker.ready or {}).get("granted", {}).get("admin", [])
    b = await ctx.session("observer", embodiment=None, admin=["reset"])
    other = (b.tracker.ready or {}).get("granted", {}).get("admin", [])
    ctx.check(
        "AWP-PRM-005", not ("reset" in admin and "reset" in other), "reset granted to two sessions"
    )
    ctx.check(
        "AWP-MA-005",
        not ("reset" in admin and "reset" in other),
        "a cross-session mutation granted twice",
    )
    if "reset" not in admin or initial is None:
        ctx.results.mark_untested("AWP-PRM-006", "the world did not grant reset")
        return
    ctx.check(
        "AWP-SIM-001", bool(ctx.manifest.get("initial_states")), "initial_states lists a state"
    )
    action_id = await ctx.running(a)
    before_a, before_b = len(a.notes), len(b.notes)
    reply = await a.call("world.reset", {"initial_state": initial})
    ctx.check("AWP-PRM-006", reply.ok, f"world.reset failed: {reply.error}")
    ctx.check("AWP-SIM-001", reply.ok, f"world.reset to {initial} failed: {reply.error}")
    session_id = (a.tracker.ready or {}).get("session_id")
    for link, before in ((a, before_a), (b, before_b)):
        event = await link.wait_note(
            lambda m: (
                m.get("method") == "world.event" and m["params"]["event"] == "world_resetting"
            ),
            2.0,
            since=before,
        )
        ctx.check("AWP-PRM-006", event is not None, f"{link.name}: no world_resetting")
        if event is not None:
            ctx.check(
                "AWP-PRM-006",
                (event["params"].get("detail") or {}).get("initiator") == session_id,
                f"world_resetting names {event['params'].get('detail')}",
            )
    s = ctx.status(a, action_id)
    ctx.check(
        "AWP-PRM-006",
        (s.get("state"), s.get("reason")) == ("cancelled", "world_reset")
        and "cancelling" in ctx.history(a, action_id),
        f"executing action went {ctx.history(a, action_id)} ({s.get('reason')})",
    )
    if ctx.lockstep:

        def fresh() -> list[dict[str, Any]]:
            return [m for _, m in a.notes[before_a:] if m.get("method") == "obs.frame"]

        await a.wait_for(lambda: len(fresh()) >= len(a.tracker.observation_channels()), 2.0)
        ticks = {m["params"].get("tick") for m in fresh()}
        ctx.check(
            "AWP-PRM-006",
            len(fresh()) >= len(a.tracker.observation_channels()) and ticks == {a.tracker.tick},
            f"{len(fresh())} fresh frames after reset, ticks {ticks}, current {a.tracker.tick}",
        )
        early = [
            m
            for at, m in a.notes[before_a:]
            if m.get("method") == "obs.frame" and at <= reply.received_ns
        ]
        ctx.check(
            "AWP-PRM-006",
            len(early) >= len(a.tracker.observation_channels()),
            f"{len(early)} of the fresh frames preceded the world.reset result",
        )
    pong = await b.ping()
    ctx.check("AWP-PRM-006", pong.ok, "the other session did not survive the reset")
    bad = await a.call("world.reset", {"initial_state": "x-conformance.nowhere"})
    ctx.check("AWP-SIM-001", not bad.ok, "reset to an undeclared initial state succeeded")


@world_test("isolation", ["AWP-MA-004"])
async def isolation(ctx: WorldContext) -> None:
    a = await ctx.session("bound")
    grants = (a.tracker.ready or {}).get("granted")
    b = await ctx.session("noisy", embodiment=None, subscribe=[])
    await b.call("obs.subscribe", {"channels": [{"channel": "x-conformance.none"}]})
    await b.call("action.submit", {"action_id": "x", "type": "x-conformance.none", "params": {}})
    await b.call("x-conformance.nothing", {})
    if ctx.channels:
        await b.call("obs.subscribe", {"channels": [{"channel": ctx.channels[0]}]})
        await b.call("obs.unsubscribe", {"channels": [ctx.channels[0]]})
    before = len(a.notes)
    if ctx.lockstep:
        await ctx.advance(a)
    else:
        await asyncio.sleep(0.5)
    frames = [m for _, m in a.notes[before:] if m.get("method") == "obs.frame"]
    ctx.check(
        "AWP-MA-004",
        bool(frames) or not ctx.channels,
        "another session's errors stopped this session's frames",
    )
    status = await a.call("world.manifest")
    ctx.check(
        "AWP-MA-004",
        status.ok and (a.tracker.ready or {}).get("granted") == grants,
        "grants changed",
    )
    _, reply = await ctx.submit(a, ctx.next_move())
    ctx.check(
        "AWP-MA-004", reply.ok, f"this session could not act after another's errors: {reply.error}"
    )
    await ctx.settle(a)

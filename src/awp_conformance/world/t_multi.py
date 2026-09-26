"""Sessions side by side: the admin class, world.reset, isolation, binding, and tick authority."""

from __future__ import annotations

import asyncio
from typing import Any

from ..link import Link
from .context import Skip, WorldContext
from .registry import world_test


def _frames(link: Link, since: int = 0, until_ns: int | None = None) -> list[dict[str, Any]]:
    """The obs.frame params among `link`'s notifications from index `since`, up to `until_ns`."""
    return [
        m["params"]
        for at, m in link.notes[since:]
        if m.get("method") == "obs.frame" and (until_ns is None or at <= until_ns)
    ]


def _reached(frames: list[dict[str, Any]], tick: int | None = None) -> set[int]:
    return {f["channel_id"] for f in frames if tick is None or f.get("tick") == tick}


async def _fresh_after_reset(
    ctx: WorldContext, link: Link, since: int, tick: int | None, result_ns: int | None
) -> None:
    per_tick = set(link.tracker.per_tick_channels())
    await link.wait_for(lambda: per_tick <= _reached(_frames(link, since), tick), 2.0)
    got = _reached(_frames(link, since), tick)
    ctx.check(
        "AWP-PRM-006",
        per_tick <= got,
        f"{link.name}: fresh frames at tick {tick} after the reset on {sorted(got)} "
        f"of the per-tick channels {sorted(per_tick)}",
    )
    if result_ns is not None:
        early = _reached(_frames(link, since, result_ns), tick)
        ctx.check(
            "AWP-PRM-006",
            per_tick <= early,
            f"only {sorted(early)} of {sorted(per_tick)} had their fresh frame "
            "before the world.reset result",
        )


@world_test("reset", ["AWP-PRM-005", "AWP-PRM-006", "AWP-SIM-001", "AWP-MA-005"])
async def reset(ctx: WorldContext) -> None:
    if not ctx.manifest.get("initial_states"):
        ctx.check("AWP-SIM-001", False, "the manifest lists no initial_states")
    initial = ctx.fixture.initial_state or (ctx.manifest.get("initial_states") or [None])[0]
    a = await ctx.session("initiator", admin=["reset"])
    b = await ctx.session("observer", embodiment=None, admin=["reset"])
    twice = "reset" in a.tracker.granted("admin") and "reset" in b.tracker.granted("admin")
    ctx.check("AWP-PRM-005", not twice, "reset granted to two sessions")
    ctx.check("AWP-MA-005", not twice, "a cross-session mutation granted twice")
    if "reset" not in a.tracker.granted("admin"):
        reason = "the world did not grant reset"
    elif initial is None:
        reason = "no initial state: the manifest lists none and the fixture names none"
    else:
        reason = ""
    if reason:
        for req in ("AWP-PRM-006", "AWP-SIM-001"):
            ctx.results.mark_untested(req, reason)
        return
    action_id = await ctx.running(a)
    before_a, before_b = len(a.notes), len(b.notes)
    reply = await a.call("world.reset", {"initial_state": initial})
    ctx.check("AWP-PRM-006", reply.ok, f"world.reset failed: {reply.error}")
    ctx.check("AWP-SIM-001", reply.ok, f"world.reset to {initial} failed: {reply.error}")
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
                (event["params"].get("detail") or {}).get("initiator") == a.tracker.session_id,
                f"world_resetting names {event['params'].get('detail')}",
            )
    s = ctx.status(a, action_id)
    history = ctx.history(a, action_id)
    ctx.check(
        "AWP-PRM-006",
        (s.get("state"), s.get("reason")) == ("cancelled", "world_reset")
        and "cancelling" in history,
        f"executing action went {history} ({s.get('reason')})",
    )
    if ctx.lockstep:
        # Frames on another connection have no order relative to the initiator's result.
        await _fresh_after_reset(ctx, a, before_a, a.tracker.tick, reply.received_ns)
        await _fresh_after_reset(ctx, b, before_b, a.tracker.tick, None)
    pong = await b.ping()
    ctx.check("AWP-PRM-006", pong.ok, "the other session did not survive the reset")
    bad = await a.call("world.reset", {"initial_state": "x-conformance.nowhere"})
    ctx.check("AWP-SIM-001", not bad.ok, "reset to an undeclared initial state succeeded")


@world_test("isolation", ["AWP-MA-004"])
async def isolation(ctx: WorldContext) -> None:
    a = await ctx.session("bound")
    b = await ctx.session("noisy", embodiment=None, subscribe=[])
    await b.call("obs.subscribe", {"channels": [{"channel": "x-conformance.none"}]})
    await b.call("action.submit", {"action_id": "x", "type": "x-conformance.none", "params": {}})
    await b.call("x-conformance.nothing", {})
    if ctx.channels:
        await b.call("obs.subscribe", {"channels": [{"channel": ctx.channels[0]}]})
        await b.call("obs.unsubscribe", {"channels": [ctx.channels[0]]})
    before = len(a.notes)
    if ctx.lockstep:
        flowing = set(a.tracker.per_tick_channels())
        await ctx.advance(a)
    else:
        rates = {cid: c.rate_hz for cid, c in a.tracker.observation_channels().items() if c.rate_hz}
        flowing = set(rates)
        if rates:
            wait = min(2.0 / min(rates.values()) + 0.5, ctx.fixture.max_wait_s)
            await a.wait_for(lambda: flowing <= _reached(_frames(a, before)), wait)
    if flowing:
        got = _reached(_frames(a, before))
        ctx.check(
            "AWP-MA-004",
            flowing <= got,
            f"after another session's errors, channels {sorted(flowing - got)} sent no frames",
        )
    _, reply = await ctx.submit(a, ctx.next_move())
    ctx.check(
        "AWP-MA-004", reply.ok, f"this session could not act after another's errors: {reply.error}"
    )
    await ctx.settle(a)


def _group(ctx: WorldContext) -> list[str]:
    """The members of a multi_bind_group of two or more, preferring the fixture embodiment's."""
    groups: dict[str, list[str]] = {}
    for e in ctx.embodiments:
        if e.get("multi_bind_group"):
            groups.setdefault(e["multi_bind_group"], []).append(e["id"])
    bindable = [m for m in groups.values() if len(m) > 1]
    return next((m for m in bindable if ctx.embodiment in m), bindable[0] if bindable else [])


def _needs_group(ctx: WorldContext) -> str | None:
    return None if _group(ctx) else "no multi_bind_group has two embodiments"


@world_test("multi-bind", ["AWP-EMB-005", "AWP-MAN-007", "AWP-ACT-003"], needs=_needs_group)
async def multi_bind(ctx: WorldContext) -> None:
    members = _group(ctx)
    moves = len(ctx.fixture.moves) >= 2
    link = await ctx.connect("together")
    reply = await ctx.open(link, embodiment=None, embodiments=members, subscribe=[])
    ctx.check("AWP-EMB-005", reply.ok, f"binding {members} together answered {reply.error}")
    ctx.check("AWP-MAN-007", reply.ok, f"session.open naming embodiments {members} failed")
    if reply.ok and moves and ctx.embodiment in members:
        _, submitted = await ctx.submit(link, ctx.next_move(), embodiment_id=ctx.embodiment)
        ctx.check(
            "AWP-EMB-005",
            submitted.ok,
            f"a submission naming embodiment_id {ctx.embodiment} answered {submitted.error}",
        )
        await ctx.settle(link)
    await ctx.close(link)
    outsider = next((e["id"] for e in ctx.embodiments if e["id"] not in members), None)
    if outsider is not None:
        mixed = await ctx.connect("mixed")
        reply = await ctx.open(
            mixed, embodiment=None, embodiments=[members[0], outsider], subscribe=[]
        )
        ctx.check(
            "AWP-EMB-005",
            reply.code == 2001,
            f"binding {members[0]} with {outsider}, outside its group, answered "
            f"{reply.error or reply.result}",
        )
        await ctx.close(mixed)
    if moves:
        one = await ctx.session("one")
        unbound = next(m for m in members if m != ctx.embodiment)
        _, reply = await ctx.submit(one, ctx.next_move(), embodiment_id=unbound)
        ctx.check(
            "AWP-ACT-003",
            reply.code == 4001,
            f"a submission naming the unbound {unbound} answered {reply.error or reply.result}",
        )


@world_test("embodiment-binding", ["AWP-MA-003", "AWP-EMB-001"])
async def embodiment_binding(ctx: WorldContext) -> None:
    for e in ctx.embodiments:
        shared = bool(e.get("shared_control"))
        if shared:
            ctx.check(
                "AWP-MA-003",
                bool(e.get("arbitration")),
                f"{e['id']} declares shared_control without arbitration",
            )
        holder = await ctx.session(f"holder-{e['id']}", embodiment=e["id"], subscribe=[])
        rival = await ctx.connect(f"rival-{e['id']}")
        reply = await ctx.open(rival, embodiment=e["id"], subscribe=[])
        if shared:
            ctx.check(
                "AWP-MA-003",
                reply.ok,
                f"{e['id']} declares shared_control but a second bind answered {reply.error}",
            )
        else:
            for req in ("AWP-MA-003", "AWP-EMB-001"):
                ctx.check(
                    req,
                    reply.code == 2001,
                    f"binding the bound {e['id']} answered {reply.error or reply.result}",
                )
        await ctx.close(rival)
        await ctx.close(holder)
    if ctx.embodiments and all(e.get("shared_control") for e in ctx.embodiments):
        ctx.na("AWP-EMB-001", "every embodiment declares shared_control")


def _second_binding(ctx: WorldContext) -> str | None:
    """An embodiment a second session can bind while another holds the fixture's."""
    return next(
        (e["id"] for e in ctx.embodiments if e["id"] != ctx.embodiment or e.get("shared_control")),
        None,
    )


async def _advance_reached(ctx: WorldContext, link: Link, tick: int) -> None:
    """The session learns of the advance to `tick` on every per-tick channel."""
    per_tick = set(link.tracker.per_tick_channels())
    await link.wait_for(lambda: per_tick <= _reached(_frames(link), tick), 2.0)
    got = _reached(_frames(link), tick)
    ctx.check(
        "AWP-TIM-003",
        per_tick <= got,
        f"{link.name}: the advance to tick {tick} reached channels {sorted(got)} "
        f"of {sorted(per_tick)}",
    )


async def _any_session(ctx: WorldContext, a: Link, b: Link) -> None:
    holders = [link for link in (a, b) if "tick" in link.tracker.granted("admin")]
    ctx.check(
        "AWP-MA-005",
        len(holders) == 1,
        f"tick granted to {len(holders)} of two lockstep sessions under any_session",
    )
    # Before any advance, while both sessions still know the current tick.
    for link in (a, b):
        if link not in holders:
            reply = await ctx.advance(link)
            ctx.check(
                "AWP-TIM-012",
                reply.code == 3006,
                f"world.tick without the tick grant answered {reply.error or reply.result}",
            )
    if len(holders) != 1:
        return
    reply = await ctx.advance(holders[0])
    ctx.check("AWP-TIM-012", reply.ok, f"world.tick with the tick grant answered {reply.error}")
    tick = (reply.result or {}).get("tick")
    if isinstance(tick, int):
        for link in (a, b):
            await _advance_reached(ctx, link, tick)


async def _barrier(ctx: WorldContext, a: Link, b: Link) -> None:
    tick = a.tracker.tick
    if tick is None:
        raise Skip("session.ready carried no tick")
    if not ctx.check(
        "AWP-TIM-012",
        b.tracker.tick == tick,
        f"two sessions of one barrier world opened at ticks {tick} and {b.tracker.tick}",
    ):
        return
    since_a, since_b = len(a.notes), len(b.notes)
    first = a.start("world.tick", {"expected_tick": tick})
    await asyncio.sleep(0.5)
    ctx.check(
        "AWP-TIM-012",
        not first.done(),
        "one session's world.tick was answered before the other session called",
    )
    alone = sorted(
        {
            f["tick"]
            for f in _frames(a, since_a) + _frames(b, since_b)
            if isinstance(f.get("tick"), int) and f["tick"] > tick
        }
    )
    ctx.check(
        "AWP-TIM-012",
        not alone,
        f"one session's world.tick advanced a barrier world on its own, to ticks {alone}",
    )
    second = await ctx.advance(b)
    try:
        reply = await asyncio.wait_for(first, 5.0)
    except TimeoutError:
        ctx.check("AWP-TIM-012", False, "the first world.tick was not answered once both called")
        return
    ticks = [(r.result or {}).get("tick") for r in (reply, second)]
    ctx.check(
        "AWP-TIM-012",
        reply.ok and second.ok and ticks == [tick + 1, tick + 1],
        f"the barrier's calls answered {ticks} ({reply.error or second.error})",
    )
    for link in (a, b):
        await _advance_reached(ctx, link, tick + 1)


@world_test("tick-authority", ["AWP-TIM-012", "AWP-MA-005", "AWP-TIM-003"], mode="lockstep")
async def tick_authority(ctx: WorldContext) -> None:
    other = _second_binding(ctx)
    if other is None:
        raise Skip("no second session can bind an embodiment")
    barrier = ctx.manifest.get("tick_authority") == "barrier"
    admin = [] if barrier else ["tick"]
    a = await ctx.session("first", admin=admin)
    subscribe = [{"channel": c} for c in ctx.observation_channels(other)]
    b = await ctx.session("second", embodiment=other, subscribe=subscribe, admin=admin)
    await (_barrier(ctx, a, b) if barrier else _any_session(ctx, a, b))

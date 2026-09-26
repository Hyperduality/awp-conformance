"""Sessions side by side: the admin class, world.reset, isolation, binding, and tick authority."""

from __future__ import annotations

import asyncio
from typing import Any

from .context import Skip, WorldContext
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


def _embodiments(ctx: WorldContext) -> list[dict[str, Any]]:
    return list(ctx.manifest.get("embodiments", []))


def _observation_channels(ctx: WorldContext, embodiment: str) -> list[dict[str, str]]:
    declared = {c["id"] for c in ctx.manifest.get("observation_channels", [])}
    e = next(e for e in _embodiments(ctx) if e["id"] == embodiment)
    return [{"channel": c} for c in e.get("channels", []) if c in declared]


def _group(ctx: WorldContext) -> list[str]:
    """The embodiments of a multi_bind_group with two or more, the fixture's first."""
    groups: dict[str, list[str]] = {}
    for e in _embodiments(ctx):
        if e.get("multi_bind_group"):
            groups.setdefault(e["multi_bind_group"], []).append(e["id"])
    shared = [m for m in groups.values() if len(m) > 1]
    return next((m for m in shared if ctx.embodiment in m), shared[0] if shared else [])


def _needs_group(ctx: WorldContext) -> str | None:
    return None if _group(ctx) else "no multi_bind_group has two embodiments"


@world_test("multi-bind", ["AWP-EMB-005", "AWP-MAN-007", "AWP-ACT-003"], needs=_needs_group)
async def multi_bind(ctx: WorldContext) -> None:
    members = _group(ctx)
    link = await ctx.connect("together")
    reply = await ctx.open(link, embodiment=None, embodiments=members, subscribe=[])
    ctx.check("AWP-EMB-005", reply.ok, f"binding {members} together answered {reply.error}")
    ctx.check("AWP-MAN-007", reply.ok, f"session.open naming embodiments {members} failed")
    if not reply.ok:
        return
    offered = {t for e in _embodiments(ctx) if e["id"] in members for t in e["action_types"]}
    granted = set((reply.result or {}).get("granted", {}).get("action_types", []))
    ctx.check(
        "AWP-EMB-005", granted <= offered, f"granted {sorted(granted)} beyond {sorted(offered)}"
    )
    moves = ctx.embodiment in members and len(ctx.fixture.moves) >= 2
    if moves:
        _, submitted = await ctx.submit(link, ctx.next_move(), embodiment_id=ctx.embodiment)
        ctx.check(
            "AWP-EMB-005",
            submitted.ok,
            f"a submission naming embodiment_id {ctx.embodiment} answered {submitted.error}",
        )
        await ctx.settle(link)
    await ctx.close(link)
    outsider = next((e["id"] for e in _embodiments(ctx) if e["id"] not in members), None)
    if outsider is not None:
        mixed = await ctx.connect("mixed")
        params = ctx.open_params(embodiment=None, embodiments=[members[0], outsider], subscribe=[])
        reply = await mixed.call("session.open", params)
        ctx.check(
            "AWP-EMB-005",
            reply.code == 2001,
            f"binding {members[0]} with {outsider}, outside its group, answered "
            f"{reply.error or reply.result}",
        )
    if moves:
        one = await ctx.session("one")
        other = next(m for m in members if m != ctx.embodiment)
        _, reply = await ctx.submit(one, ctx.next_move(), embodiment_id=other)
        ctx.check(
            "AWP-ACT-003",
            reply.code == 4001,
            f"a submission naming the unbound {other} answered {reply.error or reply.result}",
        )


@world_test("embodiment-binding", ["AWP-MA-003", "AWP-EMB-001"])
async def embodiment_binding(ctx: WorldContext) -> None:
    for e in _embodiments(ctx):
        shared = bool(e.get("shared_control"))
        if e["id"] == ctx.embodiment and not shared:
            continue  # open-refusals binds it twice
        holder = await ctx.session(f"holder-{e['id']}", embodiment=e["id"], subscribe=[])
        rival = await ctx.connect(f"rival-{e['id']}")
        reply = await ctx.open(rival, embodiment=e["id"], subscribe=[])
        if shared:
            ctx.check(
                "AWP-MA-003",
                reply.ok,
                f"{e['id']} declares shared_control but a second bind answered {reply.error}",
            )
            ctx.check("AWP-MA-003", bool(e.get("arbitration")), f"{e['id']} has no arbitration")
        else:
            for req in ("AWP-MA-003", "AWP-EMB-001"):
                ctx.check(
                    req,
                    reply.code == 2001,
                    f"binding the bound {e['id']} answered {reply.error or reply.result}",
                )
        await ctx.close(rival)
        await ctx.close(holder)


def _second_lockstep_binding(ctx: WorldContext) -> str | None:
    """An embodiment a second session can bind while another holds the fixture's."""
    return next(
        (
            e["id"]
            for e in _embodiments(ctx)
            if e["id"] != ctx.embodiment or e.get("shared_control")
        ),
        None,
    )


@world_test("tick-authority", ["AWP-TIM-012", "AWP-MA-005", "AWP-TIM-003"], mode="lockstep")
async def tick_authority(ctx: WorldContext) -> None:
    other = _second_lockstep_binding(ctx)
    if other is None:
        raise Skip("no second session can bind an embodiment")
    barrier = ctx.manifest.get("tick_authority") == "barrier"
    admin = [] if barrier else ["tick"]
    a = await ctx.session("first", admin=admin)
    b = await ctx.session(
        "second", embodiment=other, subscribe=_observation_channels(ctx, other), admin=admin
    )
    if not barrier:
        holders = [
            link
            for link in (a, b)
            if "tick" in (link.tracker.ready or {}).get("granted", {}).get("admin", [])
        ]
        ctx.check("AWP-MA-005", len(holders) <= 1, "tick granted to two sessions under any_session")
        for link in (a, b):
            if link not in holders:
                reply = await ctx.advance(link)
                ctx.check(
                    "AWP-TIM-012",
                    reply.code == 3006,
                    f"world.tick without the tick grant answered {reply.error or reply.result}",
                )
        return
    tick = a.tracker.tick
    if tick is None or b.tracker.tick != tick:
        raise Skip("the two sessions disagree on the tick")
    since = len(b.notes)
    first = a.start("world.tick", {"expected_tick": tick})
    await asyncio.sleep(0.5)
    advanced = [
        m
        for _, m in b.notes[since:]
        if m.get("method") == "obs.frame" and m["params"]["tick"] > tick
    ]
    ctx.check(
        "AWP-TIM-012",
        not first.done() and not advanced,
        "one session's world.tick advanced a barrier world on its own",
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
        frames = {
            m["params"]["channel_id"]
            for _, m in link.notes
            if m.get("method") == "obs.frame" and m["params"].get("tick") == tick + 1
        }
        granted = (link.tracker.ready or {}).get("granted", {}).get("channels", [])
        commands = {c["id"] for c in ctx.manifest.get("command_channels", [])}
        per_tick = {
            g["channel_id"]
            for g in granted
            if g.get("rate_hz") is None and g["channel"] not in commands
        }
        ctx.check(
            "AWP-TIM-003",
            per_tick <= frames,
            f"{link.name}: the advance reached channels {sorted(frames)} of {sorted(per_tick)}",
        )

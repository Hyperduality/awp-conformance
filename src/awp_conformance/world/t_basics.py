"""Discovery, versioning, the manifest, transport, security, and session establishment."""

from __future__ import annotations

import asyncio
import math
import re
from datetime import datetime
from itertools import pairwise
from urllib.parse import urlsplit, urlunsplit

from jsonschema import Draft202012Validator
from jsonschema.exceptions import SchemaError

from ..link import HandshakeRefused, now_ns
from ..scope import is_loopback
from .context import WorldContext
from .registry import world_test


@world_test(
    "manifest",
    [
        "AWP-MAN-001",
        "AWP-MAN-002",
        "AWP-MAN-003",
        "AWP-MAN-004",
        "AWP-MAN-005",
        "AWP-MAN-006",
        "AWP-MAN-007",
        "AWP-VER-008",
        "AWP-TIM-001",
        "AWP-TIM-002",
        "AWP-TIM-007",
        "AWP-CMD-002",
        "AWP-SES-001",
    ],
)
async def manifest(ctx: WorldContext) -> None:
    link = ctx.link()
    await link.connect()
    first = await link.call("initialize", ctx.agent_manifest())
    ctx.check("AWP-SES-001", first.ok, f"initialize failed: {first.error}")
    m = first.result or {}
    channels = {c["id"] for c in m.get("observation_channels", [])}
    commands = {c["id"] for c in m.get("command_channels", [])}
    types = {d["type"]: d for d in m.get("action_schemas", [])}
    for e in m.get("embodiments", []):
        missing = [c for c in e.get("channels", []) if c not in channels | commands]
        missing += [t for t in e.get("action_types", []) if t not in types]
        ctx.check(
            "AWP-MAN-001", not missing, f"embodiment {e['id']} references undefined {missing}"
        )
    ctx.check(
        "AWP-MAN-001",
        bool(m.get("embodiments")) and bool(channels) and bool(types),
        "a Core world declares at least one embodiment, observation channel, and action type",
    )
    for d in m.get("action_schemas", []):
        try:
            Draft202012Validator.check_schema(d["params_schema"])
            ctx.check("AWP-MAN-002", True, d["type"])
        except SchemaError as exc:
            ctx.check("AWP-MAN-002", False, f"{d['type']}: {exc.message}")
    again = await link.call("world.manifest")
    ctx.check("AWP-MAN-003", again.result == m, "world.manifest differs from the initialize result")
    ctx.check("AWP-MAN-004", isinstance(m.get("capabilities"), dict), "capabilities is not a map")
    caps = m.get("capabilities") or {}
    if caps.get("command_channels"):
        ctx.check("AWP-MAN-005", bool(m.get("command_channels")), "command_channels list missing")
    else:
        ctx.na("AWP-MAN-005", "command_channels not advertised")
    for d in m.get("action_schemas", []):
        if "command_channel" in d:
            ctx.check(
                "AWP-CMD-002",
                d.get("duration") == "streaming" and d["command_channel"] in commands,
                f"{d['type']}: a command_channel type is streaming and names a declared channel",
            )
    models = m.get("time_models", [])
    ctx.check(
        "AWP-TIM-001",
        bool(models) and set(models) <= {"lockstep", "streaming"},
        f"time_models {models}",
    )
    if "lockstep" in models:
        ok = m.get("tick_policy") == "on_tick" and m.get("tick_authority") in (
            "any_session",
            "barrier",
        )
        ctx.check("AWP-MAN-006", ok, "lockstep worlds declare tick_policy and tick_authority")
        ctx.check("AWP-TIM-002", m.get("tick_policy") == "on_tick", "tick_policy is on_tick")
    if "streaming" in models:
        safe = (m.get("safety_policy") or {}).get("safe_state") or {}
        ok = "behavior" in safe and "watchdog_ms" in safe
        ctx.check("AWP-MAN-006", ok, "streaming worlds declare safety_policy.safe_state")
        ctx.check("AWP-TIM-007", ok, "streaming worlds declare safety_policy.safe_state")
    groups = [e.get("multi_bind_group") for e in m.get("embodiments", [])]
    if any(groups):
        ctx.check("AWP-MAN-007", True, "multi_bind_group declared")
    else:
        for r in ("AWP-MAN-007", "AWP-EMB-005"):
            ctx.na(r, "no multi_bind_group declared")
    if not any(d.get("duration") == "streaming" for d in m.get("action_schemas", [])):
        for r in ("AWP-LIF-007", "AWP-SAF-006"):
            ctx.na(r, "no streaming-duration action type")
    policies = {
        p
        for d in m.get("action_schemas", [])
        for p in ([d["preemption"]] if isinstance(d["preemption"], str) else d["preemption"])
    }
    if "blend" not in policies:
        ctx.na("AWP-PRE-004", "no type declares blend")
    for e in m.get("embodiments", []):
        groups_of = {
            types[t].get("concurrency_group", f"_{t}")
            for t in e.get("action_types", [])
            if t in types
        }
        if len(groups_of) < 2:
            ctx.na("AWP-PRE-006", f"{e['id']}'s action types share one concurrency group")
    registered = {
        "image/jpeg",
        "image/raw",
        "audio/pcm",
        "text/event+json",
        "proprio/json",
        "servo/json",
        "pointcloud/xyz32",
        "tensor/raw",
    }
    for c in [*m.get("observation_channels", []), *m.get("command_channels", [])]:
        mod = c.get("modality", "")
        ctx.check(
            "AWP-MOD-002", mod in registered or mod.startswith("x-"), f"{c['id']}: modality {mod!r}"
        )
        ctx.check(
            "AWP-TRN-006",
            c.get("loss_class") in ("reliable", "latest-wins"),
            f"{c['id']}: loss_class",
        )
    if set(models) == {"lockstep", "streaming"}:
        ctx.check("AWP-TIM-008", True, "one manifest describes both time models")
    else:
        ctx.na("AWP-TIM-008", "one time model offered")
    if not any(c.get("id") == "scene" for c in m.get("observation_channels", [])):
        ctx.na("AWP-MA-002", "no scene channel")
    if not caps.get("replay"):
        ctx.na("AWP-AUD-005", "replay not advertised")
    version = m.get("protocol_version", "")
    ctx.check(
        "AWP-VER-008",
        bool(re.fullmatch(r"\d+\.\d+", version)),
        f"protocol_version {version!r} is MAJOR.MINOR",
    )


@world_test("version-negotiation", ["AWP-VER-002", "AWP-VER-003"])
async def version_negotiation(ctx: WorldContext) -> None:
    link = ctx.link()
    await link.connect()
    extra = {"future_field_v0_2": {"any": "value"}, "x-conformance.note": 1}
    reply = await link.call(
        "initialize", ctx.agent_manifest(protocol_versions=["99.0", "0.1"], **extra)
    )
    ctx.check("AWP-VER-003", reply.ok, f"initialize with unknown fields failed: {reply.error}")
    ctx.check(
        "AWP-VER-002",
        reply.get("protocol_version") == "0.1",
        f"offered 99.0 and 0.1; world selected {reply.get('protocol_version')!r}",
    )
    other = ctx.link("unsupported")
    await other.connect()
    refused = await other.call("initialize", ctx.agent_manifest(protocol_versions=["99.0"]))
    ctx.check(
        "AWP-VER-002",
        refused.code == 1001,
        f"initialize with only 99.0 answered {refused.error or refused.result}",
    )


@world_test("unknown-methods", ["AWP-CTL-002", "AWP-VER-007"])
async def unknown_methods(ctx: WorldContext) -> None:
    link = await ctx.connect()
    for method in ("x-conformance.nothing", "world.nothing"):
        reply = await link.call(method, {})
        ctx.check(
            "AWP-CTL-002", reply.code == -32601, f"{method} answered {reply.error or reply.result}"
        )
    caps = ctx.manifest.get("capabilities") or {}
    gated = {"task": "task.update", "snapshot": "world.snapshot"}
    if all(caps.get(key) for key in gated):
        ctx.na("AWP-VER-007", "every capability with a gated method is advertised")
    for key, method in gated.items():
        if caps.get(key):
            continue
        reply = await link.call(
            method, {"task": {"content": [{"type": "text", "text": "x"}]}} if key == "task" else {}
        )
        ctx.check(
            "AWP-VER-007",
            reply.code == -32601,
            f"{method} without capability {key} answered {reply.error or reply.result}",
        )


@world_test(
    "websocket-binding",
    ["AWP-TRN-001", "AWP-SEC-001", "AWP-SEC-002", "AWP-SEC-005", "AWP-SEC-006"],
)
async def websocket_binding(ctx: WorldContext) -> None:
    link = ctx.link()
    await link.connect()
    ctx.check(
        "AWP-TRN-001", link.subprotocol == "awp", f"selected subprotocol {link.subprotocol!r}"
    )
    await link.close()
    loopback = is_loopback(ctx.url)
    if not loopback:
        ctx.check("AWP-SEC-001", ctx.url.startswith("wss://"), "a non-loopback endpoint uses TLS")
        bare = ctx.link("anonymous")
        bare.token = None
        try:
            await bare.connect()
            reply = await bare.call("initialize", ctx.agent_manifest())
            ctx.check("AWP-SEC-002", not reply.ok, "initialize completed without credentials")
        except (HandshakeRefused, ConnectionError, OSError):
            ctx.check("AWP-SEC-002", True, "refused without credentials")
    else:
        ctx.check("AWP-SEC-001", True, "loopback endpoint")
    if ctx.token:
        sub = ctx.link("subprotocol")
        await sub.connect(credential="subprotocol")
        ctx.check(
            "AWP-SEC-005",
            sub.subprotocol == "awp",
            f"awp.bearer.<token> accepted with subprotocol {sub.subprotocol!r} selected",
        )
        await sub.close()
    parts = urlsplit(ctx.url)
    query = f"token={ctx.token or 'st_conformance_0123456789'}"
    leaky = urlunsplit((parts.scheme, parts.netloc, parts.path or "/", query, ""))
    probe = ctx.link("url-token")
    try:
        await probe.connect(url=leaky)
        reply = await probe.call("initialize", ctx.agent_manifest(), timeout=3)
        ctx.check(
            "AWP-SEC-006", not reply.ok, "a connection whose URL carries a token was accepted"
        )
    except (HandshakeRefused, ConnectionError, OSError, TimeoutError):
        ctx.check("AWP-SEC-006", True, "rejected a token in the URL")
    except Exception as exc:  # the world closed the connection
        ctx.check("AWP-SEC-006", True, f"rejected: {exc}")


@world_test(
    "session-open",
    [
        "AWP-SES-002",
        "AWP-SES-007",
        "AWP-SES-009",
        "AWP-NEG-001",
        "AWP-NEG-003",
        "AWP-CLK-002",
        "AWP-UNI-002",
        "AWP-UNI-003",
        "AWP-UNI-004",
        "AWP-TRN-004",
        "AWP-SEC-003",
        "AWP-PRM-002",
        "AWP-TIM-009",
        "AWP-OBS-005",
        "AWP-MA-001",
    ],
)
async def session_open(ctx: WorldContext) -> None:
    link = await ctx.connect()
    requested = dict.fromkeys(ctx.channels, 1.0)
    declared = {c["id"]: c for c in ctx.manifest.get("observation_channels", [])}
    subscribe = [
        {"channel": c, "rate_hz": 1.0} if declared.get(c, {}).get("rate_hz") else {"channel": c}
        for c in ctx.channels
    ]
    reply = await ctx.open(link, subscribe=subscribe)
    ctx.check("AWP-SES-002", reply.ok, f"session.open failed: {reply.error}")
    if not reply.ok:
        return
    r = reply.result or {}
    ctx.check(
        "AWP-SES-009", r.get("session_id") != r.get("session_token"), "session_id is the token"
    )
    granted = r.get("granted", {})
    commands = {c["id"] for c in ctx.manifest.get("command_channels", [])}
    names = [g["channel"] for g in granted.get("channels", []) if g["channel"] not in commands]
    ctx.check(
        "AWP-NEG-001",
        sorted(names) == sorted(ctx.channels),
        f"granted {names}, asked {ctx.channels}",
    )
    for g in granted.get("channels", []):
        if g["channel"] in commands:
            continue
        decl = declared.get(g["channel"], {})
        if decl.get("rate_hz") is None:
            ctx.check("AWP-NEG-003", g["rate_hz"] is None, f"{g['channel']}: per-tick channel rate")
        else:
            ok = (
                g["rate_hz"] is not None
                and g["rate_hz"] <= min(decl["rate_hz"], requested[g["channel"]]) + 1e-9
            )
            ctx.check(
                "AWP-NEG-003", ok, f"{g['channel']}: granted {g['rate_hz']} Hz for 1 Hz asked"
            )
    offered: list[str] = next(
        (
            e.get("action_types", [])
            for e in ctx.manifest["embodiments"]
            if e["id"] == ctx.embodiment
        ),
        [],
    )
    ctx.check(
        "AWP-PRM-002",
        set(granted.get("action_types", [])) <= set(offered),
        f"granted action types {granted.get('action_types')} beyond the embodiment's {offered}",
    )
    endpoints = r.get("stream_endpoints", [])
    ctx.check(
        "AWP-TRN-004",
        any(e.get("binding") == "inline" for e in endpoints),
        "stream_endpoints offers inline",
    )
    if all(e.get("binding") == "inline" for e in endpoints):
        for req in ("AWP-TRN-003", "AWP-TRN-010", "AWP-TRN-011", "AWP-DAT-006"):
            ctx.na(req, "only the inline binding is offered")
    if "expires_at" in granted:
        ctx.check("AWP-PRM-003", isinstance(granted["expires_at"], int), "granted.expires_at")
    else:
        ctx.na("AWP-PRM-003", "no grant expiry offered")
    ctx.na("AWP-PRM-004", "no per-type grant scopes in the granted schema")
    anchor = r.get("clock_anchor")
    try:
        ok = (
            anchor is not None
            and datetime.fromisoformat(anchor.replace("Z", "+00:00")).utcoffset() is not None
        )
    except ValueError:
        ok = False
    ctx.check("AWP-CLK-002", ok, f"clock_anchor {anchor!r} is RFC 3339 with a zone")
    tree = r.get("frame_tree")
    if ctx.check(
        "AWP-UNI-003", isinstance(tree, dict) and bool(tree.get("frames")), "frame_tree present"
    ):
        assert isinstance(tree, dict)
        ids = {f["id"] for f in tree["frames"]}
        for f in tree["frames"]:
            ctx.check(
                "AWP-UNI-003",
                f.get("parent") is None or f["parent"] in ids,
                f"frame {f['id']} names unknown parent {f.get('parent')}",
            )
            q = (f.get("transform") or {}).get("q")
            if q is not None:
                ctx.check(
                    "AWP-UNI-002", abs(math.hypot(*q) - 1) < 1e-3, f"frame {f['id']}: |q| ≠ 1"
                )
        dynamic = [f["id"] for f in tree["frames"] if f.get("dynamic")]
        if dynamic:
            ctx.check(
                "AWP-UNI-004", "awp.tf" in declared, f"dynamic frames {dynamic} without awp.tf"
            )
        else:
            ctx.na("AWP-UNI-004", "no dynamic frames")
    if ctx.lockstep:
        tick = r.get("tick")
        ctx.check("AWP-TIM-009", isinstance(tick, int), "lockstep session.ready carries tick")
        for ch in link.tracker.observation_channels().values():
            ctx.check(
                "AWP-TIM-009", ch.frames >= 1, f"{ch.name}: no initial frame after session.ready"
            )
    else:
        for ch in link.tracker.observation_channels().values():
            ctx.check(
                "AWP-OBS-005", ch.frames >= 1, f"{ch.name}: no frame within 3 s of subscribing"
            )
    other = await ctx.connect("observer")
    obs = await ctx.open(other, embodiment=None, subscribe=[])
    if obs.ok:
        ctx.facts.add("multiple sessions")
        ctx.check("AWP-MA-001", True, "a second, observer session opened alongside")
        ctx.check("AWP-SES-009", obs["session_id"] != r["session_id"], "session_id reused")
        ctx.check("AWP-SEC-003", obs["session_token"] != r["session_token"], "session token reused")
    await asyncio.sleep(0.2)
    ctx.check(
        "AWP-SES-007",
        bool(link.tracker.states),
        "no session.state notification after session.open",
    )


@world_test("one-session-per-connection", ["AWP-CTL-007"])
async def one_session(ctx: WorldContext) -> None:
    link = await ctx.session()
    again = await link.call("session.open", ctx.open_params(embodiment=None, subscribe=[]))
    ctx.check(
        "AWP-CTL-007",
        again.code == 2004,
        f"second session.open answered {again.error or again.result}",
    )


@world_test("open-refusals", ["AWP-NEG-002", "AWP-EMB-001", "AWP-MA-003"])
async def open_refusals(ctx: WorldContext) -> None:
    models = set(ctx.manifest.get("time_models", []))
    link = await ctx.connect()
    if models != {"lockstep", "streaming"}:
        other_mode = "streaming" if ctx.lockstep else "lockstep"
        reply = await link.call("session.open", ctx.open_params(mode=other_mode))
        ctx.check(
            "AWP-NEG-002",
            reply.code == 2002,
            f"unsupported mode answered {reply.error or reply.result}",
        )
    reply = await link.call("session.open", ctx.open_params(embodiment="x-conformance.nobody"))
    ctx.check(
        "AWP-NEG-002",
        reply.code == 2001,
        f"unknown embodiment answered {reply.error or reply.result}",
    )
    holder = await ctx.session("holder")
    rival = await ctx.connect("rival")
    reply = await rival.call("session.open", ctx.open_params())
    shared = any(
        e.get("shared_control") for e in ctx.manifest["embodiments"] if e["id"] == ctx.embodiment
    )
    if shared:
        ctx.na("AWP-EMB-001", "the embodiment declares shared_control")
    else:
        ctx.check(
            "AWP-EMB-001",
            reply.code == 2001,
            f"bound embodiment answered {reply.error or reply.result}",
        )
        ctx.check("AWP-MA-003", reply.code == 2001, "an embodiment bound to two sessions")
    await ctx.close(holder)


@world_test("observer-session", ["AWP-EMB-004", "AWP-PRM-001", "AWP-TIM-012"])
async def observer(ctx: WorldContext) -> None:
    link = await ctx.session("observer", embodiment=None)
    move = ctx.next_move()
    _, reply = await ctx.submit(link, move)
    ctx.check(
        "AWP-EMB-004",
        reply.code == 4001,
        f"observer submission answered {reply.error or reply.result}",
    )
    ctx.check("AWP-PRM-001", reply.code == 4001, "an observer session acted")
    if ctx.lockstep and link.tracker.tick is not None:
        tick = await ctx.advance(link)
        ctx.check(
            "AWP-TIM-012",
            tick.code == 3006,
            f"observer world.tick answered {tick.error or tick.result}",
        )


@world_test("grants", ["AWP-PRM-001", "AWP-ACT-003", "AWP-NEG-003"])
async def grants(ctx: WorldContext) -> None:
    moves = {m.type for m in ctx.moves()}
    offered = next(
        e.get("action_types", []) for e in ctx.manifest["embodiments"] if e["id"] == ctx.embodiment
    )
    others = [t for t in offered if t not in moves]
    link = await ctx.session(action_types=others)
    granted = link.tracker.ready["granted"]["action_types"] if link.tracker.ready else []
    ctx.check("AWP-NEG-003", not set(granted) & moves, f"granted {granted}, asked {others}")
    _, reply = await ctx.submit(link, ctx.next_move())
    ctx.check(
        "AWP-ACT-003", reply.code == 4001, f"ungranted type answered {reply.error or reply.result}"
    )
    ctx.check("AWP-PRM-001", reply.code == 4001, "an ungranted action type was admitted")
    await ctx.close(link)
    link = await ctx.session("embodiment")
    _, reply = await ctx.submit(link, ctx.next_move(), embodiment_id="x-conformance.nobody")
    ctx.check(
        "AWP-ACT-003",
        reply.code == 4001,
        f"ungranted embodiment answered {reply.error or reply.result}",
    )
    if "reset" not in (link.tracker.ready or {}).get("granted", {}).get("admin", []):
        reset = await link.call("world.reset", {})
        ctx.check(
            "AWP-PRM-001",
            reset.code == 4001,
            f"world.reset without grant answered {reset.error or reset.result}",
        )


@world_test("subscriptions", ["AWP-NEG-004", "AWP-DAT-003", "AWP-OBS-004", "AWP-OBS-005"])
async def subscriptions(ctx: WorldContext) -> None:
    channels = ctx.channels
    if not channels:
        ctx.na("AWP-NEG-004", "the embodiment has no channels")
        return
    first, rest = channels[0], channels[1:]
    link = await ctx.session(subscribe=[{"channel": first}])
    unknown = await link.call("obs.subscribe", {"channels": [{"channel": "x-conformance.none"}]})
    ctx.check(
        "AWP-NEG-004",
        unknown.code == 2007,
        f"unknown channel answered {unknown.error or unknown.result}",
    )
    declared = [c["id"] for c in ctx.manifest.get("observation_channels", [])]
    unreadable = [c for c in declared if c not in channels]
    if unreadable:
        reply = await link.call("obs.subscribe", {"channels": [{"channel": unreadable[0]}]})
        ctx.check(
            "AWP-NEG-004",
            reply.code == 4001,
            f"unreadable {unreadable[0]} answered {reply.error or reply.result}",
        )
    if rest:
        reply = await link.call("obs.subscribe", {"channels": [{"channel": rest[0]}]})
        commands = {c["id"] for c in ctx.manifest.get("command_channels", [])}
        got = sorted(g["channel"] for g in reply.get("granted", []) if g["channel"] not in commands)
        ctx.check("AWP-NEG-004", got == sorted([first, rest[0]]), f"subscribe returned {got}")
        await ctx.initial_frames(link)
    reply = await link.call("obs.unsubscribe", {"channels": [first]})
    got = [g["channel"] for g in reply.get("granted", [])]
    ctx.check("AWP-NEG-004", first not in got, f"unsubscribe returned {got}")
    cid = next((c for c, v in link.tracker.channels.items() if v.name == first), None)
    before = len(link.notes)
    await asyncio.sleep(0.3)
    late = [
        m
        for _, m in link.notes[before:]
        if m.get("method") == "obs.frame" and m["params"]["channel_id"] == cid
    ]
    ctx.check(
        "AWP-NEG-004", cid is None or not late, f"{len(late)} frames on {first} after unsubscribe"
    )
    if not ctx.lockstep:
        rated = [
            c
            for c in ctx.manifest["observation_channels"]
            if c["id"] in channels and c.get("rate_hz")
        ]
        if rated:
            ch = rated[0]
            want = max(1.0, ch["rate_hz"] / 4)
            reply = await link.call(
                "obs.subscribe", {"channels": [{"channel": ch["id"], "rate_hz": want}]}
            )
            grant = next((g for g in reply.get("granted", []) if g["channel"] == ch["id"]), None)
            rate = grant["rate_hz"] if grant else None
            ctx.check(
                "AWP-NEG-004",
                rate is not None and rate <= want + 1e-9,
                f"re-rated {ch['id']} to {rate}",
            )
            if grant is not None and rate:
                start = len(link.notes)
                await asyncio.sleep(2.0)
                n = sum(
                    1
                    for _, m in link.notes[start:]
                    if m.get("method") == "obs.frame"
                    and m["params"]["channel_id"] == grant["channel_id"]
                )
                ctx.check(
                    "AWP-DAT-003", n <= rate * 2.0 * 1.25 + 2, f"{n} frames in 2 s at {rate} Hz"
                )


@world_test("modalities", ["AWP-AGM-001"])
async def modalities(ctx: WorldContext) -> None:
    by_modality: dict[str, list[str]] = {}
    for c in ctx.manifest.get("observation_channels", []):
        if c["id"] in ctx.channels:
            by_modality.setdefault(c["modality"], []).append(c["id"])
    if len(by_modality) < 2:
        ctx.na("AWP-AGM-001", "the embodiment's channels share one modality")
        return
    keep = sorted(by_modality)[0]
    link = ctx.link()
    await link.connect()
    await link.call("initialize", ctx.agent_manifest(consumes_modalities=[keep]))
    link.tracker.mode = ctx.mode
    link.tracker.consumes = {keep}
    reply = await link.call("session.open", ctx.open_params())
    if reply.ok:
        await asyncio.sleep(0.5)
        if ctx.lockstep:
            await ctx.advance(link)
    else:
        ctx.check("AWP-AGM-001", True, f"refused channels of undeclared modalities: {reply.error}")


@world_test("heartbeat", ["AWP-CTL-004", "AWP-SAF-001", "AWP-CLK-007"])
async def heartbeat(ctx: WorldContext) -> None:
    link = await ctx.session()
    interval = (link.tracker.ready or {}).get("heartbeat_interval_ms", 5000)
    if interval * 2.5 / 1000 > ctx.fixture.max_wait_s and not ctx.slow:
        ctx.results.mark_untested(
            "AWP-CTL-004", f"heartbeat_interval_ms {interval} exceeds the wait budget"
        )
        return
    start = link.world_pings[-1] if link.world_pings else None
    await asyncio.sleep(interval * 2.2 / 1000)
    pings = [t for t in link.world_pings if start is None or t >= start]
    ctx.check(
        "AWP-CTL-004", len(pings) >= 2, f"{len(pings)} world pings in {2.2 * interval:.0f} ms"
    )
    gaps = [(b - a) / 1e6 for a, b in pairwise(pings)]
    ctx.check(
        "AWP-SAF-001",
        all(g <= interval * 1.2 + 200 for g in gaps),
        f"world ping gaps {[round(g) for g in gaps]} ms against heartbeat_interval_ms {interval}",
    )
    origin = 123_456_789
    pong = await link.call("ping", {"origin_ns": origin})
    if pong.ok:
        ctx.check("AWP-CLK-007", pong["origin_ns"] == origin, f"pong echoes {pong['origin_ns']}")
        ctx.check(
            "AWP-CLK-007",
            pong["receive_ns"] <= pong["transmit_ns"],
            "pong receive_ns after transmit_ns",
        )


@world_test("integer-range", ["AWP-CTL-009"])
async def integer_range(ctx: WorldContext) -> None:
    link = await ctx.session("observer", embodiment=None, subscribe=[])
    fut = link.start("ping", {"origin_ns": 2**60})
    await link.wait_for(
        lambda: fut.done() or bool(link.null_errors) or link.closed_by_world is not None, 3.0
    )
    codes = [e.get("code") for e in link.null_errors]
    if fut.done() and not fut.exception():
        codes.append(fut.result().code)
    ctx.check("AWP-CTL-009", 2006 in codes, f"ping with 2^60 answered {codes or 'nothing'}")
    closed = await link.wait_for(lambda: link.closed_by_world is not None, 3.0)
    ctx.check("AWP-CTL-009", closed, "the connection stayed open after AWP_INTEGER_RANGE")
    if closed and link.closed_by_world is not None:
        how = link.closed_by_world[1]
        ctx.check(
            "AWP-CTL-009",
            how.startswith("1002") and "AWP_INTEGER_RANGE" in how,
            f"closed with {how!r}, not 1002 AWP_INTEGER_RANGE",
        )
    reasons = [
        n.get("reason")
        for n in link.tracker.notes
        if n["method"] == "session.state" and n.get("state") == "closed"
    ]
    ctx.check(
        "AWP-CTL-009",
        reasons == ["protocol_error"],
        f"session.state closed with reasons {reasons}, not protocol_error",
    )
    if not fut.done():
        fut.cancel()


@world_test("connection-without-session", ["AWP-SES-012"])
async def idle_connection(ctx: WorldContext) -> None:
    link = await ctx.connect()
    opened = now_ns()
    await asyncio.sleep(3.0)
    if link.closed_by_world is not None:
        after = (link.closed_by_world[0] - opened) / 1e9
        ctx.should(
            "AWP-SES-012", after >= 15, f"a connection without a session closed after {after:.1f} s"
        )
        return
    reply = await link.ping()
    ctx.check("AWP-SES-012", reply.ok, f"ping without a session answered {reply.error}")

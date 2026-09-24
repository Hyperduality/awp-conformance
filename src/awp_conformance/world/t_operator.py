"""What needs an operator or the world's filesystem: the e-stop and the audit log."""

from __future__ import annotations

import asyncio
import hashlib
import json
from pathlib import Path

from ..spec import TERMINAL
from .context import WorldContext
from .registry import world_test


def _has_estop(ctx: WorldContext) -> str | None:
    if ctx.has_operator("estop_engage") and ctx.has_operator("estop_release"):
        return None
    return "no estop_engage / estop_release operator hooks in the fixture"


@world_test("estop", ["AWP-EVT-002", "AWP-LIF-008"], needs=_has_estop)
async def estop(ctx: WorldContext) -> None:
    link = await ctx.session()
    running = await ctx.running(link)
    decl = ctx.decls[ctx.moves()[0].type]
    pre = decl["preemption"] if isinstance(decl["preemption"], list) else [decl["preemption"]]
    queued = None
    if "queue" in pre:
        queued, _ = await ctx.submit(link, ctx.next_move(), preempt="queue")
    await link.call("action.cancel", {"action_id": running})
    await ctx.operator("estop_engage")
    try:
        engaged = await link.wait_note(
            lambda m: m.get("method") == "world.event" and m["params"]["event"] == "e_stop_engaged",
            3.0,
        )
        ctx.check("AWP-EVT-002", engaged is not None, "no e_stop_engaged event")
        await link.wait_for(lambda: ctx.state(link, running) in TERMINAL, 2.0)
        s = ctx.status(link, running)
        event_seq = engaged["params"]["status_seq"] if engaged else 0
        if s.get("status_seq", 0) < event_seq and s.get("reason") == "cancelled_by_agent":
            ctx.results.mark_untested("AWP-LIF-008", "the abort finished before the e-stop engaged")
        else:
            ctx.check(
                "AWP-LIF-008",
                (s.get("state"), s.get("reason")) == ("failed", "e_stop"),
                f"an e-stop during the abort ended {s.get('state')}({s.get('reason')})",
            )
        if queued is not None:
            q = ctx.status(link, queued)
            ctx.check(
                "AWP-EVT-002",
                (q.get("state"), q.get("reason")) == ("cancelled", "e_stop"),
                f"queued action ended {q}",
            )
        _, refused = await ctx.submit(link, ctx.next_move())
        ctx.check(
            "AWP-EVT-002",
            refused.code == 3005,
            f"submission during e-stop answered {refused.error or refused.result}",
        )
        ctx.check(
            "AWP-EVT-002", bool(refused.data.get("retryable")), "AWP_ESTOP_ACTIVE is retryable"
        )
    finally:
        await ctx.operator("estop_release")
    released = await link.wait_note(
        lambda m: m.get("method") == "world.event" and m["params"]["event"] == "e_stop_released",
        3.0,
    )
    ctx.check("AWP-EVT-002", released is not None, "no e_stop_released event")
    _, again = await ctx.submit(link, ctx.next_move())
    ctx.check("AWP-EVT-002", again.ok, f"admission not restored after release: {again.error}")
    await ctx.settle(link)


def _audit_files(directory: Path, session_id: str) -> list[Path]:
    return [
        p
        for p in directory.glob("*")
        if p.is_file() and session_id in p.read_text(errors="replace")
    ]


def _has_audit(ctx: WorldContext) -> str | None:
    return None if ctx.fixture.audit_dir else "no audit_dir in the fixture"


@world_test(
    "audit-log",
    ["AWP-AUD-001", "AWP-AUD-002", "AWP-AUD-003", "AWP-AUD-006", "AWP-AUD-007"],
    needs=_has_audit,
)
async def audit_log(ctx: WorldContext) -> None:
    link = await ctx.session()
    session_id = (link.tracker.ready or {}).get("session_id", "")
    token = link.tracker.token or ""
    action_id = await ctx.running(link)
    await ctx.close(link)
    await asyncio.sleep(0.3)
    assert ctx.fixture.audit_dir is not None
    files = await asyncio.to_thread(_audit_files, Path(ctx.fixture.audit_dir), session_id)
    if not ctx.check("AWP-AUD-001", bool(files), f"no audit file mentions session {session_id}"):
        return
    text = await asyncio.to_thread(files[0].read_text)
    lines = [line for line in text.splitlines() if line.strip()]
    records = [json.loads(line) for line in lines]
    ctx.check("AWP-AUD-007", "class" in records[0], "the first record does not label its class")
    fields_ok = all({"ts_mono_ns", "direction", "kind", "body"} <= r.keys() for r in records)
    ctx.check("AWP-AUD-002", fields_ok, "records lack ts_mono_ns, direction, kind, or body")
    ctx.check("AWP-AUD-006", token not in text, "the session token appears in the audit log")
    ctx.check("AWP-AUD-006", "[redacted:sha256:" in text, "no redacted credential in the audit log")
    ctx.check(
        "AWP-AUD-001", action_id in text, "the submitted action is missing from the audit log"
    )
    frames = [r for r in records if r.get("kind") == "frame"]
    ctx.check(
        "AWP-AUD-001",
        bool(frames)
        and all("payload_sha256" in r["body"] or "payload_b64" in r["body"] for r in frames),
        "frames are not recorded by payload or hash",
    )
    chained = all(
        records[i].get("prev_hash") == hashlib.sha256(lines[i - 1].encode()).hexdigest()
        for i in range(1, len(lines))
    )
    ctx.check("AWP-AUD-003", chained, "records are not hash-chained")

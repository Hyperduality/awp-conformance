"""Run an agent through the harness episodes and judge what it did."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import os
import signal
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .. import report
from ..results import Results
from ..scope import Scope
from .harness import GAP, MS, PRESESSION_OFFSET_NS, Episode, Harness

# Rows marked "both" whose agent-side duty exists only with stream bindings or command channels.
_WORLD_OR_FEATURE = (
    "AWP-TRN-002",
    "AWP-TRN-003",
    "AWP-TRN-005",
    "AWP-TRN-006",
    "AWP-TRN-007",
    "AWP-TRN-008",
    "AWP-TRN-010",
    "AWP-TRN-011",
    "AWP-DAT-002",
    "AWP-DAT-003",
    "AWP-DAT-004",
    "AWP-DAT-006",
    "AWP-DAT-007",
    "AWP-CLK-001",
    "AWP-CLK-002",
    "AWP-CLK-003",
    "AWP-CLK-004",
    "AWP-CLK-005",
    "AWP-CTL-007",
    "AWP-SAF-011",
    "AWP-VER-007",
)


@dataclass
class AgentRun:
    manifest: dict[str, Any]
    modes: list[str]
    results: Results
    episodes: list[tuple[str, str]] = field(default_factory=list)


def episodes(mode: str) -> list[Episode]:
    streaming = [Episode("frame-gaps", mode, frame_gaps=True)] if mode == "streaming" else []
    return [
        Episode("baseline", mode, interpose=True),
        *streaming,
        Episode("reconnect", mode, drop_after_admission=True, redeliver=False),
        Episode(
            "restarted-world", mode, drop_after_admission=True, forget_session=True, redeliver=False
        ),
        Episode("integer-range", mode, out_of_range=True, redeliver=False),
        Episode("malformed-frame", mode, malformed_frame=True, redeliver=False),
        Episode("invalid-manifest", mode, invalid_manifest=True),
        Episode("no-action-grants", mode, grant_no_actions=True),
        Episode("refused-submission", mode, refuse_first_submission=True, redeliver=False),
        Episode("silent-world", mode, silent_after_admission=True, redeliver=False),
    ]


async def _launch(command: list[str], h: Harness) -> asyncio.subprocess.Process:
    argv = [a.replace("{url}", h.url).replace("{token}", h.token) for a in command]
    env = {
        **os.environ,
        "AWP_URL": h.url,
        "AWP_TOKEN": h.token,
        "AWP_CONFORMANCE_EPISODE": h.episode.name,
    }
    return await asyncio.create_subprocess_exec(
        *argv,
        env=env,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
        start_new_session=True,
    )


async def _stop(proc: asyncio.subprocess.Process) -> None:
    if proc.returncode is None:
        with contextlib.suppress(ProcessLookupError):
            os.killpg(proc.pid, signal.SIGTERM)
        try:
            await asyncio.wait_for(proc.wait(), 3)
        except TimeoutError:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(proc.pid, signal.SIGKILL)
            await proc.wait()


async def run_episode(
    command: list[str],
    manifest: dict[str, Any],
    episode: Episode,
    results: Results,
    *,
    samples: dict[str, Any] | None,
    timeout: float,
) -> str:
    h = Harness(manifest, episode, results, samples=samples)
    await h.start()
    proc = await _launch(command, h)
    try:
        exited = asyncio.ensure_future(proc.wait())
        closed = asyncio.ensure_future(h.done.wait())
        await asyncio.wait({exited, closed}, timeout=timeout, return_when=asyncio.FIRST_COMPLETED)
        closed.cancel()
        if closed.done() and not exited.done():
            await asyncio.wait({exited}, timeout=2)
        exited.cancel()
    finally:
        await _stop(proc)
        await h.stop()
    judge(h)
    if not h.manifest_sent:
        return "the agent never initialized"
    return "closed its session" if h.done.is_set() else "ended without closing"


def judge(h: Harness) -> None:
    """Checks that need the whole episode."""
    e = h.episode
    if e.invalid_manifest:
        if h.manifest_sent:
            h.check(
                "AWP-AGT-002",
                not h.opened,
                "the agent opened a session against an invalid manifest",
            )
        return
    if e.grant_no_actions:
        if h.opened:
            h.check(
                "AWP-AGT-003",
                not h.submitted,
                f"submitted {len(h.submitted)} actions with none granted",
            )
            h.check("AWP-PRM-001", not h.submitted, "the agent acted without a grant")
        return
    if e.out_of_range:
        if h.range_sent:
            code, reason = h.agent_close or (None, None)
            h.check(
                "AWP-CTL-009",
                h.closed_after_range and (code, reason) == (1002, "AWP_INTEGER_RANGE"),
                "after an integer beyond 2^53-1 the agent "
                + ("sent session.close" if h.closed_after_range else "did not send session.close")
                + f" and closed the connection with {code} {reason!r}",
            )
            h.check("AWP-CTL-009", not h.resumed, "the agent resumed a session it had to end")
        return
    if e.malformed_frame:
        if h.malformed_sent:
            h.check(
                "AWP-DAT-010",
                h.stream_close == (1002, "AWP_MALFORMED"),
                f"a malformed stream frame left the stream connection "
                f"{'closed with ' + repr(h.stream_close) if h.stream_close else 'open'}",
            )
        elif h.opened:
            h.results.mark_untested("AWP-DAT-010", "the agent took its frames only inline")
        return
    if e.forget_session:
        if h.forgotten:
            h.check(
                "AWP-SES-008",
                not h.orphaned,
                f"after AWP_SESSION_UNKNOWN the agent still used the lost session: {h.orphaned}",
            )
            h.check(
                "AWP-SES-008",
                not h.stale_refs,
                f"the agent asked a new session about the lost session's actions {h.stale_refs}",
            )
        elif h.dropped:
            h.results.mark_untested("AWP-SES-008", "the agent did not resume after the drop")
        return
    if e.drop_after_admission:
        if h.dropped and not h.resumed:
            for r in ("AWP-AGT-007", "AWP-CTL-005"):
                h.results.mark_untested(r, "the agent did not resume after its connection dropped")
        elif h.resumed:
            h.check("AWP-CTL-005", True, "the agent resumed on a new connection")
        return
    if e.refuse_first_submission:
        if h.refused:
            h.check("AWP-ERR-001", True, "no identical retry after a non-retryable error")
        return
    if e.silent_after_admission:
        if h.silent_since is None:
            return
        limit = 3 * e.heartbeat_interval_ms * MS + 2_000 * MS
        detected = h.agent_closed_at is not None or h.connections > 1
        at = h.agent_closed_at or 0
        in_time = detected and (at == 0 or at - h.silent_since <= limit)
        h.check(
            "AWP-SAF-002",
            in_time,
            "the agent kept a connection whose world fell silent for 3 intervals",
        )
        return
    if not h.opened:
        return
    judge_clock(h)
    if e.frame_gaps:
        judge_gaps(h)
        return
    if e.interpose and h.done.is_set() and h.interposed:
        answered = all(h.interposed.values())
        h.check(
            "AWP-CTL-003",
            answered,
            "the agent handled a world request"
            + (" and frames" if e.mode == "streaming" else "")
            + " sent between each action.submit and its response"
            if answered
            else "the agent left a world request sent before an action.submit response unanswered",
        )
    if h.session is not None and h.done.is_set():
        if len(h.session.channels) > 1:
            h.check("AWP-OBS-003", True, "the agent tolerated frames interleaved across channels")
        else:
            h.results.mark_not_applicable("AWP-OBS-003", "the agent subscribed to one channel")
    if h.session is not None:
        h.check("AWP-TRN-004", True, "the agent ran its session on the inline binding")
        undeclared = [
            c["channel"]
            for c in h.session.channels.values()
            if h.consumes and h.channels[c["channel"]].get("modality") not in h.consumes
        ]
        h.check("AWP-MOD-002", not undeclared, f"subscribed to undeclared modalities: {undeclared}")
    if h.submitted:
        ids = [p.get("action_id") for p in h.submitted]
        h.check("AWP-AGT-004", len(set(ids)) == len(ids), f"action_ids repeated: {ids}")
        h.check("AWP-ACT-001", len(set(ids)) == len(ids), f"action_ids repeated: {ids}")
        timed = [q for q in h.submitted if "basis_ts_mono_ns" in q or "valid_until_ns" in q]
        if not timed:
            h.results.mark_not_applicable(
                "AWP-ACT-007", "the agent sent no basis_ts_mono_ns or valid_until_ns"
            )
        if not any("valid_until_ns" in q for q in h.submitted):
            h.results.mark_not_applicable("AWP-AGT-008", "the agent sent no session-clock value")
        h.check("AWP-ACT-010", len(set(ids)) == len(ids), "an admitted action_id was reused")
        h.check(
            "AWP-AGT-001",
            True,
            "the agent tolerated unknown fields in every result and notification",
        )
        h.check("AWP-DAT-005", True, "the agent tolerated reserved flag bits (4-7) on frames")
        h.check("AWP-VER-003", True, "the agent ignored unknown fields")
        if e.redeliver and h.session is not None and h.session.redelivered:
            h.check("AWP-AGT-005", True, "the agent continued after a redelivered terminal status")
            h.check("AWP-LIF-009", True, "a redelivered terminal status did not disrupt the agent")
    if e.mode == "lockstep":
        if h.submitted:
            h.check(
                "AWP-AGT-006",
                h.ticks_called > 0,
                "the agent submitted in lockstep but never called world.tick",
            )
            h.check(
                "AWP-AGT-009",
                h.ticks_called > 0,
                "the agent waited for lockstep time to pass by itself",
            )
    else:
        h.check(
            "AWP-AGT-009", h.ticks_called == 0, "the agent called world.tick in a streaming session"
        )
        start = h.session_started_ns or 0
        pings = [t for t in h.agent_pings if t >= start]
        interval = e.heartbeat_interval_ms
        gaps = [(b - a) / MS for a, b in zip([start, *pings], pings, strict=False)]
        if pings:
            h.check(
                "AWP-SAF-001",
                max(gaps) <= interval * 1.25 + 250,
                f"agent ping gaps up to {max(gaps):.0f} ms; interval {interval}",
            )
            h.check(
                "AWP-AGT-006",
                max(gaps) <= interval * 1.25 + 250,
                "the agent pings at the negotiated interval",
            )
            safe_state = (h.manifest.get("safety_policy") or {}).get("safe_state") or {}
            watchdog = safe_state.get("watchdog_ms")
            if watchdog:
                h.check(
                    "AWP-SAF-005",
                    max(gaps) <= watchdog / 2 + 250,
                    f"agent traffic gaps up to {max(gaps):.0f} ms against watchdog_ms {watchdog}",
                    should=True,
                )
            early = [t for t in pings if t - start <= 1_000 * MS]
            h.check(
                "AWP-CLK-008",
                len(early) >= 4,
                f"{len(early)} clock exchanges in the first second",
                should=True,
            )
        elif h.session is not None and (h.agent_pings or h.submitted):
            h.check("AWP-SAF-001", False, "the agent sent no ping during its session")
        lasted = ((h.agent_pings[-1] if h.agent_pings else start) - start) / 1e9
        sent = [at for at, _ in h.reports if at >= start]
        if sent:
            gaps = [(b - a) / 1e9 for a, b in zip([start, *sent], sent, strict=False)]
            h.check("AWP-OBS-007", max(gaps) <= 5.5, f"obs.report gaps up to {max(gaps):.1f} s")
        elif lasted > 5.5:
            h.check("AWP-OBS-007", False, "no obs.report in a streaming session over 5 s")


def judge_clock(h: Harness) -> None:
    """AWP-SES-012: a pong from before session.ready is on another clock (1000 s ahead) and must
    not enter the offset estimate an obs.report states."""
    if not h.presession_pings:
        h.check("AWP-SES-012", True, "the agent sent no ping before session.ready")
        return
    reports = [r for _, r in h.reports if isinstance(r.get("sync"), dict)]
    if not reports or not h.session_samples:
        h.results.mark_untested(
            "AWP-SES-012", "the agent pinged before its session but sent no obs.report after it"
        )
        return
    sync = reports[-1]["sync"]
    offset = sync.get("offset_ns")
    samples = sorted(h.session_samples)
    truth = samples[len(samples) // 2]
    ok = isinstance(offset, int) and abs(offset - truth) < PRESESSION_OFFSET_NS // 2
    h.check(
        "AWP-SES-012",
        ok,
        f"offset_ns {offset} against {truth} from in-session pings: a pre-session pong entered it",
    )
    counted = sync.get("samples")
    if isinstance(counted, int):
        h.check(
            "AWP-SES-012",
            counted <= len(h.session_samples),
            f"sync.samples {counted} counts pre-session exchanges ({len(h.session_samples)} in "
            "the session)",
        )


def judge_gaps(h: Harness) -> None:
    """AWP-DAT-001, AWP-DAT-009: the gaps an obs.report states after a seq gap and a resync."""
    done = h.gaps_done_ns
    reports = [r for at, r in h.reports if done is not None and at > done]
    if not reports:
        why = "the agent sent no obs.report after the injected seq gaps"
        for req in ("AWP-DAT-001", "AWP-DAT-009"):
            h.results.mark_untested(req, why)
        return
    counted: dict[int, int] = {}
    for _, r in h.reports:
        for cid, stats in (r.get("channels") or {}).items():
            if isinstance(stats, dict) and isinstance(stats.get("gaps"), int):
                counted[int(cid)] = counted.get(int(cid), 0) + stats["gaps"]
    s = h.session
    assert s is not None
    loss = min(s.channels)
    resync = max(s.channels)
    if loss == resync:
        got = counted.get(loss, 0)
        h.check("AWP-DAT-001", got >= GAP, f"{got} gaps reported for {GAP} missing frames")
        h.check("AWP-DAT-009", got <= GAP, f"{got} gaps reported: the gap before a resync counted")
        return
    got = counted.get(loss, 0)
    h.check("AWP-DAT-001", got == GAP, f"channel {loss}: {got} gaps reported for {GAP} missing")
    got = counted.get(resync, 0)
    h.check(
        "AWP-DAT-009",
        got == 0,
        f"channel {resync}: {got} gaps reported where only a resync frame followed a gap",
    )


async def run_agent(
    command: list[str],
    manifest: dict[str, Any],
    *,
    modes: list[str],
    samples: dict[str, Any] | None = None,
    timeout: float = 30.0,
    progress: Any = None,
) -> AgentRun:
    results = Results(side="agent")
    run = AgentRun(manifest, modes, results)
    for r in _WORLD_OR_FEATURE:
        results.mark_not_applicable(
            r, "no agent-side obligation on the inline binding without command channels"
        )
    for mode in modes:
        for episode in episodes(mode):
            outcome = await run_episode(
                command, manifest, episode, results, samples=samples, timeout=timeout
            )
            run.episodes.append((f"{mode}/{episode.name}", outcome))
            if progress is not None:
                progress(f"{mode}/{episode.name}", outcome)
    results.mark_not_applicable(
        "AWP-TIM-003", "the agent took frames on the inline binding, where they precede the result"
    )
    return run


def main_agent(args: argparse.Namespace) -> int:
    command = [a for a in args.agent_command if a != "--"]
    if not command:
        print("awp-conformance agent: give the agent command after --", file=sys.stderr)
        return 2
    manifest = json.loads(Path(args.manifest).read_text())
    samples = json.loads(Path(args.frames).read_text()) if args.frames else None
    modes = [args.mode] if args.mode else list(manifest.get("time_models", ["streaming"]))[:1]

    def progress(name: str, outcome: str) -> None:
        if not args.json:
            print(f"  {name:36} {outcome}", file=sys.stderr)

    run = asyncio.run(
        run_agent(
            command, manifest, modes=modes, samples=samples, timeout=args.timeout, progress=progress
        )
    )
    scope = Scope("agent", set(run.modes), manifest)
    rep = report.build(
        target=" ".join(command),
        scope=scope,
        results=run.results,
        extra={"episodes": [{"episode": n, "outcome": o} for n, o in run.episodes]},
    )
    if args.out:
        report.write(rep, args.out, {})
    print(json.dumps(rep, indent=2) if args.json else report.text(rep, verbose=args.verbose))
    return 0 if rep["claim"]["status"] in ("conformant", "self-assessed") else 1

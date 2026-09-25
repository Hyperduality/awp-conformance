"""Run the world tests against one endpoint."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Any

from ..fixture import Fixture
from ..link import HandshakeRefused, Link
from ..monitor import SessionTracker
from ..results import Results
from . import (
    t_actions,
    t_basics,
    t_lockstep,
    t_multi,
    t_operator,
    t_resume,
    t_streaming,
    t_streams,
)
from .context import Hook, Skip, WorldContext
from .registry import TESTS, WorldTest

# Each module registers its tests on import; stream bindings run last.
MODULES = (t_basics, t_actions, t_lockstep, t_streaming, t_resume, t_multi, t_operator, t_streams)

log = logging.getLogger("awp_conformance")


class WorldUnreachable(Exception):
    pass


@dataclass
class WorldRun:
    url: str
    manifest: dict[str, Any]
    modes: list[str]
    results: Results
    facts: set[str] = field(default_factory=set)
    traces: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    ran: list[tuple[str, str]] = field(default_factory=list)  # (test, outcome)


async def fetch_manifest(url: str, token: str | None, results: Results) -> dict[str, Any]:
    link = Link(url, SessionTracker(results, "bootstrap", {}, "streaming"), token=token)
    try:
        await link.connect()
    except (HandshakeRefused, OSError) as exc:
        raise WorldUnreachable(f"cannot connect to {url}: {exc}") from None
    try:
        reply = await link.call(
            "initialize",
            {
                "protocol_versions": ["0.1"],
                "agent": {"name": "awp-conformance", "version": "0.1.0", "vendor": "hyperduality"},
                "consumes_modalities": ["text/event+json"],
            },
        )
    finally:
        await link.close()
    if not reply.ok or reply.result is None:
        raise WorldUnreachable(f"initialize failed: {reply.error}")
    return reply.result


def selected(only: list[str] | None, test: WorldTest) -> bool:
    return not only or test.name in only


async def run_world(
    url: str,
    fixture: Fixture,
    *,
    token: str | None = None,
    modes: list[str] | None = None,
    only: list[str] | None = None,
    slow: bool = False,
    profiles: set[str] | None = None,
    hooks: dict[str, Hook] | None = None,
    progress: Any = None,
) -> WorldRun:
    results = Results()
    manifest = await fetch_manifest(url, token, results)
    problems = fixture.problems(manifest)
    if problems:
        raise WorldUnreachable("fixture does not match the manifest: " + "; ".join(problems))
    offered = list(manifest.get("time_models", []))
    run_modes = [m for m in offered if modes is None or m in modes]
    run = WorldRun(url, manifest, run_modes, results)
    for mode in run_modes:
        ctx = WorldContext(
            url,
            fixture,
            results,
            token=token,
            manifest=manifest,
            mode=mode,
            slow=slow,
            profiles=set(profiles or ()),
            hooks=hooks or {},
        )
        for test in TESTS:
            if test.mode not in ("any", mode) or not selected(only, test):
                continue
            ctx.test = f"{mode}/{test.name}"
            outcome = await _run_one(ctx, test)
            run.ran.append((ctx.test, outcome))
            if progress is not None:
                progress(ctx.test, outcome)
        run.facts |= ctx.facts
        run.traces.update(ctx.traces)
    return run


async def _run_one(ctx: WorldContext, test: WorldTest) -> str:
    reason = test.needs(ctx) if test.needs else None
    if reason is not None:
        for r in test.covers:
            ctx.results.mark_untested(r, f"{test.name}: {reason}")
        return f"skipped: {reason}"
    try:
        await asyncio.wait_for(test.fn(ctx), test.timeout_s)
        outcome = "ran"
    except Skip as exc:
        for r in test.covers:
            ctx.results.mark_untested(r, f"{test.name}: {exc}")
        outcome = f"skipped: {exc}"
    except TimeoutError:
        ctx.check(
            test.covers[0], False, f"{test.name} did not finish within {test.timeout_s:.0f} s"
        )
        outcome = "timed out"
    except Exception as exc:  # the world misbehaved in a way the test could not continue past
        log.debug("test %s raised", ctx.test, exc_info=True)
        ctx.check(test.covers[0], False, f"{test.name}: {type(exc).__name__}: {exc}")
        outcome = f"error: {exc}"
    finally:
        await ctx.cleanup()
    return outcome

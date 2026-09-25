"""The `awp-conformance` command."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path

from . import SPEC_REVISION, __version__, report
from .fixture import Fixture
from .scope import Scope, is_loopback
from .spec import REQUIREMENTS


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="awp-conformance",
        description=f"Agent World Protocol conformance suite ({SPEC_REVISION}).",
    )
    p.add_argument(
        "--version", action="version", version=f"awp-conformance {__version__} ({SPEC_REVISION})"
    )
    sub = p.add_subparsers(dest="command", required=True)

    w = sub.add_parser("world", help="test a world at a WebSocket URL")
    w.add_argument("url")
    w.add_argument("--fixture", type=Path, help="fixture JSON: moves, operator hooks, audit_dir")
    w.add_argument("--token", default=os.environ.get("AWP_TOKEN"), help="bearer token ($AWP_TOKEN)")
    w.add_argument(
        "--mode", action="append", choices=["lockstep", "streaming"], help="time models to test"
    )
    w.add_argument("--profile", action="append", default=[], help="profiles the claim names")
    w.add_argument("--only", action="append", help="run only these tests")
    w.add_argument(
        "--slow", action="store_true", help="run tests that wait longer than the fixture budget"
    )
    w.add_argument("--out", type=Path, help="write report.json and traces here")
    w.add_argument("--json", action="store_true", help="print the report as JSON")
    w.add_argument("-v", "--verbose", action="store_true")

    a = sub.add_parser("agent", help="test an agent against the harness world")
    a.add_argument("--manifest", type=Path, required=True, help="world manifest the harness serves")
    a.add_argument("--frames", type=Path, help="sample payloads per channel (JSON object)")
    a.add_argument(
        "--mode", choices=["lockstep", "streaming"], help="time model the harness offers"
    )
    a.add_argument("--timeout", type=float, default=30.0, help="seconds per episode")
    a.add_argument("--out", type=Path)
    a.add_argument("--json", action="store_true")
    a.add_argument("-v", "--verbose", action="store_true")
    a.add_argument("agent_command", nargs=argparse.REMAINDER, help="-- command that runs the agent")

    r = sub.add_parser("requirements", help="list the requirement matrix and what the suite covers")
    r.add_argument("--side", choices=["world", "agent"])

    sub.add_parser("tests", help="list the tests")
    return p


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.command == "world":
        return _world(args)
    if args.command == "agent":
        from .agent.runner import main_agent

        return main_agent(args)
    if args.command == "requirements":
        return _requirements(args)
    return _tests()


def _world(args: argparse.Namespace) -> int:
    from .world.runner import WorldUnreachable, run_world

    fixture = Fixture.load(args.fixture) if args.fixture else Fixture()

    def progress(test: str, outcome: str) -> None:
        if not args.json:
            print(f"  {test:44} {outcome}", file=sys.stderr)

    try:
        run = asyncio.run(
            run_world(
                args.url,
                fixture,
                token=args.token,
                modes=args.mode,
                only=args.only,
                slow=args.slow,
                profiles=set(args.profile),
                progress=progress,
            )
        )
    except WorldUnreachable as exc:
        print(f"awp-conformance: {exc}", file=sys.stderr)
        return 2
    scope = Scope(
        "world", set(run.modes), run.manifest, set(args.profile), is_loopback(args.url), run.facts
    )
    rep = report.build(
        target=args.url,
        scope=scope,
        results=run.results,
        extra={
            "world": run.manifest.get("world"),
            "tests": [{"test": t, "outcome": o} for t, o in run.ran],
        },
    )
    if args.out:
        report.write(rep, args.out, run.traces)
    print(json.dumps(rep, indent=2) if args.json else report.text(rep, verbose=args.verbose))
    return {"conformant": 0, "self-assessed": 0}.get(rep["claim"]["status"], 1)


def _requirements(args: argparse.Namespace) -> int:
    from .world.registry import TESTS
    from .world.runner import MODULES

    assert MODULES

    covered: dict[str, list[str]] = {}
    for t in TESTS:
        for r in t.covers:
            covered.setdefault(r, []).append(t.name)
    for req in REQUIREMENTS.values():
        if args.side and req.side not in (args.side, "both"):
            continue
        kind = req.kind_for(args.side or "world")
        tests = ", ".join(covered.get(req.id, [])) or (
            "passive" if kind in ("assert", "warning", "fallback") else kind
        )
        print(f"{req.id:12} {req.level:6} {req.side:5} {req.applies:9} {req.gate:36} {tests}")
    return 0


def _tests() -> int:
    from .world.registry import TESTS
    from .world.runner import MODULES

    assert MODULES

    for t in TESTS:
        print(f"world  {t.mode:9} {t.name:28} {' '.join(t.covers)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""The conformance report: a verdict per requirement, and the claim the verdicts support."""

from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from . import SPEC_REVISION, __version__
from .results import Outcome, Results
from .scope import Scope
from .spec import REQUIREMENTS, Requirement


@dataclass
class Row:
    requirement: Requirement
    outcome: Outcome
    detail: str


def rows(results: Results, scope: Scope) -> list[Row]:
    out: list[Row] = []
    for req in REQUIREMENTS.values():
        applicable, why = scope.applicable(req)
        if not applicable:
            out.append(Row(req, Outcome.NOT_APPLICABLE, why))
            continue
        outcome = results.verdict(req.id)
        found = results.of(req.id)
        if outcome in (Outcome.FAIL, Outcome.WARN):
            detail = "; ".join(f"[{f.test}] {f.detail}" for f in found if not f.ok)
        elif outcome is Outcome.PASS:
            detail = f"{len(found)} check(s): " + ", ".join(sorted({f.test for f in found}))
        elif outcome is Outcome.NOT_APPLICABLE:
            detail = results.not_applicable.get(req.id, "")
        elif outcome is Outcome.UNTESTED:
            detail = results.untested_reason.get(req.id, "not exercised by this suite version")
        else:
            detail = req.test
        out.append(Row(req, outcome, detail))
    return out


def claim(scope: Scope, table: list[Row]) -> dict[str, Any]:
    classes = ["Core World" if scope.side == "world" else "Core Agent", *sorted(scope.profiles)]
    counts = Counter(r.outcome for r in table)
    must = [
        r
        for r in table
        if r.outcome is not Outcome.NOT_APPLICABLE and r.requirement.level != "SHOULD"
    ]
    failed = [r.requirement.id for r in must if r.outcome is Outcome.FAIL]
    untested = [r.requirement.id for r in must if r.outcome is Outcome.UNTESTED]
    manual = [r.requirement.id for r in must if r.outcome is Outcome.MANUAL]
    subject = " + ".join(classes) + f" ({', '.join(sorted(scope.modes))})"
    suite = f"awp-conformance {__version__}"
    if failed:
        wording = f"Not conformant: {len(failed)} requirement(s) fail ({subject}, {SPEC_REVISION})"
        status = "fail"
    elif untested:
        wording = (
            f"{subject}, self-assessed against {SPEC_REVISION}; "
            f"{suite} passes every assertion it ran"
        )
        status = "self-assessed"
    else:
        wording = f"{subject}: AWP-conformant against {SPEC_REVISION} ({suite})"
        status = "conformant"
    return {
        "status": status,
        "wording": wording,
        "classes": classes,
        "modes": sorted(scope.modes),
        "failed": failed,
        "untested": untested,
        "manual_evidence_required": manual,
        "counts": {str(k): v for k, v in sorted(counts.items())},
    }


def build(
    *,
    target: str,
    scope: Scope,
    results: Results,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    table = rows(results, scope)
    return {
        "suite": {"name": "awp-conformance", "version": __version__},
        "specification": SPEC_REVISION,
        "target": target,
        "side": scope.side,
        "generated": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "claim": claim(scope, table),
        "requirements": [
            {
                "id": r.requirement.id,
                "level": r.requirement.level,
                "outcome": str(r.outcome),
                "detail": r.detail,
            }
            for r in table
        ],
        **(extra or {}),
    }


def write(report: dict[str, Any], out: Path, traces: dict[str, list[dict[str, Any]]]) -> None:
    out.mkdir(parents=True, exist_ok=True)
    (out / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    trace_dir = out / "traces"
    trace_dir.mkdir(exist_ok=True)
    for name, lines in traces.items():
        safe = name.replace("/", "--")
        (trace_dir / f"{safe}.jsonl").write_text(
            "".join(json.dumps(line, separators=(",", ":")) + "\n" for line in lines)
        )


def text(report: dict[str, Any], *, verbose: bool = False) -> str:
    lines: list[str] = []
    c = report["claim"]
    lines.append(
        f"awp-conformance {report['suite']['version']} · {report['specification']} · "
        f"{report['target']}"
    )
    lines.append("")
    shown = {"fail", "warn"} | ({"untested", "pass"} if verbose else {"untested"})
    for r in report["requirements"]:
        if r["outcome"] in shown:
            lines.append(f"  {r['outcome'].upper():9} {r['id']:12} {r['detail'][:160]}")
    lines.append("")
    lines.append("  " + ", ".join(f"{k}: {v}" for k, v in c["counts"].items()))
    if c["manual_evidence_required"]:
        lines.append(f"  manual evidence required: {', '.join(c['manual_evidence_required'])}")
    lines.append("")
    lines.append(f"  {c['wording']}")
    return "\n".join(lines)

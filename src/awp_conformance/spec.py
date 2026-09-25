"""The pinned specification revision, as data: schemas, lifecycle table, requirement matrix.

Everything the suite asserts is read from these bundled files (scripts/sync_spec.py).
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from functools import cache
from importlib.resources import files
from typing import Any

from jsonschema import Draft202012Validator
from referencing import Registry, Resource
from referencing.jsonschema import DRAFT202012

BASE = "https://agentworldprotocol.com/schemas/v0.1/"
VENDOR_FIELD = r"^x-[a-z0-9]+\."
MAX_SAFE_INT = 2**53 - 1

_ROOT = files("awp_conformance") / "_spec"


def _load_schemas() -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}

    def walk(node: Any, prefix: str) -> None:
        for child in node.iterdir():
            if child.is_dir():
                walk(child, f"{prefix}{child.name}/")
            elif child.name.endswith(".schema.json"):
                out[prefix + child.name.removesuffix(".schema.json")] = json.loads(
                    child.read_text()
                )

    walk(_ROOT / "schemas", "")
    return out


SCHEMAS = _load_schemas()

# Method → schema of its params, result, and notification params (the spec's validate.mjs map).
METHODS: dict[str, dict[str, str]] = {
    "initialize": {"params": "agent-manifest", "result": "world-manifest"},
    "world.manifest": {"params": "empty-result", "result": "world-manifest"},
    "ping": {"params": "ping", "result": "ping-result"},
    "session.open": {"params": "session-open", "result": "session-ready"},
    "session.resume": {"params": "session-resume", "result": "session-ready"},
    "session.close": {"params": "empty-result", "result": "empty-result"},
    "session.transfer": {"params": "session-transfer", "result": "session-transfer-result"},
    "session.state": {"notification": "session-state"},
    "session.telemetry": {"notification": "session-telemetry"},
    "task.update": {"params": "task-update", "result": "empty-result"},
    "obs.subscribe": {"params": "subscribe", "result": "subscribe-result"},
    "obs.unsubscribe": {"params": "unsubscribe", "result": "subscribe-result"},
    "obs.frame": {"notification": "frame-inline"},
    "obs.report": {"notification": "obs-report"},
    "cmd.frame": {"notification": "frame-inline"},
    "action.submit": {"params": "action-submit", "result": "action-submit-result"},
    "action.cancel": {"params": "action-ref", "result": "action-cancel-result"},
    "action.status": {
        "params": "action-ref",
        "result": "action-status",
        "notification": "action-status",
    },
    "world.tick": {"params": "tick", "result": "tick-result"},
    "world.snapshot": {"params": "empty-result", "result": "snapshot-result"},
    "world.restore": {"params": "restore", "result": "reset-result"},
    "world.reset": {"params": "reset", "result": "reset-result"},
    "world.event": {"notification": "world-event"},
    "safety.approval_requested": {"notification": "approval-requested"},
    "safety.approval.respond": {"params": "approval-respond", "result": "empty-result"},
}


def lint(node: Any) -> Any:
    """The sender form of a canonical schema: `x-awp-closed` and `x-awp-lint` applied."""
    if isinstance(node, list):
        return [lint(v) for v in node]
    if not isinstance(node, dict):
        return node
    out = {k: lint(v) for k, v in node.items() if k not in ("x-awp-closed", "x-awp-lint")}
    if "x-awp-lint" in node:
        out.update(lint(node["x-awp-lint"]))
    if node.get("x-awp-closed") is True:
        out["additionalProperties"] = False
        out["patternProperties"] = {VENDOR_FIELD: {}, **out.get("patternProperties", {})}
    return out


@cache
def _registry(sender: bool) -> Registry:
    return Registry().with_resources(
        (
            s["$id"],
            Resource.from_contents(lint(s) if sender else s, default_specification=DRAFT202012),
        )
        for s in SCHEMAS.values()
    )


@cache
def validator(name: str, *, sender: bool = False) -> Draft202012Validator:
    file, _, fragment = name.partition("#")
    ref = BASE + file + ".schema.json" + (f"#{fragment}" if fragment else "")
    return Draft202012Validator({"$ref": ref}, registry=_registry(sender))


def schema_errors(name: str, instance: Any, *, sender: bool = False) -> list[str]:
    return [
        f"{'/'.join(map(str, e.absolute_path)) or '<root>'}: {e.message}"
        for e in validator(name, sender=sender).iter_errors(instance)
    ]


def params_validator(manifest: dict[str, Any], index: int) -> Draft202012Validator:
    """Validator for `manifest.action_schemas[index].params_schema`, with local `$defs`."""
    uri = "urn:awp-conformance:manifest"
    doc = {**manifest, "$schema": "https://json-schema.org/draft/2020-12/schema"}
    registry = _registry(False).with_resource(
        uri, Resource.from_contents(doc, default_specification=DRAFT202012)
    )
    return Draft202012Validator(
        {"$ref": f"{uri}#/action_schemas/{index}/params_schema"}, registry=registry
    )


# ---------------------------------------------------------------------------- lifecycle

_TABLE = json.loads((_ROOT / "lifecycle.json").read_text())
STATE_CLASS: dict[str, str] = dict(_TABLE["states"])
TERMINAL = frozenset(s for s, c in STATE_CLASS.items() if c == "terminal")
PRE_EXECUTION = frozenset(s for s, c in STATE_CLASS.items() if c == "pre-execution")


@dataclass(frozen=True, slots=True)
class Edge:
    reasons: frozenset[str] | None
    via_error: bool


EDGES: dict[tuple[str, str], Edge] = {
    (t["from"], t["to"]): Edge(
        frozenset(t["reasons"]) if "reasons" in t else None, t.get("wire") == "error"
    )
    for t in _TABLE["transitions"]
}


def transition_problem(source: str, target: str, reason: str | None) -> str | None:
    """Why a status notification may not move an action from `source` to `target`, or None."""
    if source in TERMINAL:
        return f"{target} after terminal {source}"
    if source == target:
        return None if target in ("executing", "cancelling") else f"repeated {target}"
    edge = EDGES.get((source, target))
    if edge is None:
        return f"{source} → {target} is not in the transition table"
    if edge.via_error:
        return f"{source} → {target} is reported as a JSON-RPC error, not a status"
    if (
        reason
        and edge.reasons is not None
        and not reason.startswith("x-")
        and reason not in edge.reasons
    ):
        return f"reason {reason} is not permitted on {source} → {target}"
    return None


SUBMIT_FIELDS = (
    "type",
    "params",
    "embodiment_id",
    "preempt",
    "deadline_ms",
    "basis_ts_mono_ns",
    "valid_until_ns",
)


# ---------------------------------------------------------------------------- requirements


@dataclass(frozen=True, slots=True)
class Requirement:
    id: str
    side: str  # world | agent | both
    applies: str  # all | lockstep | streaming
    gate: str
    test: str
    level: str  # MUST | SHOULD | MAY
    page: str
    text: str
    agent_test: str = ""  # the agent side's own entry, on a both-sides row tested differently

    def test_for(self, side: str) -> str:
        return self.agent_test if side == "agent" and self.agent_test else self.test

    def kind_for(self, side: str) -> str:
        """assert, warning, fallback (tested where observable, else manual), manual, or
        untestable, from the matrix's entry for `side`."""
        test = self.test_for(side)
        if test.startswith("manual"):
            return "manual"
        if test.startswith("untestable"):
            return "untestable"
        if ", else manual:" in test:
            return "fallback"
        if "(warning)" in test:
            return "warning"
        return "assert"

    @property
    def area(self) -> str:
        return self.id.split("-")[1]


REQUIREMENTS: dict[str, Requirement] = {
    r["id"]: Requirement(**r) for r in json.loads((_ROOT / "requirements.json").read_text())
}

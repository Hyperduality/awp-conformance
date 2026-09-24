"""What the suite needs to know about a world that its manifest cannot say.

A manifest declares action types and their schemas, but not which parameter values make an action
run long enough to interrupt, or how an operator engages an e-stop. The fixture supplies them.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass(frozen=True, slots=True)
class ActionSpec:
    type: str
    params: dict[str, Any]

    @classmethod
    def load(cls, raw: dict[str, Any]) -> ActionSpec:
        return cls(str(raw["type"]), dict(raw.get("params", {})))


@dataclass
class Fixture:
    embodiment: str | None = None
    subscribe: list[str] | None = None
    # Two or more extended actions in one concurrency group. Alternating between them always
    # produces motion, and each must run for at least `extended_min_ms` (streaming) or
    # `extended_min_ticks` (lockstep).
    moves: list[ActionSpec] = field(default_factory=list)
    extended_min_ms: int = 600
    extended_min_ticks: int = 5
    instant: ActionSpec | None = None
    invalid: ActionSpec | None = None
    outside_envelope: ActionSpec | None = None
    initial_state: str | None = None
    # Shell commands run by the suite; `{pid}` and environment variables are expanded by the shell.
    operator: dict[str, str] = field(default_factory=dict)
    audit_dir: str | None = None
    max_wait_s: float = 45.0

    @classmethod
    def load(cls, path: Path | str) -> Fixture:
        raw = json.loads(Path(path).read_text())
        return cls.from_dict(raw)

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> Fixture:
        def spec(key: str) -> ActionSpec | None:
            return ActionSpec.load(raw[key]) if raw.get(key) else None

        return cls(
            embodiment=raw.get("embodiment"),
            subscribe=raw.get("subscribe"),
            moves=[ActionSpec.load(m) for m in raw.get("moves", [])],
            extended_min_ms=int(raw.get("extended_min_ms", 600)),
            extended_min_ticks=int(raw.get("extended_min_ticks", 5)),
            instant=spec("instant"),
            invalid=spec("invalid"),
            outside_envelope=spec("outside_envelope"),
            initial_state=raw.get("initial_state"),
            operator=dict(raw.get("operator", {})),
            audit_dir=raw.get("audit_dir"),
            max_wait_s=float(raw.get("max_wait_s", 45.0)),
        )

    def problems(self, manifest: dict[str, Any]) -> list[str]:
        """What in the fixture contradicts the manifest."""
        out: list[str] = []
        types = {d["type"]: d for d in manifest.get("action_schemas", [])}
        embodiments = {e["id"]: e for e in manifest.get("embodiments", [])}
        if self.embodiment is not None and self.embodiment not in embodiments:
            out.append(f"embodiment {self.embodiment} is not in the manifest")
        for s in (*self.moves, self.instant, self.invalid, self.outside_envelope):
            if s is not None and s.type not in types:
                out.append(f"action type {s.type} is not in the manifest")
        groups = {
            types[m.type].get("concurrency_group", f"_{m.type}")
            for m in self.moves
            if m.type in types
        }
        if len(groups) > 1:
            out.append("fixture moves must share one concurrency group")
        for m in self.moves:
            if m.type in types and types[m.type].get("duration") != "extended":
                out.append(f"move {m.type} is not an extended action type")
        return out

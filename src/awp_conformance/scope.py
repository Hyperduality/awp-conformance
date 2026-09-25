"""Which requirements a run is accountable for: side, time model, gate, and claimed profiles."""

from __future__ import annotations

import ipaddress
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlsplit

from .spec import Requirement


def is_loopback(url: str) -> bool:
    host = urlsplit(url).hostname or ""
    if host in ("localhost", ""):
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


@dataclass
class Scope:
    side: str  # world | agent
    modes: set[str]
    manifest: dict[str, Any]
    profiles: set[str] = field(default_factory=set)
    loopback: bool = True
    facts: set[str] = field(default_factory=set)  # features discovered while testing

    def feature(self, condition: str) -> bool | None:
        m = self.manifest
        decls = m.get("action_schemas", [])
        safety = m.get("safety_policy") or {}
        channels = m.get("observation_channels", [])
        known: dict[str, bool] = {
            "max_duration_ms declared": any("max_duration_ms" in d for d in decls),
            "requires_approval": any(d.get("requires_approval") for d in decls),
            "envelopes declared": bool(safety.get("envelopes")),
            "reliable channels declared": any(c.get("loss_class") == "reliable" for c in channels),
            "max_basis_age_ms declared": "max_basis_age_ms" in safety,
            "scene channel offered": any(c.get("id") == "scene" for c in channels),
            "multiple sessions": "multiple sessions" in self.facts,
            "stream endpoints offered": "stream endpoints offered" in self.facts,
        }
        return known.get(condition)

    def applicable(self, req: Requirement) -> tuple[bool, str]:
        if req.side not in (self.side, "both"):
            return False, f"{req.side}-side requirement"
        if req.applies != "all" and req.applies not in self.modes:
            return False, f"applies to {req.applies} only; tested {', '.join(sorted(self.modes))}"
        gate = req.gate
        if gate == "core":
            return True, ""
        if gate == "core (streaming)":
            return ("streaming" in self.modes), "streaming not offered"
        if gate == "core (non-loopback)":
            return (not self.loopback), "loopback endpoint"
        if gate.startswith("profile:"):
            name = gate.split(":", 1)[1]
            return (name in self.profiles), f"profile {name} not claimed"
        if gate.startswith("capability:"):
            key = gate.split(":", 1)[1]
            on = bool((self.manifest.get("capabilities") or {}).get(key))
            return on, f"capability {key} not advertised"
        if gate.startswith("feature:"):
            cond = gate.split(":", 1)[1]
            value = self.feature(cond)
            if value is None:
                return True, ""
            return value, f"{cond}: no"
        return True, ""

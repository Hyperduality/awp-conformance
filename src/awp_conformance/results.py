"""Findings against requirement IDs, and the verdict per requirement."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum

from .spec import REQUIREMENTS


class Outcome(StrEnum):
    PASS = "pass"
    FAIL = "fail"
    WARN = "warn"  # a SHOULD not met
    NOT_APPLICABLE = "n/a"  # outside the claimed classes, mode, or declared features
    UNTESTED = "untested"  # in scope, but nothing exercised it
    MANUAL = "manual"
    UNTESTABLE = "untestable"


@dataclass(frozen=True, slots=True)
class Finding:
    requirement: str
    ok: bool
    detail: str
    test: str
    should: bool = False  # the check covers a SHOULD inside a requirement that also has MUSTs


@dataclass
class Results:
    side: str = "world"
    findings: list[Finding] = field(default_factory=list)
    not_applicable: dict[str, str] = field(default_factory=dict)
    untested_reason: dict[str, str] = field(default_factory=dict)

    def check(
        self, requirement: str, ok: bool, detail: str, test: str, *, should: bool = False
    ) -> bool:
        if requirement not in REQUIREMENTS:
            raise KeyError(f"{requirement} is not in the requirement matrix")
        self.findings.append(Finding(requirement, bool(ok), detail, test, should))
        return bool(ok)

    def mark_not_applicable(self, requirement: str, reason: str) -> None:
        self.not_applicable.setdefault(requirement, reason)

    def mark_untested(self, requirement: str, reason: str) -> None:
        self.untested_reason.setdefault(requirement, reason)

    def of(self, requirement: str) -> list[Finding]:
        return [f for f in self.findings if f.requirement == requirement]

    def verdict(self, requirement: str) -> Outcome:
        req = REQUIREMENTS[requirement]
        kind = req.kind_for(self.side)
        if kind == "manual":
            return Outcome.NOT_APPLICABLE if requirement in self.not_applicable else Outcome.MANUAL
        if kind == "untestable":
            return Outcome.UNTESTABLE
        found = self.of(requirement)
        failed = [f for f in found if not f.ok]
        if failed:
            # A MAY feature, once offered, binds like a MUST; only SHOULDs degrade to warnings.
            soft = kind == "warning" or req.level == "SHOULD" or all(f.should for f in failed)
            return Outcome.WARN if soft else Outcome.FAIL
        if found:
            return Outcome.PASS
        if requirement in self.not_applicable:
            return Outcome.NOT_APPLICABLE
        return Outcome.MANUAL if kind == "fallback" else Outcome.UNTESTED

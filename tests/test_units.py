from __future__ import annotations

from awp_conformance import spec
from awp_conformance.report import build
from awp_conformance.results import Outcome, Results
from awp_conformance.scope import Scope


def test_matrix_is_bundled():
    assert len(spec.REQUIREMENTS) > 200
    assert spec.REQUIREMENTS["AWP-ACT-001"].level == "MUST"
    assert spec.REQUIREMENTS["AWP-LIF-003"].kind == "warning"


def test_transition_table():
    assert spec.transition_problem("accepted", "executing", None) is None
    assert spec.transition_problem("executing", "cancelled", None) is not None  # through cancelling
    assert spec.transition_problem("completed", "failed", None) is not None
    assert spec.transition_problem("executing", "failed", "x-acme.jam") is None


def test_should_findings_are_warnings():
    r = Results()
    r.check("AWP-SES-012", False, "closed early", "t", should=True)
    assert r.verdict("AWP-SES-012") is Outcome.WARN
    r.check("AWP-SES-012", False, "broken", "t")
    assert r.verdict("AWP-SES-012") is Outcome.FAIL


def test_claim_wording():
    r = Results()
    manifest = {"time_models": ["streaming"], "capabilities": {}}
    rep = build(target="ws://x", scope=Scope("world", {"streaming"}, manifest), results=r)
    assert rep["claim"]["status"] == "self-assessed"
    assert "self-assessed against" in rep["claim"]["wording"]
    r.check("AWP-ACT-001", False, "x", "t")
    rep = build(target="ws://x", scope=Scope("world", {"streaming"}, manifest), results=r)
    assert rep["claim"]["status"] == "fail"

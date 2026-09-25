from __future__ import annotations

import pytest

from awp_conformance import spec
from awp_conformance.report import build
from awp_conformance.results import Outcome, Results
from awp_conformance.scope import Scope


def test_matrix_is_bundled():
    assert len(spec.REQUIREMENTS) > 200
    assert spec.REQUIREMENTS["AWP-ACT-001"].level == "MUST"
    assert spec.REQUIREMENTS["AWP-LIF-003"].kind_for("world") == "warning"
    dat = spec.REQUIREMENTS["AWP-DAT-001"]
    assert (dat.kind_for("world"), dat.kind_for("agent")) == ("assert", "fallback")


def test_a_fallback_row_is_manual_only_when_nothing_observed_it():
    from awp_conformance.results import Outcome, Results

    unobserved = Results(side="agent")
    assert unobserved.verdict("AWP-DAT-001") is Outcome.MANUAL
    observed = Results(side="agent")
    observed.check("AWP-DAT-001", True, "counted", "t")
    assert observed.verdict("AWP-DAT-001") is Outcome.PASS
    failed = Results(side="agent")
    failed.check("AWP-DAT-001", False, "not counted", "t")
    assert failed.verdict("AWP-DAT-001") is Outcome.FAIL
    assert Results(side="world").verdict("AWP-DAT-001") is Outcome.UNTESTED


def test_frame_decoder_matches_every_vector():
    import json
    from importlib.resources import files

    from awp_conformance.frames import FrameError, decode

    vectors = json.loads((files("awp_conformance") / "_spec" / "frames.json").read_text())[
        "vectors"
    ]
    for v in vectors:
        data = bytes.fromhex(v["hex"])
        if v.get("expect_error"):
            with pytest.raises(FrameError) as err:
                decode(data)
            assert err.value.code == v["expect_error"], v["name"]
            continue
        f = decode(data)
        got = {
            "channel_id": f.channel_id,
            "seq": f.seq,
            "ts_mono_ns": f.ts_mono_ns,
            "flags": f.flags,
            "keyframe": f.keyframe,
            "end_of_burst": bool(f.flags & 0x02),
            "resync": f.resync,
            "payload_len": len(f.payload),
            "payload_hex": f.payload.hex(),
            "vendor": [{"type": k, "value": list(b)} for k, b in f.vendor],
            **f.ext,
        }
        assert {k: got[k] for k in v["expect"]} == v["expect"], v["name"]


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

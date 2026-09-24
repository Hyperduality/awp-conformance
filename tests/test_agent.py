"""The agent suite against the reference client's demo agent."""

from __future__ import annotations

import json
import sys
from pathlib import Path

from awp_sim.config import WorldConfig

from awp_conformance.agent.runner import run_agent

DEMO = [sys.executable, "-m", "awp_sim", "demo", "--url", "{url}", "--token", "{token}"]
SAMPLES = {
    "proprio": {"p_m": [0, 0, 0.4], "v_mps": [0, 0, 0]},
    "arm_state": {"phase": "idle", "target_m": None, "action_id": None},
}


async def test_demo_agent_passes_in_streaming():
    manifest = WorldConfig(mode="streaming").manifest()
    run = await run_agent(DEMO, manifest, modes=["streaming"], samples=SAMPLES, timeout=15)
    bad = [f for f in run.results.findings if not f.ok and not f.should]
    assert bad == []
    assert {"AWP-AGT-002", "AWP-AGT-003", "AWP-SAF-002", "AWP-CLK-009"} <= {
        f.requirement for f in run.results.findings if f.ok
    }


async def test_demo_agent_passes_in_lockstep():
    manifest = WorldConfig(mode="lockstep").manifest()
    run = await run_agent(DEMO, manifest, modes=["lockstep"], samples=SAMPLES, timeout=15)
    bad = [f for f in run.results.findings if not f.ok and not f.should]
    assert bad == []
    assert "AWP-AGT-009" in {f.requirement for f in run.results.findings if f.ok}


def test_fixture_matches_the_reference_manifest():
    from awp_conformance.fixture import Fixture

    root = Path(__file__).resolve().parent.parent
    fixture = Fixture.from_dict(json.loads((root / "fixtures" / "awp-sim.json").read_text()))
    assert fixture.problems(WorldConfig().manifest()) == []
    assert len(fixture.moves) >= 3

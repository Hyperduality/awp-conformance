"""The agent suite against awp-python's demo agent."""

from __future__ import annotations

import json
import sys
from pathlib import Path

from awp_sim.config import WorldConfig

from awp_conformance.agent.runner import run_agent

DEMO = [sys.executable, "-m", "awp.demo", "--url", "{url}", "--token", "{token}"]
SAMPLES = {
    "proprio": {"p_m": [0, 0, 0.4], "v_mps": [0, 0, 0]},
    "arm_state": {"phase": "idle", "target_m": None, "action_id": None},
}


async def test_demo_agent_passes_in_streaming():
    manifest = WorldConfig(mode="streaming").manifest()
    run = await run_agent(DEMO, manifest, modes=["streaming"], samples=SAMPLES, timeout=15)
    bad = [f for f in run.results.findings if not f.ok and not f.should]
    assert bad == []
    assert {
        "AWP-AGT-002",
        "AWP-AGT-003",
        "AWP-AGT-007",
        "AWP-CTL-003",
        "AWP-CTL-005",
        "AWP-DAT-001",
        "AWP-DAT-009",
        "AWP-OBS-003",
        "AWP-SAF-002",
        "AWP-SES-008",
        "AWP-SES-012",
        "AWP-CLK-009",
    } <= {f.requirement for f in run.results.findings if f.ok}
    assert run.results.untested_reason == {}


async def test_demo_agent_passes_in_lockstep():
    manifest = WorldConfig(mode="lockstep").manifest()
    run = await run_agent(DEMO, manifest, modes=["lockstep"], samples=SAMPLES, timeout=15)
    bad = [f for f in run.results.findings if not f.ok and not f.should]
    assert bad == []
    assert {"AWP-AGT-009", "AWP-TIM-003", "AWP-SES-008"} <= {
        f.requirement for f in run.results.findings if f.ok
    }


def patched(code: str) -> list[str]:
    """The demo agent with part of the client replaced, to check that the suite notices."""
    run = "import sys; from awp.demo import main; sys.exit(main(sys.argv[1:]))"
    return [sys.executable, "-c", f"{code}\n{run}", "--url", "{url}", "--token", "{token}"]


async def test_catches_an_agent_that_counts_the_gap_before_a_resync():
    code = """
import dataclasses
from awp.client import ClientConnection
frame = ClientConnection._frame
ClientConnection._frame = lambda self, f, *a: frame(self, dataclasses.replace(f, resync=False), *a)
"""
    manifest = WorldConfig(mode="streaming").manifest()
    run = await run_agent(patched(code), manifest, modes=["streaming"], samples=SAMPLES, timeout=15)
    assert "AWP-DAT-009" in {f.requirement for f in run.results.findings if not f.ok}


async def test_catches_an_agent_that_advances_before_the_frames_arrive():
    code = """
from awp.aio import AsyncClient
async def advance(self, count=None, timeout=10.0):
    return int((await self.call(self.conn.advance(count), timeout))["tick"])
AsyncClient.advance = advance
"""
    manifest = WorldConfig(mode="lockstep").manifest()
    run = await run_agent(patched(code), manifest, modes=["lockstep"], samples=SAMPLES, timeout=15)
    assert "AWP-TIM-003" in {f.requirement for f in run.results.findings if not f.ok}


async def test_catches_an_agent_that_keeps_a_lost_session():
    code = """
from awp.aio import AsyncClient
from awp.client import ClientConnection
from awp.errors import AwpError
ClientConnection._session_lost = lambda self, *args: None
reconnect = AsyncClient.reconnect
async def resumed_anyway(self):
    try:
        return await reconnect(self)
    except AwpError:
        return {}
AsyncClient.reconnect = resumed_anyway
"""
    manifest = WorldConfig(mode="lockstep").manifest()
    run = await run_agent(patched(code), manifest, modes=["lockstep"], samples=SAMPLES, timeout=15)
    assert "AWP-SES-008" in {f.requirement for f in run.results.findings if not f.ok}


async def test_catches_an_agent_that_drops_the_connection_on_an_out_of_range_integer():
    code = """
from awp.aio import AsyncClient
async def just_close(self):
    await self._ws.close()
AsyncClient._integer_range = just_close
"""
    manifest = WorldConfig(mode="streaming").manifest()
    run = await run_agent(patched(code), manifest, modes=["streaming"], samples=SAMPLES, timeout=15)
    assert "AWP-CTL-009" in {f.requirement for f in run.results.findings if not f.ok}


async def test_catches_an_agent_that_keeps_a_stream_after_a_malformed_frame():
    code = """
from awp.client import ClientConnection
receive = ClientConnection.receive_frame
def tolerant(self, data):
    try:
        return receive(self, data)
    except Exception:
        return []
ClientConnection.receive_frame = tolerant
"""
    manifest = WorldConfig(mode="streaming").manifest()
    run = await run_agent(patched(code), manifest, modes=["streaming"], samples=SAMPLES, timeout=15)
    assert "AWP-DAT-010" in {f.requirement for f in run.results.findings if not f.ok}


def test_fixture_matches_the_reference_manifest():
    from awp_conformance.fixture import Fixture

    root = Path(__file__).resolve().parent.parent
    fixture = Fixture.from_dict(json.loads((root / "fixtures" / "awp-sim.json").read_text()))
    assert fixture.problems(WorldConfig().manifest()) == []
    assert len(fixture.moves) >= 3


async def test_the_injected_loss_gap_never_follows_into_a_resync_frame():
    """The first frame on a stream connection is a resync, which makes the gap before it not loss
    (AWP-DAT-009); the frame-gaps stimulus skips its seqs on the next frame instead."""
    from awp_conformance import frames
    from awp_conformance.agent.harness import Episode, Harness, _Session
    from awp_conformance.results import Results

    manifest = WorldConfig(mode="streaming").manifest()
    episode = Episode("frame-gaps", "streaming", frame_gaps=True)
    harness = Harness(manifest, episode, Results(side="agent"))
    sent: list[frames.Decoded] = []

    class Stream:
        async def send(self, data: bytes) -> None:
            sent.append(frames.decode(data))

    s = _Session("s", "t", "streaming", "arm_01", 0, {1: {"channel": "proprio"}}, [])
    s.stream_ws = Stream()  # type: ignore[assignment]
    s.stream_fresh = {1}
    s.frame_seq = {1: 3}
    s.gaps = [(1, 3, False)]
    await harness._frame(s, 1)
    await harness._frame(s, 1)
    assert [(f.seq, f.resync) for f in sent] == [(4, True), (8, False)]
    assert harness.gaps_done_ns is not None

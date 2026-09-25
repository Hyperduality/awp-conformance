from __future__ import annotations

import json
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import pytest
from awp_sim.audit import AuditLog
from awp_sim.config import WorldConfig
from awp_sim.server import Server
from awp_sim.world import World

from awp_conformance.fixture import Fixture

ROOT = Path(__file__).resolve().parent.parent
FAST: dict[str, Any] = {
    "watchdog_ms": 800,
    "heartbeat_interval_ms": 500,
    "reconnect_window_ms": 4000,
}


def sim_fixture(audit_dir: Path | None = None) -> Fixture:
    raw = json.loads((ROOT / "fixtures" / "awp-sim.json").read_text())
    raw["audit_dir"] = str(audit_dir) if audit_dir else None
    raw["operator"] = {}
    return Fixture.from_dict(raw)


async def serve(world: World) -> Server:
    server = Server(world, port=0)
    await server.start()
    return server


@pytest.fixture
async def streaming_sim(tmp_path: Path) -> AsyncIterator[tuple[Server, Path]]:
    audit = tmp_path / "audit"
    world = World(WorldConfig(mode="streaming", **FAST), audit=AuditLog(audit))
    server = await serve(world)
    yield server, audit
    await server.stop()


@pytest.fixture
async def lockstep_sim(tmp_path: Path) -> AsyncIterator[tuple[Server, Path]]:
    audit = tmp_path / "audit"
    world = World(WorldConfig(mode="lockstep"), audit=AuditLog(audit))
    server = await serve(world)
    yield server, audit
    await server.stop()


@pytest.fixture
async def stream_sim(tmp_path: Path) -> AsyncIterator[tuple[Server, Path]]:
    audit = tmp_path / "audit"
    world = World(WorldConfig(mode="streaming", **FAST), audit=AuditLog(audit))
    server = Server(world, port=0, stream_binding=True)
    await server.start()
    yield server, audit
    await server.stop()

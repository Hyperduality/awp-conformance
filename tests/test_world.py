"""The world suite against the reference world: clean when it conforms, caught when not."""

from __future__ import annotations

from typing import Any

from awp.errors import AwpError, ErrorCode
from awp.lifecycle import ActionState

from awp_conformance.world.runner import run_world

from .conftest import sim_fixture


def failures(run: Any) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    for f in run.results.findings:
        if not f.ok and not f.should:
            out.setdefault(f.requirement, []).append(f"[{f.test}] {f.detail}")
    return out


async def test_streaming_reference_world_passes(streaming_sim):
    server, audit = streaming_sim
    hooks = {"estop_engage": server.engage_estop, "estop_release": server.release_estop}
    run = await run_world(server.url, sim_fixture(audit), hooks=hooks)
    assert failures(run) == {}
    assert len({f.requirement for f in run.results.findings if f.ok}) > 110


async def test_lockstep_reference_world_passes(lockstep_sim):
    server, audit = lockstep_sim
    run = await run_world(server.url, sim_fixture(audit))
    assert failures(run) == {}
    assert run.modes == ["lockstep"]


async def test_catches_a_world_that_ignores_consumes_modalities(streaming_sim):
    server, _ = streaming_sim
    server.world._readable = lambda embodiment, consumes: {"proprio", "arm_state"}
    run = await run_world(server.url, sim_fixture(), only=["modalities"])
    assert "AWP-AGM-001" in failures(run)


async def test_catches_a_status_seq_gap(streaming_sim):
    server, _ = streaming_sim
    world = server.world
    original = world._sequenced

    def skipping(s, method, params):
        if method == "action.status" and params.get("state") == "executing":
            s.next_seq()  # a status_seq the agent never receives
        return original(s, method, params)

    world._sequenced = skipping
    run = await run_world(server.url, sim_fixture(), only=["lifecycle-complete"])
    assert "AWP-CTL-008" in failures(run)


async def test_catches_a_wrong_error_code(streaming_sim):
    server, _ = streaming_sim

    def refuse(c, rid, p, now):
        raise AwpError(ErrorCode.PARAMS_INVALID, "wrong code on purpose")

    server.world._rpc_session_resume = refuse
    run = await run_world(server.url, sim_fixture(), only=["resume-unknown"])
    assert "AWP-SES-008" in failures(run)


async def test_catches_a_missing_watchdog(streaming_sim):
    server, _ = streaming_sim
    server.world._check_watchdog = lambda s, now: None
    run = await run_world(server.url, sim_fixture(), only=["watchdog"])
    assert "AWP-SAF-003" in failures(run)


async def test_catches_a_cancel_without_safe_abort(streaming_sim):
    server, _ = streaming_sim
    world = server.world

    def instant_cancel(c, rid, p, now):
        s = c.session
        a = s.actions[p["action_id"]]
        world._finish(s, a, ActionState.CANCELLED, now, reason="cancelled_by_agent")
        world._reply(
            c, rid, {k: a.status[k] for k in ("action_id", "state", "status_seq", "reason")}
        )

    world._rpc_action_cancel = instant_cancel
    run = await run_world(server.url, sim_fixture(), only=["cancel-executing"])
    assert "AWP-LIF-005" in failures(run)


STREAM_TESTS = ["session-open", "ws-stream", "lifecycle-complete", "resume-replay", "watchdog"]


async def test_ws_stream_binding_passes(stream_sim):
    server, audit = stream_sim
    run = await run_world(server.url, sim_fixture(audit), only=STREAM_TESTS)
    assert failures(run) == {}
    passed = {f.requirement for f in run.results.findings if f.ok}
    assert {"AWP-TRN-010", "AWP-TRN-012", "AWP-TRN-013", "AWP-DAT-006"} <= passed


async def test_catches_a_channel_that_stays_inline_after_moving(stream_sim):
    server, _ = stream_sim
    world = server.world

    def emit(s, g, now):  # every frame inline as well as on the stream connection
        stream = s.stream_conn
        s.stream_conn = None
        try:
            type(world)._emit_frame(world, s, g, now)
        finally:
            s.stream_conn = stream
        if stream is not None:
            g.seq -= 1
            type(world)._emit_frame(world, s, g, now)

    world._emit_frame = emit
    run = await run_world(server.url, sim_fixture(), only=["ws-stream"])
    assert "AWP-TRN-012" in failures(run)

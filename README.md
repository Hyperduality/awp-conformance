# awp-conformance

The conformance suite for the [Agent World Protocol](https://www.agentworldprotocol.com). It tests a world at a WebSocket URL, or an agent launched against the suite's own harness world, against the [requirement matrix](https://www.agentworldprotocol.com/spec/requirements) of one specification revision, and reports a verdict per requirement ID.

It targets specification revision **`0.1-draft.7`**, pinned as the `spec/` submodule. The suite shares no code with any implementation: it speaks the protocol from the bundled canonical schemas, lifecycle table, and matrix.

```bash
pip install awp-conformance
awp-conformance world ws://127.0.0.1:8710 --fixture my-world.json --out report/
awp-conformance agent --manifest manifest.json --frames frames.json -- python my_agent.py --url {url}
```

## Testing a world

The suite connects as one or more agents and checks everything the world sends: schemas in receiver and sender form, `status_seq` sequencing and replay, every action transition against the lifecycle table, frame sequencing per channel, closing, and integer bounds. Then it runs active tests named after the requirements they cover — idempotency, preemption, deadlines, the watchdog, resumption from a partial acknowledgement, half-open connections, resets, isolation between sessions, and more. `awp-conformance tests` lists them.

A manifest declares action types but not which parameter values make an action run long enough to interrupt, so a world supplies a **fixture**:

```json
{
  "embodiment": "arm_01",
  "subscribe": ["proprio", "arm_state"],
  "moves": [
    { "type": "move_to_pose", "params": { "pose": { "frame": "base", "p_m": [0.4, 0.4, 0.6], "q": [0, 0, 0, 1] } } },
    { "type": "move_to_pose", "params": { "pose": { "frame": "base", "p_m": [-0.4, 0.3, 0.2], "q": [0, 0, 0, 1] } } },
    { "type": "move_to_pose", "params": { "pose": { "frame": "base", "p_m": [0.0, -0.45, 0.5], "q": [0, 0, 0, 1] } } }
  ],
  "extended_min_ms": 1500,
  "invalid": { "type": "move_to_pose", "params": { "pose": { "frame": "base" } } },
  "outside_envelope": { "type": "move_to_pose", "params": { "pose": { "frame": "base", "p_m": [0.9, 0, 0.4], "q": [0, 0, 0, 1] } } },
  "operator": { "estop_engage": "kill -USR1 $WORLD_PID", "estop_release": "kill -USR2 $WORLD_PID" },
  "audit_dir": "./awp-audit"
}
```

| Key | Meaning |
|---|---|
| `moves` | Three or more extended actions in one concurrency group, at targets not on one line, each lasting at least `extended_min_ms` (streaming) or `extended_min_ticks` advances (lockstep). The suite always picks a target the embodiment is not near |
| `invalid` | Params that fail the type's schema (generated from the schema when absent) |
| `outside_envelope` | Params outside a declared envelope |
| `operator` | Shell commands that engage and release the e-stop |
| `audit_dir` | Where the world writes its audit log, if the suite can read it |
| `max_wait_s` | The longest single wait the suite does (default 45); `--slow` lifts it |
| `approver_token` | Bearer token of an approver connection, for worlds with `requires_approval` types |
| `approval_action` | The action to submit for approval (default: the first such type, with empty params) |
| `servo` | For command channels: `{ "action": {type, params}, "setpoint": {...}, "violation": {...} }` |

Beyond Core, the suite tests what the world offers: the `ws` stream binding, task, approval, blend, transfer, seeding, snapshots and replay, and command channels. `--profile sim` adds the sim profile to the claim.

`fixtures/awp-sim.json` is the fixture for the reference world.

## Testing an agent

`awp-conformance agent` serves the manifest you give it from a scripted harness world, launches the agent once per episode (`{url}` and `{token}` in the command, or `$AWP_URL` and `$AWP_TOKEN`), and checks what the agent sends. The episodes add the stimuli that make agent requirements observable: unknown fields on every message, reserved frame flag bits, a redelivered terminal status, an unknown world request, a dropped connection, a world that falls silent, an invalid manifest, a session with no action grants, and a refused submission.

`--frames` gives a sample payload per channel, so the agent sees observations it can parse.

## The report

Every requirement gets one outcome: `pass`, `fail`, `warn` (a SHOULD not met), `untested` (in scope but not exercised), `n/a` (outside the tested time models, gates, or declared features), `manual`, or `untestable`. The report states the claim the outcomes support (AWP-CNF-005):

- **AWP-conformant** — no failed and no untested requirement in scope, for the claimed classes and the named draft revision; manual evidence is attached separately.
- **self-assessed against 0.1-draft.N** — nothing failed, but some requirement in scope was not exercised.
- **not conformant** — something failed.

`--out DIR` writes `report.json` and a wire trace per connection in the specification's trace format.

## Development

```bash
git clone --recurse-submodules https://github.com/Hyperduality/awp-conformance
uv sync
uv run pytest                       # runs the suite against awp-sim, including deliberately broken worlds
uv run python scripts/sync_spec.py --check
```

To move to a new draft revision: check out its tag in `spec/`, run `scripts/sync_spec.py`, update `SPEC_REVISION` in `src/awp_conformance/__init__.py`, and fix what the tests report.

## License

Apache-2.0. See [LICENSE](LICENSE).

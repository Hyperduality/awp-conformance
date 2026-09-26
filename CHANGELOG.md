# Changelog

## 0.1.0a7

Targets specification revision `0.1-draft.10`.

- AWP-TIM-014: the new `barrier-calls` test runs on barrier worlds. A second `world.tick` while one is pending must fail with `AWP_BUSY`. A call with `count` 2 stays pending while another session's call with `count` 1 is answered, and both are answered together at the second advance. Closing a session releases a call it was holding back.
- AWP-MA-005 allows at most one `tick` holder under `any_session`, as draft 10 states. With no holder, each session's `world.tick` must still be refused with `AWP_TICK_NOT_AUTHORIZED`.
- AWP-EVT-004, checked on every session: `e_stop_engaged`, `e_stop_released`, `envelope_violation`, `safe_state_entered`, `safe_state_exited`, and `embodiment_transferred` must name a declared embodiment in `detail.embodiment`. In streaming, `multi-bind` lets the watchdog fire on a session bound to the group and expects one `safe_state_entered` per embodiment.
- AWP-EMB-005: in a multi-bind session, a submission without `embodiment_id` must fail with `-32602`, and one naming a member that does not offer the type with `AWP_FORBIDDEN`.
- AWP-EMB-003: a takeover naming `embodiments` must fail with `AWP_EMBODIMENT_UNAVAILABLE`, and `embodiment_transferred` must name the transferred embodiment.
- AWP-CMD-009 (warning): a live stream's `action.status` carries `stream` at least once per second.
- AWP-ROB-008 (warning): a world claiming the robotics profile advertises `capabilities.command_channels`.
- AWP-DAT-006 fails on a stream frame carrying a reserved extension type (`0x00`, `0x04`–`0x7F`).

## 0.1.0a6

Targets specification revision `0.1-draft.9`.

- A per-tick channel is one granted with `rate_hz: null`, as draft 9 defines it. A lockstep channel granted at a rate is no longer expected to deliver a frame per advance, after `session.ready`, or after a reset (AWP-TIM-003, AWP-TIM-009, AWP-DAT-003, AWP-PRM-006).
- `tick-authority`:
  - Under `any_session`, exactly one of two bound lockstep sessions must hold `tick`. The session without it must be refused with `AWP_TICK_NOT_AUTHORIZED`, and the holder's advance must reach both sessions' per-tick channels (AWP-MA-005, AWP-TIM-012, AWP-TIM-003).
  - Under `barrier`, two sessions that open at different ticks fail AWP-TIM-012 instead of skipping the test. The advance's frames can arrive after the `world.tick` result.
- `embodiment-binding` binds every embodiment twice, the fixture's included, and AWP-EMB-001 is `n/a` only when every embodiment declares `shared_control`. `open-refusals` no longer covers AWP-EMB-001 or AWP-MA-003.
- `multi-bind` tests binding outside the group and naming an unbound embodiment even when the group itself cannot be bound. It no longer checks the granted action types under AWP-EMB-005.
- `isolation` expects frames only on channels with a rate (streaming) or on per-tick channels (lockstep), and waits long enough for the slowest one. A world whose channels send only on change no longer fails AWP-MA-004.
- `reset`:
  - A manifest with no `initial_states` fails AWP-SIM-001 even when reset is not granted. AWP-SIM-001 is `untested` when the world does not grant reset.
  - Fresh frames after the reset are checked on every session, not only the initiator's.
- Without `subscribe` in the fixture, a session subscribes to the embodiment's observation channels only.
- `fixtures/awp-sim.json` no longer carries `approver_token` or `servo`. They are in `fixtures/awp-sim-features.json`.

## 0.1.0a5

Targets specification revision `0.1-draft.9`.

- Worlds with several embodiments are tested:
  - `multi-bind` binds a `multi_bind_group` in one session and submits with `embodiment_id`. Binding outside the group, or naming an unbound embodiment, must be refused (AWP-EMB-005, AWP-MAN-007, AWP-ACT-003).
  - `embodiment-binding` binds each embodiment twice. A shared one must allow it and name its arbitration; an exclusive one must refuse (AWP-MA-003, AWP-EMB-001).
  - `tick-authority` holds two lockstep sessions. Under `barrier`, one call must not advance the world, and both must be answered with the same tick. Under `any_session`, at most one of them gets `tick` (AWP-TIM-012, AWP-MA-005).
- A world that declared a `multi_bind_group` was reported untested on AWP-EMB-005.

## 0.1.0a4

Targets specification revision `0.1-draft.9`.

- The `frame-gaps` episode no longer puts its seq gap just before a resync frame. When a channel's first frame on a new stream connection followed the gap, a conformant agent was reported failing AWP-DAT-001.

## 0.1.0a3

Targets specification revision `0.1-draft.9`.

- AWP-APR-004: the `standing-approval` test grants standing approvals and checks their scope, predicate, expiry, refusals, the `approval_id` on admissions under them, and the audit log; a world that does not declare `standing_approvals` must refuse one.
- The agent suite covers every Core agent-side requirement: world traffic between a request and its response (AWP-CTL-003), frames interleaved across channels (AWP-OBS-003), a pre-session pong on another clock (AWP-SES-012), seq gaps and a resync checked against `obs.report` (AWP-DAT-001, AWP-DAT-009), stream frames after the `world.tick` result (AWP-TIM-003), and a world that has forgotten the session (AWP-SES-008). Rows the matrix gives as `<id>, else manual:` on the agent side are `manual` when the agent's messages do not show them.
- AWP-ACT-007 and AWP-AGT-008 are `n/a` for an agent that sends no basis or session-clock value; AWP-OBS-007 passes on the reports an agent sends.
- Draft 9's rules are tested on both sides: malformed frames close a stream connection with 1002 `AWP_MALFORMED` (AWP-DAT-010); an out-of-range integer ends the session with reason `protocol_error` and close code 1002 (AWP-CTL-009, with an agent episode); a lockstep resumption resyncs every per-tick channel (AWP-TIM-009); reset frames precede the result (AWP-PRM-006); an omitted `preempt` gets the first declared policy (AWP-PRE-001); `reconnect_window_ms` is at least `watchdog_ms` (AWP-SAF-003); a transition reported twice fails AWP-LIF-001; and a retry is recognised whatever its `action_id` (AWP-ERR-001).

## 0.1.0a2

Targets specification revision `0.1-draft.8`.

- AWP-DAT-002 applies to streaming only, as draft 8 states.
- Fixture key `long_params`: params that make a move outlast its type's `max_duration_ms`, so AWP-ACT-008 is tested against a world whose moves end sooner. The awp-sim fixture slows its moves to 0.01 m/s for it.
- A run no longer hangs on Python 3.11 when a heartbeat's cancellation races its reply.

## 0.1.0a1

First release, targeting specification revision `0.1-draft.7`.

- `awp-conformance world`: passive checks on every message a world sends, and active tests for discovery, versioning, transport and security, session establishment, grants and subscriptions, the action lifecycle, idempotency, preemption, deadlines, closing, lockstep advancement and its clock, streaming delivery and telemetry, the watchdog, stale intents, resumption and replay, half-open connections, reconnect-window expiry, heartbeat loss, resets, session isolation, the e-stop, and the audit log.
- Beyond Core, when offered: the `ws` stream binding (with an independent binary frame decoder checked against every spec vector), task, approval, blend, transfer, seeding, snapshots and replay, and command channels.
- `awp-conformance agent`: a harness world and seven episodes of stimuli for agent requirements.
- Reports with a verdict per requirement and the claim wording it supports; wire traces per connection.

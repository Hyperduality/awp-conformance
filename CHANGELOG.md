# Changelog

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

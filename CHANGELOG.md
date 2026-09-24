# Changelog

## 0.1.0a1

First release, targeting specification revision `0.1-draft.6`.

- `awp-conformance world`: passive checks on every message a world sends, and active tests for discovery, versioning, transport and security, session establishment, grants and subscriptions, the action lifecycle, idempotency, preemption, deadlines, closing, lockstep advancement and its clock, streaming delivery and telemetry, the watchdog, stale intents, resumption and replay, half-open connections, reconnect-window expiry, heartbeat loss, resets, session isolation, the e-stop, and the audit log.
- `awp-conformance agent`: a harness world and seven episodes of stimuli for agent requirements.
- Reports with a verdict per requirement and the claim wording it supports; wire traces per connection.

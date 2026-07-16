# Contributing

Read `AGENTS.md` and the complete relevant sections of
`docs/SPECIFICATION.md` before changing behavior. The specification is the
normative contract; code, tests, documentation, and operational procedures must
change together when that contract changes.

Changes must prioritize data safety, CSI correctness, explicit failure handling,
and maintainability. Add focused tests for success, retry, cancellation, crash,
and idempotency behavior. Do not use real Scaleway resources without explicit
approval under the repository's cost and cleanup rules.

By participating, contributors agree to follow `CODE_OF_CONDUCT.md`.
